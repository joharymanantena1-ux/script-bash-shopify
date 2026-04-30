"""
build_product_mapping.py — Construit output/product_mapping.csv

Mappe wee_product_id → shopify_product_gid pour que la Phase 2 de
import_designers_to_shopify.py puisse lier tous les produits.

Deux stratégies (choisir selon votre situation) :

  --from-shopify
      Interroge Shopify pour tous les produits qui ont le metafield
      custom.wee_product_id déjà renseigné.

  --from-db-handle
      Lit les slugs des produits dans la base Wee (filtrés par la locale
      par défaut), normalise les accents, puis croise avec les handles
      Shopify pour construire la correspondance.

Diagnostic :

  --diagnose
      Montre des exemples de slugs DB vs handles Shopify pour comprendre
      les divergences SANS écrire de fichier (lecture seule).

Usage :
  python build_product_mapping.py --diagnose
  python build_product_mapping.py --from-db-handle --dry-run
  python build_product_mapping.py --from-db-handle
  python build_product_mapping.py --from-shopify
"""

import argparse
import csv
import logging
import sys
import unicodedata
from pathlib import Path

import config
from shopify_client import ShopifyClient, ShopifyGraphQLError

# ── Logging ───────────────────────────────────────────────────────────────────

def setup_logging() -> None:
    logs_dir = Path(__file__).parent / "logs"
    logs_dir.mkdir(exist_ok=True)
    from datetime import datetime
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"product_mapping_{ts}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    logging.info("Log product mapping : %s", log_file)


logger = logging.getLogger(__name__)

_MAP_COLUMNS = ["wee_product_id", "shopify_product_gid", "source"]


# ── Normalisation slug ────────────────────────────────────────────────────────

def normalize_handle(s: str) -> str:
    """
    Normalise un slug/handle pour le matching :
    - Supprime les accents (é→e, à→a, ü→u…)
    - Minuscules
    - Espaces et underscores → tirets
    Identique à ce que Shopify fait sur les handles.
    """
    nfkd = unicodedata.normalize("NFKD", s)
    no_accents = "".join(c for c in nfkd if not unicodedata.combining(c))
    return no_accents.lower().replace(" ", "-").replace("_", "-")


# ── CSV ───────────────────────────────────────────────────────────────────────

def save_mapping(mapping: dict[str, str], sources: dict[str, str], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {"wee_product_id": wid, "shopify_product_gid": gid, "source": sources.get(wid, "")}
        for wid, gid in sorted(mapping.items(), key=lambda x: int(x[0]) if x[0].isdigit() else 0)
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_MAP_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    logger.info("product_mapping.csv sauvegardé : %d entrée(s) → %s", len(rows), path)


def load_existing_mapping(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return {row["wee_product_id"]: row["shopify_product_gid"] for row in csv.DictReader(f)}


# ── Détection table produit DB ────────────────────────────────────────────────

def detect_product_table(conn) -> dict | None:
    """
    Détecte la table produit dans la DB Wee.
    Retourne un dict avec :
      table      : nom de la table
      slug_col   : colonne contenant le slug/handle
      id_col     : colonne qui référence le product_id
      has_locale : True si la table a une colonne trans_id ou locale
    """
    from db import fetch_all

    # Quelles colonnes existent dans les tables candidates ?
    rows = fetch_all(
        conn,
        """
        SELECT TABLE_NAME, COLUMN_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = ?
          AND TABLE_NAME IN ('product', 'products', 'product_trans', 'shop_product')
        ORDER BY TABLE_NAME, COLUMN_NAME
        """,
        (config.DB_NAME,),
    )
    if not rows:
        return None

    cols_by_table: dict[str, set] = {}
    for r in rows:
        cols_by_table.setdefault(r["TABLE_NAME"], set()).add(r["COLUMN_NAME"])

    # Essaie les tables dans l'ordre de priorité
    candidates = [
        ("product_trans", "slug"),
        ("product_trans", "handle"),
        ("product", "slug"),
        ("product", "handle"),
        ("product", "url_key"),
        ("products", "slug"),
        ("products", "handle"),
    ]
    for table, slug_col in candidates:
        table_cols = cols_by_table.get(table, set())
        if slug_col not in table_cols:
            continue

        # Détermine la colonne FK vers le produit
        if table == "product":
            id_col = "id"
        else:
            # Pour product_trans, cherche product_id
            id_col = "product_id" if "product_id" in table_cols else "id"

        # Détecte si la table a une colonne locale/trans_id
        has_locale = bool({"trans_id", "locale", "lang_id", "lang"} & table_cols)

        return {
            "table": table,
            "slug_col": slug_col,
            "id_col": id_col,
            "has_locale": has_locale,
            "all_cols": table_cols,
        }

    return None


def fetch_slugs_from_db(conn, info: dict, product_ids: list[str]) -> dict[str, str]:
    """
    Retourne {wee_product_id: slug} en filtrant par locale si possible.
    """
    from db import fetch_all

    table = info["table"]
    slug_col = info["slug_col"]
    id_col = info["id_col"]
    has_locale = info["has_locale"]

    slug_by_id: dict[str, str] = {}
    batch_size = 500

    for i in range(0, len(product_ids), batch_size):
        batch = product_ids[i: i + batch_size]
        placeholders = ",".join("?" * len(batch))

        if has_locale and "trans_id" in info["all_cols"]:
            # Filtre par trans_id (locale FR par défaut)
            sql = (
                f"SELECT {id_col} AS pid, {slug_col} AS slug "
                f"FROM {table} "
                f"WHERE {id_col} IN ({placeholders}) AND trans_id = ?"
            )
            params = tuple(batch) + (config.DEFAULT_TRANS_ID,)
        else:
            sql = (
                f"SELECT {id_col} AS pid, {slug_col} AS slug "
                f"FROM {table} "
                f"WHERE {id_col} IN ({placeholders})"
            )
            params = tuple(batch)

        rows = fetch_all(conn, sql, params)
        for r in rows:
            pid = str(r["pid"])
            slug = str(r["slug"] or "").strip()
            if slug and pid not in slug_by_id:
                slug_by_id[pid] = slug

    return slug_by_id


# ── Stratégie A : depuis les metafields Shopify ───────────────────────────────

def build_from_shopify(client: ShopifyClient, links_path: Path) -> dict[str, str]:
    logger.info("Stratégie : lecture metafield custom.wee_product_id depuis Shopify...")
    logger.info("(Peut prendre plusieurs minutes)")
    mapping = client.list_all_products_with_wee_id()
    logger.info("Produits avec custom.wee_product_id : %d", len(mapping))
    if not mapping:
        logger.warning(
            "Aucun produit Shopify n'a le metafield custom.wee_product_id.\n"
            "Essayez : python build_product_mapping.py --from-db-handle"
        )
    if links_path.exists():
        with open(links_path, "r", encoding="utf-8") as f:
            all_wee_ids = {row["product_id"] for row in csv.DictReader(f)}
        covered = len(all_wee_ids & set(mapping.keys()))
        logger.info(
            "Couverture : %d / %d wee_product_id(s) retrouvés (%.1f%%)",
            covered, len(all_wee_ids),
            100 * covered / len(all_wee_ids) if all_wee_ids else 0,
        )
    return mapping


# ── Stratégie B : depuis la DB Wee (slug/handle) ─────────────────────────────

def build_from_db_handle(client: ShopifyClient, links_path: Path) -> dict[str, str]:
    """
    1. Lit les product_id distincts depuis product_designer_links.csv
    2. Cherche les slugs dans la DB (filtrés par locale par défaut)
    3. Charge tous les handles Shopify (paginé, une seule passe)
    4. Croise en normalisant les accents côté Wee
    """
    from db import get_connection

    if not links_path.exists():
        logger.error("Fichier introuvable : %s — lancez d'abord export_designers_to_csv.py", links_path)
        sys.exit(1)

    with open(links_path, "r", encoding="utf-8") as f:
        all_wee_ids = sorted({row["product_id"] for row in csv.DictReader(f) if row.get("product_id")})
    logger.info("Wee product_id distincts dans les liens : %d", len(all_wee_ids))

    with get_connection() as conn:
        info = detect_product_table(conn)
        if not info:
            logger.error(
                "Aucune table produit détectée dans la DB.\n"
                "Tables cherchées : product, product_trans, products.\n"
                "Colonnes cherchées : slug, handle, url_key."
            )
            sys.exit(1)

        logger.info(
            "Table produit détectée : %s.%s (id_col=%s, has_locale=%s)",
            info["table"], info["slug_col"], info["id_col"], info["has_locale"],
        )
        if info["has_locale"]:
            logger.info("  Filtrage par locale : trans_id=%s (DEFAULT_TRANS_ID)", config.DEFAULT_TRANS_ID)

        slug_by_id = fetch_slugs_from_db(conn, info, all_wee_ids)

    logger.info("Slugs trouvés dans la DB : %d / %d", len(slug_by_id), len(all_wee_ids))
    if len(all_wee_ids) - len(slug_by_id):
        logger.warning(
            "%d produit(s) sans slug dans la DB — ils ne seront pas mappés.",
            len(all_wee_ids) - len(slug_by_id),
        )

    if not slug_by_id:
        logger.error("Aucun slug trouvé. Vérifiez DEFAULT_TRANS_ID dans .env ou essayez --diagnose.")
        sys.exit(1)

    # Charge tous les handles Shopify en une passe (50K+ produits ≈ 3-4 min)
    logger.info("Chargement de tous les handles Shopify (paginé, ~3-4 min)...")
    shopify_handles = client.list_all_products_by_handle()
    logger.info("Produits Shopify chargés : %d", len(shopify_handles))

    # Index normalisé des handles Shopify pour le matching insensible aux accents
    shopify_normalized: dict[str, str] = {normalize_handle(h): gid for h, gid in shopify_handles.items()}

    # Croise : slug Wee → handle Shopify (exact d'abord, normalisé ensuite)
    mapping: dict[str, str] = {}
    for wee_id, slug in slug_by_id.items():
        gid = (
            shopify_handles.get(slug)
            or shopify_handles.get(normalize_handle(slug))
            or shopify_normalized.get(normalize_handle(slug))
        )
        if gid:
            mapping[wee_id] = gid

    matched = len(mapping)
    logger.info(
        "Correspondances trouvées : %d / %d wee_product_id(s) (%.1f%%)",
        matched, len(all_wee_ids),
        100 * matched / len(all_wee_ids) if all_wee_ids else 0,
    )

    # Diagnostic des non-correspondances
    unmatched_with_slug = [wid for wid in slug_by_id if wid not in mapping]
    if unmatched_with_slug:
        logger.warning(
            "%d produit(s) ont un slug en DB mais aucune correspondance Shopify.",
            len(unmatched_with_slug),
        )
        logger.warning("  Exemples de slugs DB sans match :")
        for wid in unmatched_with_slug[:5]:
            slug = slug_by_id[wid]
            logger.warning("    wee_id=%s  slug_db='%s'  normalized='%s'", wid, slug, normalize_handle(slug))
        logger.warning("  → Comparez avec vos handles Shopify Admin.")
        logger.warning("  → Lancez --diagnose pour voir 20 exemples côte à côte.")

    return mapping


# ── Mode diagnostic ───────────────────────────────────────────────────────────

def run_diagnose(client: ShopifyClient, links_path: Path) -> None:
    """
    Affiche des exemples côte à côte pour comprendre pourquoi les slugs
    ne correspondent pas. Aucune écriture.
    """
    from db import get_connection

    logger.info("=" * 60)
    logger.info("MODE DIAGNOSTIC — lecture seule, aucune écriture")
    logger.info("=" * 60)

    # ── DB side ──
    with open(links_path, "r", encoding="utf-8") as f:
        all_wee_ids = sorted({row["product_id"] for row in csv.DictReader(f) if row.get("product_id")})

    with get_connection() as conn:
        info = detect_product_table(conn)
        if not info:
            logger.error("Aucune table produit détectée dans la DB.")
            return

        logger.info("Table produit : %s  |  colonne slug : %s  |  id_col : %s  |  has_locale : %s",
                    info["table"], info["slug_col"], info["id_col"], info["has_locale"])
        logger.info("Colonnes disponibles dans %s : %s", info["table"], sorted(info["all_cols"]))

        # Exemples SANS filtre locale
        from db import fetch_all
        sample_ids = all_wee_ids[:20]
        placeholders = ",".join("?" * len(sample_ids))
        rows_no_filter = fetch_all(
            conn,
            f"SELECT {info['id_col']} AS pid, {info['slug_col']} AS slug FROM {info['table']} "
            f"WHERE {info['id_col']} IN ({placeholders}) LIMIT 30",
            tuple(sample_ids),
        )

        logger.info("-" * 60)
        logger.info("SLUGS DB (sans filtre locale) — premiers résultats :")
        for r in rows_no_filter[:15]:
            logger.info("  pid=%-8s  slug='%s'", r["pid"], r["slug"])

        # Exemples AVEC filtre locale si possible
        if info["has_locale"] and "trans_id" in info["all_cols"]:
            rows_with_locale = fetch_all(
                conn,
                f"SELECT {info['id_col']} AS pid, {info['slug_col']} AS slug FROM {info['table']} "
                f"WHERE {info['id_col']} IN ({placeholders}) AND trans_id = ? LIMIT 30",
                tuple(sample_ids) + (config.DEFAULT_TRANS_ID,),
            )
            logger.info("-" * 60)
            logger.info("SLUGS DB (filtré trans_id=%s) :", config.DEFAULT_TRANS_ID)
            for r in rows_with_locale[:15]:
                logger.info("  pid=%-8s  slug='%s'  normalized='%s'", r["pid"], r["slug"], normalize_handle(str(r["slug"] or "")))

            # Détecte quelles trans_id existent
            trans_ids = fetch_all(
                conn,
                f"SELECT DISTINCT trans_id, COUNT(*) AS cnt FROM {info['table']} GROUP BY trans_id ORDER BY cnt DESC LIMIT 10",
            )
            logger.info("-" * 60)
            logger.info("trans_id disponibles dans %s :", info["table"])
            for t in trans_ids:
                logger.info("  trans_id=%-4s  %d lignes", t["trans_id"], t["cnt"])

    # ── Shopify side ──
    logger.info("-" * 60)
    logger.info("Chargement de 250 handles Shopify (première page)...")
    gql = """
    query {
      products(first: 20, sortKey: CREATED_AT) {
        edges { node { id handle } }
      }
    }
    """
    data = client._run(gql)
    edges = data.get("products", {}).get("edges", [])
    logger.info("HANDLES SHOPIFY (20 premiers produits créés) :")
    for e in edges:
        h = e["node"]["handle"]
        logger.info("  handle='%s'  normalized='%s'", h, normalize_handle(h))

    logger.info("=" * 60)
    logger.info("INTERPRÉTATION :")
    logger.info("  Si les slugs DB ressemblent aux handles Shopify → --from-db-handle fonctionnera.")
    logger.info("  Si les formats sont complètement différents → mapping manuel ou autre stratégie.")
    logger.info("  Vérifiez le bon trans_id ci-dessus (DEFAULT_TRANS_ID=%s dans .env).", config.DEFAULT_TRANS_ID)


# ── Entrypoint ────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Construit product_mapping.csv (wee_product_id → shopify_gid)"
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--from-shopify",
        action="store_true",
        dest="from_shopify",
        help="Lit custom.wee_product_id depuis les metafields Shopify",
    )
    mode.add_argument(
        "--from-db-handle",
        action="store_true",
        dest="from_db",
        help="Lit les slugs dans la DB Wee et les associe aux handles Shopify",
    )
    mode.add_argument(
        "--diagnose",
        action="store_true",
        dest="diagnose",
        help="Affiche des exemples slugs DB vs handles Shopify pour comprendre les écarts",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Affiche le résultat sans écrire product_mapping.csv",
    )
    return parser.parse_args()


def main() -> None:
    setup_logging()
    args = parse_args()

    output_dir = config.OUTPUT_DIR
    links_path = output_dir / "product_designer_links.csv"
    mapping_path = output_dir / "product_mapping.csv"

    client = ShopifyClient(dry_run=False)

    if args.diagnose:
        if not links_path.exists():
            logger.error("Lancez d'abord : python export_designers_to_csv.py --global-export")
            sys.exit(1)
        run_diagnose(client, links_path)
        return

    if args.from_shopify:
        mapping = build_from_shopify(client, links_path)
        source_label = "shopify_metafield"
    else:
        mapping = build_from_db_handle(client, links_path)
        source_label = "db_handle"

    logger.info("=" * 60)
    logger.info("RÉSULTAT : %d correspondance(s) trouvée(s)", len(mapping))

    if not mapping:
        logger.warning("Aucune correspondance — product_mapping.csv non créé.")
        logger.warning("Lancez --diagnose pour comprendre les écarts.")
        return

    if args.dry_run:
        logger.info("[DRY-RUN] Aperçu des 10 premières entrées :")
        for wid, gid in list(mapping.items())[:10]:
            logger.info("  wee_product_id=%s → %s", wid, gid)
        logger.info("[DRY-RUN] product_mapping.csv non écrit.")
        return

    existing = load_existing_mapping(mapping_path)
    merged = {**existing, **mapping}
    sources = {wid: source_label for wid in mapping}
    save_mapping(merged, sources, mapping_path)

    logger.info("Prochaine étape :")
    logger.info("  python import_designers_to_shopify.py --global-import --dry-run")


if __name__ == "__main__":
    main()
