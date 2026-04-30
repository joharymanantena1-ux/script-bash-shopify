"""
build_product_mapping.py — Construit output/product_mapping.csv

Mappe wee_product_id → shopify_product_gid pour que la Phase 2 de
import_designers_to_shopify.py puisse lier tous les produits.

Deux stratégies (choisir selon votre situation) :

  --from-shopify
      Interroge Shopify pour tous les produits qui ont le metafield
      custom.wee_product_id déjà renseigné.
      → Idéal si une migration précédente a taggué les produits Shopify
        avec leur ID Wee.

  --from-db-handle
      Lit les slugs/handles des produits dans la base MariaDB Wee,
      puis charge tous les handles Shopify pour construire la correspondance.
      → Idéal si les handles Shopify correspondent aux slugs Wee.
      → Nécessite une table produit dans la DB (détectée automatiquement).

Usage :
  python build_product_mapping.py --from-shopify
  python build_product_mapping.py --from-db-handle
  python build_product_mapping.py --from-shopify --dry-run
"""

import argparse
import csv
import logging
import sys
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


def save_mapping(mapping: dict[str, str], sources: dict[str, str], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "wee_product_id": wid,
            "shopify_product_gid": gid,
            "source": sources.get(wid, ""),
        }
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


# ── Stratégie A : depuis les metafields Shopify ───────────────────────────────

def build_from_shopify(client: ShopifyClient, links_path: Path) -> dict[str, str]:
    """
    Interroge Shopify pour tous les produits ayant custom.wee_product_id.
    Retourne {wee_product_id: shopify_gid}.
    """
    logger.info("Stratégie : lecture metafield custom.wee_product_id depuis Shopify...")
    logger.info("(Cette opération peut prendre plusieurs minutes selon le nombre de produits)")

    mapping = client.list_all_products_with_wee_id()
    logger.info("Produits avec custom.wee_product_id : %d", len(mapping))

    if not mapping:
        logger.warning(
            "Aucun produit Shopify n'a le metafield custom.wee_product_id.\n"
            "  → Essayez --from-db-handle si les handles Shopify correspondent aux slugs Wee.\n"
            "  → Ou ajoutez manuellement wee_product_id dans les metafields Shopify."
        )

    # Comptage des wee_product_ids dans les liens pour comparer la couverture
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

def detect_product_table(conn) -> tuple[str, str] | None:
    """
    Détecte automatiquement dans quelles tables/colonnes chercher les slugs produits.
    Retourne (table_name, slug_column) ou None si non détecté.
    """
    from db import fetch_all

    result = fetch_all(
        conn,
        """
        SELECT TABLE_NAME, COLUMN_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = ?
          AND TABLE_NAME   IN ('product', 'products', 'product_trans', 'shop_product')
          AND COLUMN_NAME  IN ('slug', 'handle', 'url_key', 'url_path', 'slug_fr', 'handle_fr')
        ORDER BY TABLE_NAME, COLUMN_NAME
        """,
        (config.DB_NAME,),
    )

    if not result:
        return None

    # Priorité : table 'product' avec 'slug', puis autres combinaisons
    priority = [
        ("product", "slug"), ("product", "handle"), ("product", "url_key"),
        ("product_trans", "slug"), ("product_trans", "handle"),
        ("products", "slug"), ("products", "handle"),
    ]
    existing = {(r["TABLE_NAME"], r["COLUMN_NAME"]) for r in result}
    for candidate in priority:
        if candidate in existing:
            return candidate

    # Fallback : première entrée trouvée
    r = result[0]
    return (r["TABLE_NAME"], r["COLUMN_NAME"])


def build_from_db_handle(
    client: ShopifyClient,
    links_path: Path,
    mapping_path: Path,
) -> dict[str, str]:
    """
    1. Lit les product_id distincts depuis product_designer_links.csv
    2. Cherche le slug/handle dans la DB Wee
    3. Charge tous les handles Shopify (paginé, une seule passe)
    4. Joint les deux pour construire la correspondance
    """
    from db import get_connection, fetch_all

    if not links_path.exists():
        logger.error("Fichier introuvable : %s — lancez d'abord export_designers_to_csv.py", links_path)
        sys.exit(1)

    with open(links_path, "r", encoding="utf-8") as f:
        all_wee_ids = sorted({row["product_id"] for row in csv.DictReader(f) if row.get("product_id")})

    logger.info("Wee product_id distincts dans les liens : %d", len(all_wee_ids))

    with get_connection() as conn:
        detected = detect_product_table(conn)
        if not detected:
            logger.error(
                "Aucune table produit détectée dans la DB (product, product_trans, products).\n"
                "  Colonnes cherchées : slug, handle, url_key.\n"
                "  Essayez --from-shopify à la place."
            )
            sys.exit(1)

        table, col = detected
        logger.info("Table produit détectée : %s.%s", table, col)

        # Requête par batch de 500 IDs max pour ne pas saturer le SQL
        slug_by_id: dict[str, str] = {}
        batch_size = 500
        for i in range(0, len(all_wee_ids), batch_size):
            batch = all_wee_ids[i: i + batch_size]
            placeholders = ",".join("?" * len(batch))
            sql = f"SELECT id, {col} AS slug FROM {table} WHERE id IN ({placeholders})"
            rows = fetch_all(conn, sql, tuple(batch))
            for r in rows:
                if r.get("slug"):
                    slug_by_id[str(r["id"])] = str(r["slug"])

    logger.info("Slugs trouvés dans la DB : %d / %d", len(slug_by_id), len(all_wee_ids))

    missing_slugs = len(all_wee_ids) - len(slug_by_id)
    if missing_slugs:
        logger.warning("%d produit(s) sans slug dans la DB — ils ne seront pas mappés.", missing_slugs)

    if not slug_by_id:
        logger.error("Aucun slug trouvé — impossible de construire le mapping.")
        sys.exit(1)

    # Charge tous les handles Shopify en une passe
    logger.info("Chargement de tous les handles Shopify (paginé)...")
    shopify_handles = client.list_all_products_by_handle()
    logger.info("Produits Shopify chargés : %d", len(shopify_handles))

    # Croise : slug Wee → handle Shopify → GID
    mapping: dict[str, str] = {}
    for wee_id, slug in slug_by_id.items():
        # Essaie le slug tel quel, puis la version normalisée (minuscules, tirets)
        normalized = slug.lower().replace(" ", "-").replace("_", "-")
        gid = shopify_handles.get(slug) or shopify_handles.get(normalized)
        if gid:
            mapping[wee_id] = gid

    matched = len(mapping)
    logger.info(
        "Correspondances trouvées : %d / %d wee_product_id(s) (%.1f%%)",
        matched, len(all_wee_ids),
        100 * matched / len(all_wee_ids) if all_wee_ids else 0,
    )

    unmatched = [wid for wid in all_wee_ids if wid not in mapping]
    if unmatched:
        logger.warning(
            "%d produit(s) sans correspondance Shopify. Exemples : %s",
            len(unmatched), unmatched[:10],
        )
        logger.warning(
            "  Cause possible : le slug Wee ne correspond pas au handle Shopify.\n"
            "  Exemple slug DB : '%s' — vérifiez le handle dans Shopify Admin.",
            slug_by_id.get(unmatched[0], "?") if unmatched else "?",
        )

    return mapping


# ── Entrypoint ────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Construit product_mapping.csv (wee_product_id → shopify_gid)"
    )
    strategy = parser.add_mutually_exclusive_group(required=True)
    strategy.add_argument(
        "--from-shopify",
        action="store_true",
        dest="from_shopify",
        help="Lit custom.wee_product_id depuis les metafields Shopify",
    )
    strategy.add_argument(
        "--from-db-handle",
        action="store_true",
        dest="from_db",
        help="Lit les slugs dans la DB Wee et les associe aux handles Shopify",
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

    if args.from_shopify:
        mapping = build_from_shopify(client, links_path)
        source_label = "shopify_metafield"
    else:
        mapping = build_from_db_handle(client, links_path, mapping_path)
        source_label = "db_handle"

    logger.info("=" * 60)
    logger.info("RÉSULTAT : %d correspondance(s) trouvée(s)", len(mapping))

    if not mapping:
        logger.warning("Aucune correspondance trouvée — product_mapping.csv non créé.")
        return

    if args.dry_run:
        logger.info("[DRY-RUN] Aperçu des 10 premières entrées :")
        for wid, gid in list(mapping.items())[:10]:
            logger.info("  wee_product_id=%s → %s", wid, gid)
        logger.info("[DRY-RUN] product_mapping.csv non écrit.")
        return

    # Fusionner avec le mapping existant (sans écraser les entrées valides)
    existing = load_existing_mapping(mapping_path)
    merged = {**existing, **mapping}
    sources = {wid: source_label for wid in mapping}

    save_mapping(merged, sources, mapping_path)

    logger.info("Prochaine étape :")
    logger.info("  python import_designers_to_shopify.py --global-import --dry-run")


if __name__ == "__main__":
    main()
