"""
audit_shopify.py — Audit complet de l'état de la migration Designer

Compare les données source (CSV), l'état des caches locaux et l'état réel
de Shopify pour produire un rapport exhaustif.

Sections du rapport :
  1. DONNÉES SOURCE         — ce que la DB Wee contient
  2. ÉTAT SHOPIFY           — metaobjects et liaisons produits réels (API live)
  3. ÉTAT CACHE LOCAL       — import_state.csv et link_state.csv
  4. ANALYSE CROISÉE        — manquants, orphelins, taux de réussite
  5. RECOMMANDATIONS        — prochaines étapes suggérées

Usage :
  python audit_shopify.py              # audit complet (lent ~5 min, appels API)
  python audit_shopify.py --no-api     # audit cache + CSV uniquement (rapide)
  python audit_shopify.py --json       # sortie JSON en plus du log
"""

import argparse
import csv
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import config
from shopify_client import ShopifyClient, ShopifyGraphQLError

# ── Logging ───────────────────────────────────────────────────────────────────

def setup_logging() -> None:
    logs_dir = Path(__file__).parent / "logs"
    logs_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"audit_{ts}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    logging.info("Log audit : %s", log_file)

logger = logging.getLogger(__name__)


# ── Chargement CSV ────────────────────────────────────────────────────────────

def load_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_import_state(path: Path) -> dict[str, dict]:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return {row["wee_designer_id"]: row for row in csv.DictReader(f)}


def load_link_state(path: Path) -> set[tuple[str, str]]:
    if not path.exists():
        return set()
    with open(path, "r", encoding="utf-8") as f:
        return {
            (row["product_id"], row["wee_designer_id"])
            for row in csv.DictReader(f)
            if row.get("status") == "ok"
        }


# ── Helpers ───────────────────────────────────────────────────────────────────

def pct(part: int, total: int) -> str:
    if not total:
        return "N/A"
    return f"{100 * part / total:.1f}%"


def sep(title: str = "") -> None:
    if title:
        logger.info("=" * 60)
        logger.info("  %s", title)
        logger.info("=" * 60)
    else:
        logger.info("-" * 60)


# ── Audit ─────────────────────────────────────────────────────────────────────

def run_audit(no_api: bool, output_json: bool) -> dict:
    output_dir = config.OUTPUT_DIR
    report: dict = {}

    # ── 1. DONNÉES SOURCE ─────────────────────────────────────────────────────
    sep("1. DONNÉES SOURCE (fichiers CSV)")

    designers_rows = load_csv(output_dir / "designers.csv")
    links_rows     = load_csv(output_dir / "product_designer_links.csv")
    mapping_rows   = load_csv(output_dir / "product_mapping.csv")
    image_map_rows = load_csv(output_dir / "image_gid_map.csv")

    unique_designers_csv = {str(r["wee_designer_id"]) for r in designers_rows if r.get("wee_designer_id")}
    unique_products_csv  = {str(r["product_id"])      for r in links_rows     if r.get("product_id")}
    mapped_products      = {str(r["wee_product_id"])  for r in mapping_rows   if r.get("wee_product_id")}
    cached_images        = {r["image_file"]           for r in image_map_rows if r.get("shopify_gid") and not r["shopify_gid"].startswith("[DRY")}

    total_links          = len(links_rows)
    links_with_mapping   = sum(1 for r in links_rows if str(r.get("product_id", "")) in mapped_products)
    links_without_mapping = total_links - links_with_mapping

    logger.info("Designers uniques dans designers.csv       : %d", len(unique_designers_csv))
    logger.info("Lignes dans product_designer_links.csv     : %d", total_links)
    logger.info("Produits Wee uniques dans les liens        : %d", len(unique_products_csv))
    logger.info("Produits mappés dans product_mapping.csv   : %d", len(mapped_products))
    logger.info("Produits SANS mapping (liens impossibles)  : %d (%s)", len(unique_products_csv) - len(mapped_products & unique_products_csv), pct(len(unique_products_csv) - len(mapped_products & unique_products_csv), len(unique_products_csv)))
    logger.info("Liens avec mapping disponible              : %d (%s)", links_with_mapping, pct(links_with_mapping, total_links))
    logger.info("Images dans image_gid_map.csv              : %d", len(cached_images))

    report["source"] = {
        "unique_designers": len(unique_designers_csv),
        "total_links": total_links,
        "unique_products_in_links": len(unique_products_csv),
        "mapped_products": len(mapped_products),
        "links_with_mapping": links_with_mapping,
        "links_without_mapping": links_without_mapping,
        "cached_images": len(cached_images),
    }

    # ── 2. ÉTAT CACHE LOCAL ───────────────────────────────────────────────────
    sep("2. ÉTAT CACHE LOCAL")

    import_state = load_import_state(output_dir / "import_state.csv")
    link_state   = load_link_state(output_dir / "link_state.csv")

    cached_ok      = sum(1 for v in import_state.values() if v.get("shopify_metaobject_gid") and not v["shopify_metaobject_gid"].startswith("[DRY"))
    cached_dry_run = sum(1 for v in import_state.values() if (v.get("shopify_metaobject_gid") or "").startswith("[DRY"))
    cached_missing = len(unique_designers_csv) - len(import_state)

    logger.info("import_state.csv — entrées totales         : %d", len(import_state))
    logger.info("  dont GID réels (non dry-run)             : %d", cached_ok)
    logger.info("  dont dry-run (placeholder)               : %d", cached_dry_run)
    logger.info("  designers absents du cache               : %d", max(0, cached_missing))
    logger.info("link_state.csv  — liens OK en cache        : %d (%s)", len(link_state), pct(len(link_state), links_with_mapping))

    report["cache"] = {
        "import_state_total": len(import_state),
        "import_state_real_gids": cached_ok,
        "import_state_dry_run": cached_dry_run,
        "designers_not_in_cache": max(0, cached_missing),
        "link_state_ok": len(link_state),
    }

    # ── 3. ÉTAT SHOPIFY (API live) ────────────────────────────────────────────
    if no_api:
        sep("3. ÉTAT SHOPIFY (ignoré — --no-api)")
        logger.info("Utilisez sans --no-api pour interroger Shopify.")
        report["shopify"] = {"skipped": True}
    else:
        sep("3. ÉTAT SHOPIFY (appels API — peut prendre 5-10 min)")

        client = ShopifyClient(dry_run=False)

        # Metaobjects
        logger.info("Chargement des metaobjects designer depuis Shopify...")
        try:
            shopify_metaobjects = client.list_all_designer_metaobjects_detailed()
        except ShopifyGraphQLError as e:
            logger.error("Impossible de lister les metaobjects : %s", e)
            shopify_metaobjects = []

        total_metaobjects  = len(shopify_metaobjects)
        with_name          = sum(1 for m in shopify_metaobjects if m.get("name"))
        without_name       = total_metaobjects - with_name
        with_image         = sum(1 for m in shopify_metaobjects if m.get("has_image"))
        without_image      = total_metaobjects - with_image
        with_wee_id        = sum(1 for m in shopify_metaobjects if m.get("wee_designer_id"))

        shopify_wee_ids    = {m["wee_designer_id"] for m in shopify_metaobjects if m.get("wee_designer_id")}
        missing_in_shopify = unique_designers_csv - shopify_wee_ids
        orphan_in_shopify  = shopify_wee_ids - unique_designers_csv

        logger.info("Metaobjects designer dans Shopify          : %d / %d source (%s)", total_metaobjects, len(unique_designers_csv), pct(total_metaobjects, len(unique_designers_csv)))
        logger.info("  avec champ 'name' renseigné              : %d (%s)", with_name, pct(with_name, total_metaobjects))
        logger.info("  SANS nom (problématique)                 : %d", without_name)
        logger.info("  avec image                               : %d (%s)", with_image, pct(with_image, total_metaobjects))
        logger.info("  sans image                               : %d", without_image)
        logger.info("  avec wee_designer_id renseigné           : %d", with_wee_id)
        sep()
        logger.info("Designers dans CSV mais ABSENTS Shopify    : %d", len(missing_in_shopify))
        if missing_in_shopify:
            logger.info("  Exemples : %s", sorted(missing_in_shopify)[:10])
        logger.info("Metaobjects Shopify ORPHELINS (hors CSV)   : %d", len(orphan_in_shopify))

        # Produits liés
        logger.info("Chargement des produits avec custom.designer depuis Shopify...")
        try:
            linked_products = client.list_all_products_with_designer_metafield()
        except ShopifyGraphQLError as e:
            logger.error("Impossible de lister les produits liés : %s", e)
            linked_products = []

        total_linked   = len(linked_products)
        linked_gids    = {p["metaobject_gid"] for p in linked_products}
        # Métaobjects qui ont au moins un produit lié
        metaobjects_used = linked_gids & {m["id"] for m in shopify_metaobjects}

        logger.info("Produits avec custom.designer dans Shopify : %d (%s des liens possibles)", total_linked, pct(total_linked, links_with_mapping))
        logger.info("Metaobjects utilisés (≥1 produit lié)      : %d / %d (%s)", len(metaobjects_used), total_metaobjects, pct(len(metaobjects_used), total_metaobjects))
        logger.info("Metaobjects créés mais 0 produit lié       : %d", total_metaobjects - len(metaobjects_used))

        report["shopify"] = {
            "total_metaobjects": total_metaobjects,
            "metaobjects_with_name": with_name,
            "metaobjects_without_name": without_name,
            "metaobjects_with_image": with_image,
            "metaobjects_without_image": without_image,
            "missing_in_shopify": len(missing_in_shopify),
            "missing_wee_ids": sorted(missing_in_shopify)[:50],
            "orphan_in_shopify": len(orphan_in_shopify),
            "products_linked": total_linked,
            "metaobjects_with_at_least_one_link": len(metaobjects_used),
            "metaobjects_with_no_link": total_metaobjects - len(metaobjects_used),
        }

    # ── 4. ANALYSE CROISÉE ───────────────────────────────────────────────────
    sep("4. ANALYSE CROISÉE")

    if not no_api:
        total_mo  = report["shopify"]["total_metaobjects"]
        total_src = report["source"]["unique_designers"]
        total_lnk = report["shopify"]["products_linked"]
        possible  = report["source"]["links_with_mapping"]

        logger.info("METAOBJECTS  : %d / %d créés  (%s)", total_mo, total_src, pct(total_mo, total_src))
        logger.info("LIAISONS     : %d / %d faites (%s)", total_lnk, possible, pct(total_lnk, possible))
        logger.info("IMAGES       : %d / %d attachées (%s)", report["shopify"]["metaobjects_with_image"], total_mo, pct(report["shopify"]["metaobjects_with_image"], total_mo))

        # Liens restants à faire
        remaining_links = possible - total_lnk
        logger.info("Liens restants à faire                     : %d", remaining_links)

        # Stale GIDs estimés (in cache but not in Shopify)
        shopify_real_ids = {m["wee_designer_id"] for m in shopify_metaobjects if m.get("wee_designer_id")}
        stale_in_cache   = {wid for wid, v in import_state.items()
                            if v.get("shopify_metaobject_gid") and not v["shopify_metaobject_gid"].startswith("[DRY")
                            and wid not in shopify_real_ids}
        logger.info("GIDs obsolètes dans le cache               : %d", len(stale_in_cache))
        if stale_in_cache:
            logger.info("  wee_designer_ids concernés : %s", sorted(stale_in_cache)[:20])
    else:
        logger.info("Analyse croisée ignorée sans --no-api.")

    # ── 5. RECOMMANDATIONS ────────────────────────────────────────────────────
    sep("5. RECOMMANDATIONS")

    if not no_api:
        stale_count   = len(stale_in_cache)
        missing_count = report["shopify"]["missing_in_shopify"]
        no_link_count = report["shopify"]["metaobjects_with_no_link"]
        remaining     = possible - report["shopify"]["products_linked"]

        if stale_count:
            logger.info("⚠  %d GID(s) obsolètes en cache → relancez l'import pour les recréer automatiquement.", stale_count)
        if missing_count:
            logger.info("⚠  %d designer(s) manquants dans Shopify → relancez :", missing_count)
            logger.info("     python import_designers_to_shopify.py --global-import --no-dry-run")
        if remaining > 0:
            logger.info("⚠  %d lien(s) produit restants → relancez l'import.", remaining)
        if no_link_count:
            logger.info("⚠  %d metaobject(s) sans aucun produit lié.", no_link_count)
        if report["shopify"]["metaobjects_without_image"]:
            logger.info("ℹ  %d metaobject(s) sans image → lancez :", report["shopify"]["metaobjects_without_image"])
            logger.info("     python upload_images_to_shopify.py")
            logger.info("     puis : python import_designers_to_shopify.py --global-import --no-dry-run")

        if not stale_count and not missing_count and remaining == 0:
            logger.info("✓  Import complet — aucune action requise.")
    else:
        if report["cache"]["import_state_dry_run"]:
            logger.info("⚠  %d entrées dry-run dans le cache → relancez avec --no-dry-run.", report["cache"]["import_state_dry_run"])
        if report["cache"]["designers_not_in_cache"]:
            logger.info("⚠  %d designer(s) pas encore dans le cache.", report["cache"]["designers_not_in_cache"])

    sep()
    logger.info("Audit terminé : %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # ── JSON optionnel ────────────────────────────────────────────────────────
    if output_json:
        json_path = output_dir / "audit_report.json"
        report["generated_at"] = datetime.now().isoformat()
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        logger.info("Rapport JSON sauvegardé : %s", json_path)

    return report


# ── Entrypoint ────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit complet de la migration Designer Wee → Shopify"
    )
    parser.add_argument(
        "--no-api",
        action="store_true",
        help="Ignore les appels Shopify — audit CSV/cache uniquement (rapide)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="output_json",
        help="Sauvegarde le rapport en JSON dans output/audit_report.json",
    )
    return parser.parse_args()


def main() -> None:
    setup_logging()
    args = parse_args()
    run_audit(no_api=args.no_api, output_json=args.output_json)


if __name__ == "__main__":
    main()
