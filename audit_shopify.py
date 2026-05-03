"""
audit_shopify.py — Audit complet de l'état de la migration Designer

Compare les données source (CSV), l'état des caches locaux et l'état réel
de Shopify pour produire un rapport exhaustif avec barres de progression.

Usage :
  python audit_shopify.py              # audit complet (API live ~5 min)
  python audit_shopify.py --no-api     # audit CSV/cache uniquement (rapide)
  python audit_shopify.py --json       # + export output/audit_report.json
"""

import argparse
import csv
import json
import logging
import sys
import threading
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

logger = logging.getLogger(__name__)


# ── Affichage ─────────────────────────────────────────────────────────────────

BAR_WIDTH = 24

def progress_bar(part: int, total: int, width: int = BAR_WIDTH) -> str:
    """Retourne une barre ASCII + pourcentage + fraction."""
    if not total:
        return f"{'░' * width}  N/A"
    ratio = min(part / total, 1.0)
    filled = int(ratio * width)
    bar = "█" * filled + "░" * (width - filled)
    return f"{bar}  {ratio * 100:5.1f}%  ({part:,} / {total:,})"


def progress_line(label: str, part: int, total: int) -> None:
    logger.info("  %-32s %s", label, progress_bar(part, total))


def header(title: str) -> None:
    logger.info("")
    logger.info("╔══════════════════════════════════════════════════════════════╗")
    logger.info("║  %-60s║", title)
    logger.info("╚══════════════════════════════════════════════════════════════╝")


def section(title: str) -> None:
    logger.info("")
    logger.info("┌─ %s %s", title, "─" * max(0, 58 - len(title)))


def divider() -> None:
    logger.info("  %s", "─" * 60)


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


def pct_val(part: int, total: int) -> float:
    return round(100 * part / total, 1) if total else 0.0


# ── Audit ─────────────────────────────────────────────────────────────────────

def run_audit(no_api: bool, output_json: bool) -> dict:
    output_dir = config.OUTPUT_DIR
    report: dict = {"generated_at": datetime.now().isoformat()}

    header("AUDIT MIGRATION DESIGNER  —  " + datetime.now().strftime("%Y-%m-%d %H:%M"))

    # ── 1. DONNÉES SOURCE ─────────────────────────────────────────────────────
    section("1. DONNÉES SOURCE")

    designers_rows = load_csv(output_dir / "designers.csv")
    links_rows     = load_csv(output_dir / "product_designer_links.csv")
    mapping_rows   = load_csv(output_dir / "product_mapping.csv")
    image_map_rows = load_csv(output_dir / "image_gid_map.csv")

    unique_designers_csv = {str(r["wee_designer_id"]) for r in designers_rows if r.get("wee_designer_id")}
    unique_products_csv  = {str(r["product_id"])      for r in links_rows     if r.get("product_id")}
    mapped_products      = {str(r["wee_product_id"])  for r in mapping_rows   if r.get("wee_product_id")}
    cached_images        = {
        r["image_file"] for r in image_map_rows
        if r.get("shopify_gid") and not r["shopify_gid"].startswith("[DRY")
    }

    total_links           = len(links_rows)
    products_in_links     = len(unique_products_csv)
    mapped_in_links       = len(mapped_products & unique_products_csv)
    unmapped_in_links     = products_in_links - mapped_in_links
    links_with_mapping    = sum(1 for r in links_rows if str(r.get("product_id", "")) in mapped_products)
    links_without_mapping = total_links - links_with_mapping

    logger.info("  Designers uniques (source)             : %d", len(unique_designers_csv))
    logger.info("  Liens produit-designer (total)         : %d", total_links)
    logger.info("  Produits Wee uniques dans les liens    : %d", products_in_links)
    progress_line("Produits avec mapping Shopify", mapped_in_links, products_in_links)
    progress_line("Liens avec mapping disponible", links_with_mapping, total_links)
    logger.info("  Images dans image_gid_map.csv          : %d", len(cached_images))

    report["source"] = {
        "unique_designers": len(unique_designers_csv),
        "total_links": total_links,
        "unique_products_in_links": products_in_links,
        "mapped_products": mapped_in_links,
        "unmapped_products": unmapped_in_links,
        "links_with_mapping": links_with_mapping,
        "links_without_mapping": links_without_mapping,
        "cached_images": len(cached_images),
    }

    # ── 2. CACHE LOCAL ────────────────────────────────────────────────────────
    section("2. CACHE LOCAL")

    import_state = load_import_state(output_dir / "import_state.csv")
    link_state   = load_link_state(output_dir  / "link_state.csv")

    cached_real    = sum(1 for v in import_state.values()
                         if v.get("shopify_metaobject_gid") and not v["shopify_metaobject_gid"].startswith("[DRY"))
    cached_dryrun  = sum(1 for v in import_state.values()
                         if (v.get("shopify_metaobject_gid") or "").startswith("[DRY"))
    designers_not_cached = max(0, len(unique_designers_csv) - len(import_state))

    progress_line("Designers dans import_state.csv", cached_real, len(unique_designers_csv))
    logger.info("  ├ GID réels (non dry-run)              : %d", cached_real)
    logger.info("  ├ Dry-run (placeholders)               : %d", cached_dryrun)
    logger.info("  └ Absents du cache                     : %d", designers_not_cached)
    divider()
    progress_line("Liens dans link_state.csv", len(link_state), links_with_mapping)

    report["cache"] = {
        "import_state_total": len(import_state),
        "import_state_real_gids": cached_real,
        "import_state_dry_run": cached_dryrun,
        "designers_not_in_cache": designers_not_cached,
        "link_state_ok": len(link_state),
        "link_state_pct": pct_val(len(link_state), links_with_mapping),
    }

    # ── 3. ÉTAT SHOPIFY (API live) ────────────────────────────────────────────
    shopify_metaobjects: list[dict] = []
    linked_products: list[dict] = []

    if no_api:
        section("3. ÉTAT SHOPIFY  [ignoré — --no-api]")
        logger.info("  Utilisez sans --no-api pour interroger Shopify en direct.")
        report["shopify"] = {"skipped": True}
    else:
        section("3. ÉTAT SHOPIFY  [appels API — ~5-10 min]")
        client = ShopifyClient(dry_run=False)

        # Appels parallèles pour gagner du temps
        mo_error: list[str] = []
        lp_error: list[str] = []

        def fetch_metaobjects() -> None:
            nonlocal shopify_metaobjects
            logger.info("  → Chargement metaobjects designer...")
            try:
                shopify_metaobjects = client.list_all_designer_metaobjects_detailed()
                logger.info("  ✓ %d metaobject(s) chargé(s)", len(shopify_metaobjects))
            except ShopifyGraphQLError as e:
                mo_error.append(str(e))
                logger.error("  ✗ Metaobjects : %s", e)

        def fetch_linked_products() -> None:
            nonlocal linked_products
            logger.info("  → Chargement produits avec custom.designer...")
            try:
                linked_products = client.list_all_products_with_designer_metafield()
                logger.info("  ✓ %d produit(s) lié(s) chargé(s)", len(linked_products))
            except ShopifyGraphQLError as e:
                lp_error.append(str(e))
                logger.error("  ✗ Produits liés : %s", e)

        t1 = threading.Thread(target=fetch_metaobjects)
        t2 = threading.Thread(target=fetch_linked_products)
        t1.start(); t2.start()
        t1.join();  t2.join()

        # ── Analyse metaobjects
        total_mo       = len(shopify_metaobjects)
        with_name      = sum(1 for m in shopify_metaobjects if m.get("name"))
        without_name   = total_mo - with_name
        with_image     = sum(1 for m in shopify_metaobjects if m.get("has_image"))
        without_image  = total_mo - with_image
        with_wee_id    = sum(1 for m in shopify_metaobjects if m.get("wee_designer_id"))

        shopify_wee_ids    = {m["wee_designer_id"] for m in shopify_metaobjects if m.get("wee_designer_id")}
        missing_in_shopify = unique_designers_csv - shopify_wee_ids
        orphan_in_shopify  = shopify_wee_ids - unique_designers_csv

        divider()
        logger.info("  METAOBJECTS DESIGNER")
        progress_line("Créés dans Shopify", total_mo, len(unique_designers_csv))
        progress_line("Avec nom renseigné", with_name, total_mo)
        progress_line("Avec image attachée", with_image, total_mo)
        logger.info("  Sans nom (problématique)               : %d", without_name)
        logger.info("  Sans image                             : %d", without_image)
        logger.info("  Manquants vs source CSV                : %d", len(missing_in_shopify))
        logger.info("  Orphelins (hors CSV)                   : %d", len(orphan_in_shopify))

        # ── Analyse liaisons produits
        total_linked  = len(linked_products)
        linked_mo_ids = {p["metaobject_gid"] for p in linked_products}
        mo_ids_set    = {m["id"] for m in shopify_metaobjects}
        mo_used       = len(linked_mo_ids & mo_ids_set)
        mo_unused     = total_mo - mo_used

        divider()
        logger.info("  LIAISONS PRODUITS")
        progress_line("Produits liés dans Shopify", total_linked, links_with_mapping)
        progress_line("Metaobjects avec ≥1 produit lié", mo_used, total_mo)
        logger.info("  Metaobjects sans aucun produit lié     : %d", mo_unused)

        report["shopify"] = {
            "total_metaobjects": total_mo,
            "metaobjects_with_name": with_name,
            "metaobjects_without_name": without_name,
            "metaobjects_with_image": with_image,
            "metaobjects_without_image": without_image,
            "metaobjects_with_wee_id": with_wee_id,
            "missing_in_shopify": len(missing_in_shopify),
            "missing_wee_ids": sorted(missing_in_shopify)[:50],
            "orphan_in_shopify": len(orphan_in_shopify),
            "products_linked": total_linked,
            "metaobjects_with_link": mo_used,
            "metaobjects_without_link": mo_unused,
            "errors": mo_error + lp_error,
        }

    # ── 4. ANALYSE CROISÉE ───────────────────────────────────────────────────
    section("4. ANALYSE CROISÉE")

    if not no_api:
        total_mo  = report["shopify"]["total_metaobjects"]
        total_src = report["source"]["unique_designers"]
        total_lnk = report["shopify"]["products_linked"]
        possible  = report["source"]["links_with_mapping"]

        stale_in_cache = {
            wid for wid, v in import_state.items()
            if v.get("shopify_metaobject_gid") and not v["shopify_metaobject_gid"].startswith("[DRY")
            and wid not in shopify_wee_ids
        }

        remaining_mo    = total_src - total_mo
        remaining_links = possible - total_lnk

        logger.info("  GIDs obsolètes dans le cache           : %d", len(stale_in_cache))
        if stale_in_cache:
            logger.info("    wee_designer_ids : %s", sorted(stale_in_cache)[:15])
        logger.info("  Metaobjects encore à créer             : %d", remaining_mo)
        logger.info("  Liens encore à faire                   : %d", remaining_links)
        logger.info("  Liens impossibles (sans mapping)       : %d", links_without_mapping)

        report["cross"] = {
            "stale_gids_in_cache": len(stale_in_cache),
            "stale_wee_ids": sorted(stale_in_cache),
            "remaining_metaobjects": remaining_mo,
            "remaining_links": remaining_links,
            "impossible_links": links_without_mapping,
        }
    else:
        logger.info("  (Ignoré sans --no-api)")
        report["cross"] = {}

    # ── 5. TABLEAU DE BORD ───────────────────────────────────────────────────
    logger.info("")
    logger.info("╔══════════════════════════════════════════════════════════════╗")
    logger.info("║              TABLEAU DE BORD — AVANCEMENT GLOBAL            ║")
    logger.info("╠══════════════════════════════════════════════════════════════╣")

    if not no_api:
        total_src = report["source"]["unique_designers"]
        total_mo  = report["shopify"]["total_metaobjects"]
        total_lnk = report["shopify"]["products_linked"]
        possible  = report["source"]["links_with_mapping"]
        total_img = report["shopify"]["metaobjects_with_image"]
        map_ok    = report["source"]["mapped_products"]
        map_total = report["source"]["unique_products_in_links"]

        def dash_line(label: str, part: int, total: int) -> None:
            bar = progress_bar(part, total, width=20)
            logger.info("║  %-18s %s  ║", label, bar)

        dash_line("METAOBJECTS", total_mo, total_src)
        dash_line("LIAISONS", total_lnk, possible)
        dash_line("IMAGES", total_img, total_mo)
        dash_line("MAPPING PRODUITS", map_ok, map_total)
    else:
        map_ok    = report["source"]["mapped_products"]
        map_total = report["source"]["unique_products_in_links"]
        cache_ok  = report["cache"]["import_state_real_gids"]
        total_src = report["source"]["unique_designers"]
        link_ok   = report["cache"]["link_state_ok"]
        possible  = report["source"]["links_with_mapping"]

        def dash_line(label: str, part: int, total: int) -> None:
            bar = progress_bar(part, total, width=20)
            logger.info("║  %-18s %s  ║", label, bar)

        dash_line("CACHE DESIGNERS", cache_ok, total_src)
        dash_line("CACHE LIENS", link_ok, possible)
        dash_line("MAPPING PRODUITS", map_ok, map_total)

    logger.info("╚══════════════════════════════════════════════════════════════╝")

    # ── 6. RECOMMANDATIONS ────────────────────────────────────────────────────
    section("6. RECOMMANDATIONS")

    actions: list[str] = []

    if not no_api:
        stale_count   = len(stale_in_cache)
        missing_count = report["shopify"]["missing_in_shopify"]
        remaining_lnk = possible - report["shopify"]["products_linked"]
        no_img_count  = report["shopify"]["metaobjects_without_image"]
        no_lnk_mo     = report["shopify"]["metaobjects_without_link"]

        if stale_count:
            actions.append(f"⚠  {stale_count} GID(s) obsolètes → import les recrée automatiquement")
        if missing_count or remaining_lnk:
            actions.append("▶  python import_designers_to_shopify.py --global-import --no-dry-run")
        if no_img_count:
            actions.append(f"▶  python upload_images_to_shopify.py  ({no_img_count} images manquantes)")
        if no_lnk_mo:
            actions.append(f"ℹ  {no_lnk_mo} metaobject(s) sans produit lié (mapping absent ou produit retiré)")
        if links_without_mapping:
            actions.append(f"ℹ  {links_without_mapping} liens impossibles (produits non mappés dans Shopify)")
        if not stale_count and not missing_count and remaining_lnk == 0:
            actions.append("✓  Migration complète — aucune action requise.")
    else:
        if report["cache"]["import_state_dry_run"]:
            actions.append(f"⚠  {report['cache']['import_state_dry_run']} entrées dry-run → relancez avec --no-dry-run")
        if designers_not_cached:
            actions.append(f"⚠  {designers_not_cached} designer(s) pas encore importés")
        actions.append("ℹ  Lancez sans --no-api pour l'état réel Shopify")

    for a in actions:
        logger.info("  %s", a)

    logger.info("")
    logger.info("  Audit terminé : %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # ── JSON ──────────────────────────────────────────────────────────────────
    if output_json:
        json_path = output_dir / "audit_report.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        logger.info("  Rapport JSON : %s", json_path)

    return report


# ── Entrypoint ────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit complet de la migration Designer Wee → Shopify"
    )
    parser.add_argument(
        "--no-api",
        action="store_true",
        help="CSV/cache uniquement — pas d'appel Shopify (rapide)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="output_json",
        help="Export JSON dans output/audit_report.json",
    )
    return parser.parse_args()


def main() -> None:
    setup_logging()
    args = parse_args()
    run_audit(no_api=args.no_api, output_json=args.output_json)


if __name__ == "__main__":
    main()
