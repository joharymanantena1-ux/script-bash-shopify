"""
upload_images_to_shopify.py — Google Drive → Shopify Files (script autonome)

Utile pour pré-uploader toutes les images en batch AVANT l'import Designer.
Scan récursif de tous les sous-dossiers Drive (ex: master/0000/) avec
correspondance exacte puis fuzzy (autre ext, préfixe) pour maximiser le taux
de trouvailles.

Clé de cache : str(image_id) — identique à import_designers_to_shopify.py.

Usage :
  python upload_images_to_shopify.py           # tous les designers
  python upload_images_to_shopify.py --test    # uniquement le produit de test
  python upload_images_to_shopify.py --dry-run # simule sans écrire dans Shopify
"""

import argparse
import csv
import logging
import sys
import time
from pathlib import Path

import config
from shopify_client import ShopifyClient, ShopifyGraphQLError
import google_drive

# ── Logging ───────────────────────────────────────────────────────────────────

def setup_logging() -> None:
    logs_dir = Path(__file__).parent / "logs"
    logs_dir.mkdir(exist_ok=True)
    from datetime import datetime
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"upload_images_{ts}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    logging.info("Log upload images : %s", log_file)

logger = logging.getLogger(__name__)

# ── image_gid_map CSV ─────────────────────────────────────────────────────────

_MAP_COLUMNS = ["image_file", "shopify_gid", "source"]


def load_existing_map(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return {
            row["image_file"]: row["shopify_gid"]
            for row in csv.DictReader(f)
            if row.get("shopify_gid") and not row["shopify_gid"].startswith("[DRY-RUN")
        }


def save_map(gid_map: dict[str, str], sources: dict[str, str], path: Path) -> None:
    rows = [{"image_file": k, "shopify_gid": v, "source": sources.get(k, "")} for k, v in gid_map.items()]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_MAP_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    logger.info("image_gid_map.csv sauvegardé : %d entrée(s)", len(rows))


# ── Entrypoint ────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pré-upload des images designers vers Shopify Files")
    parser.add_argument("--test",    action="store_true", help="Traite uniquement les images du produit de test")
    parser.add_argument("--dry-run", action="store_true", dest="dry_run", help="Simule sans écrire dans Shopify")
    return parser.parse_args()


def main() -> None:
    setup_logging()
    args = parse_args()

    output_dir    = config.OUTPUT_DIR
    designers_csv = output_dir / "designers.csv"
    links_csv     = output_dir / "product_designer_links.csv"
    gid_map_csv   = output_dir / "image_gid_map.csv"

    for path in (designers_csv, links_csv):
        if not path.exists():
            logger.error("CSV introuvable : %s — lancez d'abord export_designers_to_csv.py", path)
            sys.exit(1)

    with open(designers_csv, "r", encoding="utf-8") as f:
        all_designers = list(csv.DictReader(f))
    with open(links_csv, "r", encoding="utf-8") as f:
        all_links = list(csv.DictReader(f))

    if args.test:
        if not config.TEST_PRODUCT_ID:
            logger.error("TEST_PRODUCT_ID absent du .env.")
            sys.exit(1)
        relevant_ids = {
            str(link["wee_designer_id"])
            for link in all_links
            if str(link["product_id"]) == config.TEST_PRODUCT_ID
        }
        all_designers = [d for d in all_designers if str(d["wee_designer_id"]) in relevant_ids]
        logger.info("Mode test : %d designer(s) pour product_id=%s", len(all_designers), config.TEST_PRODUCT_ID)

    # Clé de cache = str(image_id)
    unique_id_to_ext: dict[str, str] = {}
    for d in all_designers:
        image_id  = str(d.get("image_id", "")).strip()
        image_ext = (d.get("image_ext", "") or "jpg").strip()
        if image_id:
            unique_id_to_ext[image_id] = image_ext

    logger.info("Images uniques à traiter : %d", len(unique_id_to_ext))

    gid_map   = load_existing_map(gid_map_csv)
    sources: dict[str, str] = {}
    to_process = {k: v for k, v in unique_id_to_ext.items() if k not in gid_map}
    logger.info("Déjà dans le cache : %d  |  À traiter : %d",
                len(unique_id_to_ext) - len(to_process), len(to_process))

    if not to_process:
        logger.info("Toutes les images sont déjà en cache.")
        return

    # ── Connexion Google Drive ────────────────────────────────────────────────
    drive = google_drive.get_drive_service()
    if not drive:
        logger.error("Google Drive non disponible — vérifiez credential-regardbeauty.json")
        sys.exit(1)

    folder_id, drive_id = google_drive.find_folder_info(drive, config.GOOGLE_DRIVE_FOLDER_NAME)

    # Index fichiers (une passe sur le Shared Drive ou scan récursif MyDrive)
    logger.info("Construction de l'index Drive...")
    if folder_id:
        drive_index = google_drive.build_drive_index(drive, folder_id, drive_id=drive_id)
    else:
        logger.warning("Dossier Drive introuvable — recherche fichier par fichier (lent).")
        drive_index = {}

    client = ShopifyClient(dry_run=args.dry_run)
    if args.dry_run:
        logger.info("[DRY-RUN] fileCreate désactivé.")

    ok_count = err_count = nf_count = fuzzy_count = 0
    fuzzy_log: list[str] = []

    for image_id, image_ext in sorted(to_process.items()):
        canonical = f"{image_id}.{image_ext}"
        logger.info("Traitement : %s", canonical)

        # ── Résolution du fichier Drive via l'index ───────────────────────────
        if drive_index:
            file_id, matched_name = google_drive.find_in_index(drive_index, image_id, image_ext)
        else:
            # Fallback : recherche individuelle (sans index)
            file_id, matched_name = None, None
            found = google_drive.find_file(drive, canonical, folder_id)
            if found:
                file_id, matched_name = "search", canonical

        if not file_id:
            logger.warning("  Introuvable dans Drive (exact + fuzzy) : %s", canonical)
            nf_count += 1
            continue

        is_fuzzy = matched_name != canonical
        if is_fuzzy:
            logger.info("  Correspondance fuzzy : '%s' → '%s'", canonical, matched_name)
            fuzzy_log.append(f"{canonical} → {matched_name}")
            fuzzy_count += 1

        if args.dry_run:
            logger.info("  [DRY-RUN] Trouvée ('%s') — upload simulé.", matched_name)
            ok_count += 1
            continue

        # ── Téléchargement ───────────────────────────────────────────────────
        if file_id == "search":
            file_bytes = google_drive.download_file(drive, matched_name, folder_id)
        else:
            file_bytes = google_drive.download_by_id(drive, file_id)

        if not file_bytes:
            logger.warning("  Échec téléchargement : %s", matched_name)
            nf_count += 1
            continue

        logger.info("  Drive : %d Ko  ('%s')", len(file_bytes) // 1024, matched_name)

        # ── Upload Shopify ───────────────────────────────────────────────────
        try:
            gid = client.upload_image_from_bytes(matched_name, file_bytes)
        except ShopifyGraphQLError as e:
            logger.error("  Erreur Shopify : %s", e)
            err_count += 1
            continue

        if gid:
            gid_map[image_id] = gid
            sources[image_id] = "google_drive" + ("_fuzzy" if is_fuzzy else "")
            logger.info("  GID Shopify : %s", gid)
            ok_count += 1
        else:
            err_count += 1

    # ── Attendre READY ────────────────────────────────────────────────────────
    if not args.dry_run:
        new_gids = [gid_map[k] for k in to_process if k in gid_map]
        if new_gids:
            logger.info("Attente Shopify Files READY (%d fichier(s))...", len(new_gids))
            time.sleep(3)
            for gid in new_gids:
                client.wait_for_file_ready(gid, max_attempts=8, delay=2.0)

    # Fusionner avec le cache existant
    for key, gid in load_existing_map(gid_map_csv).items():
        if key not in gid_map:
            gid_map[key] = gid

    save_map(gid_map, sources, gid_map_csv)

    logger.info("=" * 60)
    logger.info("RÉSUMÉ : OK=%d  Fuzzy=%d  Erreurs=%d  Introuvables=%d",
                ok_count, fuzzy_count, err_count, nf_count)

    if fuzzy_log:
        logger.info("Correspondances fuzzy utilisées (%d) :", len(fuzzy_log))
        for line in fuzzy_log:
            logger.info("  %s", line)

    if ok_count:
        logger.info("Prochaine étape : python import_designers_to_shopify.py --global-import --no-dry-run")


if __name__ == "__main__":
    main()
