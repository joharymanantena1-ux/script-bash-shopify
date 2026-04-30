"""
upload_images_to_shopify.py — Google Drive → Shopify Files (script autonome)

Utile pour pré-uploader toutes les images en batch AVANT l'import Designer.
L'import Designer (import_designers_to_shopify.py) intègre lui-même la même
logique de résolution image : ce script est optionnel mais pratique pour
traiter les images en avance et pré-remplir image_gid_map.csv.

Clé de cache : str(image_id) — identique à import_designers_to_shopify.py.
Nom Drive  : {image_id}.{image_ext} (ex: 197526.jpg).

Usage :
  python upload_images_to_shopify.py           # tous les designers du CSV
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
    parser.add_argument("--test", action="store_true", help="Traite uniquement les images du produit de test")
    parser.add_argument("--dry-run", action="store_true", dest="dry_run", help="Simule sans écrire dans Shopify")
    return parser.parse_args()


def main() -> None:
    setup_logging()
    args = parse_args()

    output_dir = config.OUTPUT_DIR
    designers_csv = output_dir / "designers.csv"
    links_csv = output_dir / "product_designer_links.csv"
    gid_map_csv = output_dir / "image_gid_map.csv"

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

    # Clé de cache = str(image_id) — même convention que import_designers_to_shopify.py
    # Nom Drive    = {image_id}.{image_ext}
    unique_id_to_filename: dict[str, str] = {}
    for d in all_designers:
        image_id = str(d.get("image_id", "")).strip()
        image_ext = (d.get("image_ext", "") or "jpg").strip()
        if image_id:
            unique_id_to_filename[image_id] = f"{image_id}.{image_ext}"

    logger.info("Images uniques à traiter : %d", len(unique_id_to_filename))

    # Carte existante (clé = image_id)
    gid_map = load_existing_map(gid_map_csv)
    sources: dict[str, str] = {}
    to_process = {k: v for k, v in unique_id_to_filename.items() if k not in gid_map}
    logger.info("Déjà dans le cache : %d  |  À traiter : %d",
                len(unique_id_to_filename) - len(to_process), len(to_process))

    if not to_process:
        logger.info("Toutes les images sont déjà en cache.")
        return

    # Connexion Google Drive
    drive = google_drive.get_drive_service()
    if not drive:
        logger.error("Google Drive non disponible — vérifiez credential-regardbeauty.json")
        sys.exit(1)
    folder_id = google_drive.find_folder_id(drive, config.GOOGLE_DRIVE_FOLDER_NAME)

    client = ShopifyClient(dry_run=args.dry_run)
    if args.dry_run:
        logger.info("[DRY-RUN] fileCreate désactivé.")

    ok_count = err_count = nf_count = 0

    for image_id, filename in sorted(to_process.items()):
        logger.info("Traitement : %s", filename)

        if args.dry_run:
            found = google_drive.find_file(drive, filename, folder_id)
            if found:
                logger.info("  [DRY-RUN] Trouvée dans Drive — upload simulé.")
                ok_count += 1
            else:
                logger.warning("  [DRY-RUN] Introuvable dans Drive : %s", filename)
                nf_count += 1
            continue

        # Téléchargement Drive
        file_bytes = google_drive.download_file(drive, filename, folder_id)
        if not file_bytes:
            logger.warning("  Introuvable dans Drive : %s", filename)
            nf_count += 1
            continue

        logger.info("  Drive : %d Ko téléchargés", len(file_bytes) // 1024)

        # Upload Shopify
        try:
            gid = client.upload_image_from_bytes(filename, file_bytes)
        except ShopifyGraphQLError as e:
            logger.error("  Erreur Shopify : %s", e)
            err_count += 1
            continue

        if gid:
            gid_map[image_id] = gid
            sources[image_id] = "google_drive"
            logger.info("  GID Shopify : %s", gid)
            ok_count += 1
        else:
            err_count += 1

    # Attendre READY
    if not args.dry_run:
        ready_gids = [v for k, v in gid_map.items() if sources.get(k) == "google_drive"]
        if ready_gids:
            logger.info("Attente Shopify Files READY (%d fichier(s))...", len(ready_gids))
            time.sleep(3)
            for gid in ready_gids:
                client.wait_for_file_ready(gid, max_attempts=8, delay=2.0)

    # Conserver les entrées existantes
    for key, gid in load_existing_map(gid_map_csv).items():
        if key not in gid_map:
            gid_map[key] = gid

    save_map(gid_map, sources, gid_map_csv)

    logger.info("=" * 60)
    logger.info("RÉSUMÉ : OK=%d  Erreurs=%d  Introuvables=%d", ok_count, err_count, nf_count)
    if ok_count:
        logger.info("Prochaine étape : python import_designers_to_shopify.py --test --no-dry-run")


if __name__ == "__main__":
    main()
