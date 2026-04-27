"""
export_designers_to_csv.py — Étape 1 : MariaDB → CSV

Génère deux fichiers dans le dossier output/ :
  - designers.csv              : tous les designers (une ligne par designer × langue)
  - product_designer_links.csv : liaisons produit ↔ designer

Modes :
  --test          Export filtré sur TEST_PRODUCT_ID / TEST_PRODUCT_SKU / TEST_PRODUCT_HANDLE
  --global-export Export complet de tous les designers et toutes les liaisons

Usage :
  python export_designers_to_csv.py --test
  python export_designers_to_csv.py --global-export
"""

import argparse
import csv
import logging
import sys
from datetime import datetime
from pathlib import Path

import config
from db import get_connection, fetch_all

# ── Logging ──────────────────────────────────────────────────────────────────

def setup_logging() -> None:
    logs_dir = Path(__file__).parent / "logs"
    logs_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"export_{timestamp}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    logging.info("Log export : %s", log_file)


logger = logging.getLogger(__name__)

# ── Requêtes SQL ─────────────────────────────────────────────────────────────

# Tous les designers (toutes langues)
SQL_DESIGNERS_ALL = """
SELECT
    prn.id          AS wee_designer_id,
    prnt.name       AS nom,
    prnt.baseline   AS baseline,
    prnt.intro      AS introduction,
    prnt.body       AS texte,
    prnt.slug       AS slug,
    CONCAT(img.signature, '.', img.extension) AS image_file,
    img.id          AS image_id,
    img.extension   AS image_ext,
    prn.color       AS couleur,
    t.locale        AS langue
FROM product_refs_nested prn
LEFT JOIN product_refs_nested_trans prnt ON prnt.product_refs_nested_id = prn.id
LEFT JOIN image img                       ON img.id = prn.image_id
LEFT JOIN trans t                         ON t.id  = prnt.trans_id
WHERE prn.product_ref_type_id = ?
ORDER BY prn.id, t.locale
"""

# Designers liés à un produit Wee spécifique (par product_id)
SQL_DESIGNERS_FOR_PRODUCT = """
SELECT DISTINCT
    prn.id          AS wee_designer_id,
    prnt.name       AS nom,
    prnt.baseline   AS baseline,
    prnt.intro      AS introduction,
    prnt.body       AS texte,
    prnt.slug       AS slug,
    CONCAT(img.signature, '.', img.extension) AS image_file,
    img.id          AS image_id,
    img.extension   AS image_ext,
    prn.color       AS couleur,
    t.locale        AS langue
FROM product_refs_nested prn
LEFT JOIN product_refs_nested_trans prnt ON prnt.product_refs_nested_id = prn.id
LEFT JOIN image img                       ON img.id = prn.image_id
LEFT JOIN trans t                         ON t.id  = prnt.trans_id
INNER JOIN product_refs_enumerable_value prev
    ON prev.product_refs_nested_id = prn.id
   AND prev.product_ref_type_id_for_unicity = ?
WHERE prn.product_ref_type_id = ?
  AND prev.product_id = ?
ORDER BY prn.id, t.locale
"""

# Toutes les liaisons produit ↔ designer (langue par défaut uniquement)
SQL_LINKS_ALL = """
SELECT
    prev.product_id,
    prev.product_refs_nested_id AS wee_designer_id,
    prnt.name                   AS designer_nom
FROM product_refs_enumerable_value prev
JOIN product_refs_nested_trans prnt
    ON prnt.product_refs_nested_id = prev.product_refs_nested_id
   AND prnt.trans_id = ?
WHERE prev.product_ref_type_id_for_unicity = ?
ORDER BY prev.product_id
"""

# Liaisons pour un seul produit Wee
SQL_LINKS_FOR_PRODUCT = """
SELECT
    prev.product_id,
    prev.product_refs_nested_id AS wee_designer_id,
    prnt.name                   AS designer_nom
FROM product_refs_enumerable_value prev
JOIN product_refs_nested_trans prnt
    ON prnt.product_refs_nested_id = prev.product_refs_nested_id
   AND prnt.trans_id = ?
WHERE prev.product_ref_type_id_for_unicity = ?
  AND prev.product_id = ?
ORDER BY prev.product_id
"""

# ── Colonnes attendues dans les CSV ──────────────────────────────────────────

DESIGNERS_COLUMNS = [
    "wee_designer_id", "nom", "baseline", "introduction",
    "texte", "slug", "image_file", "image_id", "image_ext", "couleur", "langue",
]

LINKS_COLUMNS = ["product_id", "wee_designer_id", "designer_nom"]

# ── Écriture CSV ─────────────────────────────────────────────────────────────

def write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    """Écrit une liste de dicts dans un fichier CSV UTF-8 avec en-têtes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    logger.info("CSV écrit : %s (%d lignes)", path, len(rows))


def deduplicate(rows: list[dict], key_fields: list[str]) -> list[dict]:
    """Supprime les doublons basés sur une combinaison de champs clés."""
    seen = set()
    result = []
    for row in rows:
        key = tuple(row.get(f) for f in key_fields)
        if key not in seen:
            seen.add(key)
            result.append(row)
    removed = len(rows) - len(result)
    if removed:
        logger.warning("%d doublon(s) supprimé(s) (clé : %s)", removed, key_fields)
    return result

# ── Export principal ─────────────────────────────────────────────────────────

def export(test_mode: bool) -> None:
    output_dir = config.OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    designers_path = output_dir / "designers.csv"
    links_path = output_dir / "product_designer_links.csv"

    if test_mode:
        # Vérification : au moins un critère de test doit être renseigné
        if not config.TEST_PRODUCT_ID:
            logger.error(
                "Mode test activé mais TEST_PRODUCT_ID est vide dans .env.\n"
                "Renseignez TEST_PRODUCT_ID, TEST_PRODUCT_SKU ou TEST_PRODUCT_HANDLE."
            )
            sys.exit(1)

        product_id = int(config.TEST_PRODUCT_ID)
        logger.info("=== MODE TEST — product_id Wee : %d ===", product_id)

        with get_connection() as conn:
            designers = fetch_all(
                conn,
                SQL_DESIGNERS_FOR_PRODUCT,
                (config.DESIGNER_TYPE_ID, config.DESIGNER_TYPE_ID, product_id),
            )
            links = fetch_all(
                conn,
                SQL_LINKS_FOR_PRODUCT,
                (config.DEFAULT_TRANS_ID, config.DESIGNER_TYPE_ID, product_id),
            )
    else:
        logger.info("=== MODE GLOBAL — export complet ===")
        with get_connection() as conn:
            designers = fetch_all(
                conn,
                SQL_DESIGNERS_ALL,
                (config.DESIGNER_TYPE_ID,),
            )
            links = fetch_all(
                conn,
                SQL_LINKS_ALL,
                (config.DEFAULT_TRANS_ID, config.DESIGNER_TYPE_ID),
            )

    # Déduplication sur (wee_designer_id, langue) pour les designers
    designers = deduplicate(designers, ["wee_designer_id", "langue"])
    # Déduplication sur (product_id, wee_designer_id) pour les liens
    links = deduplicate(links, ["product_id", "wee_designer_id"])

    logger.info("Designers trouvés : %d lignes", len(designers))
    logger.info("Liaisons produit <-> designer : %d lignes", len(links))

    if not designers:
        logger.warning("Aucun designer trouvé — CSV vides générés.")

    write_csv(designers_path, DESIGNERS_COLUMNS, designers)
    write_csv(links_path, LINKS_COLUMNS, links)

    logger.info("Export terminé. Vérifiez : %s", output_dir.resolve())


# ── Entrypoint ───────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export des designers MariaDB vers CSV"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--test",
        action="store_true",
        help="Export filtré sur le produit de test (TEST_PRODUCT_ID dans .env)",
    )
    group.add_argument(
        "--global-export",
        action="store_true",
        dest="global_export",
        help="Export complet de tous les designers",
    )
    return parser.parse_args()


if __name__ == "__main__":
    setup_logging()
    args = parse_args()
    export(test_mode=args.test)
