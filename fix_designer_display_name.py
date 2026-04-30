"""
fix_designer_display_name.py — Configure le champ 'name' comme nom d'affichage
des entries metaobject Designer dans Shopify Admin.

Sans ce fix, les entries s'affichent comme 'Designer #HDDKR2BR'.
Après ce fix, elles s'affichent avec le vrai nom du designer.

Usage :
  python fix_designer_display_name.py --dry-run   # vérifie sans modifier
  python fix_designer_display_name.py             # applique le fix
"""

import argparse
import logging
import sys
from pathlib import Path

import config
from shopify_client import ShopifyClient

def setup_logging() -> None:
    logs_dir = Path(__file__).parent / "logs"
    logs_dir.mkdir(exist_ok=True)
    from datetime import datetime
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"fix_display_name_{ts}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Configure le champ 'name' comme displayNameField du metaobject designer"
    )
    parser.add_argument("--dry-run", action="store_true", help="Vérifie sans modifier Shopify")
    return parser.parse_args()


def main() -> None:
    setup_logging()
    args = parse_args()

    client = ShopifyClient(dry_run=args.dry_run)

    logger.info("Mise à jour du displayNameField du metaobject 'designer' → champ 'name'...")
    ok = client.set_metaobject_display_name_field("designer", "name")

    if ok:
        logger.info("Fix appliqué. Les entries s'affichent maintenant avec le nom du designer.")
        logger.info("Rechargez Shopify Admin > Content > Metaobjects > Designer pour vérifier.")
    else:
        logger.error("Fix échoué — vérifiez les logs ci-dessus.")
        sys.exit(1)


if __name__ == "__main__":
    main()
