"""
cleanup_shopify.py — Supprime toutes les entries metaobject 'designer' dans Shopify.

NE supprime PAS la définition du type (le schéma `designer` reste intact).
Supprime uniquement les entries (instances) créées par import_designers_to_shopify.py.

Usage :
  python cleanup_shopify.py --dry-run     # liste sans supprimer
  python cleanup_shopify.py --no-dry-run  # suppression réelle (IRRÉVERSIBLE)
"""

import argparse
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
    log_file = logs_dir / f"cleanup_{ts}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    logging.info("Log cleanup : %s", log_file)


logger = logging.getLogger(__name__)


# ── Entrypoint ────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Supprime toutes les entries metaobject designer depuis Shopify"
    )
    dry = parser.add_mutually_exclusive_group(required=True)
    dry.add_argument(
        "--dry-run",
        action="store_true",
        dest="dry_run",
        help="Liste les entries sans les supprimer",
    )
    dry.add_argument(
        "--no-dry-run",
        action="store_false",
        dest="dry_run",
        help="Suppression réelle — IRRÉVERSIBLE",
    )
    return parser.parse_args()


def main() -> None:
    setup_logging()
    args = parse_args()

    if not args.dry_run:
        logger.warning(
            ">>> MODE SUPPRESSION RÉELLE — toutes les entries 'designer' vont être supprimées <<<\n"
            "    La définition du type (schéma) sera conservée.\n"
            "    Cette action est IRRÉVERSIBLE."
        )

    client = ShopifyClient(dry_run=args.dry_run)

    logger.info("Récupération de toutes les entries metaobject 'designer' depuis Shopify...")
    try:
        gids = client.list_all_designer_metaobjects()
    except ShopifyGraphQLError as e:
        logger.error("Impossible de lister les metaobjects : %s", e)
        sys.exit(1)

    total = len(gids)
    logger.info("Entries trouvées : %d", total)

    if total == 0:
        logger.info("Aucune entry à supprimer. Terminé.")
        return

    if args.dry_run:
        logger.info("[DRY-RUN] %d entry(s) seraient supprimées :", total)
        for gid in gids[:20]:
            logger.info("  %s", gid)
        if total > 20:
            logger.info("  ... et %d autres.", total - 20)
        logger.info("[DRY-RUN] Relancez avec --no-dry-run pour effectuer la suppression.")
        return

    deleted = 0
    errors = 0
    for i, gid in enumerate(gids, 1):
        try:
            ok = client.delete_metaobject(gid)
            if ok:
                deleted += 1
                logger.info("[%d/%d] Supprimé : %s", i, total, gid)
            else:
                logger.warning("[%d/%d] Réponse vide pour : %s", i, total, gid)
                errors += 1
        except ShopifyGraphQLError as e:
            logger.error("[%d/%d] Erreur suppression %s : %s", i, total, gid, e)
            errors += 1

    logger.info("=" * 60)
    logger.info("RÉSUMÉ : Supprimés=%d  Erreurs=%d  Total=%d", deleted, errors, total)
    if deleted == total:
        logger.info("Toutes les entries ont été supprimées.")
        logger.info("Prochaine étape : python import_designers_to_shopify.py --reset-cache")
    else:
        logger.warning(
            "%d entry(s) non supprimées — relancez le script pour réessayer.", errors
        )


if __name__ == "__main__":
    main()
