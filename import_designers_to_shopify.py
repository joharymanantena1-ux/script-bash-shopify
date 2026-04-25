"""
import_designers_to_shopify.py — Étape 2 : CSV → Shopify

Lit designers.csv et product_designer_links.csv puis :
  1. Crée ou met à jour les metaobjects 'designer' dans Shopify
  2. Lie chaque produit Shopify à son designer via un metafield

Modes :
  --test           Traite uniquement le produit de test (DEFAULT)
  --global-import  Traite tous les produits (à activer volontairement)
  --dry-run        Aucune écriture Shopify (simulé) — ACTIVÉ PAR DÉFAUT via .env
  --no-dry-run     Désactive le dry-run (écriture réelle — IRRÉVERSIBLE)

Usage :
  python import_designers_to_shopify.py --test
  python import_designers_to_shopify.py --test --no-dry-run
  python import_designers_to_shopify.py --global-import --dry-run
  python import_designers_to_shopify.py --global-import --no-dry-run
"""

import argparse
import csv
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import config
from shopify_client import ShopifyClient, ShopifyGraphQLError

# ── Logging ──────────────────────────────────────────────────────────────────

def setup_logging() -> None:
    logs_dir = Path(__file__).parent / "logs"
    logs_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"import_{timestamp}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    logging.info("Log import : %s", log_file)


logger = logging.getLogger(__name__)

# ── Lecture CSV ───────────────────────────────────────────────────────────────

def read_csv(path: Path) -> list[dict]:
    """Lit un CSV UTF-8 et retourne une liste de dicts."""
    if not path.exists():
        logger.error("Fichier CSV introuvable : %s", path)
        sys.exit(1)
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


# ── Mapping produit Wee → Shopify ─────────────────────────────────────────────

def resolve_shopify_product_gid(client: ShopifyClient, wee_product_id: str) -> str | None:
    """
    Trouve le GID Shopify d'un produit à partir de son ID Wee.

    Ordre de recherche :
      1. TEST_PRODUCT_SKU    (si renseigné dans .env)
      2. TEST_PRODUCT_HANDLE (si renseigné dans .env)
      3. Metafield custom.wee_product_id (non implémenté — voir CLAUDE.md)

    Pour l'import global, vous devrez maintenir un CSV de mapping
    wee_product_id ↔ shopify_gid (voir section "Mapping" dans CLAUDE.md).
    """
    # En mode test, on utilise les critères du .env
    if config.TEST_PRODUCT_SKU:
        return client.find_product_by_sku(config.TEST_PRODUCT_SKU)

    if config.TEST_PRODUCT_HANDLE:
        return client.find_product_by_handle(config.TEST_PRODUCT_HANDLE)

    # Fallback : recherche par wee_product_id (non implémenté)
    logger.warning(
        "Impossible de résoudre le produit Shopify pour wee_product_id=%s. "
        "Renseignez TEST_PRODUCT_SKU ou TEST_PRODUCT_HANDLE dans .env.",
        wee_product_id,
    )
    return None


# ── Rapport CSV ───────────────────────────────────────────────────────────────

class ImportReport:
    """Accumule les résultats et génère import_report.csv."""

    COLUMNS = [
        "wee_designer_id", "langue", "action_metaobject", "shopify_metaobject_gid",
        "wee_product_id", "shopify_product_gid", "action_metafield", "statut", "message",
    ]

    def __init__(self, output_dir: Path) -> None:
        self._rows: list[dict] = []
        self._path = output_dir / "import_report.csv"

    def add(self, **kwargs: Any) -> None:
        self._rows.append({col: kwargs.get(col, "") for col in self.COLUMNS})

    def save(self) -> None:
        with open(self._path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.COLUMNS)
            writer.writeheader()
            writer.writerows(self._rows)
        logger.info("Rapport généré : %s (%d lignes)", self._path, len(self._rows))

    def summary(self) -> None:
        created = sum(1 for r in self._rows if r["action_metaobject"] == "created")
        updated = sum(1 for r in self._rows if r["action_metaobject"] == "updated")
        skipped = sum(1 for r in self._rows if r["statut"] == "skipped")
        errors = sum(1 for r in self._rows if r["statut"] == "error")
        mf_set = sum(1 for r in self._rows if r["action_metafield"] in ("created", "updated"))
        missing = sum(1 for r in self._rows if r["action_metafield"] == "mapping_missing")

        logger.info("=" * 60)
        logger.info("RÉSUMÉ DE L'IMPORT")
        logger.info("  Metaobjects créés      : %d", created)
        logger.info("  Metaobjects mis à jour : %d", updated)
        logger.info("  Ignorés (déjà OK)      : %d", skipped)
        logger.info("  Erreurs                : %d", errors)
        logger.info("  Metafields liés        : %d", mf_set)
        logger.info("  Produits sans mapping  : %d", missing)
        logger.info("=" * 60)


# ── Conversion CSV → champs Shopify ──────────────────────────────────────────

def designer_row_to_fields(row: dict) -> dict[str, str]:
    """
    Convertit une ligne de designers.csv en dict de champs metaobject Shopify.

    Mapping vers la définition existante dans Shopify :
      nom            (single_line_text_field)
      baseline       (multi_line_text_field)
      introduction   (multi_line_text_field)
      texte          (multi_line_text_field)
      image          (file_reference) — NON géré ici, upload séparé requis
      wee_designer_id (single_line_text_field)

    Champs ignorés (absents de la définition Shopify actuelle) :
      slug, couleur, langue — à ajouter à la définition si besoin.
    """
    return {
        "wee_designer_id": str(row.get("wee_designer_id", "")),
        "nom":             row.get("nom", ""),
        "baseline":        row.get("baseline", ""),
        "introduction":    row.get("introduction", ""),
        "texte":           row.get("texte", ""),
        # image ignorée : type file_reference, nécessite un GID de fichier uploadé
    }


# ── Logique principale ────────────────────────────────────────────────────────

def import_designers(
    designers: list[dict],
    links: list[dict],
    client: ShopifyClient,
    report: ImportReport,
    test_product_id: str | None,
) -> dict[str, str]:
    """
    Phase 1 : crée ou met à jour les metaobjects designer dans Shopify.
    Retourne un dict {wee_designer_id → shopify_metaobject_gid}.
    """
    # En mode test, on ne traite que les designers liés au produit de test
    if test_product_id:
        relevant_ids = {
            str(link["wee_designer_id"])
            for link in links
            if str(link["product_id"]) == test_product_id
        }
        designers = [d for d in designers if str(d["wee_designer_id"]) in relevant_ids]
        logger.info("Mode test : %d designer(s) à traiter pour product_id=%s", len(designers), test_product_id)

    # On regroupe par wee_designer_id (une entrée par langue → on prend la FR par défaut)
    # Pour une migration multilingue, dupliquer les metaobjects ou utiliser les Translations API.
    seen_ids: set[str] = set()
    id_to_gid: dict[str, str] = {}

    for row in designers:
        wee_id = str(row.get("wee_designer_id", ""))
        langue = row.get("langue", "")

        # Ne créer qu'un seul metaobject par designer (on prend la première occurrence = FR)
        if wee_id in seen_ids:
            logger.debug("Designer %s (%s) déjà traité — ignoré.", wee_id, langue)
            continue
        seen_ids.add(wee_id)

        fields = designer_row_to_fields(row)
        action = "skipped"
        gid = None

        try:
            existing_gid = client.find_metaobject_by_wee_id(wee_id)

            if existing_gid:
                action = "updated"
                gid = existing_gid
                client.update_metaobject(existing_gid, fields)
            else:
                action = "created"
                gid = client.create_metaobject(fields)
                # En dry-run, gid est None — on utilise un placeholder pour le rapport
                if gid is None:
                    gid = f"[DRY-RUN-{wee_id}]"

            id_to_gid[wee_id] = gid
            report.add(
                wee_designer_id=wee_id,
                langue=langue,
                action_metaobject=action,
                shopify_metaobject_gid=gid or "",
                statut="ok",
            )

        except ShopifyGraphQLError as e:
            logger.error("Erreur metaobject designer %s : %s", wee_id, e)
            report.add(
                wee_designer_id=wee_id,
                langue=langue,
                action_metaobject="error",
                statut="error",
                message=str(e),
            )

    return id_to_gid


def link_products(
    links: list[dict],
    id_to_gid: dict[str, str],
    client: ShopifyClient,
    report: ImportReport,
    test_product_id: str | None,
) -> None:
    """
    Phase 2 : lie chaque produit Shopify à son metaobject designer via metafield.
    """
    if test_product_id:
        links = [l for l in links if str(l["product_id"]) == test_product_id]
        logger.info("Mode test : %d lien(s) à traiter", len(links))

    for link in links:
        wee_product_id = str(link["product_id"])
        wee_designer_id = str(link["wee_designer_id"])
        designer_nom = link.get("designer_nom", "")

        metaobject_gid = id_to_gid.get(wee_designer_id)
        if not metaobject_gid:
            logger.warning(
                "Metaobject GID introuvable pour designer %s — lien produit %s ignoré.",
                wee_designer_id, wee_product_id,
            )
            report.add(
                wee_designer_id=wee_designer_id,
                wee_product_id=wee_product_id,
                action_metafield="skipped",
                statut="skipped",
                message=f"metaobject_gid manquant pour designer {wee_designer_id}",
            )
            continue

        # Résolution du GID Shopify du produit
        product_gid = resolve_shopify_product_gid(client, wee_product_id)
        if not product_gid:
            logger.warning(
                "Produit Shopify introuvable pour wee_product_id=%s (designer : %s)",
                wee_product_id, designer_nom,
            )
            report.add(
                wee_designer_id=wee_designer_id,
                wee_product_id=wee_product_id,
                action_metafield="mapping_missing",
                statut="skipped",
                message="product mapping missing",
            )
            continue

        try:
            existing_mf = client.get_product_metafield(product_gid, "custom", "designer")
            action = "updated" if existing_mf else "created"

            client.set_product_metafield(
                product_gid=product_gid,
                namespace="custom",
                key="designer",
                value=metaobject_gid,
                metafield_type="metaobject_reference",
            )

            report.add(
                wee_designer_id=wee_designer_id,
                wee_product_id=wee_product_id,
                shopify_product_gid=product_gid,
                shopify_metaobject_gid=metaobject_gid,
                action_metafield=action,
                statut="ok",
            )

        except ShopifyGraphQLError as e:
            logger.error(
                "Erreur metafield produit %s <-> designer %s : %s",
                wee_product_id, wee_designer_id, e,
            )
            report.add(
                wee_designer_id=wee_designer_id,
                wee_product_id=wee_product_id,
                shopify_product_gid=product_gid or "",
                action_metafield="error",
                statut="error",
                message=str(e),
            )


# ── Entrypoint ────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import des designers CSV vers Shopify"
    )

    # Périmètre
    scope = parser.add_mutually_exclusive_group(required=False)
    scope.add_argument(
        "--test",
        action="store_true",
        default=True,
        help="Traite uniquement le produit de test (DEFAULT)",
    )
    scope.add_argument(
        "--global-import",
        action="store_true",
        dest="global_import",
        help="Traite tous les produits (action volontaire)",
    )

    # Dry-run
    dry = parser.add_mutually_exclusive_group()
    dry.add_argument(
        "--dry-run",
        action="store_true",
        default=None,
        dest="dry_run_flag",
        help="Désactive l'écriture Shopify (simulation)",
    )
    dry.add_argument(
        "--no-dry-run",
        action="store_false",
        dest="dry_run_flag",
        help="Active l'écriture réelle Shopify (IRRÉVERSIBLE)",
    )

    return parser.parse_args()


def main() -> None:
    setup_logging()
    args = parse_args()

    # Le flag CLI surpasse le .env, le .env surpasse la valeur par défaut (True)
    if args.dry_run_flag is not None:
        dry_run = args.dry_run_flag
    else:
        dry_run = config.DRY_RUN

    global_mode = getattr(args, "global_import", False)
    test_mode = not global_mode

    if dry_run:
        logger.info(">>> MODE DRY-RUN ACTIF — aucune écriture Shopify <<<")
    else:
        logger.warning(">>> MODE ÉCRITURE RÉELLE SHOPIFY — les modifications sont permanentes <<<")

    output_dir = config.OUTPUT_DIR
    designers = read_csv(output_dir / "designers.csv")
    links = read_csv(output_dir / "product_designer_links.csv")
    logger.info("Designers chargés : %d lignes", len(designers))
    logger.info("Liens chargés     : %d lignes", len(links))

    client = ShopifyClient(dry_run=dry_run)
    report = ImportReport(output_dir)

    # Détermination du product_id de test
    test_product_id = config.TEST_PRODUCT_ID if test_mode else None

    if test_mode and not test_product_id:
        logger.error(
            "Mode test actif mais TEST_PRODUCT_ID absent du .env.\n"
            "Utilisez --global-import pour traiter tous les produits."
        )
        sys.exit(1)

    logger.info("Périmètre : %s", f"test (product_id={test_product_id})" if test_mode else "global")

    # Phase 1 — Metaobjects
    logger.info("--- Phase 1 : Import des metaobjects Designer ---")
    id_to_gid = import_designers(designers, links, client, report, test_product_id)

    # Phase 2 — Metafields produit
    logger.info("--- Phase 2 : Liaison produits <-> Designer ---")
    link_products(links, id_to_gid, client, report, test_product_id)

    # Rapport final
    report.save()
    report.summary()


if __name__ == "__main__":
    main()
