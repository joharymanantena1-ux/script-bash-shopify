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

def designer_row_to_fields(row: dict, image_gid: str | None = None) -> dict[str, str]:
    """
    Convertit une ligne de designers.csv en dict de champs metaobject Shopify.

    Mapping CSV → clé Shopify :
      nom       → name            (single_line_text_field, requis)
      texte     → body            (multi_line_text_field)
      couleur   → color           (single_line_text_field)
      langue    → locale          (single_line_text_field, normalisé fr_FR→fr-FR)
      image_gid → image           (file_reference, GID résolu séparément)
    """
    locale_raw = row.get("langue", "")
    locale = locale_raw.replace("_", "-") if locale_raw else ""

    fields = {
        "wee_designer_id": str(row.get("wee_designer_id", "")),
        "name":            row.get("nom", ""),
        "baseline":        row.get("baseline", ""),
        "introduction":    row.get("introduction", ""),
        "body":            row.get("texte", ""),
        "slug":            row.get("slug", ""),
        "image_file":      row.get("image_file", ""),
        "color":           row.get("couleur", ""),
        "locale":          locale,
    }
    if image_gid:
        fields["image"] = image_gid
    return fields


# ── Résolution d'image (intégrée dans l'import) ───────────────────────────────

_GID_MAP_COLUMNS = ["image_file", "shopify_gid", "source"]

def _file_hash_path(image_id: int, extension: str) -> str:
    """
    Calcule le chemin d'une image Wee depuis son ID (file_hash_12).
    Exemple : image_id=197526, ext='jpg' → '0000/0197/197526.jpg'
    Exemple : image_id=72,     ext='jpg' → '0000/0000/72.jpg'
    """
    d1 = str(image_id // 1_000_000).zfill(4)
    d2 = str((image_id % 1_000_000) // 1_000).zfill(4)
    return f"{d1}/{d2}/{image_id}.{extension}"


def _load_image_gid_map(path: Path) -> dict[str, str]:
    """Charge output/image_gid_map.csv → {image_file: shopify_gid}."""
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return {
            row["image_file"]: row["shopify_gid"]
            for row in csv.DictReader(f)
            if row.get("shopify_gid") and not row["shopify_gid"].startswith("[DRY-RUN")
        }


def _save_image_gid_map(gid_map: dict[str, str], sources: dict[str, str], path: Path) -> None:
    """Sauvegarde la carte {image_file → GID} dans output/image_gid_map.csv."""
    rows = [
        {"image_file": k, "shopify_gid": v, "source": sources.get(k, "")}
        for k, v in gid_map.items()
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_GID_MAP_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    logger.info("image_gid_map.csv mis à jour : %d entrée(s)", len(rows))


def _resolve_image_gid(
    row: dict,
    client: ShopifyClient,
    gid_map: dict[str, str],
    sources: dict[str, str],
    drive_service,
    drive_folder_id: str | None,
) -> str | None:
    """
    Résout le GID Shopify d'une image designer, dans cet ordre :
      1. Cache local (image_gid_map.csv)
      2. Recherche Shopify Files par nom de fichier ID-based (ex: 197526.jpg)
      3. IMAGE_BASE_URL → fileCreate (Shopify télécharge directement)
      4. Google Drive → téléchargement + staged upload vers Shopify Files

    La clé de cache est toujours image_id (ex: '197526') pour rester stable
    même si la signature change.
    """
    image_id_raw = row.get("image_id", "")
    image_ext = row.get("image_ext", "jpg") or "jpg"
    image_file = row.get("image_file", "")  # signature.ext (conservé pour le champ texte)

    if not image_id_raw:
        return None

    try:
        image_id = int(image_id_raw)
    except (ValueError, TypeError):
        return None

    cache_key = str(image_id)
    id_filename = f"{image_id}.{image_ext}"                     # ex: 197526.jpg
    hash_path = _file_hash_path(image_id, image_ext)            # ex: 0000/0197/197526.jpg
    drive_search_names = [id_filename, hash_path.split("/")[-1]] # [197526.jpg]

    # 1. Cache local
    if cache_key in gid_map:
        return gid_map[cache_key]

    # 2. Recherche Shopify Files (par ID-filename et par signature-filename)
    for search_name in [id_filename, image_file]:
        if not search_name:
            continue
        gid = client.find_image_gid_by_filename(search_name)
        if gid:
            logger.info("Image trouvée dans Shopify Files (%s) → %s", search_name[:30], gid)
            gid_map[cache_key] = gid
            sources[cache_key] = "shopify_files"
            return gid

    # 3. IMAGE_BASE_URL → Shopify télécharge directement l'image
    if config.IMAGE_BASE_URL:
        # On essaie d'abord le chemin ID-based, puis signature
        for url_path in [hash_path, id_filename, image_file]:
            if not url_path:
                continue
            source_url = f"{config.IMAGE_BASE_URL}/{url_path}"
            logger.info("Tentative fileCreate via IMAGE_BASE_URL : %s", source_url[:80])
            try:
                gid = client.file_create(source_url, id_filename)
                if gid:
                    ok = client.wait_for_file_ready(gid, max_attempts=8, delay=2.0)
                    if ok:
                        logger.info("  Image importée via URL → GID : %s", gid)
                        gid_map[cache_key] = gid
                        sources[cache_key] = "image_base_url"
                        return gid
                    else:
                        logger.warning("  Fichier FAILED/TIMEOUT pour %s", source_url[:60])
                        break
            except ShopifyGraphQLError as e:
                logger.debug("  fileCreate URL échoué (%s) : %s", url_path[:30], e)

    # 4. Google Drive → download + upload Shopify
    if drive_service is None:
        logger.warning(
            "Image id=%s absente de Shopify Files. Options disponibles :\n"
            "  A) Mettre IMAGE_BASE_URL dans .env (si CDN accessible)\n"
            "  B) Uploader '%s' dans Google Drive dossier '%s'\n"
            "  C) Upload manuel Shopify Files → ajouter GID dans image_gid_map.csv",
            image_id, id_filename, config.GOOGLE_DRIVE_FOLDER_NAME,
        )
        return None

    logger.info("Recherche image id=%s dans Google Drive...", image_id)
    from google_drive import download_file
    file_bytes = None
    for drive_name in drive_search_names:
        file_bytes = download_file(drive_service, drive_name, drive_folder_id)
        if file_bytes:
            logger.info("  Trouvé dans Drive : '%s' (%d Ko)", drive_name, len(file_bytes) // 1024)
            break

    if not file_bytes:
        logger.warning(
            "Image id=%s introuvable dans Drive (cherché : %s). Champ image laissé vide.",
            image_id, ", ".join(drive_search_names),
        )
        return None

    gid = client.upload_image_from_bytes(id_filename, file_bytes)
    if gid:
        logger.info("  Upload OK - GID : %s", gid)
        client.wait_for_file_ready(gid, max_attempts=8, delay=2.0)
        gid_map[cache_key] = gid
        sources[cache_key] = "google_drive"
    else:
        logger.warning("  Upload échoué pour image id=%s.", image_id)
    return gid


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

    # On regroupe par wee_designer_id (une entrée par langue → on prend fr_FR en priorité)
    # Tri : fr_FR en premier, puis les autres langues
    designers = sorted(designers, key=lambda r: (0 if r.get("langue", "") == "fr_FR" else 1))

    # Initialisation de la résolution d'images
    gid_map_path = config.OUTPUT_DIR / "image_gid_map.csv"
    image_gid_cache = _load_image_gid_map(gid_map_path)
    image_sources: dict[str, str] = {}
    if image_gid_cache:
        logger.info("Carte GID images chargée depuis CSV : %d entrée(s)", len(image_gid_cache))

    # Connexion Google Drive (optionnelle — si credentials disponibles)
    drive_service = None
    drive_folder_id = None
    try:
        from google_drive import get_drive_service, find_folder_id
        drive_service = get_drive_service()
        if drive_service:
            drive_folder_id = find_folder_id(drive_service, config.GOOGLE_DRIVE_FOLDER_NAME)
            logger.info("Google Drive disponible (dossier '%s')", config.GOOGLE_DRIVE_FOLDER_NAME)
    except Exception as exc:
        logger.debug("Google Drive non disponible : %s", exc)

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

        image_gid = _resolve_image_gid(
            row=row,
            client=client,
            gid_map=image_gid_cache,
            sources=image_sources,
            drive_service=drive_service,
            drive_folder_id=drive_folder_id,
        )
        fields = designer_row_to_fields(row, image_gid=image_gid)
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

    # Persister la carte GID images pour éviter re-uploads lors du prochain run
    if image_gid_cache:
        _save_image_gid_map(image_gid_cache, image_sources, gid_map_path)

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
