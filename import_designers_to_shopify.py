"""
import_designers_to_shopify.py — Étape 2 : CSV → Shopify

Lit designers.csv et product_designer_links.csv puis :
  1. Crée ou met à jour les metaobjects 'designer' dans Shopify
  2. Lie chaque produit Shopify à son designer via un metafield

Modes périmètre :
  --test           Traite uniquement le produit de test (DEFAULT)
  --global-import  Traite tous les produits (à activer volontairement)

Modes dry-run :
  --dry-run        Aucune écriture Shopify (simulé) — ACTIVÉ PAR DÉFAUT via .env
  --no-dry-run     Désactive le dry-run (écriture réelle — IRRÉVERSIBLE)

Utilitaires :
  --preview        Aperçu lecture seule : génère output/import_preview.csv + résumé console
  --reset-cache    Supprime output/import_state.csv et output/image_gid_map.csv

Usage :
  python import_designers_to_shopify.py --test
  python import_designers_to_shopify.py --test --no-dry-run
  python import_designers_to_shopify.py --global-import --dry-run
  python import_designers_to_shopify.py --global-import --no-dry-run
  python import_designers_to_shopify.py --test --preview
  python import_designers_to_shopify.py --test --reset-cache
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

    En mode test : utilise TEST_PRODUCT_SKU ou TEST_PRODUCT_HANDLE, UNIQUEMENT
    si wee_product_id correspond à TEST_PRODUCT_ID. Pour tout autre produit → None.

    Pour l'import global complet, un CSV de mapping wee_product_id ↔ shopify_gid
    sera nécessaire (non implémenté — voir CLAUDE.md).
    """
    # SKU/handle de test : n'est valide que pour le produit de test explicitement défini
    if config.TEST_PRODUCT_ID and str(wee_product_id) == str(config.TEST_PRODUCT_ID):
        if config.TEST_PRODUCT_SKU:
            return client.find_product_by_sku(config.TEST_PRODUCT_SKU)
        if config.TEST_PRODUCT_HANDLE:
            return client.find_product_by_handle(config.TEST_PRODUCT_HANDLE)

    # Pour tous les autres produits : pas de mapping disponible
    logger.debug(
        "Pas de mapping Shopify pour wee_product_id=%s (hors produit de test).",
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


# ── State cache (reprise après interruption) ──────────────────────────────────

_STATE_COLUMNS = [
    "wee_designer_id", "shopify_metaobject_gid",
    "image_status", "product_status", "processed_at",
]


def _load_import_state(path: Path) -> dict[str, dict]:
    """Charge output/import_state.csv → {wee_designer_id: {cols...}}."""
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return {row["wee_designer_id"]: row for row in csv.DictReader(f)}


def _save_designer_state(state: dict[str, dict], path: Path) -> None:
    """Persiste l'état complet dans import_state.csv (appelé après chaque designer)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_STATE_COLUMNS)
        writer.writeheader()
        writer.writerows(state.values())


# ── State cache Phase 2 (liens produit) ──────────────────────────────────────

_LINK_STATE_COLUMNS = ["product_id", "wee_designer_id", "status", "processed_at"]


def _load_link_state(path: Path) -> set[tuple[str, str]]:
    """Charge output/link_state.csv → ensemble de (product_id, wee_designer_id) déjà traités."""
    if not path.exists():
        return set()
    with open(path, "r", encoding="utf-8") as f:
        return {
            (row["product_id"], row["wee_designer_id"])
            for row in csv.DictReader(f)
            if row.get("status") == "ok"
        }


def _append_link_state(key: tuple[str, str], status: str, path: Path) -> None:
    """Ajoute une ligne dans link_state.csv (append, pas de réécriture complète)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_LINK_STATE_COLUMNS)
        if write_header:
            writer.writeheader()
        writer.writerow({
            "product_id": key[0],
            "wee_designer_id": key[1],
            "status": status,
            "processed_at": datetime.now().isoformat(timespec="seconds"),
        })


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

    # 4. Google Drive → vérification (dry-run) ou download + upload (réel)
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

    # En dry-run : vérifier l'existence sans télécharger (beaucoup plus rapide)
    if client.dry_run:
        from google_drive import find_file
        for drive_name in drive_search_names:
            if find_file(drive_service, drive_name, drive_folder_id):
                logger.info(
                    "  [DRY-RUN] Image trouvee dans Drive : '%s' — upload simule.", drive_name
                )
                placeholder = f"[DRY-RUN-IMAGE-{image_id}]"
                gid_map[cache_key] = placeholder
                sources[cache_key] = "google_drive_dry_run"
                return placeholder
        logger.warning(
            "Image id=%s introuvable dans Drive (cherche : %s). Champ image laisse vide.",
            image_id, ", ".join(drive_search_names),
        )
        return None

    # Mode réel : télécharger puis uploader vers Shopify Files
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

    # Chargement du state cache (reprise après interruption)
    state_path = config.OUTPUT_DIR / "import_state.csv"
    import_state = _load_import_state(state_path)
    if import_state:
        logger.info("State cache charge : %d designer(s) deja traites", len(import_state))

    seen_ids: set[str] = set()
    id_to_gid: dict[str, str] = {}
    skipped_from_cache = 0

    for row in designers:
        wee_id = str(row.get("wee_designer_id", ""))
        langue = row.get("langue", "")

        # Ne créer qu'un seul metaobject par designer (on prend la première occurrence = FR)
        if wee_id in seen_ids:
            logger.debug("Designer %s (%s) deja traite — ignore.", wee_id, langue)
            continue
        seen_ids.add(wee_id)

        # Reprise : skip si deja traite avec un vrai GID Shopify
        if wee_id in import_state:
            cached_gid = import_state[wee_id].get("shopify_metaobject_gid", "")
            if cached_gid and not cached_gid.startswith("[DRY-RUN"):
                logger.debug(
                    "Designer %s deja traite (cache) — GID : %s", wee_id, cached_gid[:50]
                )
                skipped_from_cache += 1
                id_to_gid[wee_id] = cached_gid
                report.add(
                    wee_designer_id=wee_id,
                    langue=langue,
                    action_metaobject="skipped",
                    shopify_metaobject_gid=cached_gid,
                    statut="ok",
                    message="reprise depuis cache",
                )
                continue

        image_gid = _resolve_image_gid(
            row=row,
            client=client,
            gid_map=image_gid_cache,
            sources=image_sources,
            drive_service=drive_service,
            drive_folder_id=drive_folder_id,
        )
        fields = designer_row_to_fields(row, image_gid=image_gid)

        # Shopify exige que 'name' soit non vide
        if not fields.get("name", "").strip():
            logger.warning(
                "Designer %s ignore : champ 'name' vide dans le CSV (langue=%s). "
                "Verifiez la donnee dans MariaDB.",
                wee_id, langue,
            )
            report.add(
                wee_designer_id=wee_id,
                langue=langue,
                action_metaobject="skipped",
                statut="skipped",
                message="name vide — metaobject non cree",
            )
            continue

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

            # Sauvegarder l'état immédiatement après chaque designer
            import_state[wee_id] = {
                "wee_designer_id": wee_id,
                "shopify_metaobject_gid": gid or "",
                "image_status": "ok" if image_gid else "no_image",
                "product_status": "",
                "processed_at": datetime.now().isoformat(timespec="seconds"),
            }
            _save_designer_state(import_state, state_path)

        except ShopifyGraphQLError as e:
            logger.error("Erreur metaobject designer %s : %s", wee_id, e)
            report.add(
                wee_designer_id=wee_id,
                langue=langue,
                action_metaobject="error",
                statut="error",
                message=str(e),
            )
            # Enregistrer l'erreur dans le state pour ne pas re-bloquer indéfiniment
            import_state[wee_id] = {
                "wee_designer_id": wee_id,
                "shopify_metaobject_gid": "",
                "image_status": "ok" if image_gid else "no_image",
                "product_status": "error",
                "processed_at": datetime.now().isoformat(timespec="seconds"),
            }
            _save_designer_state(import_state, state_path)

    if skipped_from_cache:
        logger.info("Phase 1 : %d designer(s) repris depuis le cache (aucun appel API).", skipped_from_cache)

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

    # Cache de reprise pour les liens déjà traités
    link_state_path = config.OUTPUT_DIR / "link_state.csv"
    done_links = _load_link_state(link_state_path)
    if done_links:
        logger.info("Link state charge : %d lien(s) deja traites (ignores).", len(done_links))

    skipped_links = 0
    missing_links = 0

    for link in links:
        wee_product_id = str(link["product_id"])
        wee_designer_id = str(link["wee_designer_id"])
        designer_nom = link.get("designer_nom", "")
        link_key = (wee_product_id, wee_designer_id)

        # Reprise : skip si lien déjà traité avec succès
        if link_key in done_links:
            skipped_links += 1
            continue

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
            missing_links += 1
            logger.debug(
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
            done_links.add(link_key)
            _append_link_state(link_key, "ok", link_state_path)

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

    if skipped_links:
        logger.info("Phase 2 : %d lien(s) repris depuis le cache (aucun appel API).", skipped_links)
    if missing_links:
        logger.info(
            "Phase 2 : %d lien(s) sans mapping produit Wee -> Shopify (ignores).", missing_links
        )


# ── Aperçu (--preview) ───────────────────────────────────────────────────────

_PREVIEW_COLUMNS = [
    "wee_designer_id", "nom", "image_id", "image_status",
    "metaobject_status", "wee_product_id", "product_status", "action_prevue",
]


def run_preview(
    designers: list[dict],
    links: list[dict],
    output_dir: Path,
    test_product_id: str | None,
) -> None:
    """
    Mode lecture seule : analyse les CSV locaux sans appel API et génère
    output/import_preview.csv avec un résumé console.
    """
    state_path = output_dir / "import_state.csv"
    gid_map_path = output_dir / "image_gid_map.csv"

    import_state = _load_import_state(state_path)
    image_gid_map = _load_image_gid_map(gid_map_path)

    # Filtrage mode test
    if test_product_id:
        relevant_ids = {
            str(l["wee_designer_id"])
            for l in links
            if str(l["product_id"]) == test_product_id
        }
        designers = [d for d in designers if str(d["wee_designer_id"]) in relevant_ids]
        links = [l for l in links if str(l["product_id"]) == test_product_id]

    # Dédupliquer les designers (FR en priorité)
    designers_sorted = sorted(
        designers, key=lambda r: (0 if r.get("langue", "") == "fr_FR" else 1)
    )
    seen: set[str] = set()
    unique_designers: list[dict] = []
    for d in designers_sorted:
        wid = str(d.get("wee_designer_id", ""))
        if wid not in seen:
            seen.add(wid)
            unique_designers.append(d)

    # Index liens : wee_designer_id → [wee_product_id, ...]
    from collections import defaultdict
    links_by_designer: dict[str, list[str]] = defaultdict(list)
    for l in links:
        links_by_designer[str(l["wee_designer_id"])].append(str(l["product_id"]))

    # Détermine si la résolution produit est possible (mode test uniquement)
    product_resolvable = bool(config.TEST_PRODUCT_SKU or config.TEST_PRODUCT_HANDLE)

    preview_rows: list[dict] = []
    for d in unique_designers:
        wee_id = str(d.get("wee_designer_id", ""))
        nom = d.get("nom", "")
        image_id = str(d.get("image_id", ""))

        image_status = "cache_ok" if image_id in image_gid_map else "a_uploader"

        cached_entry = import_state.get(wee_id, {})
        cached_gid = cached_entry.get("shopify_metaobject_gid", "")
        in_state_ok = bool(cached_gid and not cached_gid.startswith("[DRY-RUN"))
        metaobject_status = "existant_en_cache" if in_state_ok else "a_creer"
        action_prevue = "skip" if in_state_ok else "create"

        product_ids = links_by_designer.get(wee_id) or [""]
        for pid in product_ids:
            if pid and test_product_id:
                product_status = "mappable" if product_resolvable else "mapping_manquant"
            elif pid:
                product_status = "a_verifier"
            else:
                product_status = ""
            preview_rows.append({
                "wee_designer_id": wee_id,
                "nom": nom,
                "image_id": image_id,
                "image_status": image_status,
                "metaobject_status": metaobject_status,
                "wee_product_id": pid,
                "product_status": product_status,
                "action_prevue": action_prevue,
            })

    # Écriture CSV
    preview_path = output_dir / "import_preview.csv"
    output_dir.mkdir(parents=True, exist_ok=True)
    with open(preview_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_PREVIEW_COLUMNS)
        writer.writeheader()
        writer.writerows(preview_rows)

    # Résumé console
    total = len(unique_designers)
    to_skip = sum(1 for d in unique_designers if import_state.get(str(d.get("wee_designer_id", "")), {}).get("shopify_metaobject_gid", "") and not import_state[str(d.get("wee_designer_id", ""))]["shopify_metaobject_gid"].startswith("[DRY-RUN"))
    to_create = total - to_skip
    img_cached = sum(1 for d in unique_designers if str(d.get("image_id", "")) in image_gid_map)
    img_upload = total - img_cached
    links_total = sum(1 for r in preview_rows if r["wee_product_id"])
    mappable = sum(1 for r in preview_rows if r["product_status"] == "mappable")
    missing = sum(1 for r in preview_rows if r["product_status"] == "mapping_manquant")

    logger.info("=" * 60)
    logger.info("APERCU (--preview) — aucune ecriture effectuee")
    logger.info("  Designers total          : %d", total)
    logger.info("  Metaobjects a creer      : %d", to_create)
    logger.info("  Deja traites (cache)     : %d", to_skip)
    logger.info("  Images en cache          : %d", img_cached)
    logger.info("  Images a uploader        : %d", img_upload)
    logger.info("  Liens produit total      : %d", links_total)
    logger.info("  Liens mappables          : %d", mappable)
    logger.info("  Liens sans mapping       : %d", missing)
    logger.info("  Apercu genere : %s", preview_path)
    logger.info("=" * 60)


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

    # Utilitaires
    parser.add_argument(
        "--preview",
        action="store_true",
        default=False,
        help="Aperçu lecture seule : génère import_preview.csv sans aucun appel API",
    )
    parser.add_argument(
        "--reset-cache",
        action="store_true",
        dest="reset_cache",
        default=False,
        help="Supprime import_state.csv, link_state.csv et image_gid_map.csv puis quitte",
    )

    return parser.parse_args()


def main() -> None:
    setup_logging()
    args = parse_args()

    output_dir = config.OUTPUT_DIR

    # ── --reset-cache : supprime les caches locaux puis quitte ────────────────
    if args.reset_cache:
        files_to_delete = [
            output_dir / "import_state.csv",
            output_dir / "image_gid_map.csv",
            output_dir / "link_state.csv",
        ]
        for f in files_to_delete:
            if f.exists():
                f.unlink()
                logger.info("Cache supprime : %s", f)
            else:
                logger.info("Cache absent (rien a supprimer) : %s", f)
        logger.info("--reset-cache termine.")
        return

    global_mode = getattr(args, "global_import", False)
    test_mode = not global_mode

    output_dir.mkdir(parents=True, exist_ok=True)
    designers = read_csv(output_dir / "designers.csv")
    links = read_csv(output_dir / "product_designer_links.csv")
    logger.info("Designers charges : %d lignes", len(designers))
    logger.info("Liens charges     : %d lignes", len(links))

    # Détermination du product_id de test
    test_product_id = config.TEST_PRODUCT_ID if test_mode else None

    if test_mode and not test_product_id:
        logger.error(
            "Mode test actif mais TEST_PRODUCT_ID absent du .env.\n"
            "Utilisez --global-import pour traiter tous les produits."
        )
        sys.exit(1)

    logger.info("Perimetre : %s", f"test (product_id={test_product_id})" if test_mode else "global")

    # ── --preview : aperçu lecture seule, aucun appel API ────────────────────
    if args.preview:
        logger.info(">>> MODE PREVIEW — lecture seule, aucune ecriture <<<")
        run_preview(designers, links, output_dir, test_product_id)
        return

    # ── Import normal ─────────────────────────────────────────────────────────

    # Le flag CLI surpasse le .env, le .env surpasse la valeur par défaut (True)
    if args.dry_run_flag is not None:
        dry_run = args.dry_run_flag
    else:
        dry_run = config.DRY_RUN

    if dry_run:
        logger.info(">>> MODE DRY-RUN ACTIF — aucune ecriture Shopify <<<")
    else:
        logger.warning(">>> MODE ECRITURE REELLE SHOPIFY — les modifications sont permanentes <<<")

    client = ShopifyClient(dry_run=dry_run)
    report = ImportReport(output_dir)

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
