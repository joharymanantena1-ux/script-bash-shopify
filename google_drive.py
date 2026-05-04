"""
google_drive.py — Authentification et téléchargement Google Drive.

Module partagé par import_designers_to_shopify.py et upload_images_to_shopify.py.
L'authentification OAuth2 ouvre un navigateur au premier lancement puis utilise
token.json pour les runs suivants (transparent pour l'utilisateur).
"""

import io
import logging
import sys
from pathlib import Path

import config

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
logger = logging.getLogger(__name__)


def get_drive_service():
    """
    Retourne un service Google Drive authentifié.
    - Si token.json existe et est valide → réutilisé silencieusement.
    - Si expiré → refresh automatique.
    - Si absent → navigateur ouvert pour consentement OAuth2.
    Retourne None si les credentials sont absents ou si l'import Google est désactivé.
    """
    try:
        from googleapiclient.discovery import build
        from google_auth_oauthlib.flow import InstalledAppFlow
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
    except ImportError:
        logger.warning(
            "Bibliothèques Google Drive absentes. "
            "Lancez : pip install google-api-python-client google-auth-oauthlib"
        )
        return None

    creds_path = Path(config.GOOGLE_CREDENTIALS_PATH)
    token_path = Path(config.GOOGLE_TOKEN_PATH)

    if not creds_path.exists():
        logger.debug(
            "Credentials Google introuvables (%s) — upload Drive désactivé.", creds_path
        )
        return None

    creds = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            logger.info("Renouvellement du token Google Drive...")
            creds.refresh(Request())
        else:
            logger.info("Ouverture du navigateur pour l'autorisation Google Drive...")
            flow = InstalledAppFlow.from_client_secrets_file(str(creds_path), SCOPES)
            creds = flow.run_local_server(port=0)

        with open(token_path, "w") as f:
            f.write(creds.to_json())
        logger.info("Token Google Drive sauvegardé : %s", token_path)

    return build("drive", "v3", credentials=creds)


def build_drive_index(service, folder_id: str, max_depth: int = 6) -> dict[str, str]:
    """
    Construit un index {filename: file_id} pour tous les fichiers sous folder_id.

    Utilise 'ancestors' pour récupérer tout l'arbre en une seule passe paginée
    (au lieu d'une requête par sous-dossier), ce qui est ~10× plus rapide.
    """
    index: dict[str, str] = {}
    query = (
        f"'{folder_id}' in ancestors "
        "and mimeType != 'application/vnd.google-apps.folder' "
        "and trashed=false"
    )
    page_token = None
    page = 0
    while True:
        page += 1
        result = service.files().list(
            q=query,
            fields="nextPageToken, files(id, name)",
            pageSize=1000,
            pageToken=page_token,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
        ).execute()

        for f in result.get("files", []):
            name = f["name"]
            if name not in index:
                index[name] = f["id"]

        page_token = result.get("nextPageToken")
        if page % 5 == 0 or not page_token:
            logger.info("Index Drive : page %d — %d fichier(s) indexé(s)...", page, len(index))
        if not page_token:
            break

    logger.info("Index Drive construit : %d fichier(s) au total", len(index))
    return index


def find_in_index(
    index: dict[str, str],
    image_id: str,
    image_ext: str,
) -> tuple[str | None, str | None]:
    """
    Cherche image_id dans l'index Drive avec fallback progressif.
    Retourne (file_id, matched_filename) ou (None, None).

    Ordre de priorité :
      1. Exact         : {image_id}.{image_ext}
      2. Autre ext     : {image_id}.jpg/png/jpeg/webp/gif
      3. Préfixe flou  : nom commençant par {image_id}_ ou {image_id}-
    """
    # 1. Exact
    exact = f"{image_id}.{image_ext}"
    if exact in index:
        return index[exact], exact

    # 2. Même ID, autre extension
    for ext in ("jpg", "jpeg", "png", "webp", "gif"):
        name = f"{image_id}.{ext}"
        if name in index:
            return index[name], name

    # 3. Préfixe flou
    prefix_dash  = f"{image_id}-"
    prefix_under = f"{image_id}_"
    for fname, fid in index.items():
        if fname.startswith(prefix_dash) or fname.startswith(prefix_under):
            return fid, fname

    return None, None


def download_by_id(service, file_id: str) -> bytes | None:
    """Télécharge un fichier Google Drive par son ID."""
    from googleapiclient.http import MediaIoBaseDownload
    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buffer.getvalue()


def find_folder_id(service, folder_name: str) -> str | None:
    """Trouve l'ID d'un dossier Google Drive par son nom (My Drive + Shared Drives)."""
    result = service.files().list(
        q=(
            f"name='{folder_name}' "
            "and mimeType='application/vnd.google-apps.folder' "
            "and trashed=false"
        ),
        fields="files(id, name, driveId)",
        pageSize=5,
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
    ).execute()
    folders = result.get("files", [])
    if not folders:
        logger.warning("Dossier Google Drive '%s' introuvable (My Drive + Shared Drives).", folder_name)
        return None
    if len(folders) > 1:
        logger.warning(
            "%d dossiers '%s' trouvés — utilisation du premier.", len(folders), folder_name
        )
    drive_label = folders[0].get("driveId", "MyDrive")
    logger.info("Dossier Drive '%s' : %s (drive=%s)", folder_name, folders[0]["id"], drive_label)
    return folders[0]["id"]


def find_file(service, filename: str, folder_id: str | None) -> bool:
    """
    Vérifie l'existence d'un fichier dans Google Drive SANS le télécharger.
    Utile en dry-run pour valider la présence d'une image rapidement.
    """
    query = f"name='{filename}' and trashed=false"
    if folder_id:
        query += f" and '{folder_id}' in parents"

    result = service.files().list(
        q=query,
        fields="files(id, name)",
        pageSize=1,
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
    ).execute()
    files = result.get("files", [])

    if not files and folder_id:
        return find_file(service, filename, folder_id=None)
    return len(files) > 0


def download_file(service, filename: str, folder_id: str | None) -> bytes | None:
    """
    Télécharge un fichier depuis Google Drive (My Drive + Shared Drives).
    Cherche dans folder_id si fourni, puis globalement en fallback.
    Retourne les bytes du fichier ou None si introuvable.
    """
    from googleapiclient.http import MediaIoBaseDownload

    query = f"name='{filename}' and trashed=false"
    if folder_id:
        query += f" and '{folder_id}' in parents"

    result = service.files().list(
        q=query,
        fields="files(id, name, mimeType, driveId)",
        pageSize=5,
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
    ).execute()
    files = result.get("files", [])

    if not files and folder_id:
        # Fallback : chercher sans contrainte de dossier (tous les drives)
        logger.debug("'%s' absent du dossier Drive — recherche globale...", filename)
        return download_file(service, filename, folder_id=None)

    if not files:
        logger.debug("Fichier '%s' introuvable dans Google Drive.", filename)
        return None

    file_id = files[0]["id"]
    drive_id = files[0].get("driveId")
    logger.debug("Fichier Drive trouvé : %s (id=%s drive=%s)", filename, file_id, drive_id or "MyDrive")

    # supportsAllDrives requis pour télécharger depuis un Shared Drive
    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buffer.getvalue()
