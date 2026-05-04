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


def search_file_by_name(service, filename: str) -> tuple[str | None, str | None]:
    """
    Recherche un fichier Drive par son nom exact.
    Retourne (file_id, filename) ou (None, None).
    C'est la stratégie principale quand on connaît le vrai nom (signature.ext depuis la DB).
    """
    if not filename:
        return None, None
    result = service.files().list(
        q=f"name='{filename}' and trashed=false and mimeType != 'application/vnd.google-apps.folder'",
        fields="files(id, name)",
        pageSize=1,
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
    ).execute()
    files = result.get("files", [])
    if files:
        return files[0]["id"], files[0]["name"]
    return None, None


def search_file_by_id(
    service,
    image_id: str,
    image_ext: str,
    master_folder_id: str | None = None,
) -> tuple[str | None, str | None]:
    """
    Recherche une image dans Drive par son image_id.
    Stratégies dans l'ordre :
      1. Exact global : name='{image_id}.{ext}' (toutes extensions)
      2. Sous-dossiers de master/ : liste les sous-dossiers, cherche {image_id}.{ext} dedans
      3. Fuzzy global : name contains '{image_id}' (fichier ou dossier portant cet ID)
      4. Fichier dans un dossier nommé '{image_id}' (structure master/XXXX/{image_id}/image.jpg)
    Retourne (file_id, matched_filename) ou (None, None).
    """
    candidates = [f"{image_id}.{image_ext}"]
    for ext in ("jpg", "jpeg", "png", "webp", "gif"):
        name = f"{image_id}.{ext}"
        if name not in candidates:
            candidates.append(name)

    # 1. Recherche exacte globale pour chaque extension
    for name in candidates:
        result = service.files().list(
            q=f"name='{name}' and trashed=false",
            fields="files(id, name)",
            pageSize=1,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
        ).execute()
        files = result.get("files", [])
        if files:
            return files[0]["id"], files[0]["name"]

    # 2. Recherche ciblée dans les sous-dossiers de master/ (master/0000/, master/0001/…)
    if master_folder_id:
        sub_result = service.files().list(
            q=(
                f"'{master_folder_id}' in parents "
                "and mimeType='application/vnd.google-apps.folder' "
                "and trashed=false"
            ),
            fields="files(id, name)",
            pageSize=100,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
        ).execute()
        for subfolder in sub_result.get("files", []):
            sfid = subfolder["id"]
            for name in candidates:
                r = service.files().list(
                    q=f"name='{name}' and '{sfid}' in parents and trashed=false",
                    fields="files(id, name)",
                    pageSize=1,
                    includeItemsFromAllDrives=True,
                    supportsAllDrives=True,
                ).execute()
                hits = r.get("files", [])
                if hits:
                    return hits[0]["id"], hits[0]["name"]

    # 3. Fuzzy global : name contains '{image_id}'
    result = service.files().list(
        q=f"name contains '{image_id}' and trashed=false and mimeType != 'application/vnd.google-apps.folder'",
        fields="files(id, name)",
        pageSize=5,
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
    ).execute()
    for f in result.get("files", []):
        stem = f["name"].rsplit(".", 1)[0]
        if stem == image_id or f["name"].startswith(f"{image_id}_") or f["name"].startswith(f"{image_id}-"):
            return f["id"], f["name"]

    # 4. Dossier nommé '{image_id}' → premier fichier image à l'intérieur
    #    (structure : master/XXXX/{image_id}/original.jpg)
    folder_result = service.files().list(
        q=(
            f"name='{image_id}' "
            "and mimeType='application/vnd.google-apps.folder' "
            "and trashed=false"
        ),
        fields="files(id, name)",
        pageSize=1,
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
    ).execute()
    for folder in folder_result.get("files", []):
        img_result = service.files().list(
            q=(
                f"'{folder['id']}' in parents "
                "and trashed=false "
                "and (mimeType contains 'image/' or name contains '.jpg' or name contains '.png')"
            ),
            fields="files(id, name)",
            pageSize=1,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
        ).execute()
        imgs = img_result.get("files", [])
        if imgs:
            return imgs[0]["id"], imgs[0]["name"]

    return None, None


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


def find_folder_info(service, folder_name: str) -> tuple[str | None, str | None]:
    """
    Trouve un dossier Google Drive par son nom.
    Retourne (folder_id, drive_id) — drive_id est None pour My Drive.
    """
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
        logger.warning("Dossier Google Drive '%s' introuvable.", folder_name)
        return None, None
    if len(folders) > 1:
        logger.warning("%d dossiers '%s' trouvés — utilisation du premier.", len(folders), folder_name)
    folder_id = folders[0]["id"]
    drive_id   = folders[0].get("driveId") or None
    logger.info("Dossier Drive '%s' : %s (drive=%s)", folder_name, folder_id, drive_id or "MyDrive")
    return folder_id, drive_id


def find_folder_id(service, folder_name: str) -> str | None:
    """Rétrocompatibilité — retourne uniquement folder_id."""
    folder_id, _ = find_folder_info(service, folder_name)
    return folder_id


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
