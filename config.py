"""
config.py — Lecture centralisée des variables d'environnement.
Toutes les autres modules importent depuis ici, jamais directement os.environ.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Charge le fichier .env situé dans le même dossier que ce script
_env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=_env_path)


def _require(key: str) -> str:
    """Lève une erreur claire si une variable obligatoire est absente."""
    value = os.getenv(key)
    if not value:
        raise EnvironmentError(
            f"Variable d'environnement manquante : {key}\n"
            f"Vérifiez votre fichier .env (voir .env.example)"
        )
    return value


# ── Base de données ──────────────────────────────────────────────────────────
DB_HOST = _require("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "3309"))
DB_NAME = _require("DB_NAME")
DB_USER = _require("DB_USER")
DB_PASSWORD = _require("DB_PASSWORD")

# ── Shopify ──────────────────────────────────────────────────────────────────
SHOPIFY_STORE = _require("SHOPIFY_STORE")
SHOPIFY_ACCESS_TOKEN = _require("SHOPIFY_ACCESS_TOKEN")
SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2025-01")

# ── Paramètres métier ────────────────────────────────────────────────────────
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "output"))
DESIGNER_TYPE_ID = int(os.getenv("DESIGNER_TYPE_ID", "139"))
DEFAULT_TRANS_ID = int(os.getenv("DEFAULT_TRANS_ID", "1"))

# ── Paramètres de test ───────────────────────────────────────────────────────
TEST_PRODUCT_ID = os.getenv("TEST_PRODUCT_ID", "")
TEST_PRODUCT_SKU = os.getenv("TEST_PRODUCT_SKU", "")
TEST_PRODUCT_HANDLE = os.getenv("TEST_PRODUCT_HANDLE", "")

# ── Mode sécurité ────────────────────────────────────────────────────────────
# DRY_RUN=true par défaut — écriture Shopify désactivée sauf opt-in explicite
DRY_RUN = os.getenv("DRY_RUN", "true").strip().lower() != "false"

# ── Google Drive ──────────────────────────────────────────────────────────────
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "credential-regardbeauty.json")
GOOGLE_TOKEN_PATH = os.getenv("GOOGLE_TOKEN_PATH", "token.json")
GOOGLE_DRIVE_FOLDER_NAME = os.getenv("GOOGLE_DRIVE_FOLDER_NAME", "master")

# ── Source images ─────────────────────────────────────────────────────────────
# Si renseigné, Shopify téléchargera les images directement depuis cette URL publique.
# Exemple : IMAGE_BASE_URL=https://cdn.exemple.com/images/
# L'URL finale sera : {IMAGE_BASE_URL}/{image_file}
# Laisser vide si vous utilisez Google Drive ou l'upload manuel.
IMAGE_BASE_URL = os.getenv("IMAGE_BASE_URL", "").rstrip("/")

# ── Shopify GraphQL endpoint ─────────────────────────────────────────────────
SHOPIFY_GRAPHQL_URL = (
    f"https://{SHOPIFY_STORE}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"
)
