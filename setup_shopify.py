"""
setup_shopify.py — Crée la définition du metaobject 'designer' dans Shopify.

À exécuter UNE SEULE FOIS avant le premier import.
Si le type existe déjà, le script le détecte et n'écrase rien.

Usage :
  python setup_shopify.py
"""

import logging
import sys

import requests

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

DESIGNER_TYPE = "designer"

# Définition complète des champs du metaobject designer
FIELD_DEFINITIONS = [
    {
        "key": "wee_designer_id",
        "name": "Wee Designer ID",
        "type": "single_line_text_field",
        "description": "Identifiant d'origine dans la base Wee (migration)",
        "required": False,
    },
    {
        "key": "name",
        "name": "Nom",
        "type": "single_line_text_field",
        "required": True,
    },
    {
        "key": "baseline",
        "name": "Baseline",
        "type": "single_line_text_field",
        "required": False,
    },
    {
        "key": "introduction",
        "name": "Introduction",
        "type": "multi_line_text_field",
        "required": False,
    },
    {
        "key": "body",
        "name": "Description",
        "type": "multi_line_text_field",
        "required": False,
    },
    {
        "key": "slug",
        "name": "Slug",
        "type": "single_line_text_field",
        "required": False,
    },
    {
        "key": "image_file",
        "name": "Fichier image (nom)",
        "type": "single_line_text_field",
        "description": "Nom du fichier image source (ex: abc123.jpg)",
        "required": False,
    },
    {
        "key": "color",
        "name": "Couleur",
        "type": "single_line_text_field",
        "required": False,
    },
    {
        "key": "locale",
        "name": "Langue",
        "type": "single_line_text_field",
        "required": False,
    },
]


def _gql(query: str, variables: dict = None) -> dict:
    resp = requests.post(
        config.SHOPIFY_GRAPHQL_URL,
        headers={
            "X-Shopify-Access-Token": config.SHOPIFY_ACCESS_TOKEN,
            "Content-Type": "application/json",
        },
        json={"query": query, "variables": variables or {}},
    )
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        raise RuntimeError(f"Erreur GraphQL : {data['errors']}")
    return data.get("data", {})


def type_exists() -> bool:
    """Vérifie si le type 'designer' existe déjà dans Shopify."""
    data = _gql("""
        query {
          metaobjectDefinitions(first: 50) {
            edges { node { type } }
          }
        }
    """)
    types = [
        e["node"]["type"]
        for e in data.get("metaobjectDefinitions", {}).get("edges", [])
    ]
    return DESIGNER_TYPE in types


def create_definition() -> str:
    """Crée la définition du metaobject designer. Retourne son ID."""
    field_inputs = []
    for f in FIELD_DEFINITIONS:
        field_input = {
            "key": f["key"],
            "name": f["name"],
            "type": f["type"],
            "required": f.get("required", False),
        }
        if "description" in f:
            field_input["description"] = f["description"]
        field_inputs.append(field_input)

    mutation = """
    mutation CreateDesignerDefinition($definition: MetaobjectDefinitionCreateInput!) {
      metaobjectDefinitionCreate(definition: $definition) {
        metaobjectDefinition {
          id
          type
          name
          fieldDefinitions { key type { name } }
        }
        userErrors { field message code }
      }
    }
    """
    variables = {
        "definition": {
            "type": DESIGNER_TYPE,
            "name": "Designer",
            "fieldDefinitions": field_inputs,
        }
    }
    data = _gql(mutation, variables)
    result = data.get("metaobjectDefinitionCreate", {})
    errors = result.get("userErrors", [])
    if errors:
        raise RuntimeError(f"userErrors : {errors}")

    definition = result["metaobjectDefinition"]
    logger.info("Metaobject type '%s' cree avec succes (ID: %s)", definition["type"], definition["id"])
    logger.info("Champs crees :")
    for f in definition["fieldDefinitions"]:
        logger.info("  - %s (%s)", f["key"], f["type"]["name"])
    return definition["id"]


def main() -> None:
    logger.info("=== SETUP SHOPIFY : definition metaobject designer ===")
    logger.info("Store : %s", config.SHOPIFY_STORE)

    if type_exists():
        logger.info("Le type metaobject '%s' existe deja — aucune action.", DESIGNER_TYPE)
        logger.info("Pour voir ses champs, allez dans Shopify Admin > Contenu > Metaobjects.")
        sys.exit(0)

    logger.info("Type '%s' absent — creation en cours...", DESIGNER_TYPE)
    try:
        create_definition()
        logger.info("=== SETUP TERMINE ===")
        logger.info("Vous pouvez maintenant lancer l'import :")
        logger.info("  python import_designers_to_shopify.py --test")
    except Exception as e:
        logger.error("Echec de la creation : %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
