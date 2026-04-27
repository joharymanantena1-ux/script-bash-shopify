"""
verify_shopify.py — Vérification post-import du metaobject designer dans Shopify.

Vérifie :
  1. Définition du type 'designer' (champs disponibles, types, requis)
  2. Metaobjects existants : statut, image, locale fr-FR, baseline
  3. Lien produit <-> designer (metafield custom.designer)

Usage :
  python verify_shopify.py
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


def _gql(query: str, variables: dict | None = None) -> dict:
    resp = requests.post(
        config.SHOPIFY_GRAPHQL_URL,
        headers={
            "X-Shopify-Access-Token": config.SHOPIFY_ACCESS_TOKEN,
            "Content-Type": "application/json",
        },
        json={"query": query, "variables": variables or {}},
    )
    resp.raise_for_status()
    body = resp.json()
    if "errors" in body:
        raise RuntimeError(f"Erreur GraphQL : {body['errors']}")
    return body.get("data", {})


# ── 1. Définition du type ─────────────────────────────────────────────────────

def check_definition() -> None:
    logger.info("=" * 60)
    logger.info("1. DÉFINITION DU TYPE METAOBJECT 'designer'")
    logger.info("=" * 60)

    data = _gql("""
        query {
          metaobjectDefinitions(first: 50) {
            edges {
              node {
                type
                name
                id
                fieldDefinitions {
                  key
                  name
                  required
                  type { name }
                }
                capabilities {
                  publishable { enabled }
                  translatable { enabled }
                }
              }
            }
          }
        }
    """)

    defs = data.get("metaobjectDefinitions", {}).get("edges", [])
    designer_def = next(
        (e["node"] for e in defs if e["node"]["type"] == DESIGNER_TYPE), None
    )

    if not designer_def:
        logger.error("Type 'designer' INTROUVABLE dans Shopify !")
        return

    logger.info("Nom          : %s", designer_def["name"])
    logger.info("ID           : %s", designer_def["id"])
    caps = designer_def.get("capabilities", {})
    logger.info(
        "Capabilities : publishable=%s  translatable=%s",
        caps.get("publishable", {}).get("enabled", False),
        caps.get("translatable", {}).get("enabled", False),
    )
    logger.info("Champs définis :")
    for f in designer_def["fieldDefinitions"]:
        req = " [REQUIS]" if f["required"] else ""
        logger.info("  %-20s %-30s%s", f["key"], f["type"]["name"], req)


# ── 2. Metaobjects existants ──────────────────────────────────────────────────

def check_metaobjects() -> list[dict]:
    logger.info("")
    logger.info("=" * 60)
    logger.info("2. METAOBJECTS DESIGNER EXISTANTS (max 10)")
    logger.info("=" * 60)

    data = _gql("""
        query {
          metaobjects(type: "designer", first: 10) {
            edges {
              node {
                id
                handle
                updatedAt
                capabilities {
                  publishable { status }
                }
                fields {
                  key
                  value
                  type
                }
              }
            }
          }
        }
    """)

    edges = data.get("metaobjects", {}).get("edges", [])
    if not edges:
        logger.warning("Aucun metaobject 'designer' trouvé.")
        return []

    logger.info("Nombre trouvés : %d", len(edges))
    nodes = []
    for edge in edges:
        node = edge["node"]
        nodes.append(node)
        fields_map = {f["key"]: f["value"] for f in node.get("fields", [])}
        status = (
            node.get("capabilities", {})
            .get("publishable", {})
            .get("status", "N/A (publishable non activé)")
        )

        logger.info("")
        logger.info("  GID    : %s", node["id"])
        logger.info("  Handle : %s", node["handle"])
        logger.info("  Mis à jour : %s", node["updatedAt"])
        logger.info("  Statut (publishable) : %s", status)

        # Vérifications métier
        _check_field(fields_map, "name", obligatoire=True)
        _check_field(fields_map, "baseline")
        _check_field(fields_map, "locale", expected_value="fr-FR")
        _check_field(fields_map, "image")
        _check_field(fields_map, "image_file")
        _check_field(fields_map, "body", preview_chars=80)
        _check_field(fields_map, "wee_designer_id")

    return nodes


def _check_field(
    fields_map: dict,
    key: str,
    obligatoire: bool = False,
    expected_value: str | None = None,
    preview_chars: int = 40,
) -> None:
    val = fields_map.get(key)
    if val is None:
        level = "ERROR" if obligatoire else "WARNING"
        logger.log(
            logging.ERROR if obligatoire else logging.WARNING,
            "    [%s] Champ '%s' absent ou vide", level, key
        )
        return

    preview = val[:preview_chars] + ("…" if len(val) > preview_chars else "")
    if expected_value and val != expected_value:
        logger.warning(
            "    [WARN] Champ '%s' = '%s' (attendu: '%s')", key, preview, expected_value
        )
    else:
        logger.info("    [OK]   Champ '%s' = %s", key, repr(preview))


# ── 3. Lien produit ───────────────────────────────────────────────────────────

def check_product_link() -> None:
    logger.info("")
    logger.info("=" * 60)
    logger.info("3. LIEN PRODUIT <-> DESIGNER (produit de test)")
    logger.info("=" * 60)

    if not config.TEST_PRODUCT_SKU and not config.TEST_PRODUCT_HANDLE:
        logger.warning("TEST_PRODUCT_SKU et TEST_PRODUCT_HANDLE absents du .env — vérification produit ignorée.")
        return

    # Trouver le produit de test
    product_gid = _find_test_product()
    if not product_gid:
        return

    data = _gql("""
        query GetProductMetafield($id: ID!) {
          product(id: $id) {
            title
            metafield(namespace: "custom", key: "designer") {
              id
              value
              type
            }
          }
        }
    """, {"id": product_gid})

    product = data.get("product", {})
    logger.info("Produit : %s (%s)", product.get("title", "?"), product_gid)

    mf = product.get("metafield")
    if not mf:
        logger.error("[KO] Metafield custom.designer ABSENT sur ce produit.")
        return

    logger.info("[OK] Metafield custom.designer trouvé :")
    logger.info("     id    = %s", mf["id"])
    logger.info("     type  = %s", mf["type"])
    logger.info("     value = %s", mf["value"])


def _find_test_product() -> str | None:
    if config.TEST_PRODUCT_SKU:
        data = _gql("""
            query ($q: String!) {
              productVariants(first: 1, query: $q) {
                edges { node { product { id title } } }
              }
            }
        """, {"q": f"sku:{config.TEST_PRODUCT_SKU}"})
        edges = data.get("productVariants", {}).get("edges", [])
        if edges:
            return edges[0]["node"]["product"]["id"]

    if config.TEST_PRODUCT_HANDLE:
        data = _gql("""
            query ($h: String!) {
              productByHandle(handle: $h) { id title }
            }
        """, {"h": config.TEST_PRODUCT_HANDLE})
        product = data.get("productByHandle")
        if product:
            return product["id"]

    logger.warning("Produit de test introuvable avec SKU=%s / Handle=%s",
                   config.TEST_PRODUCT_SKU, config.TEST_PRODUCT_HANDLE)
    return None


# ── Entrypoint ────────────────────────────────────────────────────────────────

def main() -> None:
    logger.info("=== VERIFY SHOPIFY — %s ===", config.SHOPIFY_STORE)

    check_definition()
    check_metaobjects()
    check_product_link()

    logger.info("")
    logger.info("=== VÉRIFICATION TERMINÉE ===")


if __name__ == "__main__":
    main()
