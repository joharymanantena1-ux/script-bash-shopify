"""
shopify_client.py — Client GraphQL Admin API Shopify.

Gère :
  - Envoi de requêtes GraphQL avec retry sur rate limit (429 / THROTTLED)
  - Recherche de produit par SKU ou handle
  - Recherche/création de metaobject designer
  - Création/mise à jour de metafield produit
"""

import logging
import time
from typing import Any

import requests

import config

logger = logging.getLogger(__name__)

# Délai initial entre requêtes (respecte la limite Shopify ~2 req/s sur GraphQL)
_REQUEST_DELAY_S = 0.6
_MAX_RETRIES = 5


class ShopifyGraphQLError(Exception):
    """Levée quand Shopify renvoie une erreur dans le champ 'errors' ou 'userErrors'."""


class ShopifyClient:
    """Encapsule les appels GraphQL Shopify."""

    def __init__(self, dry_run: bool = True) -> None:
        self.dry_run = dry_run
        self._session = requests.Session()
        self._session.headers.update({
            "X-Shopify-Access-Token": config.SHOPIFY_ACCESS_TOKEN,
            "Content-Type": "application/json",
        })
        if dry_run:
            logger.info("[DRY-RUN] Aucune écriture Shopify ne sera effectuée.")

    # ── Couche transport ──────────────────────────────────────────────────────

    def _run(self, query: str, variables: dict | None = None) -> dict:
        """
        Envoie une requête GraphQL. Retente automatiquement en cas de throttling.
        Retourne le dict complet de la réponse (champ 'data').
        """
        payload = {"query": query, "variables": variables or {}}
        for attempt in range(1, _MAX_RETRIES + 1):
            time.sleep(_REQUEST_DELAY_S)
            resp = self._session.post(config.SHOPIFY_GRAPHQL_URL, json=payload)

            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", "2"))
                logger.warning("Rate limit Shopify — attente %ds (tentative %d/%d)", wait, attempt, _MAX_RETRIES)
                time.sleep(wait)
                continue

            resp.raise_for_status()
            data = resp.json()

            # Erreurs GraphQL de niveau protocole
            if "errors" in data:
                first_error = data["errors"][0]
                # THROTTLED est une erreur récupérable
                if first_error.get("extensions", {}).get("code") == "THROTTLED":
                    wait = 2 * attempt
                    logger.warning("THROTTLED Shopify — attente %ds (tentative %d/%d)", wait, attempt, _MAX_RETRIES)
                    time.sleep(wait)
                    continue
                raise ShopifyGraphQLError(f"Erreur GraphQL Shopify : {data['errors']}")

            return data.get("data", {})

        raise ShopifyGraphQLError(f"Shopify non disponible après {_MAX_RETRIES} tentatives.")

    def _run_mutation(self, query: str, variables: dict | None = None) -> dict:
        """Comme _run() mais bloqué en dry-run pour toutes les mutations."""
        if self.dry_run:
            logger.info("[DRY-RUN] Mutation ignorée : %s", query.split("(")[0].strip()[:80])
            return {}
        return self._run(query, variables)

    # ── Recherche produit ─────────────────────────────────────────────────────

    def find_product_by_sku(self, sku: str) -> str | None:
        """Retourne le GID Shopify du produit dont un variant a ce SKU, ou None."""
        query = """
        query FindProductBySku($query: String!) {
          productVariants(first: 1, query: $query) {
            edges {
              node {
                id
                sku
                product { id title }
              }
            }
          }
        }
        """
        data = self._run(query, {"query": f"sku:{sku}"})
        edges = data.get("productVariants", {}).get("edges", [])
        if edges:
            product_id = edges[0]["node"]["product"]["id"]
            logger.debug("Produit trouvé par SKU '%s' : %s", sku, product_id)
            return product_id
        logger.warning("Aucun produit Shopify trouvé pour SKU : %s", sku)
        return None

    def find_product_by_handle(self, handle: str) -> str | None:
        """Retourne le GID Shopify du produit avec ce handle, ou None."""
        query = """
        query FindProductByHandle($handle: String!) {
          productByHandle(handle: $handle) {
            id
            title
          }
        }
        """
        data = self._run(query, {"handle": handle})
        product = data.get("productByHandle")
        if product:
            logger.debug("Produit trouvé par handle '%s' : %s", handle, product["id"])
            return product["id"]
        logger.warning("Aucun produit Shopify trouvé pour handle : %s", handle)
        return None

    def find_product_by_wee_id(self, wee_product_id: str) -> str | None:
        """
        Cherche un produit Shopify via le metafield custom.wee_product_id.
        À utiliser quand les produits ont déjà été migrés avec cet identifiant.
        """
        query = """
        query FindByWeeId($namespace: String!, $key: String!, $value: String!) {
          products(first: 1, query: $value) {
            edges {
              node {
                id
                metafield(namespace: $namespace, key: $key) {
                  value
                }
              }
            }
          }
        }
        """
        # Note : la recherche full-text Shopify ne filtre pas sur les metafields directement.
        # Cette implémentation est un placeholder — en production, maintenez une table
        # de correspondance wee_product_id ↔ shopify_gid dans un CSV séparé.
        logger.warning(
            "find_product_by_wee_id(%s) : non implémenté — utilisez un CSV de mapping.",
            wee_product_id,
        )
        return None

    # ── Metaobjects ───────────────────────────────────────────────────────────

    def find_metaobject_by_wee_id(self, wee_designer_id: str) -> str | None:
        """
        Cherche un metaobject 'designer' existant dont le champ wee_designer_id
        correspond à l'ID fourni. Retourne son GID ou None.
        """
        query = """
        query FindDesignerMetaobject($type: String!, $query: String!) {
          metaobjects(type: $type, first: 5, query: $query) {
            edges {
              node {
                id
                fields {
                  key
                  value
                }
              }
            }
          }
        }
        """
        data = self._run(query, {
            "type": "designer",
            "query": f"wee_designer_id:{wee_designer_id}",
        })
        edges = data.get("metaobjects", {}).get("edges", [])
        for edge in edges:
            for field in edge["node"].get("fields", []):
                if field["key"] == "wee_designer_id" and field["value"] == str(wee_designer_id):
                    gid = edge["node"]["id"]
                    logger.debug("Metaobject designer trouvé (wee_id=%s) : %s", wee_designer_id, gid)
                    return gid
        return None

    def create_metaobject(self, fields: dict[str, str]) -> str | None:
        """
        Crée un metaobject 'designer'. Retourne son GID ou None (dry-run).
        `fields` est un dict {key: value} correspondant aux champs du metaobject.
        """
        fields_input = [{"key": k, "value": v} for k, v in fields.items() if v]
        query = """
        mutation CreateDesigner($metaobject: MetaobjectCreateInput!) {
          metaobjectCreate(metaobject: $metaobject) {
            metaobject {
              id
              handle
            }
            userErrors {
              field
              message
              code
            }
          }
        }
        """
        variables = {
            "metaobject": {
                "type": "designer",
                "fields": fields_input,
            }
        }
        data = self._run_mutation(query, variables)
        if not data:
            return None  # dry-run

        result = data.get("metaobjectCreate", {})
        user_errors = result.get("userErrors", [])
        if user_errors:
            raise ShopifyGraphQLError(f"userErrors lors de la création du metaobject : {user_errors}")

        gid = result["metaobject"]["id"]
        logger.info("Metaobject designer créé : %s", gid)
        return gid

    def update_metaobject(self, gid: str, fields: dict[str, str]) -> None:
        """Met à jour les champs d'un metaobject existant."""
        fields_input = [{"key": k, "value": v} for k, v in fields.items() if v]
        query = """
        mutation UpdateDesigner($id: ID!, $metaobject: MetaobjectUpdateInput!) {
          metaobjectUpdate(id: $id, metaobject: $metaobject) {
            metaobject {
              id
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        data = self._run_mutation(query, {"id": gid, "metaobject": {"fields": fields_input}})
        if not data:
            return  # dry-run

        user_errors = data.get("metaobjectUpdate", {}).get("userErrors", [])
        if user_errors:
            raise ShopifyGraphQLError(f"userErrors lors de la mise à jour : {user_errors}")
        logger.info("Metaobject designer mis à jour : %s", gid)

    # ── Metafields produit ────────────────────────────────────────────────────

    def get_product_metafield(self, product_gid: str, namespace: str, key: str) -> str | None:
        """Retourne la valeur du metafield produit, ou None s'il n'existe pas."""
        query = """
        query GetMetafield($id: ID!, $namespace: String!, $key: String!) {
          product(id: $id) {
            metafield(namespace: $namespace, key: $key) {
              id
              value
            }
          }
        }
        """
        data = self._run(query, {"id": product_gid, "namespace": namespace, "key": key})
        mf = data.get("product", {}).get("metafield")
        return mf["id"] if mf else None

    def set_product_metafield(
        self,
        product_gid: str,
        namespace: str,
        key: str,
        value: str,
        metafield_type: str = "metaobject_reference",
    ) -> None:
        """Crée ou met à jour un metafield sur un produit Shopify."""
        query = """
        mutation SetMetafield($metafields: [MetafieldsSetInput!]!) {
          metafieldsSet(metafields: $metafields) {
            metafields {
              id
              key
              value
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        variables = {
            "metafields": [{
                "ownerId": product_gid,
                "namespace": namespace,
                "key": key,
                "value": value,
                "type": metafield_type,
            }]
        }
        data = self._run_mutation(query, variables)
        if not data:
            return  # dry-run

        user_errors = data.get("metafieldsSet", {}).get("userErrors", [])
        if user_errors:
            raise ShopifyGraphQLError(f"userErrors metafield : {user_errors}")
        logger.info("Metafield %s.%s mis à jour sur %s", namespace, key, product_gid)
