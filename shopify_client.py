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

    def find_image_gid_by_filename(self, filename: str) -> str | None:
        """
        Cherche dans Shopify Files une image par nom de fichier.
        Stratégies successives :
          1. filename exact (ex: abc123.jpg)
          2. stem seul sans extension (Shopify peut tronquer les noms longs)
          3. 40 premiers caractères du stem (hash tronqué)
        Retourne le GID (MediaImage) ou None si introuvable.
        """
        from pathlib import Path as _Path
        stem = _Path(filename).stem          # sans extension
        candidates = [filename, stem, stem[:40]]
        search_terms = list(dict.fromkeys(c for c in candidates if c))

        gql = """
        query FindFile($query: String!) {
          files(first: 5, query: $query) {
            edges {
              node {
                ... on MediaImage {
                  id
                  fileStatus
                  image { url }
                }
              }
            }
          }
        }
        """
        for term in search_terms:
            try:
                data = self._run(gql, {"query": f"filename:{term}"})
                edges = data.get("files", {}).get("edges", [])
                for edge in edges:
                    node = edge.get("node", {})
                    gid = node.get("id")
                    if gid:
                        logger.debug("Image trouvée par term '%s…' : %s", term[:20], gid)
                        return gid
            except Exception as exc:
                logger.debug("Erreur recherche image '%s…' : %s", term[:20], exc)
        return None

    def upload_image_from_bytes(self, filename: str, file_bytes: bytes) -> str | None:
        """
        Pipeline complet : staged upload → POST S3 → fileCreate.
        Retourne le GID MediaImage ou None (dry-run / erreur).
        """
        if self.dry_run:
            logger.info("[DRY-RUN] Upload image ignoré : %s", filename[:40])
            return None

        import mimetypes
        content_type = mimetypes.guess_type(filename)[0] or "image/jpeg"

        # 1. Staged upload (création du target S3)
        target = self.staged_upload_create(filename, content_type, len(file_bytes))
        upload_url = target["url"]
        resource_url = target["resourceUrl"]
        params = {p["name"]: p["value"] for p in target["parameters"]}

        # 2. POST binaire vers S3
        import requests as _req
        files_payload = {**params, "file": (filename, file_bytes, content_type)}
        resp = _req.post(upload_url, files=files_payload)
        if resp.status_code not in (200, 201, 204):
            logger.error(
                "Erreur S3 upload '%s' : HTTP %d — %s",
                filename[:40], resp.status_code, resp.text[:200],
            )
            return None
        logger.debug("S3 upload OK : %s", filename[:40])

        # 3. fileCreate
        return self.file_create(resource_url, filename)

    def create_metaobject(self, fields: dict[str, str]) -> str | None:
        """
        Crée un metaobject 'designer' avec statut ACTIVE. Retourne son GID ou None (dry-run).
        """
        fields_input = [{"key": k, "value": v} for k, v in fields.items() if v]
        query = """
        mutation CreateDesigner($metaobject: MetaobjectCreateInput!) {
          metaobjectCreate(metaobject: $metaobject) {
            metaobject {
              id
              handle
              capabilities {
                publishable { status }
              }
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
                "capabilities": {
                    "publishable": {"status": "ACTIVE"}
                },
            }
        }
        data = self._run_mutation(query, variables)
        if not data:
            return None  # dry-run

        result = data.get("metaobjectCreate", {})
        user_errors = result.get("userErrors", [])
        if user_errors:
            raise ShopifyGraphQLError(f"userErrors lors de la création du metaobject : {user_errors}")

        mo = result["metaobject"]
        status = mo.get("capabilities", {}).get("publishable", {}).get("status", "?")
        logger.info("Metaobject designer créé : %s (statut=%s)", mo["id"], status)
        return mo["id"]

    def update_metaobject(self, gid: str, fields: dict[str, str]) -> None:
        """Met à jour les champs d'un metaobject existant et force le statut ACTIVE."""
        fields_input = [{"key": k, "value": v} for k, v in fields.items() if v]
        query = """
        mutation UpdateDesigner($id: ID!, $metaobject: MetaobjectUpdateInput!) {
          metaobjectUpdate(id: $id, metaobject: $metaobject) {
            metaobject {
              id
              capabilities {
                publishable { status }
              }
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        payload = {
            "fields": fields_input,
            "capabilities": {"publishable": {"status": "ACTIVE"}},
        }
        data = self._run_mutation(query, {"id": gid, "metaobject": payload})
        if not data:
            return  # dry-run

        result = data.get("metaobjectUpdate", {})
        user_errors = result.get("userErrors", [])
        if user_errors:
            raise ShopifyGraphQLError(f"userErrors lors de la mise à jour : {user_errors}")
        status = result.get("metaobject", {}).get("capabilities", {}).get("publishable", {}).get("status", "?")
        logger.info("Metaobject designer mis à jour : %s (statut=%s)", gid, status)

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

    # ── Upload d'images vers Shopify Files ────────────────────────────────────

    def staged_upload_create(self, filename: str, content_type: str, file_size: int) -> dict:
        """
        Crée un staged upload Shopify. Retourne {url, resourceUrl, parameters}.
        Doit être appelé avant l'upload binaire vers S3.
        """
        query = """
        mutation StagedUploadsCreate($input: [StagedUploadInput!]!) {
          stagedUploadsCreate(input: $input) {
            stagedTargets {
              url
              resourceUrl
              parameters { name value }
            }
            userErrors { field message }
          }
        }
        """
        variables = {
            "input": [{
                "resource": "IMAGE",
                "filename": filename,
                "mimeType": content_type,
                "fileSize": str(file_size),
                "httpMethod": "POST",
            }]
        }
        # Note : staged upload est une mutation mais elle ne modifie pas de données
        # publiques — on l'autorise même en dry-run pour tester le flux.
        data = self._run(query, variables)
        result = data.get("stagedUploadsCreate", {})
        user_errors = result.get("userErrors", [])
        if user_errors:
            raise ShopifyGraphQLError(f"userErrors stagedUpload : {user_errors}")
        targets = result.get("stagedTargets", [])
        if not targets:
            raise ShopifyGraphQLError("Aucun stagedTarget retourné par Shopify")
        return targets[0]

    def file_create(self, resource_url: str, filename: str) -> str | None:
        """
        Crée un fichier dans Shopify Files depuis un staged upload.
        Retourne le GID MediaImage ou None (dry-run).
        """
        query = """
        mutation FileCreate($files: [FileCreateInput!]!) {
          fileCreate(files: $files) {
            files {
              ... on MediaImage {
                id
                fileStatus
                image { url }
              }
            }
            userErrors { field message }
          }
        }
        """
        variables = {
            "files": [{
                "originalSource": resource_url,
                "contentType": "IMAGE",
                "filename": filename,
            }]
        }
        data = self._run_mutation(query, variables)
        if not data:
            return None  # dry-run

        result = data.get("fileCreate", {})
        user_errors = result.get("userErrors", [])
        if user_errors:
            raise ShopifyGraphQLError(f"userErrors fileCreate : {user_errors}")

        files = result.get("files", [])
        if not files:
            raise ShopifyGraphQLError("fileCreate : aucun fichier retourné")

        gid = files[0].get("id")
        status = files[0].get("fileStatus", "?")
        logger.info("Fichier Shopify créé : %s (statut=%s)", gid, status)
        return gid

    def wait_for_file_ready(self, gid: str, max_attempts: int = 10, delay: float = 2.0) -> bool:
        """Poll jusqu'à ce que le fichier soit en statut READY. Retourne True si prêt."""
        query = """
        query FileStatus($id: ID!) {
          node(id: $id) {
            ... on MediaImage {
              id
              fileStatus
            }
          }
        }
        """
        for attempt in range(1, max_attempts + 1):
            data = self._run(query, {"id": gid})
            status = data.get("node", {}).get("fileStatus", "UNKNOWN")
            if status == "READY":
                return True
            if status == "FAILED":
                logger.warning("Fichier %s en statut FAILED — upload échoué.", gid)
                return False
            # UPLOADING, UPLOADED, PROCESSING → état transitoire, on continue
            logger.debug("Fichier %s : %s (tentative %d/%d)", gid, status, attempt, max_attempts)
            time.sleep(delay)
        logger.warning("Fichier %s non prêt après %d tentatives.", gid, max_attempts)
        return False

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

    # ── Suppression metaobject ────────────────────────────────────────────────

    def delete_metaobject(self, gid: str) -> bool:
        """Supprime un metaobject par GID. Retourne True si supprimé."""
        query = """
        mutation DeleteMetaobject($id: ID!) {
          metaobjectDelete(id: $id) {
            deletedId
            userErrors { field message }
          }
        }
        """
        data = self._run_mutation(query, {"id": gid})
        if not data:
            return False  # dry-run
        result = data.get("metaobjectDelete", {})
        user_errors = result.get("userErrors", [])
        if user_errors:
            raise ShopifyGraphQLError(f"userErrors metaobjectDelete : {user_errors}")
        return bool(result.get("deletedId"))

    def list_all_designer_metaobjects(self) -> list[str]:
        """Retourne les GIDs de tous les metaobjects de type 'designer' (paginé)."""
        query = """
        query ListDesigners($after: String) {
          metaobjects(type: "designer", first: 250, after: $after) {
            edges { node { id } }
            pageInfo { hasNextPage endCursor }
          }
        }
        """
        gids: list[str] = []
        cursor = None
        while True:
            data = self._run(query, {"after": cursor})
            result = data.get("metaobjects", {})
            gids.extend(e["node"]["id"] for e in result.get("edges", []))
            page_info = result.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info["endCursor"]
        return gids

    # ── Listing produits (pour build_product_mapping) ─────────────────────────

    def list_all_products_with_wee_id(self) -> dict[str, str]:
        """
        Retourne {wee_product_id: shopify_gid} pour tous les produits ayant
        le metafield custom.wee_product_id renseigné.
        """
        query = """
        query GetProductsWeeId($after: String) {
          products(first: 250, after: $after) {
            edges {
              node {
                id
                metafield(namespace: "custom", key: "wee_product_id") { value }
              }
            }
            pageInfo { hasNextPage endCursor }
          }
        }
        """
        mapping: dict[str, str] = {}
        cursor = None
        page = 0
        while True:
            page += 1
            data = self._run(query, {"after": cursor})
            result = data.get("products", {})
            for edge in result.get("edges", []):
                node = edge["node"]
                mf = node.get("metafield")
                if mf and mf.get("value"):
                    mapping[mf["value"]] = node["id"]
            page_info = result.get("pageInfo", {})
            logger.debug("Page %d : %d entrée(s) avec wee_product_id", page, len(mapping))
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info["endCursor"]
        return mapping

    def list_all_products_by_handle(self) -> dict[str, str]:
        """Retourne {handle: shopify_gid} pour tous les produits Shopify (paginé)."""
        query = """
        query GetProductHandles($after: String) {
          products(first: 250, after: $after) {
            edges { node { id handle } }
            pageInfo { hasNextPage endCursor }
          }
        }
        """
        mapping: dict[str, str] = {}
        cursor = None
        page = 0
        while True:
            page += 1
            data = self._run(query, {"after": cursor})
            result = data.get("products", {})
            for edge in result.get("edges", []):
                node = edge["node"]
                mapping[node["handle"]] = node["id"]
            page_info = result.get("pageInfo", {})
            logger.debug("Page %d : %d produit(s) chargés au total", page, len(mapping))
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info["endCursor"]
        return mapping
