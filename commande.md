# Commandes — Migration Designer (Wee → Shopify)

Toutes les commandes à exécuter dans l'ordre, **une étape à la fois**.  
Chaque étape doit réussir avant de passer à la suivante.

---

## Prérequis

Remplir le fichier `.env` (copier depuis `.env.example`) avec :

- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`
- `SHOPIFY_STORE`, `SHOPIFY_ACCESS_TOKEN`
- `TEST_PRODUCT_ID` (ID Wee du produit de test)
- `TEST_PRODUCT_SKU` ou `TEST_PRODUCT_HANDLE` (pour retrouver le produit dans Shopify)
- `GOOGLE_CREDENTIALS_PATH` (chemin vers `credential-regardbeauty.json`)
- `DRY_RUN=true` (laisser activé jusqu'à validation complète)

Installer les dépendances :

```bash
pip install -r requirements.txt
```

---

## PHASE 0 — Vérifications préalables

### Étape 0.1 — Vérifier la connexion à la base de données

```bash
python check_db.py
```

> Contrôle : connexion MariaDB, tables, type Designer (ID=139), comptage.
> Résultat attendu : tous les contrôles `[OK]`.

---

### Étape 0.2 — Créer le type metaobject `designer` dans Shopify

> **À exécuter UNE SEULE FOIS.** Si le type existe déjà, le script ne modifie rien.

```bash
python setup_shopify.py
```

---

### Étape 0.3 — Corriger le nom d'affichage (une seule fois)

> Fait que Shopify affiche le nom du designer au lieu de "Designer #XXXXX".

```bash
python fix_designer_display_name.py
```

---

## PHASE 1 — Export MariaDB → CSV

```bash
python export_designers_to_csv.py --global-export
```

> Génère `output/designers.csv` et `output/product_designer_links.csv`.

---

## PHASE 2 — Mapping produits Wee → Shopify

Construit `output/product_mapping.csv` pour que l'import puisse lier chaque produit.  
Plusieurs stratégies disponibles — les lancer dans l'ordre pour maximiser la couverture.

### Stratégie 1 — EAN13 (recommandée en premier)

```bash
python build_product_mapping.py --from-db-ean --dry-run   # vérifier le taux de match
python build_product_mapping.py --from-db-ean
```

### Stratégie 2 — Titre produit (complément)

```bash
python build_product_mapping.py --from-db-title --dry-run
python build_product_mapping.py --from-db-title
```

### Stratégie 3 — Handle/slug (si EAN13 insuffisant)

```bash
python build_product_mapping.py --from-db-handle
```

### Stratégie 4 — Metafield Shopify (si wee_product_id déjà renseigné)

```bash
python build_product_mapping.py --from-shopify
```

### Diagnostic (comprendre les écarts)

```bash
python build_product_mapping.py --diagnose
```

> Les stratégies se **mergent** automatiquement dans `product_mapping.csv`.
> Lancez-les toutes pour obtenir la meilleure couverture.

---

## PHASE 3 — Upload des images vers Shopify

> Pré-uploade toutes les images depuis Google Drive vers Shopify Files.
> Remplit `output/image_gid_map.csv` (cache des GID images).

```bash
python upload_images_to_shopify.py
```

Options :

```bash
python upload_images_to_shopify.py --dry-run   # simuler sans uploader
python upload_images_to_shopify.py --test      # seulement les images du produit de test
```

> La recherche Drive suit l'ordre : bucket ciblé (`master/0000/XXXX/`) → exact global → fuzzy.
> Durée estimée : 30-60 min pour ~600 images.

---

## PHASE 4 — Import global vers Shopify

### Dry-run (vérification avant écriture)

```bash
python import_designers_to_shopify.py --global-import --dry-run
```

> Vérifier `output/import_report.csv` : zéro ligne `statut=error`.

### Import réel

```bash
python import_designers_to_shopify.py --global-import --no-dry-run
```

> **Irréversible.** Crée les metaobjects Designer et lie les produits.
> L'import reprend automatiquement depuis les caches si interrompu.

### Options avancées

```bash
# Écraser les liaisons produit existantes (par défaut : préservation MERGE)
python import_designers_to_shopify.py --global-import --no-dry-run --force-update

# Nettoyer les GIDs obsolètes du cache avant de relancer
python import_designers_to_shopify.py --purge-stale
```

---

## PHASE 5 — Audit et vérification

```bash
python audit_shopify.py
```

> Affiche un tableau de bord complet :
>
> - % Metaobjects créés dans Shopify
> - % Liaisons produit effectuées
> - % Images uploadées
> - % Mapping produits couverts
> - GIDs obsolètes dans le cache
> - Recommandations d'actions

Options :

```bash
python audit_shopify.py --no-api    # analyse locale uniquement (sans appels Shopify)
python audit_shopify.py --json      # sortie JSON pour traitement automatisé
```

---

## Commandes utilitaires

### Reset du cache des liens uniquement (sans toucher les metaobjects ni les images)

```bash
python import_designers_to_shopify.py --reset-link-cache
```

> Supprime `output/link_state.csv`.
> À utiliser quand des liens sont en cache mais pas réellement dans Shopify.
> Relancez ensuite `--global-import --no-dry-run`.

### Reset complet de tous les caches locaux

```bash
python import_designers_to_shopify.py --reset-cache
```

> Supprime `import_state.csv`, `link_state.csv`, `image_gid_map.csv`.
> ⚠️ Les images devront être ré-uploadées.

### Supprimer tous les metaobjects Designer dans Shopify

```bash
python cleanup_shopify.py
```

> ⚠️ Irréversible. Supprime toutes les entrées Designer dans Shopify.
> À utiliser uniquement pour repartir à zéro.

---

## Séquence rapide — Corriger sans repartir à zéro

Si les metaobjects existent déjà et qu'il faut juste corriger les liaisons et images :

```bash
python upload_images_to_shopify.py
python import_designers_to_shopify.py --reset-link-cache
python import_designers_to_shopify.py --global-import --no-dry-run
python audit_shopify.py
```

---

## Séquence complète — Repartir à zéro

```bash
python cleanup_shopify.py
python import_designers_to_shopify.py --reset-cache
python export_designers_to_csv.py --global-export
python build_product_mapping.py --from-db-ean
python build_product_mapping.py --from-db-title
python upload_images_to_shopify.py
python import_designers_to_shopify.py --global-import --no-dry-run
python audit_shopify.py
```

---

## Résumé des fichiers générés

| Fichier | Contenu |
| --- | --- |
| `output/designers.csv` | Données designers exportées depuis MariaDB |
| `output/product_designer_links.csv` | Liaisons produit ↔ designer |
| `output/product_mapping.csv` | Correspondances wee_product_id → shopify_gid |
| `output/image_gid_map.csv` | Cache des GID Shopify des images uploadées |
| `output/import_state.csv` | Cache Phase 1 — metaobjects (reprise après interruption) |
| `output/link_state.csv` | Cache Phase 2 — liens produit (reprise après interruption) |
| `output/import_report.csv` | Rapport détaillé de chaque run d'import |
| `logs/import_*.log` | Logs horodatés de chaque import |
| `logs/export_*.log` | Logs horodatés de chaque export |
| `logs/upload_images_*.log` | Logs horodatés de chaque upload d'images |
| `logs/product_mapping_*.log` | Logs horodatés de chaque build du mapping |
| `logs/audit_*.log` | Logs horodatés de chaque audit |
