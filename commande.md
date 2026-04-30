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

> Contrôle : connexion MariaDB, existence des tables, présence du type Designer (ID=139), comptage des données.  
> Résultat attendu : tous les contrôles `[OK]`.

---

### Étape 0.2 — Créer le type metaobject `designer` dans Shopify

> **À exécuter UNE SEULE FOIS.** Si le type existe déjà, le script le détecte et ne modifie rien.

```bash
python setup_shopify.py
```

> Résultat attendu : `Type 'designer' créé` ou `Type 'designer' déjà existant — aucune modification`.

---

## PHASE 1 — Test sur un seul produit

### Étape 1.1 — Exporter le produit de test depuis MariaDB vers CSV

```bash
python export_designers_to_csv.py --test
```

> Génère `output/designers.csv` et `output/product_designer_links.csv` filtrés sur `TEST_PRODUCT_ID`.  
> Vérifier les fichiers CSV : les colonnes `image_id` et `image_ext` doivent être remplies.

---

### Étape 1.2 — Aperçu avant import (lecture seule, aucun appel API)

```bash
python import_designers_to_shopify.py --test --preview
```

> Génère `output/import_preview.csv` et affiche dans la console :
>
> - Nombre de designers à créer / déjà en cache
> - Statut des images (cache ou à uploader)
> - Liens produit mappables ou manquants
>
> Vérifier que `product_status = mappable` pour le produit de test.

---

### Étape 1.3 — Import en dry-run (simulation, aucune écriture Shopify)

```bash
python import_designers_to_shopify.py --test --dry-run
```

> Simule la création des metaobjects et liaisons sans rien écrire dans Shopify.  
> Vérifier `output/import_report.csv` : zéro ligne `statut=error`.  
> Les logs sont dans `logs/import_YYYYMMDD_HHMMSS.log`.

---

### Étape 1.4 — Import réel sur le produit de test

> **Irréversible.** Créé les metaobjects et metafields dans Shopify.

```bash
python import_designers_to_shopify.py --test --no-dry-run
```

> Résultat attendu dans les logs :
>
> - `action_metaobject = created`
> - `action_metafield = created`
> - `statut = ok`
>
> `output/import_state.csv` est créé avec le GID Shopify du designer.

---

### Étape 1.5 — Vérifier le résultat dans Shopify

```bash
python verify_shopify.py
```

> Vérifie pour le produit de test :
>
> - Définition du type `designer` (champs et types)
> - Metaobject : statut ACTIVE, image présente, locale `fr-FR`, baseline
> - Metafield produit `custom.designer` lié au bon GID
>
> Résultat attendu : tous les contrôles `[OK]`.

---

## PHASE 2 — Import global (tous les designers)

> **Seulement après validation complète de la Phase 1.**

### Étape 2.1 — Export global depuis MariaDB

```bash
python export_designers_to_csv.py --global-export
```

> Génère `output/designers.csv` et `output/product_designer_links.csv` complets.

---

### Étape 2.2 — (Optionnel) Pré-uploader toutes les images en batch

> Utile si vous avez beaucoup d'images. Évite les timeouts pendant l'import.

```bash
python upload_images_to_shopify.py
```

> Remplit `output/image_gid_map.csv`. L'import suivant utilisera ce cache.

---

### Étape 2.3 — Aperçu global

```bash
python import_designers_to_shopify.py --global-import --preview
```

> Vérifier les compteurs : designers à créer, images à uploader, liens sans mapping.

---

### Étape 2.4 — Import global en dry-run

```bash
python import_designers_to_shopify.py --global-import --dry-run
```

> Vérifier `output/import_report.csv` : zéro `statut=error` avant de continuer.

---

### Étape 2.5 — Import global réel

> **Irréversible. Action volontaire.**

```bash
python import_designers_to_shopify.py --global-import --no-dry-run
```

> L'import reprend automatiquement là où il s'est arrêté si interrompu (`import_state.csv` pour Phase 1, `link_state.csv` pour Phase 2).

---

## Commandes utilitaires

### Remettre les caches à zéro (nouveau départ)

```bash
python import_designers_to_shopify.py --reset-cache
```

> Supprime `output/import_state.csv` et `output/image_gid_map.csv`.  
> À utiliser si vous voulez forcer un re-import complet depuis zéro.

---

### Re-vérifier Shopify à tout moment

```bash
python verify_shopify.py
```

---

## Résumé des fichiers générés

| Fichier | Contenu |
| --- | --- |
| `output/designers.csv` | Données designers exportées depuis MariaDB |
| `output/product_designer_links.csv` | Liaisons produit ↔ designer |
| `output/import_report.csv` | Rapport de chaque run d'import |
| `output/import_preview.csv` | Aperçu généré par `--preview` |
| `output/import_state.csv` | Cache Phase 1 — metaobjects (reprise après interruption) |
| `output/link_state.csv` | Cache Phase 2 — liens produit (reprise après interruption) |
| `output/image_gid_map.csv` | Cache des GID Shopify des images |
| `logs/import_*.log` | Logs horodatés de chaque import |
| `logs/export_*.log` | Logs horodatés de chaque export |
