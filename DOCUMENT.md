# Documentation complète — Migration Designer (Wee → Shopify)

## Sommaire

1. [Vue d'ensemble](#1-vue-densemble)
2. [Architecture et fichiers](#2-architecture-et-fichiers)
3. [Configuration (.env)](#3-configuration-env)
4. [Étape 0 — Vérification de la base de données](#4-étape-0--vérification-de-la-base-de-données)
5. [Étape 1 — Export MariaDB → CSV](#5-étape-1--export-mariadb--csv)
6. [Étape 2 — Setup Shopify (une seule fois)](#6-étape-2--setup-shopify-une-seule-fois)
7. [Étape 3 — Import CSV → Shopify](#7-étape-3--import-csv--shopify)
8. [Fichiers CSV produits](#8-fichiers-csv-produits)
9. [Rapport d'import](#9-rapport-dimport)
10. [Mode dry-run vs écriture réelle](#10-mode-dry-run-vs-écriture-réelle)
11. [Gestion des erreurs et retry](#11-gestion-des-erreurs-et-retry)
12. [Workflow complet recommandé](#12-workflow-complet-recommandé)

---

## 1. Vue d'ensemble

Ce projet migre les données **Designer** depuis une base **MariaDB** (plateforme Wee) vers **Shopify** via l'API GraphQL Admin.

```
MariaDB (Wee)
     │
     │  export_designers_to_csv.py
     ▼
output/designers.csv
output/product_designer_links.csv
     │
     │  import_designers_to_shopify.py
     ▼
Shopify (Metaobjects + Metafields)
```

**Pourquoi passer par des CSV ?**
- Contrôle total avant tout envoi vers Shopify
- Possibilité de tester sur un seul produit
- Relances propres en cas d'erreur partielle
- Traçabilité complète via `import_report.csv`

---

## 2. Architecture et fichiers

```
designer-migration/
├── .env                             # Variables d'environnement (ne pas committer)
├── .env.example                     # Modèle à copier pour créer .env
├── requirements.txt                 # Dépendances Python
│
├── config.py                        # Lecture centralisée du .env
├── db.py                            # Connexion MariaDB (context manager)
│
├── check_db.py                      # Diagnostic base de données
├── export_designers_to_csv.py       # Étape 1 : SQL → CSV
├── setup_shopify.py                 # Création du type metaobject (une fois)
├── import_designers_to_shopify.py   # Étape 3 : CSV → Shopify
├── shopify_client.py                # Client GraphQL Shopify (bas niveau)
│
├── logs/                            # Logs horodatés générés automatiquement
└── output/                          # CSV générés
    ├── designers.csv                # Données designers (une ligne par langue)
    ├── product_designer_links.csv   # Liaisons produit ↔ designer
    └── import_report.csv            # Rapport de chaque import
```

### Rôle de chaque fichier Python

| Fichier | Rôle |
|---|---|
| [config.py](config.py) | Charge le `.env` et expose toutes les variables. Lève une erreur claire si une variable obligatoire est manquante. |
| [db.py](db.py) | Fournit `get_connection()` (context manager) et `fetch_all()` pour exécuter des requêtes MariaDB. |
| [check_db.py](check_db.py) | Vérifie la connexion, les tables, le type Designer et compte les données disponibles. |
| [export_designers_to_csv.py](export_designers_to_csv.py) | Interroge MariaDB et écrit `designers.csv` + `product_designer_links.csv`. |
| [setup_shopify.py](setup_shopify.py) | Crée la définition du metaobject `designer` dans Shopify (à exécuter une seule fois). |
| [shopify_client.py](shopify_client.py) | Client GraphQL Shopify bas niveau : recherche produit, création/mise à jour metaobject, création metafield. |
| [import_designers_to_shopify.py](import_designers_to_shopify.py) | Orchestre l'import : lit les CSV, appelle le client Shopify, génère le rapport. |

---

## 3. Configuration (.env)

Copiez `.env.example` en `.env` et renseignez toutes les valeurs.

```ini
# ── Base de données MariaDB ──────────────────────────────────────────────────
DB_HOST=localhost
DB_PORT=3306
DB_NAME=nom_de_la_base
DB_USER=utilisateur
DB_PASSWORD=mot_de_passe

# ── Shopify Admin API ────────────────────────────────────────────────────────
SHOPIFY_STORE=votre-boutique.myshopify.com
SHOPIFY_ACCESS_TOKEN=shpat_xxxxxxxxxxxxxxxxxxxx
SHOPIFY_API_VERSION=2025-01

# ── Paramètres d'export ──────────────────────────────────────────────────────
OUTPUT_DIR=output
DESIGNER_TYPE_ID=139         # ID du type Designer dans MariaDB
DEFAULT_TRANS_ID=1           # ID de la langue par défaut (1 = FR)

# ── Produit de test (au moins un des trois) ──────────────────────────────────
TEST_PRODUCT_ID=12345        # ID Wee du produit de test
TEST_PRODUCT_SKU=MON-SKU-001 # SKU Shopify du produit de test
TEST_PRODUCT_HANDLE=mon-handle

# ── Sécurité ─────────────────────────────────────────────────────────────────
DRY_RUN=true                 # true = simulation, false = écriture réelle
```

### Variables obligatoires

| Variable | Obligatoire | Description |
|---|---|---|
| `DB_HOST` | Oui | Hôte MariaDB |
| `DB_PORT` | Non (3309) | Port MariaDB |
| `DB_NAME` | Oui | Nom de la base |
| `DB_USER` | Oui | Utilisateur DB |
| `DB_PASSWORD` | Oui | Mot de passe DB |
| `SHOPIFY_STORE` | Oui | Domaine `.myshopify.com` |
| `SHOPIFY_ACCESS_TOKEN` | Oui | Token Admin API |
| `DRY_RUN` | Non (true) | Mode simulation |
| `TEST_PRODUCT_ID` | Pour le mode `--test` | ID Wee du produit de test |

---

## 4. Étape 0 — Vérification de la base de données

Avant tout export, vérifiez que la base est accessible et contient les données attendues.

```bash
python check_db.py
```

### Ce que le script vérifie

1. **Connexion MariaDB** — répond à `SELECT 1`
2. **Tables requises** — les 7 tables nécessaires existent :
   - `product_ref_type`
   - `product_ref_type_trans`
   - `product_refs_nested`
   - `product_refs_nested_trans`
   - `product_refs_enumerable_value`
   - `image`
   - `trans`
3. **Type Designer** — `product_ref_type.id = 139` (ou votre `DESIGNER_TYPE_ID`)
4. **Langue par défaut** — `trans.id = 1` (ou votre `DEFAULT_TRANS_ID`)
5. **Comptages** — nombre de designers, traductions, liaisons, produits distincts
6. **Produit de test** — si `TEST_PRODUCT_ID` est renseigné, vérifie qu'il a des designers liés

### Codes de sortie

| Code | Signification |
|---|---|
| `0` | Tous les contrôles passent — prêt pour la migration |
| `1` | Au moins un contrôle critique échoue |

### Exemple de sortie

```
[OK  ] Connexion MariaDB
[OK  ] Tables requises (toutes présentes)
[OK  ] Type Designer ID=139 dans product_ref_type
[OK  ] DEFAULT_TRANS_ID=1 dans trans (locale=fr_FR)
  Designers distincts         : 42
  Lignes de traduction        : 168
  Liaisons produit<->designer : 315
  Produits distincts liés     : 210
RÉSULTAT : OK — la base est prête pour la migration.
```

---

## 5. Étape 1 — Export MariaDB → CSV

Ce script lit la base MariaDB et génère deux fichiers CSV dans `output/`.

```bash
# Mode test (un seul produit — recommandé pour valider)
python export_designers_to_csv.py --test

# Mode global (tous les designers et toutes les liaisons)
python export_designers_to_csv.py --global-export
```

### Schéma de données source (MariaDB)

```
product_ref_type         → type "Designer" (ID = 139)
product_refs_nested      → entités Designer (id, image_id, color)
product_refs_nested_trans → contenu éditorial multilingue (name, slug, body…)
product_refs_enumerable_value → liaison produit ↔ designer
image                    → images (signature + extension)
trans                    → langues (id, locale)
```

### Requêtes SQL exécutées

**Mode `--global-export` — tous les designers :**
```sql
SELECT
    prn.id          AS wee_designer_id,
    prnt.name       AS nom,
    prnt.baseline   AS baseline,
    prnt.intro      AS introduction,
    prnt.body       AS texte,
    prnt.slug       AS slug,
    CONCAT(img.signature, '.', img.extension) AS image_file,
    prn.color       AS couleur,
    t.locale        AS langue
FROM product_refs_nested prn
LEFT JOIN product_refs_nested_trans prnt ON prnt.product_refs_nested_id = prn.id
LEFT JOIN image img                       ON img.id = prn.image_id
LEFT JOIN trans t                         ON t.id  = prnt.trans_id
WHERE prn.product_ref_type_id = ?          -- DESIGNER_TYPE_ID = 139
ORDER BY prn.id, t.locale
```

**Mode `--test` — un produit spécifique :**  
Même requête + `INNER JOIN product_refs_enumerable_value` filtré sur `product_id = TEST_PRODUCT_ID`.

**Liaisons produit ↔ designer :**
```sql
SELECT
    prev.product_id,
    prev.product_refs_nested_id AS wee_designer_id,
    prnt.name                   AS designer_nom
FROM product_refs_enumerable_value prev
JOIN product_refs_nested_trans prnt
    ON prnt.product_refs_nested_id = prev.product_refs_nested_id
   AND prnt.trans_id = ?           -- DEFAULT_TRANS_ID = 1
WHERE prev.product_ref_type_id_for_unicity = ?  -- DESIGNER_TYPE_ID
```

### Déduplication

Le script supprime automatiquement les doublons :
- `designers.csv` : clé `(wee_designer_id, langue)`
- `product_designer_links.csv` : clé `(product_id, wee_designer_id)`

### Fichiers générés

Voir la section [Fichiers CSV produits](#8-fichiers-csv-produits) pour le détail des colonnes.

---

## 6. Étape 2 — Setup Shopify (une seule fois)

Avant le premier import, le type metaobject `designer` doit exister dans Shopify.

```bash
python setup_shopify.py
```

Le script vérifie d'abord si le type existe déjà. Si oui, il n'écrase rien. Si non, il crée la définition complète.

### Champs créés dans Shopify

| Clé | Nom affiché | Type Shopify | Obligatoire |
|---|---|---|---|
| `wee_designer_id` | Wee Designer ID | `single_line_text_field` | Non |
| `name` | Nom | `single_line_text_field` | Oui |
| `baseline` | Baseline | `single_line_text_field` | Non |
| `introduction` | Introduction | `multi_line_text_field` | Non |
| `body` | Description | `multi_line_text_field` | Non |
| `slug` | Slug | `single_line_text_field` | Non |
| `image_file` | Fichier image (nom) | `single_line_text_field` | Non |
| `color` | Couleur | `single_line_text_field` | Non |
| `locale` | Langue | `single_line_text_field` | Non |

> **Note :** `image_file` stocke uniquement le nom du fichier source. L'upload des images vers Shopify est un processus séparé et n'est pas géré par ce projet.

---

## 7. Étape 3 — Import CSV → Shopify

Ce script lit les CSV générés à l'étape 1 et pousse les données dans Shopify.

```bash
# Simulation test (sans écriture — recommandé en premier)
python import_designers_to_shopify.py --test

# Simulation globale
python import_designers_to_shopify.py --global-import --dry-run

# Import réel test (écriture sur un produit)
python import_designers_to_shopify.py --test --no-dry-run

# Import réel global (IRRÉVERSIBLE — uniquement après validation complète)
python import_designers_to_shopify.py --global-import --no-dry-run
```

### Priorité du mode dry-run

Le flag CLI surpasse toujours le `.env` :

```
--no-dry-run  >  --dry-run  >  DRY_RUN dans .env  >  true (défaut)
```

### Phase 1 — Import des metaobjects Designer

Pour chaque designer dans `designers.csv` :

1. Recherche d'un metaobject existant par `wee_designer_id` (via GraphQL)
2. Si trouvé → **mise à jour** des champs (`metaobjectUpdate`)
3. Si absent → **création** (`metaobjectCreate`)
4. Un seul metaobject par designer (la première occurrence dans le CSV, généralement FR)

**Champs envoyés à Shopify :**

| Champ CSV | Champ metaobject | Commentaire |
|---|---|---|
| `wee_designer_id` | `wee_designer_id` | Identifiant de traçabilité |
| `nom` | `nom` | Nom du designer |
| `baseline` | `baseline` | Phrase courte |
| `introduction` | `introduction` | Texte d'intro |
| `texte` | `texte` | Corps de texte HTML |
| `image_file` | — | Non envoyé (upload séparé) |

### Phase 2 — Liaison produit ↔ Designer

Pour chaque ligne dans `product_designer_links.csv` :

1. Récupération du GID du metaobject créé en Phase 1
2. Résolution du produit Shopify (par SKU ou handle, depuis `.env`)
3. Vérification d'un metafield `custom.designer` existant
4. Création ou mise à jour du metafield (`metafieldsSet`)

**Metafield créé sur le produit :**

| Propriété | Valeur |
|---|---|
| namespace | `custom` |
| key | `designer` |
| type | `metaobject_reference` |
| value | GID du metaobject Designer |

### Résolution produit Wee → Shopify

L'ordre de recherche pour trouver le produit Shopify :

1. `TEST_PRODUCT_SKU` → recherche par SKU variant
2. `TEST_PRODUCT_HANDLE` → recherche par handle produit
3. Metafield `custom.wee_product_id` → **non implémenté** (voir note ci-dessous)

> **Pour l'import global**, vous devez maintenir un CSV de correspondance `wee_product_id ↔ shopify_gid`. La recherche Shopify ne filtre pas directement sur les metafields. Cette table de mapping est à construire séparément.

---

## 8. Fichiers CSV produits

### `output/designers.csv`

Une ligne par **designer × langue**.

| Colonne | Type | Description |
|---|---|---|
| `wee_designer_id` | entier | ID dans la base MariaDB |
| `nom` | texte | Nom du designer |
| `baseline` | texte | Phrase d'accroche |
| `introduction` | texte | Texte d'introduction |
| `texte` | HTML | Description complète (peut contenir des balises `<p>`) |
| `slug` | texte | URL-slug du designer |
| `image_file` | texte | Nom du fichier image (ex: `abc123.jpg`) |
| `couleur` | texte | Couleur associée (code hex ou nom) |
| `langue` | locale | Code langue (ex: `fr_FR`, `en_US`, `de_DE`) |

**Exemple :**
```
wee_designer_id,nom,baseline,introduction,texte,slug,image_file,couleur,langue
1902,Emil Thorup,,Nous créons du mobilier...,<p>Emil Thorup est...</p>,emil-thorup,,,fr_FR
1902,Emil Thorup,,We create furniture...,<p>Emil Thorup is...</p>,emil-thorup,,,en_US
```

### `output/product_designer_links.csv`

Une ligne par **liaison produit ↔ designer** (langue par défaut uniquement).

| Colonne | Type | Description |
|---|---|---|
| `product_id` | entier | ID produit dans MariaDB (Wee) |
| `wee_designer_id` | entier | ID designer dans MariaDB |
| `designer_nom` | texte | Nom du designer (langue par défaut) |

**Exemple :**
```
product_id,wee_designer_id,designer_nom
193579,1902,Emil Thorup
```

---

## 9. Rapport d'import

Après chaque import, un fichier `output/import_report.csv` est généré.

### Colonnes du rapport

| Colonne | Description |
|---|---|
| `wee_designer_id` | ID designer Wee |
| `langue` | Langue traitée |
| `action_metaobject` | `created` / `updated` / `skipped` / `error` |
| `shopify_metaobject_gid` | GID Shopify du metaobject (ou `[DRY-RUN-xxx]`) |
| `wee_product_id` | ID produit Wee |
| `shopify_product_gid` | GID Shopify du produit |
| `action_metafield` | `created` / `updated` / `mapping_missing` / `skipped` / `error` |
| `statut` | `ok` / `skipped` / `error` |
| `message` | Détail en cas d'erreur ou de skip |

### Résumé affiché en fin d'import

```
RÉSUMÉ DE L'IMPORT
  Metaobjects créés      : 5
  Metaobjects mis à jour : 0
  Ignorés (déjà OK)      : 0
  Erreurs                : 0
  Metafields liés        : 5
  Produits sans mapping  : 0
```

### Exemple de rapport (import réel)

```csv
wee_designer_id,langue,action_metaobject,shopify_metaobject_gid,wee_product_id,...,statut
1902,de_DE,updated,gid://shopify/Metaobject/193323696267,,,,,ok,
1902,,,,193579,gid://shopify/Product/8135326007435,updated,ok,
```

---

## 10. Mode dry-run vs écriture réelle

| Mode | Metaobjects | Metafields | Rapport | Logs |
|---|---|---|---|---|
| dry-run | Aucune écriture (GID = `[DRY-RUN-xxx]`) | Aucune écriture | Généré | Généré |
| no-dry-run | Créés / mis à jour dans Shopify | Créés / mis à jour | Généré | Généré |

En dry-run, toutes les requêtes de **lecture** Shopify sont effectuées normalement (recherche de produit, vérification metaobject existant). Seules les **mutations** (écriture) sont bloquées.

---

## 11. Gestion des erreurs et retry

### Rate limit Shopify

Le client `ShopifyClient` gère automatiquement le throttling :

- Délai de base : **0.6 s** entre chaque requête (~2 req/s)
- Sur HTTP 429 : attend `Retry-After` secondes (en-tête Shopify)
- Sur erreur GraphQL `THROTTLED` : backoff exponentiel (`2 × tentative`)
- Maximum **5 tentatives** avant d'abandonner

### Erreurs non fatales

- Designer introuvable côté Shopify → log + ligne `mapping_missing` dans le rapport → continue
- Produit Shopify non résolu → log + ligne `mapping_missing` → continue
- `userErrors` Shopify → log erreur + ligne `error` dans le rapport → continue sur le suivant

### Erreurs fatales

- Variable `.env` manquante → arrêt immédiat avec message clair
- Fichier CSV absent → arrêt immédiat
- `TEST_PRODUCT_ID` absent en mode test → arrêt immédiat

---

## 12. Workflow complet recommandé

### Première fois (mise en place)

```bash
# 1. Copier et renseigner le .env
cp .env.example .env
# (éditer .env avec vos vraies valeurs)

# 2. Installer les dépendances
pip install -r requirements.txt

# 3. Vérifier la base de données
python check_db.py

# 4. Créer le type metaobject dans Shopify (une seule fois)
python setup_shopify.py
```

### Phase de test (recommandée avant tout import global)

```bash
# 5. Exporter un produit de test
python export_designers_to_csv.py --test

# 6. Vérifier output/designers.csv et output/product_designer_links.csv

# 7. Simuler l'import (dry-run)
python import_designers_to_shopify.py --test

# 8. Vérifier output/import_report.csv et les logs/

# 9. Import réel sur le produit de test
python import_designers_to_shopify.py --test --no-dry-run

# 10. Vérifier dans Shopify Admin que le metaobject et le metafield sont corrects
```

### Import global (production)

```bash
# 11. Export complet
python export_designers_to_csv.py --global-export

# 12. Vérifier les CSV (nombre de lignes, données)

# 13. Dry-run global
python import_designers_to_shopify.py --global-import --dry-run

# 14. Analyser import_report.csv — zéro erreur requis avant de continuer

# 15. Import réel global (IRRÉVERSIBLE)
python import_designers_to_shopify.py --global-import --no-dry-run

# 16. Vérifier le rapport final
```

### Checklist avant `--no-dry-run --global-import`

- [ ] `.env` renseigné avec les vraies valeurs de production
- [ ] `check_db.py` retourne code 0
- [ ] Export CSV testé sur un produit (`--test`) — données correctes
- [ ] `setup_shopify.py` exécuté — type `designer` présent dans Shopify Admin
- [ ] Import dry-run validé — `import_report.csv` sans erreur
- [ ] Correspondances produits Wee ↔ Shopify vérifiées (SKU ou handle corrects)
- [ ] Logs du dry-run analysés — zéro `mapping_missing` non attendu
