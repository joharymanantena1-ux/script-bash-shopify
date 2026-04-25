# Designer Migration — Wee (MariaDB) → Shopify

## Contexte du projet

Migration des données "Designer" depuis une base MariaDB (plateforme Wee/Designer) vers Shopify via API GraphQL Admin.

**Approche retenue :** SQL MariaDB → CSV de contrôle → Import Shopify via API  
**Raison :** Contrôle total, possibilité de tester sur un seul produit, relances propres.

---

## Architecture du projet

```
designer-migration/
├── CLAUDE.md                        # Ce fichier
├── .env                             # Variables d'environnement (ne pas committer)
├── .env.example                     # Exemple de .env
├── requirements.txt
├── config.py                        # Lecture centralisée des variables .env
├── db.py                            # Connexion MariaDB
├── export_designers_to_csv.py       # Étape 1 : SQL → CSV
├── shopify_client.py                # Client GraphQL Shopify
├── import_designers_to_shopify.py   # Étape 2 : CSV → Shopify
├── logs/                            # Logs horodatés
└── output/                          # CSV générés
    ├── designers.csv
    ├── product_designer_links.csv
    └── import_report.csv
```

---

## Schéma de données source (MariaDB)

| Table | Rôle |
|---|---|
| `product_ref_type` | Type "Designer" (ID = 139) |
| `product_refs_nested` | Entités Designer (id, image_id, color) |
| `product_refs_nested_trans` | Contenu éditorial multilingue (name, slug, body…) |
| `product_refs_enumerable_value` | Liaison produit ↔ designer |
| `image` | Images (signature + extension) |
| `trans` | Langues (id, locale) |

**ID du type Designer :** `DESIGNER_TYPE_ID=139`  
**ID langue par défaut (FR) :** `DEFAULT_TRANS_ID=1`

---

## Mapping vers Shopify

### Metaobject `designer`

| Champ source (CSV) | Champ Shopify | Type Shopify |
|---|---|---|
| `wee_designer_id` | `wee_designer_id` | single_line_text_field |
| `nom` | `name` | single_line_text_field |
| `baseline` | `baseline` | single_line_text_field |
| `introduction` | `introduction` | multi_line_text_field |
| `texte` | `body` | multi_line_text_field |
| `slug` | `slug` | single_line_text_field |
| `image_file` | `image_file` | single_line_text_field |
| `couleur` | `color` | single_line_text_field |
| `langue` | `locale` | single_line_text_field |

### Metafield produit

| Propriété | Valeur |
|---|---|
| namespace | `custom` |
| key | `designer` |
| type | `metaobject_reference` |
| value | GID du metaobject Designer |

---

## Correspondance produit Wee → Shopify

Le `product_id` dans `product_designer_links.csv` est l'ID Wee (MariaDB).  
Le script recherche le produit Shopify correspondant par :
1. Metafield `custom.wee_product_id` (si déjà tagué)
2. SKU (via `TEST_PRODUCT_SKU`)
3. Handle (via `TEST_PRODUCT_HANDLE`)

Si aucune correspondance → log `"product mapping missing"` et skip.

---

## Workflow d'exécution

### Phase 1 — Export CSV

```bash
# Mode test (un produit)
python export_designers_to_csv.py --test

# Mode global
python export_designers_to_csv.py --global-export
```

### Phase 2 — Import Shopify

```bash
# Dry-run test (défaut)
python import_designers_to_shopify.py --test

# Dry-run global
python import_designers_to_shopify.py --global-import --dry-run

# Import réel test
python import_designers_to_shopify.py --test --no-dry-run

# Import réel global (volontaire)
python import_designers_to_shopify.py --global-import --no-dry-run
```

---

## Checklist avant production

- [ ] `.env` renseigné avec les vraies valeurs
- [ ] Export CSV testé sur un produit (`--test`)
- [ ] Metaobject type `designer` créé manuellement dans Shopify Admin
- [ ] Import dry-run validé (vérifier `import_report.csv`)
- [ ] Correspondances produits Wee ↔ Shopify vérifiées
- [ ] Zéro erreur dans les logs avant `--no-dry-run`

---

## Variables d'environnement importantes

| Variable | Usage |
|---|---|
| `DRY_RUN=true` | Désactive toute écriture Shopify |
| `TEST_PRODUCT_ID` | ID Wee du produit de test |
| `TEST_PRODUCT_SKU` | SKU Shopify du produit de test |
| `TEST_PRODUCT_HANDLE` | Handle Shopify du produit de test |
| `DESIGNER_TYPE_ID` | ID du type Designer dans MariaDB (139) |
| `DEFAULT_TRANS_ID` | ID langue par défaut (1 = FR) |

---

## Notes importantes

- Ne jamais committer `.env`
- Le mode `--global-import` avec `--no-dry-run` est **irréversible sur Shopify**
- Toujours valider `import_report.csv` après chaque dry-run
- Le metaobject type `designer` doit exister dans Shopify **avant** l'import
- L'image n'est pas uploadée automatiquement — `image_file` stocke le nom du fichier source
