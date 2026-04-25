"""
check_db.py — Vérification de la base de données avant migration.

Contrôle :
  1. Connexion MariaDB (host, port, credentials)
  2. Existence des 7 tables nécessaires au projet
  3. Présence du type Designer (DESIGNER_TYPE_ID=139)
  4. Comptage des données : designers, traductions, liaisons produits
  5. Présence du DEFAULT_TRANS_ID dans la table trans

Usage :
  python check_db.py

Code de sortie :
  0 — tous les contrôles passent
  1 — au moins un contrôle critique échoue
"""

import logging
import sys

import config
from db import get_connection, fetch_all

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# Tables requises pour le projet
REQUIRED_TABLES = [
    "product_ref_type",
    "product_ref_type_trans",
    "product_refs_nested",
    "product_refs_nested_trans",
    "product_refs_enumerable_value",
    "image",
    "trans",
]


def check_connection(conn) -> bool:
    """Vérifie que la connexion répond."""
    try:
        rows = fetch_all(conn, "SELECT 1 AS ok")
        return rows[0]["ok"] == 1
    except Exception as e:
        logger.error("La connexion ne répond pas : %s", e)
        return False


def check_tables(conn) -> tuple[bool, list[str]]:
    """Vérifie que toutes les tables requises existent dans la base."""
    rows = fetch_all(
        conn,
        "SELECT TABLE_NAME FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'",
        (config.DB_NAME,),
    )
    existing = {r["TABLE_NAME"] for r in rows}
    missing = [t for t in REQUIRED_TABLES if t not in existing]
    return len(missing) == 0, missing


def check_designer_type(conn) -> tuple[bool, dict | None]:
    """Vérifie que le type Designer (ID=139) existe dans product_ref_type."""
    rows = fetch_all(
        conn,
        "SELECT id FROM product_ref_type WHERE id = ?",
        (config.DESIGNER_TYPE_ID,),
    )
    found = rows[0] if rows else None
    return bool(found), found


def check_default_locale(conn) -> tuple[bool, dict | None]:
    """Vérifie que le DEFAULT_TRANS_ID existe dans la table trans."""
    rows = fetch_all(
        conn,
        "SELECT id, locale FROM trans WHERE id = ?",
        (config.DEFAULT_TRANS_ID,),
    )
    found = rows[0] if rows else None
    return bool(found), found


def count_designers(conn) -> int:
    """Nombre de designers distincts (toutes langues confondues)."""
    rows = fetch_all(
        conn,
        "SELECT COUNT(DISTINCT id) AS cnt FROM product_refs_nested "
        "WHERE product_ref_type_id = ?",
        (config.DESIGNER_TYPE_ID,),
    )
    return rows[0]["cnt"]


def count_translations(conn) -> int:
    """Nombre de lignes de traduction liées aux designers."""
    rows = fetch_all(
        conn,
        "SELECT COUNT(*) AS cnt FROM product_refs_nested_trans prnt "
        "INNER JOIN product_refs_nested prn ON prn.id = prnt.product_refs_nested_id "
        "WHERE prn.product_ref_type_id = ?",
        (config.DESIGNER_TYPE_ID,),
    )
    return rows[0]["cnt"]


def count_links(conn) -> int:
    """Nombre de liaisons produit ↔ designer."""
    rows = fetch_all(
        conn,
        "SELECT COUNT(*) AS cnt FROM product_refs_enumerable_value "
        "WHERE product_ref_type_id_for_unicity = ?",
        (config.DESIGNER_TYPE_ID,),
    )
    return rows[0]["cnt"]


def count_distinct_products(conn) -> int:
    """Nombre de produits distincts ayant au moins un designer."""
    rows = fetch_all(
        conn,
        "SELECT COUNT(DISTINCT product_id) AS cnt FROM product_refs_enumerable_value "
        "WHERE product_ref_type_id_for_unicity = ?",
        (config.DESIGNER_TYPE_ID,),
    )
    return rows[0]["cnt"]


def check_test_product(conn) -> tuple[bool, int]:
    """Si TEST_PRODUCT_ID est renseigné, vérifie qu'il a bien des designers liés."""
    if not config.TEST_PRODUCT_ID:
        return True, 0  # pas configuré → pas un problème
    rows = fetch_all(
        conn,
        "SELECT COUNT(*) AS cnt FROM product_refs_enumerable_value "
        "WHERE product_id = ? AND product_ref_type_id_for_unicity = ?",
        (int(config.TEST_PRODUCT_ID), config.DESIGNER_TYPE_ID),
    )
    cnt = rows[0]["cnt"]
    return cnt > 0, cnt


# ── Runner principal ──────────────────────────────────────────────────────────

def run_checks() -> bool:
    """
    Lance tous les contrôles et affiche un rapport.
    Retourne True si tout est OK, False si au moins un contrôle critique échoue.
    """
    all_ok = True

    logger.info("=" * 60)
    logger.info("VÉRIFICATION BASE DE DONNÉES")
    logger.info("Hôte    : %s:%s", config.DB_HOST, config.DB_PORT)
    logger.info("Base    : %s", config.DB_NAME)
    logger.info("=" * 60)

    try:
        with get_connection() as conn:

            # 1 — Connexion
            ok = check_connection(conn)
            _log("Connexion MariaDB", ok, critical=True)
            if not ok:
                return False  # inutile de continuer
            all_ok &= ok

            # 2 — Tables
            ok, missing = check_tables(conn)
            if ok:
                _log("Tables requises (toutes présentes)", ok)
            else:
                _log(f"Tables manquantes : {missing}", ok, critical=True)
            all_ok &= ok

            # 3 — Type Designer
            ok, row = check_designer_type(conn)
            _log(
                f"Type Designer ID={config.DESIGNER_TYPE_ID} dans product_ref_type",
                ok,
                critical=True,
            )
            all_ok &= ok

            # 4 — Langue par défaut
            ok, row = check_default_locale(conn)
            detail = f"(locale={row['locale']})" if row else ""
            _log(
                f"DEFAULT_TRANS_ID={config.DEFAULT_TRANS_ID} dans trans {detail}",
                ok,
                critical=True,
            )
            all_ok &= ok

            # 5 — Comptages
            logger.info("-" * 60)
            logger.info("DONNÉES DISPONIBLES")
            nb_designers = count_designers(conn)
            nb_trans = count_translations(conn)
            nb_links = count_links(conn)
            nb_products = count_distinct_products(conn)

            logger.info("  Designers distincts        : %d", nb_designers)
            logger.info("  Lignes de traduction       : %d", nb_trans)
            logger.info("  Liaisons produit<->designer : %d", nb_links)
            logger.info("  Produits distincts liés    : %d", nb_products)

            if nb_designers == 0:
                logger.warning("  ⚠ Aucun designer trouvé — vérifiez DESIGNER_TYPE_ID.")
                all_ok = False

            # 6 — Produit de test
            logger.info("-" * 60)
            if config.TEST_PRODUCT_ID:
                ok, cnt = check_test_product(conn)
                _log(
                    f"TEST_PRODUCT_ID={config.TEST_PRODUCT_ID} -> {cnt} designer(s) lie(s)",
                    ok,
                    critical=False,
                )
                if not ok:
                    logger.warning(
                        "  TEST_PRODUCT_ID=%s n'a aucun designer lié — "
                        "vérifiez la valeur dans .env.",
                        config.TEST_PRODUCT_ID,
                    )
            else:
                logger.warning(
                    "  TEST_PRODUCT_ID non configuré dans .env — "
                    "le mode --test ne fonctionnera pas."
                )

    except Exception as e:
        logger.error("Erreur fatale : %s", e)
        return False

    logger.info("=" * 60)
    if all_ok:
        logger.info("RÉSULTAT : OK — la base est prête pour la migration.")
    else:
        logger.error("RÉSULTAT : ÉCHEC — corrigez les erreurs ci-dessus avant de continuer.")
    logger.info("=" * 60)

    return all_ok


def _log(label: str, ok: bool, critical: bool = False) -> None:
    """Affiche une ligne de résultat avec un indicateur visuel OK / FAIL."""
    status = "OK  " if ok else ("FAIL" if critical else "WARN")
    level = logging.INFO if ok else (logging.ERROR if critical else logging.WARNING)
    logger.log(level, "  [%s] %s", status, label)


# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    success = run_checks()
    sys.exit(0 if success else 1)
