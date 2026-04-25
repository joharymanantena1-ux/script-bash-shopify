"""
db.py — Connexion MariaDB et exécution de requêtes.
Fournit un context manager propre avec fermeture automatique de la connexion.
"""

import logging
from contextlib import contextmanager
from typing import Generator

import mariadb

import config

logger = logging.getLogger(__name__)


@contextmanager
def get_connection() -> Generator[mariadb.Connection, None, None]:
    """
    Context manager : ouvre une connexion MariaDB et la ferme proprement.

    Usage :
        with get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT ...")
    """
    conn = None
    try:
        conn = mariadb.connect(
            host=config.DB_HOST,
            port=config.DB_PORT,
            database=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            autocommit=True,
        )
        logger.info("Connexion MariaDB établie (%s:%s/%s)", config.DB_HOST, config.DB_PORT, config.DB_NAME)
        yield conn
    except mariadb.Error as e:
        logger.error("Erreur de connexion MariaDB : %s", e)
        raise
    finally:
        if conn:
            conn.close()
            logger.debug("Connexion MariaDB fermée.")


def fetch_all(conn: mariadb.Connection, query: str, params: tuple = ()) -> list[dict]:
    """
    Exécute une requête SELECT et retourne une liste de dictionnaires.
    Chaque dictionnaire représente une ligne (clé = nom de colonne).
    """
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query, params)
        rows = cursor.fetchall()
        cursor.close()
        return rows
    except mariadb.Error as e:
        logger.error("Erreur SQL : %s\nRequête : %s\nParamètres : %s", e, query, params)
        raise
