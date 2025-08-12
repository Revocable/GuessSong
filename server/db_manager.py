
import sqlite3
import logging
from pathlib import Path
from typing import List, Dict, Optional, Literal

# --- Configuração ---
DB_PATH = Path("cache.db")
logger = logging.getLogger(__name__)

# --- Tipos de Status ---
TrackStatus = Literal["pending", "downloaded", "failed", "failed_permanent"]

def get_db_connection():
    """Cria e retorna uma conexão com o banco de dados."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def setup_database():
    """Cria a tabela de faixas se ela não existir."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tracks (
                    id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    artist TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    filepath TEXT
                )
            """)
            conn.commit()
        logger.info("Banco de dados SQLite configurado com sucesso.")
    except sqlite3.Error as e:
        logger.error(f"Erro ao configurar o banco de dados: {e}")
        raise

def add_tracks_to_db(tracks: List[Dict]):
    """
    Adiciona uma lista de faixas ao banco de dados com status 'pending'.
    Ignora faixas que já existem no banco.
    """
    if not tracks:
        return

    tracks_to_insert = []
    for track in tracks:
        track_id = track.get('id')
        title = track.get('name')
        artist = track.get('artists', [{}])[0].get('name', 'Artista Desconhecido')
        if track_id and title:
            tracks_to_insert.append((track_id, title, artist))

    if not tracks_to_insert:
        return

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # O "OR IGNORE" previne erros se a faixa (chave primária) já existir
            cursor.executemany(
                "INSERT OR IGNORE INTO tracks (id, title, artist) VALUES (?, ?, ?)",
                tracks_to_insert
            )
            conn.commit()
        logger.info(f"{cursor.rowcount} novas faixas adicionadas ao banco de dados para processamento.")
    except sqlite3.Error as e:
        logger.error(f"Erro ao adicionar faixas ao banco de dados: {e}")

def update_track_status(track_id: str, status: TrackStatus, filepath: Optional[str] = None):
    """Atualiza o status e o número de tentativas de uma faixa."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            if filepath:
                cursor.execute(
                    "UPDATE tracks SET status = ?, filepath = ?, attempts = attempts + 1 WHERE id = ?",
                    (status, filepath, track_id)
                )
            else:
                cursor.execute(
                    "UPDATE tracks SET status = ?, attempts = attempts + 1 WHERE id = ?",
                    (status, track_id)
                )
            conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Erro ao atualizar o status da faixa {track_id}: {e}")

def get_tracks_by_status(status: TrackStatus) -> List[Dict]:
    """Busca todas as faixas com um determinado status."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id, title, artist, filepath FROM tracks WHERE status = ?", (status,))
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
    except sqlite3.Error as e:
        logger.error(f"Erro ao buscar faixas por status '{status}': {e}")
        return []

def get_all_processed_track_ids() -> set:
    """
    Retorna um conjunto de IDs de faixas que já foram baixadas com sucesso
    ou que falharam permanentemente, para evitar re-processamento.
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM tracks WHERE status IN ('downloaded', 'failed_permanent')")
            rows = cursor.fetchall()
            return {row['id'] for row in rows}
    except sqlite3.Error as e:
        logger.error(f"Erro ao buscar IDs de faixas processadas: {e}")
        return set()

# Inicializa o banco de dados na importação do módulo
setup_database()
