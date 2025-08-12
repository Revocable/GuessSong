import asyncio
import os
import random
import logging
import shutil
import sys
import argparse
import sqlite3
from pathlib import Path
import spotipy
import yt_dlp
from yt_dlp.utils import DownloadError
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv
from typing import List, Dict, Optional

# --- Configuração de Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
load_dotenv()

# --- Constantes ---
AUDIO_DIR = Path("static/audio")
DOWNLOAD_DURATION = 30  # Duração fixa de 30 segundos
DB_PATH = "music_cache.db"

# --- Verificação de Downloader Otimizado ---
ARIA2C_PATH = shutil.which("aria2c")
if not ARIA2C_PATH:
    logger.warning("AVISO: O downloader 'aria2c' não foi encontrado. Os downloads podem ser mais lentos.")
else:
    logger.info(f"Usando downloader otimizado: {ARIA2C_PATH}")

# --- Configuração do Spotify ---
try:
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
        client_id=os.getenv("SPOTIPY_CLIENT_ID"),
        client_secret=os.getenv("SPOTIPY_CLIENT_SECRET")
    ))
    sp.search("test", limit=1)
    logger.info("Conexão com Spotify estabelecida com sucesso.")
except Exception as e:
    logger.error(f"ERRO: Falha ao conectar com o Spotify. Verifique suas credenciais. Detalhes: {e}")
    sp = None
    sys.exit(1)

# --- Criação do Diretório de Áudio ---
AUDIO_DIR.mkdir(parents=True, exist_ok=True)


# === GERENCIADOR DE BANCO DE DADOS INTEGRADO ===
class DatabaseManager:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self.init_database()

    def init_database(self):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS tracks (
                        id TEXT PRIMARY KEY, title TEXT NOT NULL, artist TEXT NOT NULL,
                        status TEXT DEFAULT 'pending', filepath TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )''')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_tracks_status ON tracks(status)')
            logger.info(f"Banco de dados inicializado: {self.db_path}")
        except Exception as e:
            logger.error(f"Erro ao inicializar banco de dados: {e}")
            raise

    def get_connection(self):
        return sqlite3.connect(self.db_path)

    def add_tracks_to_db(self, tracks: List[Dict]):
        if not tracks: return
        try:
            with self.get_connection() as conn:
                track_data = [(t['id'], t['title'], t['artist']) for t in tracks]
                conn.executemany("INSERT OR IGNORE INTO tracks (id, title, artist, status) VALUES (?, ?, ?, 'pending')", track_data)
        except Exception as e:
            logger.error(f"Erro ao adicionar tracks ao banco: {e}")
            raise

    def get_tracks_by_status(self, status: str) -> List[Dict]:
        try:
            with self.get_connection() as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute("SELECT id, title, artist, filepath FROM tracks WHERE status = ? ORDER BY created_at", (status,))
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Erro ao buscar tracks com status '{status}': {e}")
            return []

    def get_all_track_ids(self) -> set:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT id FROM tracks')
                return {row[0] for row in cursor.fetchall()}
        except Exception as e:
            logger.error(f"Erro ao buscar IDs de tracks: {e}")
            return set()

    def update_track_status(self, track_id: str, status: str, filepath: Optional[str] = None):
        try:
            with self.get_connection() as conn:
                if filepath:
                    conn.execute("UPDATE tracks SET status = ?, filepath = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?", (status, filepath, track_id))
                else:
                    conn.execute("UPDATE tracks SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?", (status, track_id))
        except Exception as e:
            logger.error(f"Erro ao atualizar status da track {track_id}: {e}")

    def get_stats(self) -> Dict[str, int]:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT status, COUNT(*) FROM tracks GROUP BY status')
                return dict(cursor.fetchall())
        except Exception as e:
            logger.error(f"Erro ao buscar estatísticas: {e}")
            return {}

db = DatabaseManager()

def _download_song_segment(search_query: str, output_path: Path, duration: int):
    """
    Baixa um segmento de áudio usando o método de pós-processamento,
    que é mais estável e corrige o erro 'ffmpeg exited with code 8'.
    """
    # O ponto de início continua aleatório, mas a duração será fixa.
    start_time = random.randint(20, 70)

    ydl_opts = {
        'format': 'bestaudio/best',
        # Método estável: baixar e depois cortar com ffmpeg.
        # Isso corrige o 'exit code 8' e garante a duração fixa.
        'postprocessor_args': [
            '-ss', str(start_time),  # Ponto de início do corte
            '-t', str(duration),     # Duração exata do corte
        ],
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'webm',
            'preferredquality': '64'
        }],
        'outtmpl': str(output_path).replace('.webm', ''),
        'quiet': not logger.isEnabledFor(logging.DEBUG),
        'noprogress': True,
        'default_search': 'ytsearch1:',
        'retries': 2,
    }

    if ARIA2C_PATH:
        ydl_opts['external_downloader'] = ARIA2C_PATH
        ydl_opts['external_downloader_args'] = ['-x', '16', '-s', '16', '-k', '1M', '--console-log-level=warn']

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([search_query])

        # yt-dlp adiciona a extensão, então o caminho final é este
        expected_file = output_path.with_suffix('.webm')
        if expected_file.exists() and expected_file.stat().st_size > 1000:
            return True, expected_file
        else:
            logger.debug(f"Download parece ter falhado, arquivo não encontrado ou pequeno: {expected_file}")
            return False, None
    except DownloadError as e:
        logger.debug(f"yt-dlp DownloadError para '{search_query}': {e}")
        return False, None
    except Exception as e:
        logger.debug(f"Exceção inesperada no download para '{search_query}': {e}")
        return False, None

def cleanup_failed_download(track_id: str):
    base_path = AUDIO_DIR / track_id
    for file_path in AUDIO_DIR.glob(f"{base_path.name}*"):
        try:
            logger.debug(f"Limpando arquivo: {file_path}")
            file_path.unlink()
        except OSError as e:
            logger.debug(f"Erro ao limpar arquivo {file_path}: {e}")

async def download_track_async(track: Dict, is_retry=False):
    track_id, title, artist = track['id'], track['title'], track['artist']
    log_prefix = "RE-TENTATIVA" if is_retry else "1ª tentativa"
    logger.info(f"BAIXANDO ({log_prefix}): '{title} - {artist}'")
    
    cleanup_failed_download(track_id)

    search_queries = [
        f"{artist} {title} official audio",
        f"{artist} - {title}",
        f"{title} {artist}",
    ]
    
    for i, query in enumerate(search_queries):
        logger.debug(f"Tentativa {i+1}/{len(search_queries)} para '{title}' com a query: '{query}'")
        try:
            success, final_path = await asyncio.to_thread(
                _download_song_segment, query, AUDIO_DIR / track_id, DOWNLOAD_DURATION
            )
            if success:
                logger.info(f"✅ SUCESSO: '{title}' baixado ({final_path.stat().st_size} bytes).")
                db.update_track_status(track_id, 'downloaded', str(final_path))
                return 'downloaded'
        except Exception as e:
            logger.error(f"Exceção no worker de download para '{title}': {e}")
            continue

    final_status = 'failed_permanent' if is_retry else 'failed'
    logger.error(f"❌ FALHA: Não foi possível baixar '{title}' (testadas {len(search_queries)} queries).")
    db.update_track_status(track_id, final_status)
    cleanup_failed_download(track_id)
    return final_status

async def fetch_playlist_tracks(playlist_url: str) -> List[Dict]:
    try:
        logger.info(f"Buscando faixas da playlist: {playlist_url}")
        results = await asyncio.to_thread(sp.playlist_items, playlist_url, fields='items.track.id,items.track.name,items.track.artists.name,next')
        tracks = []
        while results:
            for item in results.get('items', []):
                if track := item.get('track'):
                    if track.get('id'):
                        tracks.append({
                            'id': track['id'], 'title': track['name'],
                            'artist': ', '.join(a['name'] for a in track.get('artists', []))
                        })
            if results['next']:
                results = await asyncio.to_thread(sp.next, results)
            else:
                break
        logger.info(f"Encontradas {len(tracks)} faixas na playlist.")
        return tracks
    except Exception as e:
        logger.error(f"Não foi possível buscar a playlist '{playlist_url}'. Erro: {e}")
        return []

async def process_downloads(tracks_to_process: List[Dict], concurrency: int, is_retry=False):
    if not tracks_to_process: return {}
    semaphore = asyncio.Semaphore(concurrency)
    stats = {'downloaded': 0, 'failed': 0, 'failed_permanent': 0}

    async def run_with_semaphore(track):
        async with semaphore:
            result = await download_track_async(track, is_retry)
            if result in stats: stats[result] += 1
            await asyncio.sleep(random.uniform(0.5, 1.5))

    await asyncio.gather(*(run_with_semaphore(t) for t in tracks_to_process))
    return stats

def verify_downloaded_files():
    logger.info("Verificando integridade dos arquivos baixados...")
    downloaded_tracks = db.get_tracks_by_status('downloaded')
    missing_files = 0
    for track in downloaded_tracks:
        filepath = track.get('filepath')
        if not filepath or not Path(filepath).exists() or Path(filepath).stat().st_size < 1000:
            logger.warning(f"Arquivo ausente para '{track['title']}'. Resetando para 'pending'.")
            db.update_track_status(track['id'], 'pending')
            missing_files += 1
    if missing_files > 0:
        logger.info(f"↻ {missing_files} faixas foram resetadas para 'pending'.")
    else:
        logger.info("✅ Todos os arquivos baixados estão íntegros.")

async def main(playlist_urls: List[str], concurrency: int):
    if not sp: return
    logger.info("=" * 60 + "\nINICIANDO PROCESSO DE CACHE DE MÚSICAS\n" + "=" * 60)
    logger.info(f"Nível de concorrência: {concurrency}\nBanco de dados: {DB_PATH}")
    logger.info(f"Estado inicial do banco: {db.get_stats()}")
    verify_downloaded_files()
    
    existing_ids = db.get_all_track_ids()
    logger.info(f"{len(existing_ids)} faixas já no banco de dados.")

    new_tracks_to_add = []
    for url in playlist_urls:
        tracks = await fetch_playlist_tracks(url)
        unprocessed = [t for t in tracks if t['id'] not in existing_ids]
        new_tracks_to_add.extend(unprocessed)
        logger.info(f"Novas faixas desta playlist: {len(unprocessed)}")

    if new_tracks_to_add:
        db.add_tracks_to_db(new_tracks_to_add)
        logger.info(f"✅ Adicionadas {len(new_tracks_to_add)} novas faixas ao banco.")
    else:
        logger.info("Nenhuma faixa nova encontrada.")

    logger.info("\n" + "=" * 60 + "\nFASE 1: PRIMEIRA TENTATIVA DE DOWNLOAD\n" + "=" * 60)
    pending_tracks = db.get_tracks_by_status('pending')
    if pending_tracks:
        logger.info(f"Encontradas {len(pending_tracks)} faixas pendentes.")
        await process_downloads(pending_tracks, concurrency, is_retry=False)
    else:
        logger.info("Nenhuma faixa pendente para a primeira tentativa.")

    logger.info("\n" + "=" * 60 + "\nFASE 2: SEGUNDA TENTATIVA PARA FALHAS\n" + "=" * 60)
    failed_tracks = db.get_tracks_by_status('failed')
    if failed_tracks:
        logger.info(f"Encontradas {len(failed_tracks)} faixas para nova tentativa.")
        await process_downloads(failed_tracks, concurrency, is_retry=True)
    else:
        logger.info("Nenhuma faixa falhou na primeira tentativa. Ótimo!")

    logger.info("\n" + "=" * 60 + "\nRESUMO FINAL\n" + "=" * 60)
    final_stats = db.get_stats()
    logger.info("Estado final do banco:")
    emojis = {'pending': '⏳', 'downloaded': '✅', 'failed': '🔄', 'failed_permanent': '❌'}
    for status, count in sorted(final_stats.items()):
        logger.info(f"  {emojis.get(status, '❓')} {status.capitalize()}: {count}")

    audio_files = list(AUDIO_DIR.glob("*.webm"))
    total_size_mb = sum(f.stat().st_size for f in audio_files) / (1024 * 1024)
    logger.info(f"\nArquivos de áudio:\n  📁 Total de arquivos: {len(audio_files)}\n  💾 Tamanho total: {total_size_mb:.1f} MB")
    logger.info("=" * 60)

if __name__ == "__main__":
    if os.path.basename(os.getcwd()) == 'GuessSong': os.chdir('server')
    
    parser = argparse.ArgumentParser(description="Cache de Músicas do Spotify com SQLite integrado.")
    parser.add_argument('urls', nargs='+', help="Uma ou mais URLs de playlists do Spotify.")
    parser.add_argument('--concurrency', type=int, default=2, help="Número de downloads simultâneos.")
    parser.add_argument('--debug', action='store_true', help="Ativar logs de debug para diagnóstico.")
    
    args = parser.parse_args()
    
    if args.debug: logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        asyncio.run(main(args.urls, args.concurrency))
    except KeyboardInterrupt:
        logger.info("\n🛑 Processo interrompido pelo usuário.")
    except Exception as e:
        logger.error(f"Erro inesperado na execução: {e}", exc_info=args.debug)