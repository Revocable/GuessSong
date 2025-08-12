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
import ffmpeg
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv
from typing import List, Dict, Optional, Tuple

# --- Configuração de Logging ---
# Garante que o logger não tenha handlers duplicados
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
load_dotenv()

# --- Constantes ---
AUDIO_DIR = Path("static/audio")
DOWNLOAD_DURATION = 30
DB_PATH = "music_cache.db"

# --- Verificação de Downloader Otimizado ---
ARIA2C_PATH = shutil.which("aria2c")
if ARIA2C_PATH:
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
    sys.exit(1)

AUDIO_DIR.mkdir(parents=True, exist_ok=True)

# === GERENCIADOR DE BANCO DE DADOS (Sem alterações) ===
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
        return sqlite3.connect(self.db_path, check_same_thread=False)

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
                return {row[0] for row in conn.execute('SELECT id FROM tracks')}
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

db = DatabaseManager()

# === LÓGICA DE DOWNLOAD REFEITA E ROBUSTA ===

def cleanup_files(*files: Path):
    """Remove um ou mais arquivos, ignorando erros se não existirem."""
    for file in files:
        try:
            if file.exists():
                file.unlink()
        except OSError as e:
            logger.debug(f"Não foi possível limpar o arquivo {file}: {e}")

def run_download_and_cut(search_query: str, output_filepath: Path) -> bool:
    """
    Abordagem em 2 passos: baixa um clipe curto e depois corta com ffmpeg.
    Isso é MUITO mais confiável do que o pós-processamento do yt-dlp.
    """
    temp_filepath = output_filepath.with_suffix('.temp.webm')
    cleanup_files(temp_filepath, output_filepath) # Garante um início limpo

    try:
        # --- ETAPA 1: Baixar os primeiros 90 segundos ---
        logger.debug(f"Etapa 1: Baixando clipe temporário para '{search_query}'")
        ydl_opts = {
            'format': 'bestaudio/best',
            'outtmpl': str(temp_filepath),
            'default_search': 'ytsearch1:',
            # Baixa apenas os primeiros 90 segundos para ser rápido
            'download_ranges': yt_dlp.utils.download_range_func(None, [(0, 90)]),
            'quiet': True,
            'noprogress': True,
        }
        if ARIA2C_PATH:
            ydl_opts['external_downloader'] = ARIA2C_PATH

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([search_query])

        if not temp_filepath.exists() or temp_filepath.stat().st_size < 10000:
            logger.debug("Download temporário falhou ou arquivo é muito pequeno.")
            cleanup_files(temp_filepath)
            return False

        # --- ETAPA 2: Cortar um trecho de 30 segundos do arquivo temporário ---
        start_time = random.randint(15, 55) # Ponto de início aleatório dentro do clipe de 90s
        logger.debug(f"Etapa 2: Cortando trecho de {DOWNLOAD_DURATION}s a partir de {start_time}s.")
        
        # Usando ffmpeg-python para segurança e controle
        (
            ffmpeg
            .input(str(temp_filepath), ss=start_time, t=DOWNLOAD_DURATION)
            .output(str(output_filepath), acodec='libopus', audio_bitrate='64k', loglevel='error')
            .overwrite_output()
            .run()
        )
        
        if not output_filepath.exists() or output_filepath.stat().st_size < 5000:
            logger.error("Corte com FFmpeg falhou, arquivo final não criado ou muito pequeno.")
            cleanup_files(temp_filepath, output_filepath)
            return False

        cleanup_files(temp_filepath) # Limpa o arquivo temporário se tudo deu certo
        return True

    except Exception as e:
        logger.error(f"Falha no processo de download/corte para '{search_query}'. Erro: {e}")
        # Garante a limpeza total em caso de qualquer falha
        cleanup_files(temp_filepath, output_filepath)
        return False

async def download_track_async(track: Dict, is_retry=False):
    """Tenta baixar a faixa usando múltiplas queries e a nova função robusta."""
    track_id, title, artist = track['id'], track['title'], track['artist']
    log_prefix = "RE-TENTATIVA" if is_retry else "1ª tentativa"
    logger.info(f"BAIXANDO ({log_prefix}): '{title} - {artist}'")
    
    final_filepath = AUDIO_DIR / f"{track_id}.webm"

    search_queries = [
        f"{artist} {title} official audio",
        f"{artist} - {title}",
        f"{title} {artist}",
    ]
    
    for i, query in enumerate(search_queries):
        logger.debug(f"Tentativa {i+1}/{len(search_queries)} com query: '{query}'")
        
        success = await asyncio.to_thread(run_download_and_cut, query, final_filepath)
        
        if success:
            logger.info(f"✅ SUCESSO: '{title}' baixado e processado.")
            db.update_track_status(track_id, 'downloaded', str(final_filepath))
            return 'downloaded'

    final_status = 'failed_permanent' if is_retry else 'failed'
    logger.error(f"❌ FALHA: Não foi possível baixar '{title}' (testadas {len(search_queries)} queries).")
    db.update_track_status(track_id, final_status)
    return final_status

# === FUNÇÕES PRINCIPAIS (com pequenas otimizações) ===

async def fetch_all_playlists(playlist_urls: List[str]) -> List[Dict]:
    """Busca todas as playlists de forma concorrente."""
    async def fetch(url):
        try:
            logger.info(f"Buscando faixas da playlist: {url}")
            results = await asyncio.to_thread(sp.playlist_items, url, fields='items.track.id,items.track.name,items.track.artists.name,next')
            tracks = []
            while results:
                for item in results.get('items', []):
                    if (track := item.get('track')) and track.get('id'):
                        tracks.append({
                            'id': track['id'], 'title': track['name'],
                            'artist': ', '.join(a['name'] for a in track.get('artists', []))
                        })
                results = await asyncio.to_thread(sp.next, results) if results.get('next') else None
            logger.info(f"Encontradas {len(tracks)} faixas em {url.split('/')[-1]}")
            return tracks
        except Exception as e:
            logger.error(f"Não foi possível buscar a playlist '{url}'. Erro: {e}")
            return []

    all_tracks_nested = await asyncio.gather(*(fetch(url) for url in playlist_urls))
    return [track for sublist in all_tracks_nested for track in sublist]

async def process_downloads(tracks_to_process: List[Dict], concurrency: int, is_retry=False):
    if not tracks_to_process: return
    semaphore = asyncio.Semaphore(concurrency)

    async def run_with_semaphore(track):
        async with semaphore:
            await download_track_async(track, is_retry)
            await asyncio.sleep(random.uniform(0.2, 0.8))

    await asyncio.gather(*(run_with_semaphore(t) for t in tracks_to_process))

def verify_downloaded_files():
    logger.info("Verificando integridade dos arquivos baixados...")
    downloaded_tracks = db.get_tracks_by_status('downloaded')
    missing_files = 0
    for track in downloaded_tracks:
        filepath = track.get('filepath')
        if not filepath or not Path(filepath).exists() or Path(filepath).stat().st_size < 5000:
            logger.warning(f"Arquivo ausente para '{track['title']}'. Resetando para 'pending'.")
            db.update_track_status(track['id'], 'pending')
            missing_files += 1
    if missing_files > 0:
        logger.info(f"↻ {missing_files} faixas foram resetadas para 'pending'.")
    else:
        logger.info("✅ Todos os arquivos baixados estão íntegros.")

async def main(playlist_urls: List[str], concurrency: int):
    logger.info("=" * 60 + "\nINICIANDO PROCESSO DE CACHE DE MÚSICAS\n" + "=" * 60)
    verify_downloaded_files()
    
    existing_ids = db.get_all_track_ids()
    logger.info(f"{len(existing_ids)} faixas já no banco de dados.")

    all_tracks_from_spotify = await fetch_all_playlists(playlist_urls)
    new_tracks_to_add = [t for t in all_tracks_from_spotify if t['id'] not in existing_ids]

    if new_tracks_to_add:
        db.add_tracks_to_db(new_tracks_to_add)
        logger.info(f"✅ Adicionadas {len(new_tracks_to_add)} novas faixas ao banco.")
    else:
        logger.info("Nenhuma faixa nova para adicionar.")

    # FASE 1
    logger.info("\n" + "=" * 60 + "\nFASE 1: PRIMEIRA TENTATIVA DE DOWNLOAD\n" + "=" * 60)
    pending_tracks = db.get_tracks_by_status('pending')
    if pending_tracks:
        logger.info(f"Encontradas {len(pending_tracks)} faixas pendentes.")
        await process_downloads(pending_tracks, concurrency, is_retry=False)
    else:
        logger.info("Nenhuma faixa pendente para a primeira tentativa.")

    # FASE 2
    logger.info("\n" + "=" * 60 + "\nFASE 2: SEGUNDA TENTATIVA PARA FALHAS\n" + "=" * 60)
    failed_tracks = db.get_tracks_by_status('failed')
    if failed_tracks:
        logger.info(f"Encontradas {len(failed_tracks)} faixas para nova tentativa.")
        await process_downloads(failed_tracks, concurrency, is_retry=True)
    else:
        logger.info("Nenhuma faixa falhou na primeira tentativa. Ótimo!")

    # RESUMO FINAL
    logger.info("\n" + "=" * 60 + "\nRESUMO FINAL\n" + "=" * 60)
    final_stats = db.get_stats()
    logger.info("Estado final do banco:")
    emojis = {'pending': '⏳', 'downloaded': '✅', 'failed': '🔄', 'failed_permanent': '❌'}
    for status, count in sorted(final_stats.items()):
        logger.info(f"  {emojis.get(status, '❓')} {status.capitalize()}: {count}")

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