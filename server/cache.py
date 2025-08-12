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
from yt_dlp.utils import download_range_func, DownloadError
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv
from typing import List, Dict, Optional

# --- Configuração de Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
load_dotenv()

# --- Constantes ---
AUDIO_DIR = Path("static/audio")
DOWNLOAD_DURATION = 30  # segundos
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
        """Inicializa o banco de dados com as tabelas necessárias"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS tracks (
                        id TEXT PRIMARY KEY,
                        title TEXT NOT NULL,
                        artist TEXT NOT NULL,
                        status TEXT DEFAULT 'pending',
                        filepath TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_tracks_status ON tracks(status)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_tracks_artist ON tracks(artist)')
                conn.commit()
            logger.info(f"Banco de dados inicializado: {self.db_path}")
        except Exception as e:
            logger.error(f"Erro ao inicializar banco de dados: {e}")
            raise

    def get_connection(self):
        """Retorna uma conexão com o banco de dados"""
        return sqlite3.connect(self.db_path)

    def add_tracks_to_db(self, tracks: List[Dict]):
        """Adiciona uma lista de tracks ao banco de dados, ignorando duplicatas."""
        if not tracks:
            return
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                track_data = [(t['id'], t['title'], t['artist']) for t in tracks]
                cursor.executemany('''
                    INSERT OR IGNORE INTO tracks (id, title, artist, status)
                    VALUES (?, ?, ?, 'pending')
                ''', track_data)
                conn.commit()
                # O log agora é feito na função principal para evitar redundância
        except Exception as e:
            logger.error(f"Erro ao adicionar tracks ao banco: {e}")
            raise

    def get_tracks_by_status(self, status: str) -> List[Dict]:
        """Busca tracks por um status específico."""
        try:
            with self.get_connection() as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT id, title, artist, filepath FROM tracks WHERE status = ? ORDER BY created_at", (status,)
                )
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Erro ao buscar tracks com status '{status}': {e}")
            return []

    def get_all_track_ids(self) -> set:
        """Retorna um set com os IDs de todas as tracks no banco."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT id FROM tracks')
                return {row[0] for row in cursor.fetchall()}
        except Exception as e:
            logger.error(f"Erro ao buscar IDs de tracks: {e}")
            return set()

    def update_track_status(self, track_id: str, status: str, filepath: Optional[str] = None):
        """Atualiza o status e, opcionalmente, o caminho do arquivo de uma track."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                if filepath:
                    cursor.execute('''
                        UPDATE tracks SET status = ?, filepath = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    ''', (status, filepath, track_id))
                else:
                    cursor.execute('''
                        UPDATE tracks SET status = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    ''', (status, track_id))
                conn.commit()
        except Exception as e:
            logger.error(f"Erro ao atualizar status da track {track_id}: {e}")

    def get_stats(self) -> Dict[str, int]:
        """Retorna estatísticas do banco (contagem por status)."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT status, COUNT(*) FROM tracks GROUP BY status')
                return {status: count for status, count in cursor.fetchall()}
        except Exception as e:
            logger.error(f"Erro ao buscar estatísticas: {e}")
            return {}

# Inicializar o gerenciador de banco de dados
db = DatabaseManager()


def _download_song_segment(search_query: str, output_path: Path, duration: int = 30):
    """Baixa um segmento de áudio usando o método moderno de yt-dlp."""
    duration = 30
    start_time = random.randint(20, 70)
    end_time = start_time + duration

    ydl_opts = {
        'format': 'bestaudio/best',
        # Usar o método moderno e eficiente do yt-dlp para baixar trechos
        'download_ranges': download_range_func(None, [(start_time, end_time)]),
        'force_keyframes_at_cuts': True,
        'postprocessors': [{'key': 'FFmpegExtractAudio', 'preferredcodec': 'webm', 'preferredquality': '64'}],
        'outtmpl': str(output_path).replace('.webm', ''),
        'quiet': not logger.isEnabledFor(logging.DEBUG),
        'noprogress': True,
        'default_search': 'ytsearch1:',
        'retries': 2, # Tentar novamente em caso de falhas de rede
    }

    if ARIA2C_PATH:
        ydl_opts['external_downloader'] = ARIA2C_PATH
        ydl_opts['external_downloader_args'] = ['-x', '16', '-s', '16', '-k', '1M', '--console-log-level=warn']

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([search_query])

        expected_file = output_path.with_suffix('.webm')
        if expected_file.exists() and expected_file.stat().st_size > 1000:
            return True, expected_file
        else:
            return False, None
    except DownloadError as e:
        logger.debug(f"yt-dlp DownloadError para '{search_query}': {e}")
        return False, None
    except Exception as e:
        logger.debug(f"Exceção inesperada no download para '{search_query}': {e}")
        return False, None


def cleanup_failed_download(track_id: str):
    """Remove arquivos corrompidos ou temporários de um download com falha."""
    base_path = AUDIO_DIR / track_id
    # Remover todos os arquivos que começam com o ID da track (e.g., .webm, .part, .ytdl)
    for file_path in AUDIO_DIR.glob(f"{base_path.name}*"):
        try:
            logger.debug(f"Limpando arquivo: {file_path}")
            file_path.unlink()
        except OSError as e:
            logger.debug(f"Erro ao limpar arquivo {file_path}: {e}")


async def download_track_async(track: Dict, is_retry=False):
    """Wrapper assíncrono para o download de uma faixa, com lógica de tentativas."""
    track_id, title, artist = track['id'], track['title'], track['artist']
    log_prefix = "RE-TENTATIVA" if is_retry else "1ª tentativa"
    logger.info(f"BAIXANDO ({log_prefix}): '{title} - {artist}'")
    
    # Limpar arquivos de tentativas anteriores antes de começar
    cleanup_failed_download(track_id)

    # Tentar diferentes variações de busca
    search_queries = [
        f"{artist} {title} official audio",
        f"{artist} - {title}",
        f"{title} {artist}",
    ]
    
    for i, query in enumerate(search_queries):
        logger.debug(f"Tentativa {i+1}/{len(search_queries)} para '{title}' com a query: '{query}'")
        try:
            success, final_path = await asyncio.to_thread(
                _download_song_segment, query, AUDIO_DIR / f"{track_id}.webm", DOWNLOAD_DURATION
            )
            if success:
                logger.info(f"✅ SUCESSO: '{title}' baixado ({final_path.stat().st_size} bytes).")
                db.update_track_status(track_id, 'downloaded', str(final_path))
                return 'downloaded'
        except Exception as e:
            logger.error(f"Exceção não tratada no worker de download para '{title}': {e}")
            continue # Tenta a próxima query

    # Se todas as queries falharam
    final_status = 'failed_permanent' if is_retry else 'failed'
    logger.error(f"❌ FALHA: Não foi possível baixar '{title}' (testadas {len(search_queries)} queries).")
    db.update_track_status(track_id, final_status)
    cleanup_failed_download(track_id) # Limpeza final
    return final_status


async def fetch_playlist_tracks(playlist_url: str) -> List[Dict]:
    """Busca todas as faixas de uma playlist do Spotify de forma assíncrona."""
    try:
        logger.info(f"Buscando faixas da playlist: {playlist_url}")
        results = await asyncio.to_thread(sp.playlist_items, playlist_url, fields='items.track.id,items.track.name,items.track.artists.name,next')
        tracks = []
        while results:
            for item in results.get('items', []):
                if track := item.get('track'):
                    if track.get('id'):
                        tracks.append({
                            'id': track['id'],
                            'title': track['name'],
                            'artist': ', '.join(artist['name'] for artist in track.get('artists', []))
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
    """Processa uma lista de downloads com um nível de concorrência definido."""
    if not tracks_to_process:
        return {}

    semaphore = asyncio.Semaphore(concurrency)
    stats = {'downloaded': 0, 'failed': 0, 'failed_permanent': 0}

    async def run_with_semaphore(track):
        async with semaphore:
            result = await download_track_async(track, is_retry)
            if result in stats:
                stats[result] += 1
            await asyncio.sleep(random.uniform(0.5, 1.5)) # Pausa para evitar rate limiting

    await asyncio.gather(*(run_with_semaphore(t) for t in tracks_to_process))
    return stats


def verify_downloaded_files():
    """Verifica se os arquivos marcados como 'downloaded' existem e os reseta se não."""
    logger.info("Verificando integridade dos arquivos baixados...")
    downloaded_tracks = db.get_tracks_by_status('downloaded')
    missing_files = 0
    for track in downloaded_tracks:
        filepath = track.get('filepath')
        if not filepath or not Path(filepath).exists() or Path(filepath).stat().st_size < 1000:
            logger.warning(f"Arquivo ausente ou corrompido para '{track['title']}'. Resetando para 'pending'.")
            db.update_track_status(track['id'], 'pending')
            missing_files += 1
    if missing_files > 0:
        logger.info(f"↻ {missing_files} faixas foram resetadas para 'pending' devido a arquivos ausentes.")
    else:
        logger.info("✅ Todos os arquivos baixados estão íntegros.")


async def main(playlist_urls: List[str], concurrency: int):
    """Função principal para orquestrar o processo de cache."""
    if not sp: return

    logger.info("=" * 60)
    logger.info("INICIANDO PROCESSO DE CACHE DE MÚSICAS")
    logger.info("=" * 60)
    logger.info(f"Nível de concorrência: {concurrency}")
    logger.info(f"Banco de dados: {DB_PATH}")

    # Verificar estatísticas iniciais e integridade dos arquivos
    logger.info(f"Estado inicial do banco: {db.get_stats()}")
    verify_downloaded_files()
    
    # 1. Buscar faixas das playlists e adicionar as novas ao DB
    try:
        existing_ids = db.get_all_track_ids()
        logger.info(f"{len(existing_ids)} faixas já no banco de dados.")
    except Exception as e:
        logger.error(f"Erro ao acessar banco de dados: {e}")
        return

    new_tracks_to_add = []
    for url in playlist_urls:
        tracks_from_playlist = await fetch_playlist_tracks(url)
        unprocessed = [t for t in tracks_from_playlist if t['id'] not in existing_ids]
        new_tracks_to_add.extend(unprocessed)
        logger.info(f"Novas faixas desta playlist: {len(unprocessed)}")

    if new_tracks_to_add:
        db.add_tracks_to_db(new_tracks_to_add)
        logger.info(f"✅ Adicionadas {len(new_tracks_to_add)} novas faixas ao banco.")
    else:
        logger.info("Nenhuma faixa nova encontrada nas playlists.")

    # 2. Primeira tentativa de download para faixas pendentes
    logger.info("\n" + "=" * 60 + "\nFASE 1: PRIMEIRA TENTATIVA DE DOWNLOAD\n" + "=" * 60)
    pending_tracks = db.get_tracks_by_status('pending')
    if pending_tracks:
        logger.info(f"Encontradas {len(pending_tracks)} faixas pendentes para download.")
        await process_downloads(pending_tracks, concurrency, is_retry=False)
    else:
        logger.info("Nenhuma faixa pendente para a primeira tentativa.")

    # 3. Segunda tentativa para faixas que falharam
    logger.info("\n" + "=" * 60 + "\nFASE 2: SEGUNDA TENTATIVA PARA FALHAS\n" + "=" * 60)
    failed_tracks = db.get_tracks_by_status('failed')
    if failed_tracks:
        logger.info(f"Encontradas {len(failed_tracks)} faixas para nova tentativa.")
        await process_downloads(failed_tracks, concurrency, is_retry=True)
    else:
        logger.info("Nenhuma faixa falhou na primeira tentativa. Ótimo!")

    # 4. Resumo final
    logger.info("\n" + "=" * 60 + "\nRESUMO FINAL\n" + "=" * 60)
    final_stats = db.get_stats()
    logger.info("Estado final do banco:")
    for status, count in sorted(final_stats.items()):
        emoji = {'pending': '⏳', 'downloaded': '✅', 'failed': '🔄', 'failed_permanent': '❌'}.get(status, '❓')
        logger.info(f"  {emoji} {status.capitalize()}: {count}")

    audio_files = list(AUDIO_DIR.glob("*.webm"))
    total_size_mb = sum(f.stat().st_size for f in audio_files) / (1024 * 1024)
    logger.info("\nArquivos de áudio:")
    logger.info(f"  📁 Total de arquivos: {len(audio_files)}")
    logger.info(f"  💾 Tamanho total: {total_size_mb:.1f} MB")
    logger.info("=" * 60)


if __name__ == "__main__":
    if os.path.basename(os.getcwd()) == 'GuessSong':
        os.chdir('server')
    
    parser = argparse.ArgumentParser(description="Cache de Músicas do Spotify com SQLite integrado.")
    parser.add_argument('urls', nargs='+', help="Uma ou mais URLs de playlists do Spotify.")
    parser.add_argument('--concurrency', type=int, default=2, help="Número de downloads simultâneos (recomendado: 2-4).")
    parser.add_argument('--debug', action='store_true', help="Ativar logs de debug para diagnóstico.")
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        asyncio.run(main(args.urls, args.concurrency))
    except KeyboardInterrupt:
        logger.info("\n🛑 Processo interrompido pelo usuário.")
        logger.info("As faixas já baixadas foram salvas no banco de dados.")
    except Exception as e:
        logger.error(f"Erro inesperado na execução principal: {e}", exc_info=args.debug)