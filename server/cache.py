import asyncio
import os
import random
import logging
import shutil
import sys
import argparse
from pathlib import Path
import spotipy
import yt_dlp
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv
from typing import List, Dict

# Importa o novo gerenciador de banco de dados
import db_manager

# --- Configuração de Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
load_dotenv()

# --- Constantes ---
AUDIO_DIR = Path("static/audio")
DOWNLOAD_DURATION = 30  # segundos

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
except Exception as e:
    logger.error(f"ERRO: Falha ao conectar com o Spotify. Verifique suas credenciais. Detalhes: {e}")
    sp = None
    sys.exit(1)

# --- Criação do Diretório de Áudio ---
AUDIO_DIR.mkdir(parents=True, exist_ok=True)

def _download_song_segment(search_query: str, output_path: Path, duration: int):
    """Baixa um segmento de áudio de uma música usando yt-dlp."""
    ydl_opts = {
        'format': 'bestaudio/best',
        'postprocessor_args': ['-ss', str(random.randint(20, 70)), '-t', str(duration)],
        'postprocessors': [{'key': 'FFmpegExtractAudio', 'preferredcodec': 'webm', 'preferredquality': '64'}],
        'outtmpl': str(output_path.with_suffix('')), # yt-dlp adiciona a extensão
        'quiet': True,
        'noprogress': True,
        'default_search': 'ytsearch1',
    }
    if ARIA2C_PATH:
        ydl_opts['external_downloader'] = ARIA2C_PATH
        ydl_opts['external_downloader_args'] = ['-x', '16', '-s', '16', '-k', '1M', '--console-log-level=warn']
    
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([search_query])
        return output_path.exists() and output_path.stat().st_size > 0
    except Exception:
        # O yt-dlp já loga o erro, então apenas retornamos False
        return False

def cleanup_failed_download(filepath: Path):
    """Remove arquivos corrompidos ou de 0 bytes."""
    if filepath and filepath.exists():
        try:
            logger.debug(f"Limpando arquivo falho: {filepath}")
            filepath.unlink()
        except OSError as e:
            logger.error(f"Erro ao limpar o arquivo {filepath}: {e}")

async def download_track_async(track: Dict, is_retry=False):
    """Wrapper assíncrono para o download de uma faixa, com atualização no DB."""
    track_id = track['id']
    title = track['title']
    artist = track['artist']
    output_path = AUDIO_DIR / f"{track_id}.webm"
    
    logger.info(f"BAIXANDO ({'nova tentativa' if is_retry else '1ª tentativa'}): '{title} - {artist}'")
    search_query = f"{artist} - {title} audio"
    
    success = await asyncio.to_thread(_download_song_segment, search_query, output_path, DOWNLOAD_DURATION)
    
    if success:
        logger.info(f"SUCESSO: '{title}' baixado.")
        db_manager.update_track_status(track_id, 'downloaded', str(output_path))
        return 'downloaded'
    else:
        logger.error(f"FALHA: Não foi possível baixar '{title}'.")
        # Se for uma nova tentativa, marca como falha permanente. Senão, apenas como falha.
        final_status = 'failed_permanent' if is_retry else 'failed'
        db_manager.update_track_status(track_id, final_status)
        cleanup_failed_download(output_path)
        return final_status

async def fetch_playlist_tracks(playlist_url: str) -> List[Dict]:
    """Busca todas as faixas de uma playlist do Spotify."""
    try:
        logger.info(f"Buscando faixas da playlist: {playlist_url}")
        results = await asyncio.to_thread(sp.playlist_tracks, playlist_url)
        tracks = []
        while results:
            tracks.extend([item['track'] for item in results['items'] if item and item.get('track') and item['track'].get('id')])
            if results['next']:
                results = await asyncio.to_thread(sp.next, results)
            else:
                results = None
        logger.info(f"Encontradas {len(tracks)} faixas na playlist.")
        return tracks
    except Exception as e:
        logger.error(f"Não foi possível buscar a playlist '{playlist_url}'. Erro: {e}")
        return []

async def process_downloads(tracks_to_process: List[Dict], concurrency: int, is_retry=False):
    """Processa uma lista de downloads com um nível de concorrência definido."""
    if not tracks_to_process:
        return {}

    tasks = [download_track_async(track, is_retry) for track in tracks_to_process]
    semaphore = asyncio.Semaphore(concurrency)
    
    stats = {'downloaded': 0, 'failed': 0, 'failed_permanent': 0}

    async def run_with_semaphore(task):
        async with semaphore:
            result = await task
            if result in stats:
                stats[result] += 1

    await asyncio.gather(*(run_with_semaphore(t) for t in tasks))
    return stats

async def main(playlist_urls: List[str], concurrency: int):
    """Função principal para orquestrar o processo de cache."""
    if not sp:
        return

    logger.info("--- Iniciando processo de cache de músicas ---")
    logger.info(f"Nível de concorrência: {concurrency}")

    # 1. Buscar todas as faixas das playlists e adicionar ao DB
    all_tracks_from_spotify = []
    processed_ids = db_manager.get_all_processed_track_ids()
    logger.info(f"{len(processed_ids)} faixas já processadas encontradas no DB.")

    for url in playlist_urls:
        tracks = await fetch_playlist_tracks(url)
        # Filtra faixas que já foram processadas com sucesso ou falha permanente
        unprocessed_tracks = [t for t in tracks if t['id'] not in processed_ids]
        all_tracks_from_spotify.extend(unprocessed_tracks)
    
    db_manager.add_tracks_to_db(all_tracks_from_spotify)

    # 2. Primeira tentativa de download para faixas pendentes
    logger.info("\n--- Fase 1: Primeira Tentativa de Download ---")
    pending_tracks = db_manager.get_tracks_by_status('pending')
    logger.info(f"Encontradas {len(pending_tracks)} faixas pendentes para baixar.")
    
    stats1 = await process_downloads(pending_tracks, concurrency, is_retry=False)

    # 3. Nova tentativa para faixas que falharam
    logger.info("\n--- Fase 2: Nova Tentativa para Downloads Falhos ---")
    failed_tracks = db_manager.get_tracks_by_status('failed')
    logger.info(f"Encontradas {len(failed_tracks)} faixas que falharam para tentar novamente.")

    # Limpa arquivos corrompidos antes de tentar de novo
    for track in failed_tracks:
        if track.get('filepath'):
            cleanup_failed_download(Path(track['filepath']))

    stats2 = await process_downloads(failed_tracks, concurrency, is_retry=True)

    # 4. Exibir resumo
    total_downloaded = stats1.get('downloaded', 0) + stats2.get('downloaded', 0)
    total_failed_perm = stats2.get('failed_permanent', 0)

    logger.info("\n--- Processo de Cache Concluído ---")
    logger.info(f"Músicas Baixadas com Sucesso: {total_downloaded}")
    logger.info(f"Falhas Permanentes (após 2 tentativas): {total_failed_perm}")
    logger.info("------------------------------------")

if __name__ == "__main__":
    # Muda o diretório de trabalho para a pasta 'server' se necessário
    if os.path.basename(os.getcwd()) == 'GuessSong':
        os.chdir('server')

    parser = argparse.ArgumentParser(description="Cache de Músicas do Spotify com SQLite e nova tentativa.")
    parser.add_argument('urls', nargs='+', help="Uma ou mais URLs de playlists do Spotify.")
    parser.add_argument('--concurrency', type=int, default=10, help="Número de downloads simultâneos.")
    
    args = parser.parse_args()

    try:
        asyncio.run(main(args.urls, args.concurrency))
    except KeyboardInterrupt:
        logger.info("\nProcesso interrompido pelo usuário. Saindo.")