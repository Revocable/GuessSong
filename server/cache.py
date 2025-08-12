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
    logger.info("Conexão com Spotify estabelecida com sucesso.")
except Exception as e:
    logger.error(f"ERRO: Falha ao conectar com o Spotify. Verifique suas credenciais. Detalhes: {e}")
    sp = None
    sys.exit(1)

# --- Criação do Diretório de Áudio ---
AUDIO_DIR.mkdir(parents=True, exist_ok=True)

def _download_song_segment(search_query: str, output_path: Path, duration: int):
    """Baixa um segmento de áudio de uma música usando yt-dlp."""
    
    # Configuração mais robusta do yt-dlp
    ydl_opts = {
        'format': 'bestaudio[ext=webm]/bestaudio[ext=m4a]/bestaudio/best',
        'outtmpl': str(output_path.with_suffix('.%(ext)s')),
        'quiet': False,  # Mudado para False para debug
        'no_warnings': False,
        'extractaudio': True,
        'audioformat': 'webm',
        'audioquality': '64K',
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'webm',
            'preferredquality': '64',
        }],
        'postprocessor_args': [
            '-ss', str(random.randint(20, 70)),  # início aleatório
            '-t', str(duration)  # duração
        ],
        'default_search': 'ytsearch1:',
        'ignoreerrors': False,
        'retries': 3,
        'fragment_retries': 3,
        'skip_unavailable_fragments': True,
        'keep_fragments': False,
        'extract_flat': False,
        'writethumbnail': False,
        'writeinfojson': False,
        'writesubtitles': False,
        'writeautomaticsub': False,
    }
    
    # Adicionar aria2c se disponível
    if ARIA2C_PATH:
        ydl_opts['external_downloader'] = ARIA2C_PATH
        ydl_opts['external_downloader_args'] = [
            '-x', '8', '-s', '8', '-k', '1M', 
            '--console-log-level=warn',
            '--summary-interval=0'
        ]

    try:
        logger.debug(f"Tentando download: {search_query}")
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # Primeiro, extrair informações sem baixar
            try:
                info = ydl.extract_info(search_query, download=False)
                if not info or 'entries' not in info or not info['entries']:
                    logger.error(f"Nenhum resultado encontrado para: {search_query}")
                    return False
                    
                video_info = info['entries'][0]
                logger.debug(f"Encontrado: {video_info.get('title', 'Título desconhecido')}")
                
            except Exception as e:
                logger.error(f"Erro ao extrair informações para '{search_query}': {e}")
                return False
            
            # Agora fazer o download
            try:
                ydl.download([search_query])
            except Exception as e:
                logger.error(f"Erro durante o download de '{search_query}': {e}")
                return False
        
        # Verificar se o arquivo foi criado com sucesso
        possible_extensions = ['.webm', '.m4a', '.mp3', '.opus']
        actual_file = None
        
        for ext in possible_extensions:
            potential_file = output_path.with_suffix(ext)
            if potential_file.exists() and potential_file.stat().st_size > 1000:  # pelo menos 1KB
                actual_file = potential_file
                break
        
        if actual_file:
            # Renomear para .webm se não for
            if actual_file.suffix != '.webm':
                final_path = output_path.with_suffix('.webm')
                actual_file.rename(final_path)
                actual_file = final_path
            
            logger.debug(f"Download concluído: {actual_file} ({actual_file.stat().st_size} bytes)")
            return True
        else:
            logger.error(f"Arquivo não foi criado ou está corrompido para: {search_query}")
            return False
            
    except Exception as e:
        logger.error(f"Erro inesperado ao baixar '{search_query}': {e}")
        return False

def cleanup_failed_download(filepath: Path):
    """Remove arquivos corrompidos ou de 0 bytes."""
    if not filepath:
        return
        
    # Limpar arquivos com diferentes extensões
    possible_extensions = ['.webm', '.m4a', '.mp3', '.opus', '.part', '.tmp']
    
    for ext in possible_extensions:
        file_to_clean = filepath.with_suffix(ext)
        if file_to_clean.exists():
            try:
                logger.debug(f"Limpando arquivo falho: {file_to_clean}")
                file_to_clean.unlink()
            except OSError as e:
                logger.error(f"Erro ao limpar o arquivo {file_to_clean}: {e}")

async def download_track_async(track: Dict, is_retry=False):
    """Wrapper assíncrono para o download de uma faixa, com atualização no DB."""
    track_id = track['id']
    title = track['title']
    artist = track['artist']
    output_path = AUDIO_DIR / f"{track_id}.webm"
    
    logger.info(f"BAIXANDO ({'nova tentativa' if is_retry else '1ª tentativa'}): '{title} - {artist}'")
    
    # Melhorar a query de busca
    # Limpar caracteres especiais que podem causar problemas
    clean_title = ''.join(char for char in title if char.isalnum() or char.isspace())
    clean_artist = ''.join(char for char in artist if char.isalnum() or char.isspace())
    
    search_query = f"{clean_artist} {clean_title}"
    
    # Limpar arquivos anteriores antes de tentar
    cleanup_failed_download(output_path)
    
    try:
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
            
    except Exception as e:
        logger.error(f"Erro durante download de '{title}': {e}")
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
            for item in results['items']:
                if item and item.get('track') and item['track'].get('id'):
                    track = item['track']
                    # Estruturar os dados da track
                    track_data = {
                        'id': track['id'],
                        'title': track['name'],
                        'artist': ', '.join([artist['name'] for artist in track['artists']])
                    }
                    tracks.append(track_data)
            
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
    
    # Reduzir concorrência para evitar rate limiting
    actual_concurrency = min(concurrency, 5)  # Máximo 5 downloads simultâneos
    
    tasks = [download_track_async(track, is_retry) for track in tracks_to_process]
    semaphore = asyncio.Semaphore(actual_concurrency)
    
    stats = {'downloaded': 0, 'failed': 0, 'failed_permanent': 0}
    
    async def run_with_semaphore(task):
        async with semaphore:
            result = await task
            if result in stats:
                stats[result] += 1
            # Pequena pausa entre downloads para evitar rate limiting
            await asyncio.sleep(0.5)
    
    await asyncio.gather(*(run_with_semaphore(t) for t in tasks), return_exceptions=True)
    return stats

async def main(playlist_urls: List[str], concurrency: int):
    """Função principal para orquestrar o processo de cache."""
    if not sp:
        return
    
    logger.info("--- Iniciando processo de cache de músicas ---")
    logger.info(f"Nível de concorrência: {concurrency}")
    
    # 1. Buscar todas as faixas das playlists e adicionar ao DB
    all_tracks_from_spotify = []
    
    # Verificar se o DB está funcionando corretamente
    try:
        processed_ids = db_manager.get_all_processed_track_ids()
        logger.info(f"{len(processed_ids)} faixas já processadas encontradas no DB.")
    except Exception as e:
        logger.error(f"Erro ao acessar banco de dados: {e}")
        processed_ids = set()
    
    for url in playlist_urls:
        tracks = await fetch_playlist_tracks(url)
        # Filtra faixas que já foram processadas com sucesso ou falha permanente
        unprocessed_tracks = [t for t in tracks if t['id'] not in processed_ids]
        all_tracks_from_spotify.extend(unprocessed_tracks)
    
    if all_tracks_from_spotify:
        try:
            db_manager.add_tracks_to_db(all_tracks_from_spotify)
            logger.info(f"Adicionadas {len(all_tracks_from_spotify)} novas faixas ao banco de dados.")
        except Exception as e:
            logger.error(f"Erro ao adicionar faixas ao banco de dados: {e}")
            return
    
    # 2. Primeira tentativa de download para faixas pendentes
    logger.info("\n--- Fase 1: Primeira Tentativa de Download ---")
    try:
        pending_tracks = db_manager.get_tracks_by_status('pending')
        logger.info(f"Encontradas {len(pending_tracks)} faixas pendentes para baixar.")
        
        # Se não há faixas pendentes mas acabamos de adicionar faixas, há um problema
        if len(pending_tracks) == 0 and len(all_tracks_from_spotify) > 0:
            logger.warning("AVISO: Faixas foram adicionadas mas nenhuma está com status 'pending'")
            logger.info("Verificando todos os status no banco de dados...")
            
            # Debug: verificar todos os status
            all_statuses = {}
            try:
                # Assumindo que existe um método para pegar todas as tracks (precisamos criar se não existir)
                all_tracks = db_manager.get_all_tracks()  # Método que talvez precise ser implementado
                for track in all_tracks:
                    status = track.get('status', 'unknown')
                    all_statuses[status] = all_statuses.get(status, 0) + 1
                
                logger.info(f"Status das faixas no DB: {all_statuses}")
                
                # Se todas estão como 'downloaded' mas não temos arquivos, resetar
                if all_statuses.get('downloaded', 0) > 0:
                    logger.info("Verificando se arquivos realmente existem...")
                    downloaded_tracks = db_manager.get_tracks_by_status('downloaded')
                    files_missing = 0
                    for track in downloaded_tracks:
                        if track.get('filepath'):
                            file_path = Path(track['filepath'])
                            if not file_path.exists() or file_path.stat().st_size == 0:
                                files_missing += 1
                                # Resetar status para pending
                                db_manager.update_track_status(track['id'], 'pending')
                    
                    if files_missing > 0:
                        logger.info(f"Resetados {files_missing} tracks para 'pending' (arquivos não encontrados)")
                        pending_tracks = db_manager.get_tracks_by_status('pending')
                        
            except Exception as e:
                logger.error(f"Erro ao verificar status das tracks: {e}")
                # Como fallback, pegar as primeiras 10 tracks para testar
                logger.info("Tentando pegar algumas tracks para teste...")
                pending_tracks = all_tracks_from_spotify[:10] if all_tracks_from_spotify else []
                
    except Exception as e:
        logger.error(f"Erro ao buscar faixas pendentes: {e}")
        pending_tracks = []
    
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
    parser.add_argument('--concurrency', type=int, default=3, help="Número de downloads simultâneos (recomendado: 3-5).")
    
    args = parser.parse_args()
    
    try:
        asyncio.run(main(args.urls, args.concurrency))
    except KeyboardInterrupt:
        logger.info("\nProcesso interrompido pelo usuário. Saindo.")