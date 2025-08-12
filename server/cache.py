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
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Criar a tabela tracks se não existir
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
            
            # Criar índices para melhor performance
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_tracks_status ON tracks(status)
            ''')
            
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_tracks_artist ON tracks(artist)
            ''')
            
            conn.commit()
            conn.close()
            logger.info(f"Banco de dados inicializado: {self.db_path}")
            
        except Exception as e:
            logger.error(f"Erro ao inicializar banco de dados: {e}")
            raise
    
    def get_connection(self):
        """Retorna uma conexão com o banco de dados"""
        return sqlite3.connect(self.db_path)
    
    def add_tracks_to_db(self, tracks: List[Dict]):
        """Adiciona tracks ao banco de dados"""
        if not tracks:
            return
            
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            for track in tracks:
                cursor.execute('''
                    INSERT OR IGNORE INTO tracks (id, title, artist, status)
                    VALUES (?, ?, ?, 'pending')
                ''', (track['id'], track['title'], track['artist']))
            
            conn.commit()
            conn.close()
            logger.info(f"Adicionadas {len(tracks)} tracks ao banco de dados")
            
        except Exception as e:
            logger.error(f"Erro ao adicionar tracks ao banco: {e}")
            raise
    
    def get_tracks_by_status(self, status: str) -> List[Dict]:
        """Busca tracks por status"""
        try:
            conn = self.get_connection()
            conn.row_factory = sqlite3.Row  # Para acessar por nome da coluna
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT id, title, artist, status, filepath
                FROM tracks 
                WHERE status = ?
                ORDER BY created_at
            ''', (status,))
            
            tracks = []
            for row in cursor.fetchall():
                tracks.append({
                    'id': row['id'],
                    'title': row['title'],
                    'artist': row['artist'],
                    'status': row['status'],
                    'filepath': row['filepath']
                })
            
            conn.close()
            return tracks
            
        except Exception as e:
            logger.error(f"Erro ao buscar tracks com status '{status}': {e}")
            return []
    
    def get_all_processed_track_ids(self) -> set:
        """Retorna IDs de todas as tracks já processadas"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('SELECT id FROM tracks')
            ids = {row[0] for row in cursor.fetchall()}
            
            conn.close()
            return ids
            
        except Exception as e:
            logger.error(f"Erro ao buscar IDs processados: {e}")
            return set()
    
    def update_track_status(self, track_id: str, status: str, filepath: Optional[str] = None):
        """Atualiza o status de uma track"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            if filepath:
                cursor.execute('''
                    UPDATE tracks 
                    SET status = ?, filepath = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (status, filepath, track_id))
            else:
                cursor.execute('''
                    UPDATE tracks 
                    SET status = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (status, track_id))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Erro ao atualizar status da track {track_id}: {e}")
    
    def get_stats(self) -> Dict[str, int]:
        """Retorna estatísticas do banco"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT status, COUNT(*) 
                FROM tracks 
                GROUP BY status
            ''')
            
            stats = {}
            for row in cursor.fetchall():
                stats[row[0]] = row[1]
            
            conn.close()
            return stats
            
        except Exception as e:
            logger.error(f"Erro ao buscar estatísticas: {e}")
            return {}

# Inicializar o gerenciador de banco de dados
db = DatabaseManager()

def _download_song_segment(search_query: str, output_path: Path, duration: int):
    """Baixa um segmento de áudio de uma música usando yt-dlp."""
    
    # Configuração mais robusta do yt-dlp
    ydl_opts = {
        'format': 'bestaudio[ext=webm]/bestaudio[ext=m4a]/bestaudio/best',
        'outtmpl': str(output_path.with_suffix('.%(ext)s')),
        'quiet': True,
        'no_warnings': True,
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
        'ignoreerrors': True,
        'retries': 2,
        'fragment_retries': 2,
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
            '-x', '4', '-s', '4', '-k', '1M', 
            '--console-log-level=error',
            '--summary-interval=0'
        ]

    try:
        logger.debug(f"Tentando download: {search_query}")
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # Fazer o download diretamente
            ydl.download([search_query])
        
        # Verificar se o arquivo foi criado com sucesso
        possible_extensions = ['.webm', '.m4a', '.mp3', '.opus', '.ogg']
        actual_file = None
        
        # Primeiro verificar se o arquivo base existe
        base_path = output_path.with_suffix('')
        parent_dir = base_path.parent
        base_name = base_path.name
        
        # Procurar por arquivos com o nome base e extensões válidas
        for file_path in parent_dir.glob(f"{base_name}.*"):
            if file_path.suffix.lower() in possible_extensions and file_path.stat().st_size > 5000:  # pelo menos 5KB
                actual_file = file_path
                logger.debug(f"Arquivo encontrado: {actual_file} ({actual_file.stat().st_size} bytes)")
                break
        
        if actual_file:
            # Renomear para .webm se não for
            final_path = output_path.with_suffix('.webm')
            if actual_file != final_path:
                try:
                    if final_path.exists():
                        final_path.unlink()  # Remove arquivo existente
                    actual_file.rename(final_path)
                    logger.debug(f"Arquivo renomeado para: {final_path}")
                except OSError as e:
                    logger.debug(f"Erro ao renomear arquivo (mas download ok): {e}")
                    final_path = actual_file  # Manter o arquivo original
            
            logger.debug(f"Download concluído: {final_path} ({final_path.stat().st_size} bytes)")
            return True
        else:
            # Debug: listar todos os arquivos no diretório
            all_files = list(parent_dir.glob(f"{base_name}*"))
            logger.debug(f"Arquivo não encontrado. Arquivos similares: {[f.name for f in all_files]}")
            return False
            
    except Exception as e:
        logger.debug(f"Erro durante download de '{search_query}': {e}")
        return False

def cleanup_failed_download(filepath: Path):
    """Remove arquivos corrompidos ou de 0 bytes."""
    if not filepath:
        return
        
    # Limpar arquivos com diferentes extensões
    possible_extensions = ['.webm', '.m4a', '.mp3', '.opus', '.ogg', '.part', '.tmp', '.ytdl']
    
    base_path = filepath.with_suffix('')
    parent_dir = base_path.parent
    base_name = base_path.name
    
    # Remover todos os arquivos que começam com o nome base
    for file_path in parent_dir.glob(f"{base_name}*"):
        if file_path.suffix.lower() in possible_extensions or file_path.suffix in ['.part', '.tmp', '.ytdl']:
            try:
                logger.debug(f"Limpando arquivo: {file_path}")
                file_path.unlink()
            except OSError as e:
                logger.debug(f"Erro ao limpar arquivo {file_path}: {e}")

async def download_track_async(track: Dict, is_retry=False):
    """Wrapper assíncrono para o download de uma faixa, com atualização no DB."""
    track_id = track['id']
    title = track['title']
    artist = track['artist']
    output_path = AUDIO_DIR / f"{track_id}.webm"
    
    logger.info(f"BAIXANDO ({'nova tentativa' if is_retry else '1ª tentativa'}): '{title} - {artist}'")
    
    # Melhorar a query de busca
    # Limpar caracteres especiais que podem causar problemas
    clean_title = ''.join(char for char in title if char.isalnum() or char.isspace() or char in '-_')
    clean_artist = ''.join(char for char in artist if char.isalnum() or char.isspace() or char in '-_')
    
    # Tentar diferentes estratégias de busca
    search_queries = [
        f"{clean_artist} {clean_title}",
        f"{clean_artist} - {clean_title}",
        f"{clean_title} {clean_artist}",
        f'"{clean_artist}" "{clean_title}"'
    ]
    
    # Limpar arquivos anteriores antes de tentar
    cleanup_failed_download(output_path)
    
    success = False
    
    # Tentar diferentes queries até uma funcionar
    for i, search_query in enumerate(search_queries):
        if i > 0:  # A partir da segunda tentativa, informar
            logger.debug(f"Tentativa {i+1} com query: {search_query}")
        
        try:
            success = await asyncio.to_thread(_download_song_segment, search_query, output_path, DOWNLOAD_DURATION)
            
            if success:
                # Verificar se o arquivo realmente existe
                final_file = output_path.with_suffix('.webm')
                
                # Procurar qualquer arquivo com o nome base se .webm não existir
                if not final_file.exists():
                    base_path = output_path.with_suffix('')
                    for file_path in base_path.parent.glob(f"{base_path.name}.*"):
                        if file_path.suffix.lower() in ['.webm', '.m4a', '.mp3', '.opus'] and file_path.stat().st_size > 5000:
                            final_file = file_path
                            break
                
                if final_file.exists() and final_file.stat().st_size > 5000:
                    logger.info(f"SUCESSO: '{title}' baixado ({final_file.stat().st_size} bytes).")
                    db.update_track_status(track_id, 'downloaded', str(final_file))
                    return 'downloaded'
                else:
                    logger.debug(f"Arquivo não existe após download 'bem-sucedido': {final_file}")
                    success = False
            
            if success:
                break
                
        except Exception as e:
            logger.debug(f"Erro na tentativa {i+1} para '{title}': {e}")
            continue
    
    # Se chegou aqui, todas as tentativas falharam
    logger.error(f"FALHA: Não foi possível baixar '{title}' (testadas {len(search_queries)} queries).")
    final_status = 'failed_permanent' if is_retry else 'failed'
    db.update_track_status(track_id, final_status)
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
    
    # Controle de concorrência conservador
    actual_concurrency = min(concurrency, 3)  # Máximo 3 downloads simultâneos
    
    tasks = [download_track_async(track, is_retry) for track in tracks_to_process]
    semaphore = asyncio.Semaphore(actual_concurrency)
    
    stats = {'downloaded': 0, 'failed': 0, 'failed_permanent': 0}
    
    async def run_with_semaphore(task):
        async with semaphore:
            result = await task
            if result in stats:
                stats[result] += 1
            # Pausa entre downloads para evitar rate limiting
            await asyncio.sleep(1.0)
    
    results = await asyncio.gather(*(run_with_semaphore(t) for t in tasks), return_exceptions=True)
    
    # Tratar exceções
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(f"Exceção no download {i}: {result}")
            stats['failed'] += 1
    
    return stats

async def main(playlist_urls: List[str], concurrency: int):
    """Função principal para orquestrar o processo de cache."""
    if not sp:
        return
    
    logger.info("=" * 60)
    logger.info("INICIANDO PROCESSO DE CACHE DE MÚSICAS")
    logger.info("=" * 60)
    logger.info(f"Nível de concorrência: {concurrency}")
    logger.info(f"Banco de dados: {DB_PATH}")
    
    # Verificar estatísticas iniciais
    initial_stats = db.get_stats()
    logger.info(f"Estado inicial do banco: {initial_stats}")
    
    # 1. Buscar todas as faixas das playlists e adicionar ao DB
    all_tracks_from_spotify = []
    
    try:
        processed_ids = db.get_all_processed_track_ids()
        logger.info(f"{len(processed_ids)} faixas já no banco de dados.")
    except Exception as e:
        logger.error(f"Erro ao acessar banco de dados: {e}")
        processed_ids = set()
    
    for url in playlist_urls:
        tracks = await fetch_playlist_tracks(url)
        # Filtrar faixas que ainda não estão no banco
        unprocessed_tracks = [t for t in tracks if t['id'] not in processed_ids]
        all_tracks_from_spotify.extend(unprocessed_tracks)
        logger.info(f"Novas faixas desta playlist: {len(unprocessed_tracks)}")
    
    if all_tracks_from_spotify:
        try:
            db.add_tracks_to_db(all_tracks_from_spotify)
            logger.info(f"✅ Adicionadas {len(all_tracks_from_spotify)} novas faixas ao banco.")
        except Exception as e:
            logger.error(f"Erro ao adicionar faixas ao banco: {e}")
            return
    else:
        logger.info("Nenhuma faixa nova encontrada.")
    
    # 2. Primeira tentativa de download para faixas pendentes
    logger.info("\n" + "=" * 60)
    logger.info("FASE 1: PRIMEIRA TENTATIVA DE DOWNLOAD")
    logger.info("=" * 60)
    
    try:
        pending_tracks = db.get_tracks_by_status('pending')
        logger.info(f"Encontradas {len(pending_tracks)} faixas pendentes para download.")
        
        if len(pending_tracks) == 0:
            logger.info("💡 Nenhuma faixa pendente. Verificando estado do banco...")
            current_stats = db.get_stats()
            logger.info(f"Estado atual: {current_stats}")
            
            # Se há tracks downloaded mas arquivos podem estar faltando
            if current_stats.get('downloaded', 0) > 0:
                downloaded_tracks = db.get_tracks_by_status('downloaded')
                missing_files = 0
                for track in downloaded_tracks:
                    if track.get('filepath'):
                        file_path = Path(track['filepath'])
                        if not file_path.exists() or file_path.stat().st_size < 5000:
                            db.update_track_status(track['id'], 'pending')
                            missing_files += 1
                
                if missing_files > 0:
                    logger.info(f"↻ Resetadas {missing_files} faixas (arquivos ausentes)")
                    pending_tracks = db.get_tracks_by_status('pending')
        
    except Exception as e:
        logger.error(f"Erro ao buscar faixas pendentes: {e}")
        pending_tracks = []
    
    stats1 = await process_downloads(pending_tracks, concurrency, is_retry=False)
    
    # 3. Segunda tentativa para faixas que falharam
    logger.info("\n" + "=" * 60)
    logger.info("FASE 2: SEGUNDA TENTATIVA PARA FALHAS")
    logger.info("=" * 60)
    
    try:
        failed_tracks = db.get_tracks_by_status('failed')
        logger.info(f"Encontradas {len(failed_tracks)} faixas para nova tentativa.")
        
        # Limpar arquivos corrompidos antes de tentar novamente
        for track in failed_tracks:
            if track.get('filepath'):
                cleanup_failed_download(Path(track['filepath']))
    
    except Exception as e:
        logger.error(f"Erro ao buscar faixas falhadas: {e}")
        failed_tracks = []
    
    stats2 = await process_downloads(failed_tracks, concurrency, is_retry=True)
    
    # 4. Resumo final
    logger.info("\n" + "=" * 60)
    logger.info("RESUMO FINAL")
    logger.info("=" * 60)
    
    total_downloaded = stats1.get('downloaded', 0) + stats2.get('downloaded', 0)
    total_failed_temp = stats1.get('failed', 0)
    total_failed_perm = stats1.get('failed_permanent', 0) + stats2.get('failed_permanent', 0)
    
    final_stats = db.get_stats()
    
    logger.info(f"Nesta execução:")
    logger.info(f"  ✅ Baixadas: {total_downloaded}")
    logger.info(f"  ⏳ Falharam (nova tentativa): {total_failed_temp}")
    logger.info(f"  ❌ Falhas permanentes: {total_failed_perm}")
    
    logger.info(f"\nEstado final do banco:")
    for status, count in final_stats.items():
        emoji = {'pending': '⏳', 'downloaded': '✅', 'failed': '🔄', 'failed_permanent': '❌'}.get(status, '❓')
        logger.info(f"  {emoji} {status}: {count}")
    
    # Verificar arquivos no diretório
    audio_files = list(AUDIO_DIR.glob("*.webm")) + list(AUDIO_DIR.glob("*.m4a")) + list(AUDIO_DIR.glob("*.mp3"))
    total_size_mb = sum(f.stat().st_size for f in audio_files) / (1024 * 1024)
    
    logger.info(f"\nArquivos de áudio:")
    logger.info(f"  📁 Total de arquivos: {len(audio_files)}")
    logger.info(f"  💾 Tamanho total: {total_size_mb:.1f} MB")
    logger.info("=" * 60)

if __name__ == "__main__":
    # Muda o diretório de trabalho para a pasta 'server' se necessário
    if os.path.basename(os.getcwd()) == 'GuessSong':
        os.chdir('server')
    
    parser = argparse.ArgumentParser(description="Cache de Músicas do Spotify com SQLite integrado.")
    parser.add_argument('urls', nargs='+', help="Uma ou mais URLs de playlists do Spotify.")
    parser.add_argument('--concurrency', type=int, default=2, help="Número de downloads simultâneos (recomendado: 2-3).")
    parser.add_argument('--debug', action='store_true', help="Ativar logs de debug.")
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        asyncio.run(main(args.urls, args.concurrency))
    except KeyboardInterrupt:
        logger.info("\n🛑 Processo interrompido pelo usuário.")
        logger.info("As faixas já baixadas foram salvas no banco de dados.")
    except Exception as e:
        logger.error(f"Erro inesperado: {e}")
        import traceback
        traceback.print_exc()