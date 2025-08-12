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
import time

# --- Configuração de Logging ---
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('cache.log')
    ]
)
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
    logger.info("Conexão with Spotify estabelecida com sucesso.")
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
                    error_count INTEGER DEFAULT 0,
                    last_error TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Adicionar colunas se não existirem (para compatibilidade)
            try:
                cursor.execute('ALTER TABLE tracks ADD COLUMN error_count INTEGER DEFAULT 0')
            except sqlite3.OperationalError:
                pass  # Coluna já existe
            
            try:
                cursor.execute('ALTER TABLE tracks ADD COLUMN last_error TEXT')
            except sqlite3.OperationalError:
                pass  # Coluna já existe
            
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
    
    def get_tracks_by_status(self, status: str, limit: Optional[int] = None) -> List[Dict]:
        """Busca tracks por status com limite opcional"""
        try:
            conn = self.get_connection()
            conn.row_factory = sqlite3.Row  # Para acessar por nome da coluna
            cursor = conn.cursor()
            
            query = '''
                SELECT id, title, artist, status, filepath, error_count, last_error
                FROM tracks 
                WHERE status = ?
                ORDER BY created_at
            '''
            
            if limit:
                query += f' LIMIT {limit}'
            
            cursor.execute(query, (status,))
            
            tracks = []
            for row in cursor.fetchall():
                tracks.append({
                    'id': row['id'],
                    'title': row['title'],
                    'artist': row['artist'],
                    'status': row['status'],
                    'filepath': row['filepath'],
                    'error_count': row['error_count'] or 0,
                    'last_error': row['last_error']
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
    
    def update_track_status(self, track_id: str, status: str, filepath: Optional[str] = None, error_msg: Optional[str] = None):
        """Atualiza o status de uma track"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Incrementar contador de erro se for falha
            if status in ['failed', 'failed_permanent'] and error_msg:
                cursor.execute('''
                    UPDATE tracks 
                    SET status = ?, filepath = ?, error_count = error_count + 1, 
                        last_error = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (status, filepath, error_msg, track_id))
            elif filepath:
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
            
            # Adicionar estatísticas de erro
            cursor.execute('SELECT COUNT(*) FROM tracks WHERE error_count > 0')
            stats['tracks_with_errors'] = cursor.fetchone()[0]
            
            cursor.execute('SELECT AVG(error_count) FROM tracks WHERE error_count > 0')
            avg_errors = cursor.fetchone()[0]
            stats['avg_error_count'] = round(avg_errors, 2) if avg_errors else 0
            
            conn.close()
            return stats
            
        except Exception as e:
            logger.error(f"Erro ao buscar estatísticas: {e}")
            return {}

# Inicializar o gerenciador de banco de dados
db = DatabaseManager()

def test_yt_dlp():
    """Testa se yt-dlp está funcionando corretamente"""
    logger.info("Testando yt-dlp...")
    test_opts = {
        'quiet': True,
        'no_warnings': True,
        'extract_flat': True
    }
    
    try:
        with yt_dlp.YoutubeDL(test_opts) as ydl:
            info = ydl.extract_info("ytsearch1:test music", download=False)
            if info and 'entries' in info and len(info['entries']) > 0:
                logger.info("✅ yt-dlp está funcionando corretamente")
                return True
            else:
                logger.error("❌ yt-dlp não retornou resultados de busca")
                return False
    except Exception as e:
        logger.error(f"❌ Erro ao testar yt-dlp: {e}")
        return False

def _download_song_segment(search_query: str, output_path: Path, duration: int = 30):
    """Baixa um segmento de áudio de uma música usando yt-dlp."""
    
    start_time = random.randint(20, 70)
    
    # Configurações mais robustas do yt-dlp
    ydl_opts = {
        'format': 'bestaudio[ext=webm]/bestaudio[ext=m4a]/bestaudio/best',
        'postprocessor_args': ['-ss', str(start_time), '-t', str(duration)],
        'postprocessors': [{
            'key': 'FFmpegExtractAudio', 
            'preferredcodec': 'webm', 
            'preferredquality': '128'  # Melhor qualidade
        }],
        'outtmpl': str(output_path).replace('.webm', ''),
        'quiet': True,
        'noprogress': True,
        'default_search': 'ytsearch1:',
        'socket_timeout': 60,
        'retries': 3,
        'fragment_retries': 3,
        'ignoreerrors': False,
        'no_warnings': True,
        'extract_flat': False,
        'writesubtitles': False,
        'writeautomaticsub': False,
    }
    
    if ARIA2C_PATH:
        ydl_opts['external_downloader'] = ARIA2C_PATH
        ydl_opts['external_downloader_args'] = [
            '-x', '8',  # Reduzir conexões simultâneas
            '-s', '8',
            '-k', '1M',
            '--console-log-level=warn',
            '--max-tries=3',
            '--retry-wait=2'
        ]

    try:
        logger.debug(f"Tentando baixar: {search_query}")
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([search_query])
        
        # Verificar se o arquivo foi criado
        possible_files = [
            Path(str(output_path).replace('.webm', '') + '.webm'),
            Path(str(output_path).replace('.webm', '') + '.m4a'),
            Path(str(output_path).replace('.webm', '') + '.mp3'),
        ]
        
        for expected_file in possible_files:
            if expected_file.exists() and expected_file.stat().st_size > 5000:  # Pelo menos 5KB
                logger.debug(f"Download bem-sucedido: {expected_file} ({expected_file.stat().st_size} bytes)")
                return str(expected_file)
        
        logger.debug(f"Nenhum arquivo válido encontrado para: {search_query}")
        return False
            
    except yt_dlp.DownloadError as e:
        logger.debug(f"Erro de download para '{search_query}': {e}")
        return False
    except Exception as e:
        logger.debug(f"Erro inesperado no download de '{search_query}': {e}")
        return False

def cleanup_failed_download(filepath: Path):
    """Remove arquivos corrompidos ou de 0 bytes."""
    if not filepath:
        return
    
    base_path = Path(str(filepath).replace('.webm', ''))
    parent_dir = base_path.parent
    base_name = base_path.name
    
    # Lista de extensões para limpar
    extensions_to_clean = ['.webm', '.m4a', '.mp3', '.part', '.tmp', '.ytdl', '.f*']
    
    for ext in extensions_to_clean:
        if '*' in ext:
            # Para padrões como .f*
            for file_path in parent_dir.glob(f"{base_name}{ext}"):
                try:
                    logger.debug(f"Limpando arquivo: {file_path}")
                    file_path.unlink()
                except OSError as e:
                    logger.debug(f"Erro ao limpar arquivo {file_path}: {e}")
        else:
            file_path = parent_dir / f"{base_name}{ext}"
            if file_path.exists():
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
    
    # Não baixar novamente se já existe um arquivo válido
    if track.get('filepath'):
        existing_file = Path(track['filepath'])
        if existing_file.exists() and existing_file.stat().st_size > 5000:
            logger.debug(f"Arquivo já existe: {title}")
            return 'already_downloaded'
    
    retry_text = 'nova tentativa' if is_retry else '1ª tentativa'
    logger.info(f"BAIXANDO ({retry_text}): '{title} - {artist}'")
    
    # Melhorar a query de busca com limpeza mais agressiva
    def clean_text(text):
        # Remover caracteres especiais e normalizar
        import re
        text = re.sub(r'[^\w\s-]', '', text)  # Manter apenas letras, números, espaços e hífens
        text = ' '.join(text.split())  # Normalizar espaços
        return text
    
    clean_title = clean_text(title)
    clean_artist = clean_text(artist)
    
    # Estratégias de busca melhoradas
    search_queries = [
        f"{clean_artist} {clean_title}",
        f"{clean_title} {clean_artist}",
        f"{clean_artist} - {clean_title}",
        f'"{clean_artist}" "{clean_title}"',
        f"{clean_title}",  # Só o título como último recurso
    ]
    
    # Se for retry, tentar queries mais específicas primeiro
    if is_retry:
        search_queries = [
            f'"{clean_artist}" "{clean_title}" official',
            f"{clean_artist} {clean_title} audio",
            f"{clean_artist} {clean_title} music video",
        ] + search_queries
    
    # Limpar arquivos anteriores antes de tentar
    cleanup_failed_download(output_path)
    
    success = False
    downloaded_file = None
    last_error = None
    
    # Tentar diferentes queries até uma funcionar
    for i, search_query in enumerate(search_queries):
        if i > 0:  # A partir da segunda tentativa, informar
            logger.debug(f"Tentativa {i+1}/{len(search_queries)} com query: {search_query}")
        
        try:
            result = await asyncio.to_thread(_download_song_segment, search_query, output_path, DOWNLOAD_DURATION)
            
            if result and result != False:
                downloaded_file = result
                success = True
                logger.info(f"SUCESSO: '{title}' baixado como {Path(downloaded_file).name}")
                db.update_track_status(track_id, 'downloaded', downloaded_file)
                return 'downloaded'
            
        except Exception as e:
            last_error = str(e)
            logger.debug(f"Erro na tentativa {i+1} para '{title}': {e}")
            continue
    
    # Se chegou aqui, todas as tentativas falharam
    error_count = track.get('error_count', 0) + 1
    final_status = 'failed_permanent' if (is_retry or error_count >= 3) else 'failed'
    
    logger.error(f"FALHA: '{title}' - {len(search_queries)} tentativas (erro #{error_count})")
    
    db.update_track_status(track_id, final_status, error_msg=last_error)
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
                    # Filtrar tracks que não são música (podcasts, etc.)
                    if track.get('type') == 'track' and track.get('preview_url') is not None:
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
                
        logger.info(f"Encontradas {len(tracks)} faixas válidas na playlist.")
        return tracks
        
    except Exception as e:
        logger.error(f"Não foi possível buscar a playlist '{playlist_url}'. Erro: {e}")
        return []

async def process_downloads(tracks_to_process: List[Dict], concurrency: int, is_retry=False, batch_size: int = 50):
    """Processa downloads em lotes menores com controle de concorrência."""
    if not tracks_to_process:
        return {}
    
    # Processar em lotes menores para evitar sobrecarregar
    stats = {'downloaded': 0, 'failed': 0, 'failed_permanent': 0, 'already_downloaded': 0}
    total_tracks = len(tracks_to_process)
    
    for i in range(0, total_tracks, batch_size):
        batch = tracks_to_process[i:i+batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (total_tracks + batch_size - 1) // batch_size
        
        logger.info(f"📦 Processando lote {batch_num}/{total_batches} ({len(batch)} tracks)")
        
        # Controle de concorrência conservador
        actual_concurrency = min(concurrency, 2)  # Máximo 2 downloads simultâneos
        semaphore = asyncio.Semaphore(actual_concurrency)
        
        async def run_with_semaphore(track):
            async with semaphore:
                result = await download_track_async(track, is_retry)
                if result in stats:
                    stats[result] += 1
                # Pausa entre downloads para ser gentil com os serviços
                await asyncio.sleep(2.0)
                return result
        
        tasks = [run_with_semaphore(track) for track in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Tratar exceções
        for j, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Exceção no download do lote {batch_num}, track {j}: {result}")
                stats['failed'] += 1
        
        # Pausa entre lotes
        if i + batch_size < total_tracks:
            logger.info(f"⏳ Pausando 5s antes do próximo lote...")
            await asyncio.sleep(5.0)
    
    return stats

async def main(playlist_urls: List[str], concurrency: int, limit: Optional[int] = None):
    """Função principal para orquestrar o processo de cache."""
    if not sp:
        return
    
    # Testar yt-dlp primeiro
    if not test_yt_dlp():
        logger.error("❌ yt-dlp não está funcionando. Verifique a instalação.")
        return
    
    logger.info("=" * 60)
    logger.info("INICIANDO PROCESSO DE CACHE DE MÚSICAS")
    logger.info("=" * 60)
    logger.info(f"Nível de concorrência: {concurrency}")
    logger.info(f"Banco de dados: {DB_PATH}")
    if limit:
        logger.info(f"Limite de downloads: {limit}")
    
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
        pending_tracks = db.get_tracks_by_status('pending', limit=limit)
        logger.info(f"Encontradas {len(pending_tracks)} faixas pendentes para download.")
        
        if len(pending_tracks) == 0:
            logger.info("💡 Nenhuma faixa pendente. Verificando arquivos existentes...")
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
                pending_tracks = db.get_tracks_by_status('pending', limit=limit)
        
    except Exception as e:
        logger.error(f"Erro ao buscar faixas pendentes: {e}")
        pending_tracks = []
    
    stats1 = await process_downloads(pending_tracks, concurrency, is_retry=False)
    
    # 3. Segunda tentativa para faixas que falharam (apenas se não há limite ou se o limite não foi atingido)
    if not limit or stats1.get('downloaded', 0) < limit:
        logger.info("\n" + "=" * 60)
        logger.info("FASE 2: SEGUNDA TENTATIVA PARA FALHAS")
        logger.info("=" * 60)
        
        remaining_limit = None
        if limit:
            remaining_limit = limit - stats1.get('downloaded', 0)
            if remaining_limit <= 0:
                logger.info("Limite de downloads atingido.")
                stats2 = {}
            else:
                logger.info(f"Tentando mais {remaining_limit} downloads...")
        
        if not limit or remaining_limit > 0:
            try:
                failed_tracks = db.get_tracks_by_status('failed', limit=remaining_limit)
                logger.info(f"Encontradas {len(failed_tracks)} faixas para nova tentativa.")
            except Exception as e:
                logger.error(f"Erro ao buscar faixas falhadas: {e}")
                failed_tracks = []
            
            stats2 = await process_downloads(failed_tracks, concurrency, is_retry=True)
        else:
            stats2 = {}
    else:
        logger.info("Limite de downloads atingido, pulando fase 2.")
        stats2 = {}
    
    # 4. Resumo final
    logger.info("\n" + "=" * 60)
    logger.info("RESUMO FINAL")
    logger.info("=" * 60)
    
    total_downloaded = stats1.get('downloaded', 0) + stats2.get('downloaded', 0)
    total_failed_temp = stats1.get('failed', 0)
    total_failed_perm = stats1.get('failed_permanent', 0) + stats2.get('failed_permanent', 0)
    total_already_downloaded = stats1.get('already_downloaded', 0) + stats2.get('already_downloaded', 0)
    
    final_stats = db.get_stats()
    
    logger.info(f"Nesta execução:")
    logger.info(f"  ✅ Baixadas: {total_downloaded}")
    logger.info(f"  📁 Já existiam: {total_already_downloaded}")
    logger.info(f"  ⏳ Falharam (nova tentativa): {total_failed_temp}")
    logger.info(f"  ❌ Falhas permanentes: {total_failed_perm}")
    
    logger.info(f"\nEstado final do banco:")
    for status, count in final_stats.items():
        if status in ['pending', 'downloaded', 'failed', 'failed_permanent']:
            emoji = {'pending': '⏳', 'downloaded': '✅', 'failed': '🔄', 'failed_permanent': '❌'}.get(status, '❓')
            logger.info(f"  {emoji} {status}: {count}")
    
    if final_stats.get('tracks_with_errors', 0) > 0:
        logger.info(f"  📊 Tracks com erros: {final_stats['tracks_with_errors']}")
        logger.info(f"  📊 Média de erros: {final_stats['avg_error_count']}")
    
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
    parser.add_argument('--concurrency', type=int, default=2, help="Número de downloads simultâneos (recomendado: 1-2).")
    parser.add_argument('--limit', type=int, help="Limite máximo de downloads por execução.")
    parser.add_argument('--debug', action='store_true', help="Ativar logs de debug.")
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try: