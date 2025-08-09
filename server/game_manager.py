import asyncio
import os
import random
import time
import logging
import re  # Módulo de expressões regulares
from pathlib import Path
import spotipy
import yt_dlp
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv
from fastapi import WebSocket
from typing import Dict, List, Set, Optional

# --- Configuração ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
load_dotenv()

try:
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
        client_id=os.getenv("SPOTIPY_CLIENT_ID"),
        client_secret=os.getenv("SPOTIPY_CLIENT_SECRET")
    ))
except Exception:
    logger.error("ERRO: Verifique suas credenciais do Spotify no arquivo .env.")
    sp = None

Path("static/audio").mkdir(parents=True, exist_ok=True)

# --- FUNÇÃO DE NORMALIZAÇÃO ATUALIZADA ---
def normalize_string(text: str) -> str:
    """
    Prepara uma string para uma comparação justa:
    1. Remove conteúdo entre parênteses e colchetes (ex: feat, remix, live).
    2. Converte para minúsculas.
    3. Remove caracteres especiais (mantém apenas letras, números e espaços).
    4. Normaliza múltiplos espaços para um único espaço.
    5. Remove espaços nas pontas.
    """
    # Passo 1: Remove conteúdo entre parênteses () e colchetes []
    text = re.sub(r'\s*[\(\[].*?[\)\]]', '', text).strip()
    
    # Passo 2: Converte para minúsculas
    text = text.lower()
    
    # Passo 3: Remove qualquer coisa que não seja letra, número ou espaço
    text = re.sub(r'[^a-z0-9\s]', '', text)
    
    # Passo 4 e 5: Normaliza espaços
    text = ' '.join(text.split())
    return text.strip()


# --- Estruturas de Dados ---
class Player:
    def __init__(self, username: str, websocket: Optional[WebSocket]):
        self.username = username
        self.websocket = websocket
        self.score = 0
        self.has_answered = False
        self.gave_up = False # Novo atributo

    def to_dict(self):
        # Inclui o novo atributo no dicionário
        return {"username": self.username, "score": self.score, "has_answered": self.has_answered, "gave_up": self.gave_up}

class GameRoom:
    def __init__(self, host: Player, playlist_url: str, round_duration: int):
        self.room_id = "".join(random.choices("ABCDEFGHJKLMNPQRSTUVWXYZ23456789", k=5))
        self.host = host
        self.players: Dict[str, Player] = {}
        self.playlist_url = playlist_url
        self.game_settings = {"round_duration": round_duration, "total_rounds": 10}
        self.game_state = "LOBBY"
        self.current_round = 0
        self.current_song = None
        self.round_start_time = 0
        self.game_tracks: List[Dict] = []
        self._round_end_event = asyncio.Event()
        self._round_task: Optional[asyncio.Task] = None
    
    async def broadcast(self, message: dict):
        websockets = [p.websocket for p in self.players.values() if p.websocket]
        for ws in websockets:
            try:
                await ws.send_json(message)
            except Exception as e:
                logger.warning(f"Não foi possível enviar mensagem para um websocket: {e}")

    async def broadcast_player_update(self):
        player_list = [p.to_dict() for p in self.players.values()]
        await self.broadcast({
            "type": "update_players",
            "players": sorted(player_list, key=lambda p: p['score'], reverse=True),
            "host_username": self.host.username
        })

    async def add_player(self, player: Player):
        if player.username not in self.players:
            self.players[player.username] = player
        else:
            self.players[player.username].websocket = player.websocket
        await self.broadcast_player_update()

    async def remove_player(self, username: str):
        if username in self.players:
            player = self.players.pop(username)
            if not self.players or player.username == self.host.username:
                self.game_state = "GAME_OVER"
                if self._round_task:
                    self._round_task.cancel()
            else:
                await self.broadcast_player_update()
    
    def download_song_segment(self, search_query: str, output_path: str, duration: int):
        start_time = random.randint(20, 70)
        ydl_opts = {
            'format': 'bestaudio/best',
            'postprocessor_args': ['-ss', str(start_time), '-t', str(duration)],
            'postprocessors': [{'key': 'FFmpegExtractAudio', 'preferredcodec': 'mp3'}],
            'outtmpl': output_path.replace('.mp3', ''),
            'quiet': True,
            'default_search': 'ytsearch1',
        }
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([search_query])
            logger.info(f"Segmento baixado: {output_path}")
        except Exception as e:
            logger.error(f"Falha no download para '{search_query}': {e}")

    async def prepare_game_tracks(self):
        self.game_state = "PREPARING"
        await self.broadcast({"type": "system_message", "message": "Buscando todas as músicas da playlist...", "level": "info"})
        
        try:
            results = sp.playlist_tracks(self.playlist_url)
            spotify_tracks = []
            while results:
                spotify_tracks.extend([item['track'] for item in results['items'] if item and item.get('track')])
                if results['next']:
                    results = sp.next(results)
                else:
                    results = None
            
            logger.info(f"Encontradas {len(spotify_tracks)} músicas na playlist.")

            if not spotify_tracks:
                await self.broadcast({"type": "system_message", "message": "Playlist não encontrada ou vazia.", "level": "error"})
                return False

            await self.broadcast({"type": "system_message", "message": f"{len(spotify_tracks)} músicas encontradas. Preparando o jogo...", "level": "info"})

            random.shuffle(spotify_tracks)
            # Garante que não selecionamos mais rodadas do que músicas disponíveis
            num_rounds = min(self.game_settings["total_rounds"], len(spotify_tracks))
            self.game_settings["total_rounds"] = num_rounds # Atualiza o número total de rodadas
            selected_tracks = spotify_tracks[:num_rounds]
            
            self.game_tracks = []
            download_tasks = []
            for track in selected_tracks:
                if not track or not track.get('name') or not track.get('artists'):
                    continue # Pula faixas inválidas
                
                title, artist = track['name'], track['artists'][0]['name']
                filename = f"{track['id']}.mp3"
                filepath = Path("static/audio") / filename
                self.game_tracks.append({"title": title, "artist": artist, "file": filename})
                
                if not filepath.exists():
                    task = asyncio.to_thread(self.download_song_segment, f"{artist} - {title} audio", str(filepath), self.game_settings["round_duration"])
                    download_tasks.append(task)
            
            if download_tasks:
                await self.broadcast({"type": "system_message", "message": f"Baixando {len(download_tasks)} músicas. Isso pode levar um momento...", "level": "info"})
                await asyncio.gather(*download_tasks)
            
            return True
        except Exception as e:
            logger.error(f"Erro ao preparar as músicas: {e}")
            await self.broadcast({"type": "system_message", "message": "Erro ao processar a playlist do Spotify.", "level": "error"})
            return False

    async def start_game(self, starter_username: str):
        if starter_username != self.host.username or self.game_state != "LOBBY": return
        
        success = await self.prepare_game_tracks()
        if not success:
            self.game_state = "LOBBY"; return

        self.game_state = "PLAYING"
        self.current_round = 0
        for player in self.players.values(): player.score = 0
        
        await self.broadcast_player_update()
        await self.broadcast({"type": "system_message", "message": "O jogo vai começar!", "level": "info"})
        await asyncio.sleep(3)
        self._round_task = asyncio.create_task(self.game_loop())

    async def game_loop(self):
        while self.current_round < len(self.game_tracks) and self.game_state == "PLAYING":
            await self.run_next_round()
        await self.end_game()

    async def run_next_round(self):
        self.current_round += 1
        self.current_song = self.game_tracks[self.current_round - 1]
        # Reseta o status de 'desistiu' e 'respondeu' para todos os jogadores
        for p in self.players.values(): 
            p.has_answered = False
            p.gave_up = False
        self._round_end_event.clear()
        
        await self.broadcast_player_update()
        await self.broadcast({
            "type": "start_round",
            "round": self.current_round, "total_rounds": len(self.game_tracks),
            "duration": self.game_settings["round_duration"],
            "song_url": f"/static/audio/{self.current_song['file']}"
        })
        self.round_start_time = time.time()
        
        try:
            # Espera o evento de fim de rodada ou o timeout
            await asyncio.wait_for(self._round_end_event.wait(), timeout=self.game_settings["round_duration"] + 2)
        except asyncio.TimeoutError:
            pass # O timeout é o fluxo normal se ninguém acertar
        finally:
            # Garante que o fim da rodada seja processado
            await self.end_round()

    async def handle_guess(self, username: str, guess_text: str):
        player = self.players.get(username)
        # Impede o palpite se o jogo não estiver rodando, ou se o jogador já respondeu ou desistiu
        if self.game_state != "PLAYING" or not player or player.has_answered or player.gave_up: 
            return

        normalized_title = normalize_string(self.current_song['title'])
        normalized_guess = normalize_string(guess_text)

        is_correct = normalized_guess == normalized_title
        
        if is_correct:
            time_taken = time.time() - self.round_start_time
            points = max(10, 100 - int(time_taken * 5))
            player.score += points
            player.has_answered = True
            
            await self.broadcast({"type": "system_message", "message": f"✅ {username} acertou!", "level": "info"})
            await self.broadcast_player_update()

            # Se todos os jogadores ativos (que não desistiram) responderam, termina a rodada
            if all(p.has_answered or p.gave_up for p in self.players.values()):
                self._round_end_event.set()
        else:
            # Envia feedback de erro apenas para o jogador que errou
            if player.websocket:
                try:
                    await player.websocket.send_json({
                        "type": "guess_result", 
                        "correct": False, 
                        "message": "Você errou! Tente novamente."
                    })
                except Exception as e:
                    logger.warning(f"Não foi possível enviar feedback de erro para {username}: {e}")


    async def handle_give_up(self, username: str):
        player = self.players.get(username)
        if self.game_state != "PLAYING" or not player or player.has_answered or player.gave_up:
            return

        player.gave_up = True
        logger.info(f"Jogador {username} desistiu da rodada.")
        
        # Notifica a todos sobre a atualização do status do jogador
        await self.broadcast_player_update()
        
        # Envia uma mensagem de sistema informando que o jogador desistiu
        await self.broadcast({"type": "system_message", "message": f"⚠️ {username} desistiu da rodada!", "level": "info"})

        # Se todos os jogadores desistiram ou responderam, termina a rodada
        if all(p.has_answered or p.gave_up for p in self.players.values()):
            logger.info("Todos os jogadores desistiram ou responderam. Encerrando a rodada.")
            self._round_end_event.set()


    async def end_round(self):
        # Previne chamadas múltiplas
        if self.game_state == "ROUND_OVER":
            return
        self.game_state = "ROUND_OVER"
        
        await self.broadcast({
            "type": "round_result",
            "correct_title": self.current_song['title'],
            "correct_artist": self.current_song['artist']
        })
        await asyncio.sleep(5)
        self.game_state = "PLAYING" # Prepara para a próxima rodada no loop

    async def end_game(self):
        self.game_state = "GAME_OVER"
        player_list = [p.to_dict() for p in self.players.values()]
        winner = max(player_list, key=lambda p: p['score'], default=None)
        await self.broadcast({
            "type": "game_over",
            "scoreboard": sorted(player_list, key=lambda p: p['score'], reverse=True),
            "winner": winner
        })

class GameManager:
    def __init__(self):
        self.rooms: Dict[str, GameRoom] = {}

    def create_room(self, host: Player, playlist: str, duration: int) -> GameRoom:
        room = GameRoom(host, playlist, duration)
        self.rooms[room.room_id] = room
        return room

    def get_room(self, room_id: str) -> Optional[GameRoom]:
        return self.rooms.get(room_id)
        
    def remove_room(self, room_id: str):
        if room_id in self.rooms:
            del self.rooms[room_id]
            logger.info(f"Sala {room_id} removida.")

game_manager = GameManager()