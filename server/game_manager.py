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

    def to_dict(self):
        return {"username": self.username, "score": self.score, "has_answered": self.has_answered}

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
        await self.broadcast({"type": "system_message", "message": "Buscando playlist e baixando músicas...", "level": "info"})
        results = sp.playlist_tracks(self.playlist_url)
        spotify_tracks = [item['track'] for item in results['items'] if item and item.get('track')]
        random.shuffle(spotify_tracks)
        selected_tracks = spotify_tracks[:self.game_settings["total_rounds"]]
        
        self.game_tracks = []
        download_tasks = []
        for track in selected_tracks:
            title, artist = track['name'], track['artists'][0]['name']
            filename = f"{track['id']}.mp3"
            filepath = Path("static/audio") / filename
            self.game_tracks.append({"title": title, "artist": artist, "file": filename})
            
            if not filepath.exists():
                task = asyncio.to_thread(self.download_song_segment, f"{artist} - {title} audio", str(filepath), self.game_settings["round_duration"])
                download_tasks.append(task)
        
        if download_tasks:
            await asyncio.gather(*download_tasks)
        return True

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
        for p in self.players.values(): p.has_answered = False
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
            await asyncio.wait_for(self._round_end_event.wait(), timeout=self.game_settings["round_duration"] + 2)
        except asyncio.TimeoutError:
            pass
        finally:
            await self.end_round()

    async def handle_guess(self, username: str, guess_text: str):
        player = self.players.get(username)
        if self.game_state != "PLAYING" or not player or player.has_answered: 
            return

        # Normaliza tanto o título correto quanto a resposta do usuário usando a nova função
        normalized_title = normalize_string(self.current_song['title'])
        normalized_guess = normalize_string(guess_text)

        # A comparação agora é muito mais flexível
        is_correct = normalized_guess == normalized_title
        
        if is_correct:
            time_taken = time.time() - self.round_start_time
            points = max(10, 100 - int(time_taken * 5))
            player.score += points
            player.has_answered = True
            
            await self.broadcast({"type": "system_message", "message": f"✅ {username} acertou!", "level": "info"})
            await self.broadcast_player_update()

            if all(p.has_answered for p in self.players.values()):
                self._round_end_event.set()

    async def end_round(self):
        self.game_state = "ROUND_OVER"
        await self.broadcast({
            "type": "round_result",
            "correct_title": self.current_song['title'],
            "correct_artist": self.current_song['artist']
        })
        await asyncio.sleep(5)
        self.game_state = "PLAYING"

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