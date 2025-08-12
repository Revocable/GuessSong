import asyncio
import os
import random
import time
import logging
import re
import shutil
from pathlib import Path
import spotipy
import yt_dlp
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv
from fastapi import WebSocket
from typing import Dict, List, Set, Optional, Tuple

# --- Configuração ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
load_dotenv()

# --- Cache de Playlist ---
playlist_cache: Dict[str, Tuple[float, List[Dict]]] = {}
CACHE_TTL = 300  # 5 minutos

# --- Verificação de Downloader Otimizado ---
ARIA2C_PATH = shutil.which("aria2c")
if not ARIA2C_PATH:
    logger.warning("AVISO: O downloader 'aria2c' não foi encontrado no seu sistema. Os downloads de músicas podem ser significativamente mais lentos. Instale-o para obter o melhor desempenho.")
else:
    logger.info(f"Usando downloader otimizado: {ARIA2C_PATH}")

try:
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
        client_id=os.getenv("SPOTIPY_CLIENT_ID"),
        client_secret=os.getenv("SPOTIPY_CLIENT_SECRET")
    ))
except Exception:
    logger.error("ERRO: Verifique suas credenciais do Spotify no arquivo .env.")
    sp = None

Path("static/audio").mkdir(parents=True, exist_ok=True)

def normalize_string(text: str) -> str:
    text = text.lower()
    text = re.sub(r'\s*[\(\[].*(feat|ft|with|remix|remaster|live|edit|version|deluxe)[\)\]].*', '', text, flags=re.IGNORECASE).strip()
    text = text.split(' - ')[0]
    text = re.sub(r'[\(\)\[\]]', '', text).strip()
    text = re.sub(r"[^a-z0-9\\s']", '', text)
    text = ' '.join(text.split())
    return text.strip()

class Player:
    def __init__(self, username: str, websocket: Optional[WebSocket]):
        self.username = username
        self.websocket = websocket
        self.score = 0
        self.wins = 0
        self.has_answered = False
        self.gave_up = False
        self.guess_time: Optional[float] = None

    def to_dict(self):
        return {"username": self.username, "score": self.score, "wins": self.wins, "has_answered": self.has_answered, "gave_up": self.gave_up, "guess_time": self.guess_time}

    def reset_for_new_round(self):
        self.has_answered = False
        self.gave_up = False
        self.guess_time = None

    def reset_for_new_game(self):
        self.reset_for_new_round()
        self.score = 0

class GameRoom:
    def __init__(self, host: Player, playlist_url: str, round_duration: int, total_rounds: int):
        self.room_id = "".join(random.choices("ABCDEFGHJKLMNPQRSTUVWXYZ23456789", k=5))
        self.host = host
        self.players: Dict[str, Player] = {}
        self.playlist_url = playlist_url
        self.playlist_name: Optional[str] = None
        self.playlist_cover_image_url: Optional[str] = None
        self.playlist_owner_name: Optional[str] = None
        self.game_settings = {"round_duration": round_duration, "total_rounds": total_rounds}
        self.game_state = "LOBBY"
        self.current_round = 0
        self.current_song: Optional[Dict] = None
        self.round_start_time = 0
        self.game_tracks: List[Dict] = []
        self.all_playlist_titles: List[str] = []
        self.played_track_ids: Set[str] = set()
        self.first_track_ready = False
        self._round_end_event = asyncio.Event()
        self._preparation_complete_event = asyncio.Event()
        self._round_task: Optional[asyncio.Task] = None
        self._download_tasks: List[asyncio.Task] = []

    async def fetch_playlist_details(self):
        try:
            playlist = await asyncio.to_thread(sp.playlist, self.playlist_url)
            self.playlist_name = playlist.get('name')
            self.playlist_owner_name = playlist.get('owner', {}).get('display_name')
            if playlist.get('images'):
                self.playlist_cover_image_url = playlist['images'][0]['url']
            logger.info(f"Fetched details for playlist: {self.playlist_name}")
            return True
        except Exception as e:
            logger.error(f"Could not fetch playlist details for {self.playlist_url}: {e}")
            return False

    async def broadcast(self, message: dict, to_host_only=False):
        if to_host_only:
            websockets = [self.host.websocket] if self.host.websocket and self.host.username in self.players else []
        else:
            websockets = [p.websocket for p in self.players.values() if p.websocket]
        
        for ws in websockets:
            try:
                await ws.send_json(message)
            except Exception as e:
                logger.warning(f"Could not send message to a websocket: {e}")

    async def broadcast_player_update(self):
        player_list = [p.to_dict() for p in self.players.values()]
        await self.broadcast({"type": "update_players", "players": sorted(player_list, key=lambda p: p['score'], reverse=True), "host_username": self.host.username})

    async def add_player(self, player: Player):
        if player.username not in self.players:
            self.players[player.username] = player
        else:
            # If a player reconnects, update their websocket object
            self.players[player.username].websocket = player.websocket

        # If the game has already been prepared, the new player needs the track titles
        # for the autocomplete functionality.
        if self._preparation_complete_event.is_set() and player.websocket:
            try:
                logger.info(f"Sending prepared game details to late-joining player: {player.username}")
                await player.websocket.send_json({"type": "game_prepared", "titles": self.all_playlist_titles})
            except Exception as e:
                logger.warning(f"Could not send prepared game details to player {player.username}: {e}")

        # After adding, broadcast the new player list to everyone in the room
        await self.broadcast_player_update()

    async def remove_player(self, username: str):
        if username in self.players:
            self.players.pop(username)
            if not self.players or username == self.host.username:
                self.game_state = "GAME_OVER"
                if self._round_task: self._round_task.cancel()
                for task in self._download_tasks: task.cancel()
            else:
                await self.broadcast_player_update()

    def _download_song_segment(self, search_query: str, output_path: str, duration: int):
        start_time = random.randint(20, 70)
        ydl_opts = {
            'format': 'bestaudio/best',
            'postprocessor_args': ['-ss', str(start_time), '-t', str(duration)],
            'postprocessors': [{'key': 'FFmpegExtractAudio', 'preferredcodec': 'webm', 'preferredquality': '64'}],
            'outtmpl': output_path.replace('.webm', ''),
            'quiet': True,
            'default_search': 'ytsearch1',
        }
        if ARIA2C_PATH:
            ydl_opts['external_downloader'] = ARIA2C_PATH
            ydl_opts['external_downloader_args'] = ['-x', '16', '-s', '16', '-k', '1M']
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([search_query])
            logger.info(f"Segment downloaded: {output_path}")
            return True
        except Exception as e:
            logger.error(f"Download failed for '{search_query}': {e}")
            return False

    async def _download_wrapper(self, track: Dict):
        track['download_status'] = 'downloading'
        filepath = Path("static/audio") / track['file']
        success = await asyncio.to_thread(self._download_song_segment, f"{track['artist']} - {track['title']} audio", str(filepath), self.game_settings["round_duration"])
        track['download_status'] = 'downloaded' if success else 'failed'
        logger.info(f"Track {track['title']} status: {track['download_status']}")

    async def prepare_game_in_background(self, is_rematch=False, starter_username=None):
        logger.info(f"Room {self.room_id}: Starting background preparation (is_rematch={is_rematch}).")
        
        if is_rematch:
            await self.broadcast({"type": "rematch_initiated", "message": "O anfitrião iniciou um novo jogo! Preparando novas músicas..."})

        await self.fetch_playlist_details()
        await self.broadcast({"type": "playlist_details_updated", "playlist_name": self.playlist_name, "playlist_cover_image_url": self.playlist_cover_image_url, "playlist_owner_name": self.playlist_owner_name})

        success = await self.prepare_game_tracks()
        if not success:
            await self.broadcast({"type": "system_message", "message": "Failed to prepare new tracks.", "level": "error"})
            self.game_state = "LOBBY"
            return

        await self.broadcast({"type": "game_prepared", "titles": self.all_playlist_titles})
        
        self._preparation_complete_event.set()
        logger.info(f"Room {self.room_id}: Track preparation is complete.")

        if is_rematch:
            logger.info(f"Room {self.room_id}: Rematch preparation complete, starting game automatically.")
            await self.start_game(starter_username)
        else:
            # Integrated host notification logic. This runs after preparation is done.
            if self.game_tracks:
                first_track = self.game_tracks[0]
                # If there's a download task, wait for it to complete
                if first_track.get('download_task'):
                    await first_track['download_task']
                
                # After waiting (or if no task existed, e.g., cached), check status
                if first_track['download_status'] == 'downloaded':
                    logger.info(f"Room {self.room_id}: First track is ready.")
                    self.first_track_ready = True
                    # This check ensures we notify a host that is already connected.
                    if self.host.username in self.players and self.players[self.host.username].websocket:
                        logger.info(f"Room {self.room_id}: Host is connected. Notifying host.")
                        await self.broadcast({"type": "host_ready_to_start"}, to_host_only=True)

    async def prepare_game_tracks(self):
        try:
            if self.playlist_url in playlist_cache and (time.time() - playlist_cache[self.playlist_url][0]) < CACHE_TTL:
                logger.info("Fetching playlist from cache.")
                spotify_tracks = playlist_cache[self.playlist_url][1]
            else:
                logger.info("Fetching playlist from Spotify API.")
                results = await asyncio.to_thread(sp.playlist_tracks, self.playlist_url)
                spotify_tracks = []
                while results:
                    spotify_tracks.extend([item['track'] for item in results['items'] if item and item.get('track') and item['track'].get('id')])
                    if results['next']:
                        results = await asyncio.to_thread(sp.next, results)
                    else:
                        results = None
                playlist_cache[self.playlist_url] = (time.time(), spotify_tracks)

            self.all_playlist_titles = [track['name'] for track in spotify_tracks if track and track.get('name')]
            unplayed_tracks = [t for t in spotify_tracks if t['id'] not in self.played_track_ids]
            if not unplayed_tracks:
                await self.broadcast({"type": "system_message", "message": "All tracks from this playlist have been played!", "level": "error"})
                return False

            random.shuffle(unplayed_tracks)
            num_rounds = min(self.game_settings["total_rounds"], len(unplayed_tracks))
            self.game_settings["total_rounds"] = num_rounds
            selected_tracks = unplayed_tracks[:num_rounds]
            
            self.game_tracks = []
            for track_data in selected_tracks:
                if not track_data.get('name') or not track_data.get('artists'): continue
                filepath = Path("static/audio") / f"{track_data['id']}.webm"
                self.game_tracks.append({"id": track_data['id'], "title": track_data['name'], "artist": track_data['artists'][0]['name'], "file": f"{track_data['id']}.webm", "download_status": 'downloaded' if filepath.exists() else 'pending', "download_task": None})
            
            self._download_tasks = [asyncio.create_task(self._download_wrapper(track)) for track in self.game_tracks if track['download_status'] == 'pending']
            for i, track in enumerate(t for t in self.game_tracks if t['download_status'] == 'pending'):
                track['download_task'] = self._download_tasks[i]

            return True
        except Exception as e:
            logger.error(f"Error preparing tracks: {e}")
            await self.broadcast({"type": "system_message", "message": "Error processing Spotify playlist.", "level": "error"})
            return False

    async def start_game(self, starter_username: str):
        if starter_username != self.host.username: return
        if self.game_state == "PLAYING": return

        await self._preparation_complete_event.wait()

        if not self.game_tracks:
            self.game_state = "LOBBY"
            await self.broadcast({"type": "system_message", "message": "Could not prepare any tracks.", "level": "error"})
            return

        first_track = self.game_tracks[0]
        if first_track['download_status'] != 'downloaded':
            logger.info(f"Room {self.room_id}: Waiting for first track download to complete before starting.")
            if first_track['download_task']:
                await first_track['download_task']
    
        if first_track['download_status'] != 'downloaded':
            await self.broadcast({"type": "system_message", "message": "Failed to download the first song.", "level": "error"})
            return

        for player in self.players.values(): player.reset_for_new_game()
        self.current_round = 0
        
        self.game_state = "PLAYING"
        await self.broadcast_player_update()
        await self.broadcast({"type": "system_message", "message": "Game is about to start!", "level": "info"})
        self._round_task = asyncio.create_task(self.game_loop())

    async def game_loop(self):
        while self.current_round < len(self.game_tracks) and self.game_state == "PLAYING":
            await self.run_next_round()
        await self.end_game()

    async def run_next_round(self):
        self.current_round += 1
        self.current_song = self.game_tracks[self.current_round - 1]

        if self.current_song['download_status'] != 'downloaded':
            await self.broadcast({"type": "system_message", "message": f"Downloading song for round {self.current_round}...", "level": "info"})
            if self.current_song['download_task']: await self.current_song['download_task']
        
        if self.current_song['download_status'] == 'failed':
            await self.broadcast({"type": "system_message", "message": "Could not download song, skipping round.", "level": "error"})
            await asyncio.sleep(3)
            return

        for p in self.players.values(): p.reset_for_new_round()
        self._round_end_event.clear()
        
        await self.broadcast_player_update()
        await self.broadcast({"type": "round_countdown"})
        await asyncio.sleep(4.1)

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
        logger.info(f"Entering handle_guess for {username}. Current game_state: {self.game_state}")
        player = self.players.get(username)
        if self.game_state != "PLAYING" or not player or player.has_answered or player.gave_up:
            logger.info(f"handle_guess conditions not met for {username}. game_state: {self.game_state}, player: {player}, has_answered: {player.has_answered if player else 'N/A'}, gave_up: {player.gave_up if player else 'N/A'}")
            return

        normalized_guess = normalize_string(guess_text)
        normalized_song_title = normalize_string(self.current_song['title'])

        logger.info(f"Raw Guess: {guess_text}")
        logger.info(f"Raw Song Title: {self.current_song['title']}")
        logger.info(f"Normalized Guess: {normalized_guess}")
        logger.info(f"Normalized Song Title: {normalized_song_title}")

        if normalized_guess == normalized_song_title:
            time_taken = time.time() - self.round_start_time
            player.has_answered = True
            player.guess_time = time_taken
            logger.info(f"Player {username} guessed correctly in {time_taken:.1f}s.")
            await self.broadcast({"type": "system_message", "message": f"✅ {username} acertou!", "level": "info"})
            await self.broadcast_player_update()
            if all(p.has_answered or p.gave_up for p in self.players.values()): self._round_end_event.set()
        else:
            await player.websocket.send_json({"type": "guess_result", "correct": False, "message": "Você errou! Tente novamente."})
            logger.info(f"Sent wrong guess feedback to {username}.")

    async def handle_give_up(self, username: str):
        player = self.players.get(username)
        if self.game_state != "PLAYING" or not player or player.has_answered or player.gave_up: return
        player.gave_up = True
        await self.broadcast_player_update()
        await self.broadcast({"type": "system_message", "message": f"⚠️ {username} desistiu da rodada!", "level": "info"})
        if all(p.has_answered or p.gave_up for p in self.players.values()): self._round_end_event.set()

    async def end_round(self):
        if self.game_state == "ROUND_OVER": return
        self.game_state = "ROUND_OVER"

        answered_players = sorted([p for p in self.players.values() if p.has_answered], key=lambda p: p.guess_time)
        
        acertadores_totais = len(answered_players)
        total_jogadores = len(self.players)
        tempo_maximo = self.game_settings["round_duration"]

        for i, player in enumerate(answered_players):
            posicao_acerto = i + 1
            tempo_resposta = player.guess_time

            # Calculate points
            pontos_base = 50
            penalidade_tempo = (tempo_resposta / tempo_maximo) * 0.5
            pontos = pontos_base * (1 - penalidade_tempo)
            if pontos < 10:
                pontos = 10

            # First guess bonus
            bonus_primeiro = 0
            if posicao_acerto == 1 and acertadores_totais > 1:
                tempo_segundo_colocado = answered_players[1].guess_time
                diferenca_segundos = tempo_segundo_colocado - tempo_resposta
                bonus_primeiro = round(10 * (1 - (diferenca_segundos / 5)))
                if bonus_primeiro < 0:
                    bonus_primeiro = 0
            
            # Solo guess bonus
            bonus_unico = 0
            if acertadores_totais == 1:
                bonus_unico = 5 * (total_jogadores - 1)

            pontuacao_final = round(pontos + bonus_primeiro + bonus_unico)
            player.score += pontuacao_final
            
            await self.broadcast({"type": "system_message", "message": f"✨ {player.username} ganhou {pontuacao_final} pontos!", "level": "info"})

        await self.broadcast_player_update()

        for p in self.players.values():
            logger.info(f"Player {p.username} score at end of round: {p.score}")
        await self.broadcast({"type": "round_result", "correct_title": self.current_song['title'], "correct_artist": self.current_song['artist']})
        await asyncio.sleep(3)
        self.game_state = "PLAYING"

    async def end_game(self):
        self.game_state = "GAME_OVER"
        self.played_track_ids.update(t['id'] for t in self.game_tracks)

        player_list = [p.to_dict() for p in self.players.values()]
        if player_list:
            winner = max(player_list, key=lambda p: p['score'])
            
            # Increment wins for the winner
            winner_player = self.players.get(winner['username'])
            if winner_player:
                winner_player.wins += 1
            
            # Get updated player list with new win count
            player_list = [p.to_dict() for p in self.players.values()]
        else:
            winner = None

        await self.broadcast({"type": "game_over", "scoreboard": sorted(player_list, key=lambda p: p['score'], reverse=True), "winner": winner})

    async def reset_for_new_game(self, new_playlist_url: Optional[str], starter_username: str):
        if starter_username != self.host.username: return

        logger.info(f"Room {self.room_id}: Host initiated a new game.")
        
        if new_playlist_url:
            logger.info(f"Room {self.room_id}: Using new playlist: {new_playlist_url}")
            self.playlist_url = new_playlist_url
            playlist_cache.pop(self.playlist_url, None)
            self.played_track_ids.clear()

        self.game_state = "LOBBY"
        self.game_tracks = []
        self.all_playlist_titles = []
        self.first_track_ready = False
        self._preparation_complete_event.clear()
        for task in self._download_tasks: task.cancel()
        self._download_tasks = []

        for player in self.players.values():
            player.reset_for_new_game()

        await self.broadcast_player_update()

        asyncio.create_task(self.prepare_game_in_background(is_rematch=True, starter_username=starter_username))

class GameManager:
    def __init__(self):
        self.rooms: Dict[str, GameRoom] = {}

    def create_room(self, host: Player, playlist: str, duration: int, total_rounds: int) -> GameRoom:
        room = GameRoom(host, playlist, duration, total_rounds)
        self.rooms[room.room_id] = room
        asyncio.create_task(room.prepare_game_in_background())
        return room

    def get_room(self, room_id: str) -> Optional[GameRoom]:
        return self.rooms.get(room_id)
        
    def remove_room(self, room_id: str):
        if room_id in self.rooms:
            room = self.rooms[room_id]
            if room._round_task: room._round_task.cancel()
            for task in room._download_tasks: task.cancel()
            del self.rooms[room_id]
            logger.info(f"Sala {room_id} removida.")

game_manager = GameManager()