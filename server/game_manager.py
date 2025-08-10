import asyncio
import os
import random
import time
import logging
import re
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
    text = text.lower()
    text = re.sub(r'\s*[\(\[].*(feat|ft|with|remix|remaster|live|edit|version|deluxe)[\)\]].*', '', text, flags=re.IGNORECASE).strip()
    text = text.split(' - ')[0]
    text = re.sub(r'[\(\)\[\]]', '', text).strip()
    text = re.sub(r"[^a-z0-9\\s']", '', text)
    text = ' '.join(text.split())
    return text.strip()


# --- Estruturas de Dados ---
class Player:
    def __init__(self, username: str, websocket: Optional[WebSocket]):
        self.username = username
        self.websocket = websocket
        self.score = 0
        self.has_answered = False
        self.gave_up = False
        self.guess_time: Optional[float] = None

    def to_dict(self):
        return {
            "username": self.username, 
            "score": self.score, 
            "has_answered": self.has_answered, 
            "gave_up": self.gave_up,
            "guess_time": self.guess_time
        }

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

    async def broadcast(self, message: dict):
        websockets = [p.websocket for p in self.players.values() if p.websocket]
        for ws in websockets:
            try:
                await ws.send_json(message)
            except Exception as e:
                logger.warning(f"Could not send message to a websocket: {e}")

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

        if self._preparation_complete_event.is_set() and player.websocket:
            try:
                await player.websocket.send_json({
                    "type": "playlist_details_updated",
                    "playlist_name": self.playlist_name,
                    "playlist_cover_image_url": self.playlist_cover_image_url,
                    "playlist_owner_name": self.playlist_owner_name,
                })
                await player.websocket.send_json({
                    "type": "game_prepared",
                    "titles": self.all_playlist_titles
                })
            except Exception as e:
                logger.warning(f"Could not send initial details to player {player.username}: {e}")

        await self.broadcast_player_update()

    async def remove_player(self, username: str):
        if username in self.players:
            player = self.players.pop(username)
            if not self.players or player.username == self.host.username:
                self.game_state = "GAME_OVER"
                if self._round_task:
                    self._round_task.cancel()
                for task in self._download_tasks:
                    task.cancel()
            else:
                await self.broadcast_player_update()

    def _download_song_segment(self, search_query: str, output_path: str, duration: int):
        start_time = random.randint(20, 70)
        ydl_opts = {
            'format': 'bestaudio/best',
            'postprocessor_args': ['-ss', str(start_time), '-t', str(duration)],
            'postprocessors': [{'key': 'FFmpegExtractAudio', 'preferredcodec': 'mp3'}],
            'outtmpl': output_path.replace('.mp3', ''),
            'quiet': True,
            'default_search': 'ytsearch1',
            'external_downloader': '/snap/bin/aria2c',
            'external_downloader_args': ['-x', '10'],
        }
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
        
        success = await asyncio.to_thread(
            self._download_song_segment,
            f"{track['artist']} - {track['title']} audio",
            str(filepath),
            self.game_settings["round_duration"]
        )
        
        track['download_status'] = 'downloaded' if success else 'failed'
        logger.info(f"Track {track['title']} status: {track['download_status']}")

    async def prepare_game_in_background(self):
        logger.info(f"Room {self.room_id}: Starting background preparation.")
        await self.fetch_playlist_details()
        await self.broadcast({
            "type": "playlist_details_updated",
            "playlist_name": self.playlist_name,
            "playlist_cover_image_url": self.playlist_cover_image_url,
            "playlist_owner_name": self.playlist_owner_name,
        })

        success = await self.prepare_game_tracks()

        if success:
            await self.broadcast({
                "type": "game_prepared",
                "titles": self.all_playlist_titles
            })
        
        self._preparation_complete_event.set()
        logger.info(f"Room {self.room_id}: Background preparation finished.")

    async def prepare_game_tracks(self):
        try:
            results = sp.playlist_tracks(self.playlist_url)
            spotify_tracks = []
            while results:
                spotify_tracks.extend([item['track'] for item in results['items'] if item and item.get('track') and item['track'].get('id')])
                if results['next']:
                    results = sp.next(results)
                else:
                    results = None
            
            logger.info(f"Found {len(spotify_tracks)} tracks in playlist.")
            
            self.all_playlist_titles = [track['name'] for track in spotify_tracks if track and track.get('name')]

            unplayed_tracks = [t for t in spotify_tracks if t['id'] not in self.played_track_ids]
            logger.info(f"Found {len(unplayed_tracks)} unplayed tracks.")

            if not unplayed_tracks:
                await self.broadcast({"type": "system_message", "message": "No unplayed tracks found in this playlist.", "level": "error"})
                return False

            random.shuffle(unplayed_tracks)
            num_rounds = min(self.game_settings["total_rounds"], len(unplayed_tracks))
            self.game_settings["total_rounds"] = num_rounds
            selected_tracks = unplayed_tracks[:num_rounds]
            
            self.game_tracks = []
            for track_data in selected_tracks:
                if not track_data.get('name') or not track_data.get('artists'):
                    continue
                
                filepath = Path("static/audio") / f"{track_data['id']}.mp3"
                track_info = {
                    "id": track_data['id'],
                    "title": track_data['name'],
                    "artist": track_data['artists'][0]['name'],
                    "file": f"{track_data['id']}.mp3",
                    "download_status": 'downloaded' if filepath.exists() else 'pending',
                    "download_task": None
                }
                self.game_tracks.append(track_info)

            self._download_tasks = []
            for track in self.game_tracks:
                if track['download_status'] == 'pending':
                    task = asyncio.create_task(self._download_wrapper(track))
                    track['download_task'] = task
                    self._download_tasks.append(task)

            return True
        except Exception as e:
            logger.error(f"Error preparing tracks: {e}")
            await self.broadcast({"type": "system_message", "message": "Error processing Spotify playlist.", "level": "error"})
            return False

    async def start_game(self, starter_username: str):
        if starter_username != self.host.username or self.game_state not in ["LOBBY", "GAME_OVER"]:
            return
        
        await self.broadcast({"type": "system_message", "message": "Starting game, preparing first round...", "level": "info"})
        
        await self._preparation_complete_event.wait()

        if not self.game_tracks:
            self.game_state = "LOBBY"
            await self.broadcast({"type": "system_message", "message": "Could not prepare any tracks for the game.", "level": "error"})
            return

        for player in self.players.values():
            player.reset_for_new_game()
        self.current_round = 0
        self.played_track_ids.update(t['id'] for t in self.game_tracks)

        first_track = self.game_tracks[0]
        if first_track['download_status'] != 'downloaded':
            await self.broadcast({"type": "system_message", "message": "Downloading the first song to begin...", "level": "info"})
            
            if first_track['download_task']:
                try:
                    await asyncio.wait_for(first_track['download_task'], timeout=120)
                except asyncio.TimeoutError:
                    logger.error(f"Timeout downloading first track: {first_track['title']}")
                    first_track['download_status'] = 'failed'

        if first_track['download_status'] != 'downloaded':
            self.game_state = "LOBBY"
            await self.broadcast({"type": "system_message", "message": "Failed to download the first song. Please try again.", "level": "error"})
            return

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

        while self.current_song['download_status'] != 'downloaded':
            if self.current_song['download_status'] == 'failed':
                await self.broadcast({"type": "system_message", "message": f"Could not download song for this round, skipping...", "level": "error"})
                await asyncio.sleep(3)
                return

            await self.broadcast({"type": "system_message", "message": f"Downloading song for round {self.current_round}... please wait.", "level": "info"})
            await asyncio.sleep(1)

        for p in self.players.values():
            p.reset_for_new_round()
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
        if self.game_state != "PLAYING" or not player or player.has_answered or player.gave_up:
            return

        normalized_title = normalize_string(self.current_song['title'])
        normalized_guess = normalize_string(guess_text)

        if normalized_guess == normalized_title:
            time_taken = time.time() - self.round_start_time
            points = max(10, 100 - int(time_taken * 5))
            player.score += points
            player.has_answered = True
            player.guess_time = time_taken
            
            await self.broadcast({"type": "system_message", "message": f"✅ {username} acertou em {time_taken:.1f}s!", "level": "info"})
            await self.broadcast_player_update()

            if all(p.has_answered or p.gave_up for p in self.players.values()):
                self._round_end_event.set()
        else:
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
        
        await self.broadcast_player_update()
        
        await self.broadcast({"type": "system_message", "message": f"⚠️ {username} desistiu da rodada!", "level": "info"})

        if all(p.has_answered or p.gave_up for p in self.players.values()):
            logger.info("Todos os jogadores desistiram ou responderam. Encerrando a rodada.")
            self._round_end_event.set()


    async def end_round(self):
        if self.game_state == "ROUND_OVER":
            return
        self.game_state = "ROUND_OVER"
        
        await self.broadcast({
            "type": "round_result",
            "correct_title": self.current_song['title'],
            "correct_artist": self.current_song['artist']
        })
        await asyncio.sleep(3)
        self.game_state = "PLAYING"

    async def end_game(self):
        self.game_state = "GAME_OVER"
        player_list = [p.to_dict() for p in self.players.values()]
        winner = max(player_list, key=lambda p: p['score'], default=None)
        
        self.played_track_ids.update(t['id'] for t in self.game_tracks)

        await self.broadcast({
            "type": "game_over",
            "scoreboard": sorted(player_list, key=lambda p: p['score'], reverse=True),
            "winner": winner
        })

    async def reset_for_new_game(self, new_playlist_url: Optional[str], starter_username: str):
        if starter_username != self.host.username:
            return

        if new_playlist_url:
            self.playlist_url = new_playlist_url
            self._preparation_complete_event.clear()
            self.game_tracks = []
            self.all_playlist_titles = []
            asyncio.create_task(self.prepare_game_in_background())

        await self.start_game(starter_username)


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
            if room._round_task:
                room._round_task.cancel()
            for task in room._download_tasks:
                task.cancel()
            del self.rooms[room_id]
            logger.info(f"Sala {room_id} removida.")

game_manager = GameManager()
