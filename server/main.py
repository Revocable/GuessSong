from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from game_manager import game_manager, Player, sp
from spotipy import SpotifyException
import logging
import uvicorn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/client", StaticFiles(directory="../client/public"), name="client")

# Modelo atualizado para a requisição de criação de sala
class CreateRoomRequest(BaseModel):
    username: str
    playlist_url: str
    round_duration: int
    total_rounds: int

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    with open("../client/public/index.html") as f:
        return HTMLResponse(content=f.read(), status_code=200)

@app.post("/create-room")
async def create_room(request: CreateRoomRequest):
    if "open.spotify.com/playlist/" not in request.playlist_url:
        raise HTTPException(status_code=400, detail="URL da playlist do Spotify inválida.")
    if request.round_duration not in [15, 20, 30, 60]:
         raise HTTPException(status_code=400, detail="Duração da rodada inválida.")
    if not (request.total_rounds > 0):
        raise HTTPException(status_code=400, detail="Número de rodadas inválido. Deve ser um número positivo.")
    
    player = Player(request.username, None)
    room = game_manager.create_room(player, request.playlist_url, request.round_duration, request.total_rounds)
    
    # Fetch playlist details after creating the room
    success = await room.fetch_playlist_details()
    if not success:
        game_manager.remove_room(room.room_id)
        raise HTTPException(status_code=400, detail="Could not fetch details for the provided Spotify playlist. Make sure it's a valid, public playlist.")

    logger.info(f"Sala {room.room_id} criada por {request.username} com duração de {request.round_duration}s e {request.total_rounds} rodadas")
    return {"room_id": room.room_id}

@app.get("/search-playlists")
async def search_playlists(query: str):
    if not query:
        return []
    try:
        results = sp.search(q=query, type='playlist', limit=10)
        playlists = []
        for item in results['playlists']['items']:
            if item:
                playlists.append({
                    "id": item['id'],
                    "nome": item['name'],
                    "criador": item['owner']['display_name'],
                    "url_imagem": item['images'][0]['url'] if item['images'] else ""
                })
        return playlists
    except SpotifyException as e:
        logger.error(f"Error searching playlists: {e}")
        raise HTTPException(status_code=500, detail="Error searching playlists on Spotify.")

@app.websocket("/ws/{room_id}/{username}")
async def websocket_endpoint(websocket: WebSocket, room_id: str, username: str):
    await websocket.accept()
    room = game_manager.get_room(room_id)

    if not room:
        logger.warning(f"Connection attempt to non-existent room {room_id}")
        await websocket.send_json({"type": "error", "message": "Sala não encontrada."})
        await websocket.close(code=4004)
        return

    # Unify player object handling. The host is created first, then connects.
    # Other players are created when they connect.
    if username == room.host.username:
        player = room.host
        player.websocket = websocket
    else:
        # Check if a player with the same name is already connected.
        if username in room.players and room.players[username].websocket:
            await websocket.send_json({"type": "error", "message": f"O nome de usuário '{username}' já está em uso nesta sala."})
            await websocket.close(code=4009)
            return
        player = Player(username=username, websocket=websocket)

    try:
        await room.add_player(player)
        logger.info(f"Player {username} connected to room {room_id}.")
        
        # CRITICAL FIX: Send the room_joined event to the client that just connected.
        await websocket.send_json({
            "type": "room_joined",
            "room_id": room.room_id,
            "is_host": username == room.host.username,
            "players": [p.to_dict() for p in room.players.values()],
            "host_username": room.host.username,
            "playlist_name": room.playlist_name,
            "playlist_owner_name": room.playlist_owner_name,
            "playlist_cover_image_url": room.playlist_cover_image_url,
        })

        # If the host connects and the first track is already ready, notify them.
        if username == room.host.username and room.first_track_ready:
            logger.info(f"Host {username} connected and first track is ready. Notifying host.")
            await websocket.send_json({"type": "host_ready_to_start"})

        while True:
            data = await websocket.receive_json()
            logger.info(f"Received from {username} in {room_id}: {data}")
            
            if data['type'] == 'start_game':
                await room.start_game(username)
            elif data['type'] == 'submit_guess':
                await room.handle_guess(username, data['guess'])
            elif data['type'] == 'give_up':
                await room.handle_give_up(username)
            elif data['type'] == 'play_again':
                await room.reset_for_new_game(data.get('playlist_url'), username)

    except WebSocketDisconnect:
        logger.info(f"Player {username} disconnected from room {room_id}")
        await room.remove_player(username)
        if not room.players:
            game_manager.remove_room(room_id)
    except Exception as e:
        logger.error(f"Error in websocket for {username} in {room_id}: {e}", exc_info=True)
        await room.remove_player(username)
        # Ensure the websocket is closed on unexpected errors
        if not websocket.client_state == 'DISCONNECTED':
            await websocket.close(code=1011)

if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)