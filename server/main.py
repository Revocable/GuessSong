from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from game_manager import game_manager, Player
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
    round_duration: int # Nova configuração

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
    
    player = Player(request.username, None)
    room = game_manager.create_room(player, request.playlist_url, request.round_duration)
    
    # Fetch playlist details after creating the room
    success = await room.fetch_playlist_details()
    if not success:
        game_manager.remove_room(room.room_id)
        raise HTTPException(status_code=400, detail="Could not fetch details for the provided Spotify playlist. Make sure it's a valid, public playlist.")

    logger.info(f"Sala {room.room_id} criada por {request.username} com duração de {request.round_duration}s")
    return {"room_id": room.room_id}

@app.websocket("/ws/{room_id}/{username}")
async def websocket_endpoint(websocket: WebSocket, room_id: str, username: str):
    await websocket.accept()
    
    room = game_manager.get_room(room_id)
    if not room:
        await websocket.send_json({"type": "error", "message": "Sala não encontrada."})
        await websocket.close(); return

    if username in room.players and room.players[username].websocket is not None:
        await websocket.send_json({"type": "error", "message": f"Nome '{username}' já em uso."})
        await websocket.close(); return

    player = Player(username, websocket)
    await room.add_player(player)
    
    await websocket.send_json({
        "type": "room_joined",
        "room_id": room.room_id,
        "is_host": username == room.host.username,
        "host_username": room.host.username,
        "players": [p.to_dict() for p in room.players.values()],
        "playlist_name": room.playlist_name,
        "playlist_cover_image_url": room.playlist_cover_image_url,
        "playlist_owner_name": room.playlist_owner_name,
    })

    try:
        while True:
            data = await websocket.receive_json()
            if data["type"] == "start_game":
                await room.start_game(username)
            elif data["type"] == "submit_guess":
                await room.handle_guess(username, data["guess"])
            elif data["type"] == "give_up": # Novo tipo de mensagem
                await room.handle_give_up(username)
            elif data["type"] == "play_again":
                new_playlist_url = data.get("playlist_url")
                await room.reset_for_new_game(new_playlist_url, username)
    except WebSocketDisconnect:
        logger.info(f"Jogador {username} desconectou da sala {room_id}")
        await room.remove_player(username)
        if not room.players:
            game_manager.remove_room(room_id)

if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)