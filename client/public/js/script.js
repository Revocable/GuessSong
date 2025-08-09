document.addEventListener('DOMContentLoaded', () => {
    // --- Elementos do DOM ---
    const loadingOverlay = document.getElementById('loading-overlay');
    const loadingMessage = document.getElementById('loading-message');
    const entryView = document.getElementById('entry-view');
    const lobbyView = document.getElementById('lobby-view');
    const gameView = document.getElementById('game-view');
    const gameOverView = document.getElementById('game-over-view');

    const usernameInput = document.getElementById('username-input');
    const playlistUrlInput = document.getElementById('playlist-url-input');
    const durationSelect = document.getElementById('duration-select');
    const createRoomBtn = document.getElementById('create-room-btn');
    const joinRoomBtn = document.getElementById('join-room-btn');
    const roomCodeInput = document.getElementById('room-code-input');
    const errorMessage = document.getElementById('error-message');

    const roomCodeDisplay = document.getElementById('room-code-display');
    const playerListLobby = document.getElementById('player-list-lobby');
    const startGameBtn = document.getElementById('start-game-btn');
    const waitingForHost = document.getElementById('waiting-for-host');

    const currentRoundEl = document.getElementById('current-round');
    const totalRoundsEl = document.getElementById('total-rounds');
    const myScoreEl = document.getElementById('my-score');
    const timerBar = document.getElementById('timer-bar');
    const systemMessageDisplay = document.getElementById('system-message-display');
    const guessInput = document.getElementById('guess-input');
    const submitGuessBtn = document.getElementById('submit-guess-btn');
    const scoreboard = document.getElementById('scoreboard');

    const winnerAnnouncement = document.getElementById('winner-announcement');
    const finalScoreboard = document.getElementById('final-scoreboard');
    const playAgainBtn = document.getElementById('play-again-btn');

    // --- Estado do Jogo ---
    let ws = null;
    let username = '';
    let roomId = '';
    let isHost = false;
    let audio = new Audio();
    let systemMessageTimeout;

    // --- Funções Auxiliares de UI ---
    function showLoading(message) {
        loadingMessage.textContent = message;
        loadingOverlay.classList.remove('hidden');
    }
    function hideLoading() {
        loadingOverlay.classList.add('hidden');
    }

    function showView(viewId) {
        [entryView, lobbyView, gameView, gameOverView].forEach(v => v.classList.add('hidden'));
        document.getElementById(viewId).classList.remove('hidden');
    }

    function displayError(message) {
        errorMessage.textContent = message;
        setTimeout(() => errorMessage.textContent = '', 4000);
    }

    function updatePlayerList(players, container, hostUsername) {
        container.innerHTML = '';
        players.sort((a, b) => b.score - a.score).forEach((player, index) => {
            const playerEl = document.createElement('div');
            playerEl.className = 'flex justify-between items-center bg-slate-700 p-3 rounded-lg transition-all duration-300';
            
            const hostIcon = player.username === hostUsername ? '<i class="fas fa-crown text-yellow-400 ml-2" title="Host"></i>' : '';
            const answeredIcon = player.has_answered ? '<i class="fas fa-check-circle text-green-400 ml-2" title="Acertou!"></i>' : '';
            
            playerEl.innerHTML = `
                <div class="flex items-center">
                    <span class="font-bold text-lg mr-3 w-6 text-center">${index + 1}.</span>
                    <span>${player.username} ${player.username === username ? '<span class="text-purple-400 font-semibold ml-1">(Você)</span>' : ''} ${hostIcon} ${answeredIcon}</span>
                </div>
                <span class="font-bold text-purple-400 text-lg">${player.score} pts</span>
            `;
            container.appendChild(playerEl);
        });
    }
    
    function showSystemMessage(message, level = "info") {
        clearTimeout(systemMessageTimeout);
        systemMessageDisplay.textContent = message;
        systemMessageDisplay.className = 'text-center font-semibold h-6 mb-4 transition-opacity duration-300';
        if (level === 'error') {
            systemMessageDisplay.classList.add('text-red-400');
        } else {
            systemMessageDisplay.classList.add('text-green-400');
        }

        if (level !== 'error') {
            systemMessageTimeout = setTimeout(() => {
                systemMessageDisplay.textContent = '';
            }, 4000);
        }
    }

    // --- Lógica de Conexão ---
    async function createAndConnect() {
        username = usernameInput.value.trim();
        const playlistUrl = playlistUrlInput.value.trim();
        const roundDuration = parseInt(durationSelect.value, 10);

        if (!username) {
            displayError("Por favor, digite um nome de usuário.");
            return;
        }
        if (!playlistUrl || !playlistUrl.includes('spotify.com/playlist/')) {
            displayError("Por favor, insira uma URL de playlist do Spotify válida.");
            return;
        }
        
        showLoading("Criando sala...");

        try {
            const response = await fetch('/create-room', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    username: username,
                    playlist_url: playlistUrl,
                    round_duration: roundDuration
                }),
            });
            
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.detail || 'Falha ao criar a sala.');
            }
            
            roomId = data.room_id;
            isHost = true;
            await connectWebSocket(roomId);

        } catch (error) {
            hideLoading();
            displayError(error.message);
        }
    }

    async function joinAndConnect() {
        username = usernameInput.value.trim();
        const code = roomCodeInput.value.trim().toUpperCase();
        if (!username) {
            displayError("Por favor, digite um nome de usuário.");
            return;
        }
        if (!code) {
            displayError("Por favor, insira um código de sala.");
            return;
        }
        showLoading("Entrando na sala...");
        await connectWebSocket(code);
    }

    async function connectWebSocket(joinRoomId) {
        roomId = joinRoomId;
        const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        ws = new WebSocket(`${wsProtocol}//${window.location.host}/ws/${roomId}/${encodeURIComponent(username)}`);

        ws.onopen = () => console.log("Conectado ao servidor WebSocket.");

        ws.onmessage = (event) => handleWebSocketMessage(JSON.parse(event.data));

        ws.onclose = () => {
            console.log("Desconectado do servidor WebSocket.");
            hideLoading();
            showView('entry-view');
            displayError("Conexão perdida. Tente novamente.");
            ws = null;
        };

        ws.onerror = (error) => {
            console.error("Erro no WebSocket:", error);
            hideLoading();
            displayError("Ocorreu um erro na conexão.");
            ws = null;
        };
    }

    function handleWebSocketMessage(data) {
        console.log("Mensagem recebida:", data);
        hideLoading(); // Esconde o loading em qualquer mensagem recebida
        switch (data.type) {
            case 'error':
                displayError(data.message);
                if (ws) ws.close();
                showView('entry-view');
                break;
            case 'room_joined':
                isHost = data.is_host;
                roomCodeDisplay.textContent = data.room_id;
                if (isHost) {
                    startGameBtn.classList.remove('hidden');
                    waitingForHost.classList.add('hidden');
                } else {
                    startGameBtn.classList.add('hidden');
                    waitingForHost.classList.remove('hidden');
                }
                showView('lobby-view');
                break;
            case 'update_players':
                const currentView = lobbyView.offsetParent !== null ? playerListLobby : scoreboard;
                updatePlayerList(data.players, currentView, data.host_username);
                if (gameView.offsetParent !== null) {
                    const myPlayer = data.players.find(p => p.username === username);
                    if (myPlayer) myScoreEl.textContent = myPlayer.score;
                }
                break;
            case 'system_message':
                if (data.message.toLowerCase().includes('baixando')) {
                    showLoading(data.message);
                } else {
                    showSystemMessage(data.message, data.level);
                }
                break;
            case 'start_round':
                showView('game-view');
                systemMessageDisplay.textContent = '';
                guessInput.value = '';
                guessInput.disabled = false;
                submitGuessBtn.disabled = false;
                guessInput.focus();
                currentRoundEl.textContent = data.round;
                totalRoundsEl.textContent = data.total_rounds;
                
                timerBar.style.animation = 'none';
                void timerBar.offsetWidth; // Trigger reflow
                timerBar.style.animation = `progress ${data.duration}s linear forwards`;
                
                audio.src = data.song_url;
                audio.play().catch(e => console.error("Erro ao tocar áudio:", e));
                break;
            case 'round_result':
                audio.pause();
                audio.currentTime = 0;
                showSystemMessage(`A resposta era: ${data.correct_title} - ${data.correct_artist}`);
                guessInput.disabled = true;
                submitGuessBtn.disabled = true;
                break;
            case 'game_over':
                audio.pause();
                showView('game-over-view');
                const winner = data.winner;
                if (winner) {
                    winnerAnnouncement.innerHTML = `
                        <i class="fas fa-crown text-yellow-400 text-5xl mb-2"></i>
                        <p class="text-2xl font-bold">${winner.username} é o vencedor!</p>
                        <p class="text-slate-400">com ${winner.score} pontos</p>
                    `;
                } else {
                    winnerAnnouncement.innerHTML = `<p class="text-2xl font-bold">O jogo terminou em empate ou sem jogadores!</p>`;
                }
                updatePlayerList(data.scoreboard, finalScoreboard, data.host_username);
                break;
        }
    }

    function sendMessage(data) {
        if (ws && ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify(data));
        }
    }
    
    // --- Event Listeners ---
    createRoomBtn.addEventListener('click', createAndConnect);
    joinRoomBtn.addEventListener('click', joinAndConnect);

    startGameBtn.addEventListener('click', () => {
        startGameBtn.disabled = true;
        startGameBtn.innerHTML = '<i class="fas fa-circle-notch spinner"></i> Iniciando...';
        sendMessage({ type: 'start_game' });
    });

    submitGuessBtn.addEventListener('click', () => {
        const guess = guessInput.value.trim();
        if (guess && !guessInput.disabled) {
            sendMessage({ type: 'submit_guess', guess: guess });
            guessInput.value = '';
        }
    });
    
    guessInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
            submitGuessBtn.click();
        }
    });
    
    playAgainBtn.addEventListener('click', () => {
       window.location.reload();
    });

    roomCodeDisplay.addEventListener('click', () => {
        navigator.clipboard.writeText(roomCodeDisplay.textContent).then(() => {
            const originalText = roomCodeDisplay.textContent;
            roomCodeDisplay.textContent = 'Copiado!';
            setTimeout(() => {
                roomCodeDisplay.textContent = originalText;
            }, 1500);
        });
    });
});