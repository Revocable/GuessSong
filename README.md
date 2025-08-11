# 🎶 Guess the Song - Multiplayer

> Um jogo multiplayer em tempo real para adivinhar músicas de playlists do Spotify com seus amigos.


Um jogo web interativo onde os jogadores podem criar salas, escolher uma playlist do Spotify e competir para ver quem adivinha o nome da música mais rápido.

## ✨ Funcionalidades

- **Salas Multiplayer em Tempo Real:** Crie uma sala privada e convide seus amigos com um link ou código.
- **Integração com Spotify:** Use qualquer playlist pública do Spotify para o jogo.
- **Busca de Playlists:** Encontre playlists diretamente na interface sem precisar do link.
- **Contagem de Pontos Dinâmica:** A pontuação é baseada na velocidade da sua resposta.
- **Histórico de Partidas:** O jogo acompanha o número de vitórias de cada jogador na sessão.
- **Interface Moderna:** Design responsivo e agradável com temas customizáveis.

## 🛠️ Tecnologias Utilizadas

- **Frontend:**
  - HTML5
  - [TailwindCSS](https://tailwindcss.com/) para estilização.
  - JavaScript (Vanilla) para a lógica do cliente e interatividade.
  - WebSockets para comunicação em tempo real.

- **Backend:**
  - [Python 3](https://www.python.org/)
  - [FastAPI](https://fastapi.tiangolo.com/) para a API e gerenciamento de WebSockets.
  - [Spotipy](https://spotipy.readthedocs.io/) para interagir com a API do Spotify.
  - [yt-dlp](https://github.com/yt-dlp/yt-dlp) para baixar os trechos das músicas do YouTube.

- **Opcional (Recomendado):**
  - [aria2c](https://aria2.github.io/) para acelerar significativamente o download das músicas.

---

## 🚀 Como Rodar o Projeto

Siga estes passos para configurar e rodar o projeto em sua máquina local.

### Pré-requisitos

- **Python 3.8+**
- **pip** (gerenciador de pacotes do Python)
- **(Opcional)** `aria2c` instalado e acessível no PATH do seu sistema para downloads mais rápidos.

### 1. Clone o Repositório

```bash
git clone https://github.com/Revocable/GuessSong.git
cd GuessSong
```

### 2. Configure o Backend

Todos os comandos a seguir devem ser executados dentro da pasta `server`.

```bash
cd server
```

**a. Crie e ative um ambiente virtual:**

- No Windows:
  ```bash
  python -m venv venv
  .\venv\Scripts\activate
  ```
- No macOS/Linux:
  ```bash
  python3 -m venv venv
  source venv/bin/activate
  ```

**b. Instale as dependências:**

```bash
pip install -r requirements.txt
```

### 3. Configure as Variáveis de Ambiente

Você precisa de credenciais da API do Spotify para que o jogo possa buscar as playlists e as músicas.

**a. Obtenha as credenciais do Spotify:**

1.  Vá para o [Spotify Developer Dashboard](https://developer.spotify.com/dashboard/).
2.  Faça login e crie um novo aplicativo (App).
3.  Copie o `Client ID` e o `Client Secret` do seu aplicativo.

**b. Crie o arquivo `.env`:**

1.  Dentro da pasta `server`, crie um arquivo chamado `.env`.
2.  Adicione o seguinte conteúdo ao arquivo, substituindo pelos valores que você copiou:

    ```env
    SPOTIPY_CLIENT_ID=SEU_CLIENT_ID_DO_SPOTIFY
    SPOTIPY_CLIENT_SECRET=SEU_CLIENT_SECRET_DO_SPOTIFY
    ```

### 4. Inicie o Servidor

Ainda na pasta `server`, execute o seguinte comando para iniciar o backend:

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

O servidor estará rodando e pronto para aceitar conexões.

## 🎮 Como Jogar

1.  Abra a pagina localhost:8000 no seu navegador
2.  Digite seu nome de usuário.
3.  **Para criar uma sala:**
    - Busque por uma playlist ou cole o link de uma playlist do Spotify.
    - Selecione a duração dos trechos e o número de rodadas.
    - Clique em "Criar Sala".
    - Compartilhe o código da sala ou o link com seus amigos!
4.  **Para entrar em uma sala:**
    - Cole o código da sala fornecido por um amigo.
    - Clique em "Entrar".

Divirta-se!
