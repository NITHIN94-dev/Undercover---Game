# Who's the Undercover?

A real-time 2–8 player bluff-and-deduction party game. Players join the same room by code, submit one-word clues at the same time, vote publicly, and score across 3, 5, or 10 rounds.

## Run locally on Windows

1. Install Python 3.11+ from https://www.python.org/downloads/ if Python is not already installed.
2. Unzip this project.
3. Open PowerShell in the project folder.
4. Run:

```powershell
py -m pip install -r requirements.txt
py server.py
```

5. Open **http://localhost:3001** in your browser.

## Test with multiple devices on the same Wi‑Fi

The server already listens on all network interfaces. Find your computer's local IPv4 address with:

```powershell
ipconfig
```

On each phone or other computer connected to the same Wi‑Fi, open:

```text
http://YOUR-PC-IP:3001
```

Example: `http://192.168.1.25:3001`

Windows Firewall may ask whether Python should be allowed through the firewall. Allow it on your **Private network** so other devices on the same Wi‑Fi can connect.

## Game rules

- 2–8 players.
- One player is the Undercover each round.
- Rounds alternate between related-word and blind Undercover modes.
- Everyone submits exactly one clue word at the same time.
- Clues are revealed together.
- Voting is public.
- Ties trigger a runoff between tied candidates.
- Correct Innocent voters get +2.
- A surviving Undercover gets +4.
- A surviving Undercover who guesses the main word gets +1.
- The host chooses 3, 5, or 10 rounds.

## Publish with Render

This project uses an aiohttp WebSocket server, so publish it as a Render **Web Service**, not a static site. Render supports public WebSocket connections on web services.

Build Command: `pip install -r requirements.txt`
Start Command: `python server.py`

The server reads Render's `PORT` environment variable automatically, and the browser uses `wss://` when the site is served over HTTPS.
