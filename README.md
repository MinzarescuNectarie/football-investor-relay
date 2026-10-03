# Football Investor relay

This server connects the native game clients over WebSockets. Players make outbound connections, so they do not need router port forwarding. A room host still runs the game rules; this service forwards authenticated room messages and manages room names, short codes, passwords, capacity and discovery. Rooms disappear when the host disconnects or the service restarts. There is no saved game progress, database, AI or paid API.

The service is implemented and locally tested. **It is not yet deployed to a public address.**

## Deploy with a new free account

1. Create a free [GitHub account](https://github.com/signup) and an empty repository, for example `football-investor-relay`.
2. Upload the contents of this `relay` folder into that repository's root. Include `server.py`, `requirements.txt`, `render.yaml`, `.python-version` and this README. The separate `FootballInvestor-Relay.zip` contains only these server files; do not upload the Windows game or music.
3. Create a [Render account](https://dashboard.render.com/register) and link your GitHub repository.
4. Choose **New → Blueprint** and select this repository. Review the service: `football-investor-relay`, Python, **Free** plan. The included `render.yaml` supplies the build/start settings and the bind address. Create it on the Free plan.
5. If using **New → Web Service** instead, choose Python, build command `pip install -r requirements.txt`, start command `python server.py`, Free plan, health check `/health`, and environment variable `BIND_ADDRESS=0.0.0.0`. Keep the included `.python-version` file to select the latest Python 3.12 patch.
6. Wait for the service to become Live. Open its `https://...onrender.com/health` address; it should return the service name and protocol number.
7. In the game, open **Settings → General → Internet relay address**, enter `wss://YOUR-SERVICE.onrender.com/ws`, and choose **Apply relay address**. Everyone uses the same address. Then choose **Play → Online → Internet relay → Create/Search**. Share the short `FI-R-...` room code to join directly. Private codes end in `-P` and open the password prompt.
8. To preconfigure new copies, put that address in `relay-config.json` beside the game executable: `{"url":"wss://YOUR-SERVICE.onrender.com/ws"}`. This is used when there is no saved address; players with an old address should update Settings.

Keep the service on Free; without a payment method Render suspends free services rather than charging extra bandwidth usage. A host restart closes all active rooms.

Render's Free service can sleep after 15 idle minutes and take about a minute to wake. The game allows 75 seconds for initial connection. Free services also have usage limits and can restart; this is appropriate for testing and small hobby sessions, not guaranteed continuous uptime. See [Render Free documentation](https://render.com/docs/free) and [WebSocket hosting documentation](https://render.com/docs/websocket).

## Test locally

Requires Python 3.12 or newer. Open a terminal in this folder:

```text
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
.venv\Scripts\python server.py
```

On Windows, `Start-Local-Relay.cmd` performs those steps. The default bind address is loopback and the port is 8080. In Settings, use `ws://127.0.0.1:8080/ws`. Launch two copies of the game on this computer, create a room in one and join in the other. Each game can choose a different nickname. A loopback address does not connect friends on other computers.

Server tests: `.venv\Scripts\python test_server.py -v`. Game integration test: run `source/tests/relay.gd` in Godot while the local server is running. It tests private room creation, browser listings, wrong passwords, capacity, hidden identities, picking, locking, advancing a round and disconnects. Standard gameplay unit/round tests still pass.

## Operations and limits

- `/ws`: version 1 JSON WebSocket protocol. `/health`: health check with aggregate room count; no passwords or room secrets.
- One running instance; rooms are in memory. No shared storage or multi-instance scaling is implemented.
- Maximum 50 rooms, 200 connections, 5 participants per room and 256 KiB per message. Per-connection token limits and outbound queue limits bound basic abuse; this is not a comprehensive public anti-abuse service.
- Passwords are stored only as salted scrypt hashes in memory. Password entry travels over validated TLS for public `wss://` connections. Requests are not logged. The relay can see forwarded game state and room metadata; guests cannot see hidden footballer identities because the game host strips them before sending.
- The socket determines the participant ID and room; clients cannot choose another sender ID. Only a room host can publish state. Guests may send pick/lock requests and the host validates them with existing budget and round rules.
- Room names are unique after trimming and case folding, within this running relay. Public listings include names and occupancy for private rooms, but not their passwords. Host authority and game fairness are still trusted, as in the original game.
- Configure `PORT` and `BIND_ADDRESS` via environment variables. Render supplies `PORT` automatically and terminates TLS. Use only public `wss://` addresses in the game; plain `ws://` is permitted solely for localhost testing.
