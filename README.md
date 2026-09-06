# THOUSAND DAYS relay

Lobby relay for the game's "Play with friends" mode: 6-letter lobby codes, one host, up to 7
guests, and it only forwards messages. No dependencies.

Run anywhere with Node 18+: `npm start` (listens on `PORT`, default 8787).
Players connect the game to `wss://<your-host>/relay`.
