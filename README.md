# Wild East – game + server (Node 18+)

Files: index.html (the game), server.js, package.json, node_modules/ws.

    npm start      -> http://localhost:3000

The server prints which index.html it serves (size + md5) when it starts, and /version shows the same
from the browser. If the game looks wrong on Render, open https://YOUR-APP.onrender.com/version and compare
with your local file (md5 37ba01f3e06c4b3da765338bc2995364 = the file you gave me). A different number means
GitHub/Render has an older index.html.

GitHub/Render: commit ALL of these files with index.html at the TOP of the repo (not in a subfolder).
Render settings: Build Command `npm install` (or empty), Start Command `npm start`.
Env vars: PORT (Render sets it), ALLOWED_ORIGINS, MAX_PEERS_PER_ROOM, MAX_ROOMS. Health: /health.
