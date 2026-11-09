# Wild East: hosting with online multiplayer

`server.js` serves the game and runs the online town. No packages to install.

Try it on your computer: `node server.js`, then open http://localhost:3000

## Put it on the internet (one link for everything)
Use any host that runs Node and supports WebSockets. On Render.com (free plan):
1. Put these files (index.html, server.js, package.json) in a GitHub repo.
2. Render: New > Web Service > pick the repo. Build command: leave empty. Start command: `npm start`.
3. Open the .onrender.com link it gives you. Send that link to friends. ONLINE TOWN works.
Railway, Fly.io, Glitch and a plain VPS work the same way (`npm start`).

Free plans sleep when idle, so the first visit after a while can take about a minute to wake up.

## Static hosting for the game, server somewhere else
If the page is on Netlify, GitHub Pages and the like, the game works but ONLINE TOWN needs the server.
Deploy server.js as above, then either set `window.WILD_EAST_SERVER="wss://YOUR-SERVER/ws"` near the top of the script in index.html,
or share the link with `?server=wss://YOUR-SERVER/ws` on the end.
