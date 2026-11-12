// Wild East game server
//   - serves the game from ./public  (open http://localhost:3000)
//   - relays online rooms over WebSocket at /ws  (the game's built-in client talks to exactly this)
//
// Protocol (JSON text frames), matching the client in index.html (wsRoom):
//   client -> server   {t:"join", room}            first message: pick a room
//                      {t:"p", presence}           patch of my presence (merged on the server, sent to the others)
//                      {t:"e", topic, data}        an event for everybody in the room (the sender hears it too)
//   server -> client   {t:"hello", id, peers}      you are in; `peers` = everyone already there with their presence
//                      {t:"p", peer, presence}     a peer's full presence
//                      {t:"e", topic, data, peer}  an event (peer = who sent it, set by the server)
//                      {t:"gone", peer}            a peer left
"use strict";
const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = +process.env.PORT || 3000;
const HOST = process.env.HOST || "0.0.0.0";
// The game is index.html next to this file (the same place a plain GitHub repo keeps it). A ./public folder is only used if there is no index.html here.
function findRoot() { // find the folder holding index.html: next to this file, ./public, or one folder down (e.g. an unzipped "wild-east" folder)
  const tries = [__dirname, path.join(__dirname, "public")];
  try { for (const d of fs.readdirSync(__dirname, { withFileTypes: true })) if (d.isDirectory() && d.name !== "node_modules" && !d.name.startsWith(".")) tries.push(path.join(__dirname, d.name), path.join(__dirname, d.name, "public")); } catch (e) {}
  for (const t of tries) if (fs.existsSync(path.join(t, "index.html"))) return t;
  return __dirname;
}
const PUBLIC = findRoot();
const HIDDEN = new Set(["server.js", "package.json", "package-lock.json", "readme.md"]);
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || "").split(",").map(s => s.trim()).filter(Boolean); // empty = any origin
const MAX_PEERS_PER_ROOM = +process.env.MAX_PEERS_PER_ROOM || 64;
const MAX_ROOMS = +process.env.MAX_ROOMS || 200;
const MAX_MSG_BYTES = 8192;          // a whole frame (events are meant to stay under ~4 KiB)
const RATE_PER_SEC = 90, RATE_BURST = 150; // messages per connection (presence alone is ~13/s)
const TOPIC_RE = /^[a-z][a-z0-9_.-]{0,47}$/;
const MAX_PRESENCE_KEYS = 48;

const rooms = new Map(); // name -> Map(id -> client)

// ---------- static files ----------
const MIME = { ".html": "text/html; charset=utf-8", ".js": "text/javascript; charset=utf-8", ".css": "text/css; charset=utf-8", ".json": "application/json", ".png": "image/png", ".jpg": "image/jpeg", ".svg": "image/svg+xml", ".wav": "audio/wav", ".mp3": "audio/mpeg", ".ogg": "audio/ogg", ".obj": "text/plain; charset=utf-8", ".ico": "image/x-icon", ".txt": "text/plain; charset=utf-8" };
const server = http.createServer((req, res) => {
  let url;
  try { url = new URL(req.url, "http://x"); } catch (e) { res.writeHead(400).end("bad request"); return; }
  if (url.pathname === "/health") {
    let peers = 0; for (const r of rooms.values()) peers += r.size;
    res.writeHead(200, { "content-type": "application/json", "cache-control": "no-store" });
    res.end(JSON.stringify({ ok: true, rooms: rooms.size, peers }));
    return;
  }
  if (url.pathname === "/version") { // which game file is this server really serving?
    const f = path.join(PUBLIC, "index.html");
    try { const b = fs.readFileSync(f); res.writeHead(200, { "content-type": "application/json", "cache-control": "no-store" }); res.end(JSON.stringify({ file: f, bytes: b.length, md5: crypto.createHash("md5").update(b).digest("hex") })); }
    catch (e) { res.writeHead(404).end("no index.html"); }
    return;
  }
  if (req.method !== "GET" && req.method !== "HEAD") { res.writeHead(405, { allow: "GET, HEAD" }).end(); return; }
  let rel;
  try { rel = decodeURIComponent(url.pathname); } catch (e) { res.writeHead(400).end("bad request"); return; }
  if (rel === "/" || rel === "") rel = "/index.html";
  const file = path.normalize(path.join(PUBLIC, rel));
  if (file !== PUBLIC && !file.startsWith(PUBLIC + path.sep)) { res.writeHead(403).end("forbidden"); return; } // no ../ tricks
  const top = path.relative(PUBLIC, file).split(path.sep)[0].toLowerCase();
  if (top === "node_modules" || top.startsWith(".") || HIDDEN.has(top)) { res.writeHead(404, { "content-type": "text/plain" }).end("not found"); return; }
  fs.stat(file, (err, st) => {
    if (err || !st.isFile()) { res.writeHead(404, { "content-type": "text/plain" }).end("not found: no " + rel + " in " + PUBLIC + " (looking for index.html next to server.js)"); return; }
    res.writeHead(200, {
      "content-type": MIME[path.extname(file).toLowerCase()] || "application/octet-stream",
      "content-length": st.size,
      "cache-control": rel === "/index.html" ? "no-cache" : "public, max-age=3600",
    });
    if (req.method === "HEAD") { res.end(); return; }
    fs.createReadStream(file).pipe(res);
  });
});

// ---------- rooms ----------
const wss = new WebSocketServer({
  server, path: "/ws", maxPayload: MAX_MSG_BYTES,
  verifyClient: ({ origin }) => !ALLOWED_ORIGINS.length || !origin || ALLOWED_ORIGINS.includes(origin),
});
const newId = () => crypto.randomBytes(5).toString("hex");
const cleanRoom = s => { s = String(s == null ? "" : s).trim().slice(0, 48); return /^[A-Za-z0-9 _.\-]+$/.test(s) ? s : "wild-east-town"; };
const send = (c, o) => { if (c.ws.readyState === 1) c.ws.send(JSON.stringify(o)); };
const broadcast = (room, o, except) => { const s = JSON.stringify(o); for (const c of room.values()) if (c !== except && c.ws.readyState === 1) c.ws.send(s); };

function leave(c) {
  if (!c.room) return;
  const room = rooms.get(c.name);
  if (room) { room.delete(c.id); if (!room.size) rooms.delete(c.name); else broadcast(room, { t: "gone", peer: c.id }); }
  c.room = null;
}

wss.on("connection", ws => {
  const c = { ws, id: null, name: null, room: null, presence: {}, tokens: RATE_BURST, last: Date.now(), alive: true, strikes: 0 };
  const joinTimer = setTimeout(() => ws.close(1008, "join first"), 10000);
  ws.on("pong", () => { c.alive = true; });
  ws.on("message", (buf, isBinary) => {
    if (isBinary) return;
    const now = Date.now();
    c.tokens = Math.min(RATE_BURST, c.tokens + (now - c.last) / 1000 * RATE_PER_SEC); c.last = now;
    if (c.tokens < 1) { if (++c.strikes > 300) ws.close(1008, "too fast"); return; } // over the limit: drop the message
    c.tokens -= 1;
    let m; try { m = JSON.parse(buf.toString()); } catch (e) { return; }
    if (!m || typeof m !== "object") return;
    if (m.t === "join") {
      if (c.room) return; // one room per connection
      const name = cleanRoom(m.room);
      let room = rooms.get(name);
      if (!room) { if (rooms.size >= MAX_ROOMS) { ws.close(1013, "server busy"); return; } room = new Map(); rooms.set(name, room); }
      if (room.size >= MAX_PEERS_PER_ROOM) { ws.close(1013, "room full"); return; }
      clearTimeout(joinTimer);
      c.id = newId(); while (room.has(c.id)) c.id = newId();
      c.name = name; c.room = room;
      const peers = []; for (const o of room.values()) peers.push({ peer: o.id, presence: o.presence });
      room.set(c.id, c);
      send(c, { t: "hello", id: c.id, peers });
    } else if (!c.room) {
      return; // anything before a join is ignored
    } else if (m.t === "p") {
      const p = m.presence;
      if (!p || typeof p !== "object" || Array.isArray(p)) return;
      for (const k of Object.keys(p)) {
        if (!(k in c.presence) && Object.keys(c.presence).length >= MAX_PRESENCE_KEYS) continue;
        c.presence[k] = p[k];
      }
      broadcast(c.room, { t: "p", peer: c.id, presence: c.presence }, c);
    } else if (m.t === "e") {
      if (typeof m.topic !== "string" || !TOPIC_RE.test(m.topic)) return;
      broadcast(c.room, { t: "e", topic: m.topic, data: m.data === undefined ? null : m.data, peer: c.id }); // the sender gets it too
    }
  });
  ws.on("close", () => { clearTimeout(joinTimer); leave(c); });
  ws.on("error", () => { try { ws.terminate(); } catch (e) {} });
  c.ws._c = c;
});

// drop connections that stopped answering (closed laptop, lost network)
const beat = setInterval(() => {
  for (const ws of wss.clients) { const c = ws._c; if (c && !c.alive) { ws.terminate(); continue; } if (c) c.alive = false; try { ws.ping(); } catch (e) {} }
}, 30000);
wss.on("close", () => clearInterval(beat));

try { const b = fs.readFileSync(path.join(PUBLIC, "index.html")); console.log("serving " + path.join(PUBLIC, "index.html") + "  " + b.length + " bytes  md5 " + crypto.createHash("md5").update(b).digest("hex")); } catch (e) { console.log("WARNING: no index.html found in " + PUBLIC); }
server.listen(PORT, HOST, () => console.log("Wild East server on http://" + (HOST === "0.0.0.0" ? "localhost" : HOST) + ":" + PORT + "  (WebSocket at /ws)"));
const stop = () => { console.log("shutting down"); clearInterval(beat); for (const ws of wss.clients) ws.close(1001, "server restarting"); server.close(() => process.exit(0)); setTimeout(() => process.exit(0), 2000).unref(); };
process.on("SIGINT", stop); process.on("SIGTERM", stop);
