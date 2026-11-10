// Wild East: one-file game server. Serves the game and relays online-town messages over WebSocket.
// Run:  node server.js   (Node 18+, no packages to install).  PORT env var sets the port (default 3000).
const http = require("http"), fs = require("fs"), path = require("path"), crypto = require("crypto"), zlib = require("zlib");
const PORT = process.env.PORT || 3000, MAX_PER_ROOM = 40, MAX_TOTAL = 600, MAX_MSG = 8192;
const html = fs.readFileSync(path.join(__dirname, "index.html")), gz = zlib.gzipSync(html, { level: 9 });

const server = http.createServer((req, res) => {
  const u = req.url.split("?")[0];
  if (u === "/" || u === "/index.html") {
    const ae = /\bgzip\b/.test(req.headers["accept-encoding"] || "");
    res.writeHead(200, { "content-type": "text/html; charset=utf-8", "cache-control": "no-cache", ...(ae ? { "content-encoding": "gzip" } : {}) });
    res.end(ae ? gz : html);
  } else if (u === "/health") { res.writeHead(200); res.end("ok"); }
  else { res.writeHead(404); res.end("not found"); }
});

// ---- minimal WebSocket (RFC 6455) so the server needs no dependencies
function frame(str, op = 1) {
  const b = Buffer.from(str), n = b.length;
  const h = n < 126 ? Buffer.from([0x80 | op, n]) : Buffer.from([0x80 | op, 126, n >> 8, n & 255]);
  return Buffer.concat([h, b]);
}
const rooms = new Map(); let total = 0, nextId = 1;
const newId = () => (nextId++).toString(36) + crypto.randomBytes(3).toString("hex");
const send = (c, o) => { if (!c.sock.destroyed) c.sock.write(frame(JSON.stringify(o))); };

server.on("upgrade", (req, sock) => {
  const key = req.headers["sec-websocket-key"];
  if (req.url.split("?")[0] !== "/ws" || String(req.headers.upgrade).toLowerCase() !== "websocket" || !key || total >= MAX_TOTAL) { sock.destroy(); return; }
  sock.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " +
    crypto.createHash("sha1").update(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest("base64") + "\r\n\r\n");
  total++;
  const c = { sock, id: newId(), room: null, presence: null, alive: true, tokens: 60, last: Date.now() };
  let buf = Buffer.alloc(0);
  const close = () => { try { sock.write(Buffer.from([0x88, 0])); } catch (e) {} sock.destroy(); };
  sock.on("data", d => {
    buf = Buffer.concat([buf, d]); c.alive = true;
    while (buf.length >= 2) {
      const op = buf[0] & 15, fin = buf[0] & 128, masked = buf[1] & 128; let len = buf[1] & 127, off = 2;
      if (len === 126) { if (buf.length < 4) return; len = buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { close(); return; }
      if (!masked || len > MAX_MSG) { close(); return; }
      if (buf.length < off + 4 + len) return;
      const mask = buf.slice(off, off + 4), p = Buffer.from(buf.slice(off + 4, off + 4 + len));
      for (let i = 0; i < len; i++) p[i] ^= mask[i & 3];
      buf = buf.slice(off + 4 + len);
      if (op === 8) { close(); return; }
      if (op === 9) { sock.write(frame(p.toString(), 10)); continue; }
      if (op === 10) continue;
      if (op !== 1 || !fin) { close(); return; }
      handle(c, p.toString());
    }
  });
  const gone = () => {
    if (c.gone) return; c.gone = true; total--;
    if (c.room) { const r = rooms.get(c.room); if (r) { r.delete(c.id); if (!r.size) rooms.delete(c.room); else for (const o of r.values()) send(o, { t: "gone", peer: c.id }); } }
  };
  sock.on("close", gone); sock.on("error", () => { gone(); sock.destroy(); });
});

function handle(c, text) {
  const now = Date.now(); c.tokens = Math.min(120, c.tokens + (now - c.last) * 0.06); c.last = now;   // about 60 messages a second
  if (c.tokens < 1) return; c.tokens--;
  let m; try { m = JSON.parse(text); } catch (e) { return; }
  if (!m || typeof m !== "object") return;
  if (m.t === "join" && !c.room) {
    if (typeof m.room !== "string" || !/^[\w.-]{1,48}$/.test(m.room)) { c.sock.destroy(); return; }
    let r = rooms.get(m.room); if (!r) rooms.set(m.room, r = new Map());
    if (r.size >= MAX_PER_ROOM) { c.sock.destroy(); return; }
    send(c, { t: "hello", id: c.id, peers: [...r.values()].map(o => ({ peer: o.id, presence: o.presence })) });
    r.set(c.id, c); c.room = m.room; return;
  }
  const r = c.room && rooms.get(c.room); if (!r) return;
  if (m.t === "e" && typeof m.topic === "string" && /^[a-z][a-z0-9_.-]{0,47}$/.test(m.topic) && text.length <= 4096)
    for (const o of r.values()) send(o, { t: "e", topic: m.topic, data: m.data, peer: c.id });
  else if (m.t === "p" && m.presence && typeof m.presence === "object" && !Array.isArray(m.presence)) {
    c.presence = { ...(c.presence || {}), ...m.presence };
    for (const o of r.values()) if (o !== c) send(o, { t: "p", peer: c.id, presence: c.presence });
  }
}

// drop connections that stopped answering
setInterval(() => { for (const r of rooms.values()) for (const c of r.values()) { if (!c.alive) { c.sock.destroy(); continue; } c.alive = false; try { c.sock.write(frame("", 9)); } catch (e) {} } }, 30000);
server.listen(PORT, () => console.log("Wild East running on http://localhost:" + PORT));
