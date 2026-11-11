// Wild East game server. Serves index.html and relays multiplayer over a WebSocket at /ws.
// No dependencies: it only needs Node 18 or newer, so there is nothing to npm install.
//   node server.js        (set PORT to change the port, default 3000)
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto'), zlib = require('zlib');
const PORT = +process.env.PORT || 3000;
const INDEX = path.join(__dirname, 'index.html');
const MAX_PAYLOAD = 32 * 1024;   // largest message a client may send
const ROOM_MAX = 32;             // players per room
const IP_MAX = 12;               // sockets per IP address

// ---- static page (cached and gzipped; re-read if the file changes)
let page = null;
function getPage() {
  const st = fs.statSync(INDEX);
  if (!page || page.mtime !== +st.mtime) {
    const raw = fs.readFileSync(INDEX);
    page = { mtime: +st.mtime, raw, gz: zlib.gzipSync(raw, { level: 6 }) };
  }
  return page;
}
const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/' || url === '/index.html') {
    try {
      const p = getPage(), gz = /\bgzip\b/.test(req.headers['accept-encoding'] || '');
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache', ...(gz ? { 'Content-Encoding': 'gzip' } : {}) });
      res.end(gz ? p.gz : p.raw);
    } catch (e) { res.writeHead(500); res.end('index.html not found next to server.js'); }
  } else if (url === '/health') { res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('ok'); }
  else { res.writeHead(404); res.end('not found'); }
});

// ---- minimal WebSocket (RFC 6455) so no package is needed
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const rooms = new Map();         // room name -> Map(id -> client)
const ipCount = new Map();

server.on('upgrade', (req, socket) => {
  const url = (req.url || '').split('?')[0], key = req.headers['sec-websocket-key'];
  if (url !== '/ws' || !key || String(req.headers.upgrade).toLowerCase() !== 'websocket') { socket.destroy(); return; }
  const ip = String(req.headers['x-forwarded-for'] || socket.remoteAddress || '').split(',')[0].trim();
  if ((ipCount.get(ip) || 0) >= IP_MAX) { socket.write('HTTP/1.1 429 Too Many Requests\r\nConnection: close\r\n\r\n'); socket.destroy(); return; }
  ipCount.set(ip, (ipCount.get(ip) || 0) + 1);
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' +
    crypto.createHash('sha1').update(key + GUID).digest('base64') + '\r\n\r\n');
  socket.setNoDelay(true);
  new Client(socket, ip);
});

function frame(op, payload) {
  const n = payload.length;
  const head = n < 126 ? Buffer.from([0x80 | op, n]) : n < 65536 ? Buffer.from([0x80 | op, 126, n >> 8, n & 255])
    : Buffer.concat([Buffer.from([0x80 | op, 127]), (() => { const b = Buffer.alloc(8); b.writeBigUInt64BE(BigInt(n)); return b; })()]);
  return Buffer.concat([head, payload]);
}

class Client {
  constructor(socket, ip) {
    this.s = socket; this.ip = ip; this.id = crypto.randomBytes(6).toString('hex'); this.room = null; this.presence = Object.create(null);
    this.buf = Buffer.alloc(0); this.frags = []; this.fragLen = 0; this.alive = true; this.closed = false;
    this.tokens = 60; this.last = Date.now();
    socket.on('data', d => this.onData(d));
    socket.on('close', () => this.cleanup());
    socket.on('error', () => this.cleanup());
    setTimeout(() => { if (!this.room) this.close(1008); }, 10000).unref();   // never joined a room: drop it
  }
  send(o) { if (!this.closed) try { this.s.write(frame(1, Buffer.from(JSON.stringify(o)))); } catch (e) {} }
  close(code = 1000) { if (this.closed) return; this.leave(); try { const b = Buffer.alloc(2); b.writeUInt16BE(code); this.s.write(frame(8, b)); } catch (e) {} this.s.end(); setTimeout(() => this.cleanup(), 100); }
  cleanup() {
    if (this.closed) return; this.closed = true;
    ipCount.set(this.ip, Math.max(0, (ipCount.get(this.ip) || 1) - 1));
    this.leave(); try { this.s.destroy(); } catch (e) {}
  }
  leave() {
    const r = this.room && rooms.get(this.room); if (!r) return;
    r.delete(this.id); this.room = null;
    for (const c of r.values()) c.send({ t: 'gone', peer: this.id });
    if (!r.size) for (const [k, v] of rooms) if (v === r) rooms.delete(k);
  }
  onData(d) {
    this.buf = Buffer.concat([this.buf, d]);
    for (;;) {
      const b = this.buf; if (b.length < 2) return;
      const fin = !!(b[0] & 0x80), op = b[0] & 15, masked = !!(b[1] & 0x80); let len = b[1] & 127, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) return; const big = b.readBigUInt64BE(2); if (big > BigInt(MAX_PAYLOAD)) return this.close(1009); len = Number(big); off = 10; }
      if (!masked) return this.close(1002);
      if (len > MAX_PAYLOAD || this.fragLen + len > MAX_PAYLOAD) return this.close(1009);
      if (b.length < off + 4 + len) return;
      const mask = b.subarray(off, off + 4), data = Buffer.from(b.subarray(off + 4, off + 4 + len));
      for (let i = 0; i < len; i++) data[i] ^= mask[i & 3];
      this.buf = b.subarray(off + 4 + len);
      if (op === 8) return this.close(1000);
      if (op === 9) { try { this.s.write(frame(10, data)); } catch (e) {} continue; }
      if (op === 10) { this.alive = true; continue; }
      if (op === 1 || op === 2 || op === 0) {
        this.frags.push(data); this.fragLen += len;
        if (fin) { const msg = Buffer.concat(this.frags).toString('utf8'); this.frags = []; this.fragLen = 0; this.onMessage(msg); }
      }
    }
  }
  onMessage(txt) {
    const now = Date.now(); this.tokens = Math.min(60, this.tokens + (now - this.last) * 0.04); this.last = now;
    if (this.tokens < 1) return; this.tokens -= 1;               // flood guard: roughly 40 messages per second
    let m; try { m = JSON.parse(txt); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;
    if (m.t === 'join') {
      if (this.room) this.leave();
      const name = String(m.room == null ? 'lobby' : m.room).slice(0, 40) || 'lobby';
      let r = rooms.get(name); if (!r) { r = new Map(); rooms.set(name, r); }
      if (r.size >= ROOM_MAX) { this.send({ t: 'full' }); return this.close(1013); }
      this.room = name;
      const peers = [...r.values()].map(c => ({ peer: c.id, presence: c.presence }));
      r.set(this.id, this);
      this.send({ t: 'hello', id: this.id, peers });
    } else if (m.t === 'p') {
      const r = this.room && rooms.get(this.room), clean = cleanPresence(m.presence); if (!r || !clean) return;
      this.presence = Object.assign(Object.create(null), this.presence, clean);   // keep everything the player sends, so new fields just work
      for (const c of r.values()) if (c !== this) c.send({ t: 'p', peer: this.id, presence: this.presence });
    } else if (m.t === 'e') {
      const r = this.room && rooms.get(this.room), topic = String(m.topic || ''); if (!r || !/^[A-Za-z0-9_-]{1,24}$/.test(topic)) return;
      const out = { t: 'e', topic, data: m.data, peer: this.id };
      for (const c of r.values()) c.send(out);                    // everyone in the room, sender included (the game checks isMe itself)
    }
  }
}

// presence is plain data: numbers, booleans, short strings, plus the appearance object "a". Any short key is accepted.
const okKey = k => /^[A-Za-z][A-Za-z0-9_]{0,11}$/.test(k);
function scalar(v) {
  if (typeof v === 'number') return Number.isFinite(v) ? Math.max(-1e4, Math.min(1e4, v)) : undefined;
  if (typeof v === 'boolean') return v;
  if (typeof v === 'string') return v.slice(0, 40);
}
function cleanPresence(p) {
  if (!p || typeof p !== 'object' || Array.isArray(p)) return null;
  const o = Object.create(null); let n = 0;
  for (const k of Object.keys(p)) {
    if (!okKey(k) || ++n > 32) continue;
    const v = p[k];
    if (v && typeof v === 'object' && !Array.isArray(v)) {
      const a = Object.create(null); let m = 0;
      for (const kk of Object.keys(v)) { if (!okKey(kk) || ++m > 24) continue; const s = scalar(v[kk]); if (s !== undefined) a[kk] = s; }
      o[k] = a;
    } else { const s = scalar(v); if (s !== undefined) o[k] = s; }
  }
  return o;
}

// drop connections that stopped answering
setInterval(() => {
  for (const r of rooms.values()) for (const c of r.values()) {
    if (!c.alive) { c.cleanup(); continue; }
    c.alive = false; try { c.s.write(frame(9, Buffer.alloc(0))); } catch (e) {}
  }
}, 30000).unref();

server.listen(PORT, () => console.log('Wild East server on http://localhost:' + PORT + '  (WebSocket at /ws)'));
