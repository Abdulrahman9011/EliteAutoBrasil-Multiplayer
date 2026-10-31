// Moto Online room relay. Zero dependencies (Node 18+): a tiny WebSocket server written with node:http + node:crypto.
// Players connect, create/join a room by code (max 5), and every message a player sends is forwarded to the others.
// Run:  node server.js        (PORT env var is honoured, e.g. on Render/Railway/Fly)
'use strict';
const http = require('node:http');
const crypto = require('node:crypto');

const PORT = parseInt(process.env.PORT || '8080', 10);
const MAX_PLAYERS = 5;
const MAX_FRAME = 4096;               // bytes; game messages are ~200 bytes
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

const rooms = new Map();              // code -> { clients: Map<id, Client> }

class Client {
  constructor(socket) {
    this.socket = socket; this.buf = Buffer.alloc(0); this.room = null; this.id = 0; this.alive = true; this.closed = false;
    this.frag = [];
  }
  sendText(str) {
    if (this.closed || this.socket.destroyed) return;
    const payload = Buffer.from(str, 'utf8');
    this.socket.write(Buffer.concat([frameHeader(0x1, payload.length), payload]));
  }
  sendRaw(op, payload) {
    if (this.closed || this.socket.destroyed) return;
    this.socket.write(Buffer.concat([frameHeader(op, payload.length), payload]));
  }
}

function frameHeader(op, len) {
  if (len < 126) return Buffer.from([0x80 | op, len]);
  if (len < 65536) { const h = Buffer.alloc(4); h[0] = 0x80 | op; h[1] = 126; h.writeUInt16BE(len, 2); return h; }
  const h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeBigUInt64BE(BigInt(len), 2); return h;
}

function newCode() {
  let c;
  do { c = ''; for (let i = 0; i < 5; i++) c += ALPHABET[crypto.randomInt(ALPHABET.length)]; } while (rooms.has(c));
  return c;
}

function broadcast(room, from, str) {
  for (const c of room.clients.values()) if (c !== from) c.sendText(str);
}

function leaveRoom(c) {
  const room = c.room;
  if (!room) return;
  room.clients.delete(c.id);
  broadcast(room, c, JSON.stringify({ t: 'left', id: c.id }));
  if (room.clients.size === 0) rooms.delete(c.roomCode);
  c.room = null; c.id = 0;
}

function joinRoom(c, code, create) {
  const room = rooms.get(code);
  if (!room) return c.sendText(JSON.stringify({ t: 'err', m: 'Room not found. Check the code.' }));
  if (room.clients.size >= MAX_PLAYERS) return c.sendText(JSON.stringify({ t: 'err', m: 'Room is full (max 5 players).' }));
  let id = 1; while (room.clients.has(id)) id++;
  const peers = [...room.clients.keys()];
  c.room = room; c.roomCode = code; c.id = id;
  room.clients.set(id, c);
  c.sendText(JSON.stringify({ t: 'room', code, id, peers, map: room.map || '' }));
  broadcast(room, c, JSON.stringify({ t: 'joined', id }));
}

function onMessage(c, text) {
  let m; try { m = JSON.parse(text); } catch { return; }
  if (!m || typeof m !== 'object') return;
  if (m.t === 's') {                         // state update: forward to everyone else in the room
    if (!c.room) return;
    m.id = c.id;
    broadcast(c.room, c, JSON.stringify(m));
  } else if (m.t === 'create') {
    if (c.room) leaveRoom(c);
    const code = newCode();
    rooms.set(code, { clients: new Map(), map: String(m.map || '').replace(/[^a-z0-9_-]/gi, '').slice(0, 24) });
    joinRoom(c, code, true);
  } else if (m.t === 'join') {
    if (c.room) leaveRoom(c);
    joinRoom(c, String(m.code || '').toUpperCase().trim(), false);
  } else if (m.t === 'leave') {
    leaveRoom(c);
  }
}

function closeClient(c) {
  if (c.closed) return;
  c.closed = true; leaveRoom(c);
  try { c.socket.destroy(); } catch {}
}

function parse(c) {
  for (;;) {
    const b = c.buf;
    if (b.length < 2) return;
    const fin = (b[0] & 0x80) !== 0, op = b[0] & 0x0f, masked = (b[1] & 0x80) !== 0;
    let len = b[1] & 0x7f, off = 2;
    if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (b.length < 10) return; const big = b.readBigUInt64BE(2); if (big > BigInt(MAX_FRAME)) return closeClient(c); len = Number(big); off = 10; }
    if (len > MAX_FRAME || !masked) return closeClient(c);   // clients must mask; refuse oversize frames
    if (b.length < off + 4 + len) return;
    const mask = b.subarray(off, off + 4);
    const data = Buffer.from(b.subarray(off + 4, off + 4 + len));
    for (let i = 0; i < data.length; i++) data[i] ^= mask[i & 3];
    c.buf = b.subarray(off + 4 + len);
    if (op === 0x8) { c.sendRaw(0x8, Buffer.alloc(0)); return closeClient(c); }
    if (op === 0x9) { c.sendRaw(0xA, data); continue; }
    if (op === 0xA) { c.alive = true; continue; }
    if (op === 0x1 || op === 0x0) {
      c.frag.push(data);
      if (fin) { const txt = Buffer.concat(c.frag).toString('utf8'); c.frag = []; onMessage(c, txt); }
    }
  }
}

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'text/plain' });
  res.end('Moto Online relay OK. rooms=' + rooms.size + '\n');
});

server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || (req.headers.upgrade || '').toLowerCase() !== 'websocket') { socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true);
  const c = new Client(socket);
  socket.on('data', d => { c.buf = Buffer.concat([c.buf, d]); parse(c); });
  socket.on('close', () => closeClient(c));
  socket.on('error', () => closeClient(c));
});

// keep connections alive through proxies, drop dead ones
const clients = () => { const all = []; for (const r of rooms.values()) for (const c of r.clients.values()) all.push(c); return all; };
setInterval(() => {
  for (const c of clients()) {
    if (!c.alive) { closeClient(c); continue; }
    c.alive = false; c.sendRaw(0x9, Buffer.alloc(0));
  }
}, 25000).unref();

server.listen(PORT, () => console.log('Moto Online relay listening on :' + PORT));
