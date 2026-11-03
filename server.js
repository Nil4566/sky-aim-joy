// Sky Aim — server ทำหน้าที่ "ส่งต่อข้อมูล" ระหว่างจอ host กับมือถือ (client) เท่านั้น
// ตรรกะของเกมทั้งหมดอยู่ที่หน้า host

const path = require('path');
const http = require('http');
const express = require('express');
const { Server } = require('socket.io');

const PORT = process.env.PORT || 3000;
const app = express();
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 64 * 1024 });

app.use(express.static(path.join(__dirname, 'public')));
app.get('/healthz', (_req, res) => res.type('text').send('ok'));

/**
 * rooms: รหัสห้อง -> { host: socketId, clients: Set<socketId>, last: payload ล่าสุดของ lobby/target/end }
 */
const rooms = new Map();
const CACHED_TYPES = new Set(['lobby', 'target', 'end']);
const GAME_TYPES = new Set(['lobby', 'target', 'hit', 'skip', 'end']);
const ACTIONS = new Set(['start', 'hint', 'skip']);
const chan = (code) => 'room:' + code;

function allocateCode(preferred) {
  if (typeof preferred === 'string' && /^\d{4}$/.test(preferred) && !rooms.has(preferred)) {
    return preferred;
  }
  if (rooms.size >= 9000) return null;
  let code;
  do {
    code = String(1000 + Math.floor(Math.random() * 9000));
  } while (rooms.has(code));
  return code;
}

function notifyCount(code) {
  const r = rooms.get(code);
  if (!r) return;
  io.to(r.host).emit('clients', { count: r.clients.size, ids: [...r.clients] });
}

/** ห้องที่ socket นี้เป็น client อยู่ (ตรวจว่ายังเป็นสมาชิกจริง) */
function clientRoom(socket) {
  const code = socket.data.room;
  if (!code) return null;
  const r = rooms.get(code);
  return r && r.clients.has(socket.id) ? r : null;
}

function leaveAsClient(socket) {
  const code = socket.data.room;
  if (!code) return;
  socket.leave(chan(code));
  socket.data.room = null;
  const r = rooms.get(code);
  if (r && r.clients.delete(socket.id)) notifyCount(code);
}

function closeRoom(code) {
  const r = rooms.get(code);
  if (!r) return;
  io.to(chan(code)).emit('host-left');
  for (const id of r.clients) {
    const s = io.sockets.sockets.get(id);
    if (s) {
      s.leave(chan(code));
      s.data.room = null;
    }
  }
  rooms.delete(code);
}

const num = (v) => {
  const n = Number(v);
  return Number.isFinite(n) ? Math.max(-180, Math.min(180, n)) : 0;
};

io.on('connection', (socket) => {
  // ---------- HOST ----------
  socket.on('host:create', (data, ack) => {
    if (typeof ack !== 'function') return;
    leaveAsClient(socket);
    if (socket.data.hostOf && rooms.has(socket.data.hostOf)) {
      const code = socket.data.hostOf;
      return ack({ ok: true, room: code, count: rooms.get(code).clients.size });
    }
    const preferred = data && data.room != null ? String(data.room) : '';
    const code = allocateCode(preferred);
    if (!code) return ack({ ok: false, error: 'full' });
    rooms.set(code, { host: socket.id, clients: new Set(), last: null });
    socket.data.hostOf = code;
    ack({ ok: true, room: code, count: 0, reused: code === preferred });
  });

  socket.on('game', (payload) => {
    const code = socket.data.hostOf;
    const r = code && rooms.get(code);
    if (!r || r.host !== socket.id) return;
    if (!payload || typeof payload !== 'object' || !GAME_TYPES.has(payload.type)) return;
    if (CACHED_TYPES.has(payload.type)) r.last = payload;
    io.to(chan(code)).emit('game', payload);
  });

  // ---------- CLIENT (มือถือ) ----------
  socket.on('client:join', (data, ack) => {
    const reply = typeof ack === 'function' ? ack : () => {};
    const code = String((data && data.room) || '').trim();
    if (!/^\d{4}$/.test(code)) return reply({ ok: false, error: 'bad-code' });
    if (socket.data.hostOf) return reply({ ok: false, error: 'is-host' });
    const r = rooms.get(code);
    if (!r) return reply({ ok: false, error: 'not-found' });
    if (socket.data.room !== code || !r.clients.has(socket.id)) {
      leaveAsClient(socket);
      socket.join(chan(code));
      socket.data.room = code;
      r.clients.add(socket.id);
      notifyCount(code);
    }
    reply({ ok: true, room: code });
    if (r.last) socket.emit('game', r.last); // ให้มือถือที่เข้ามาทีหลังเห็นสถานะล่าสุด
  });

  socket.on('client:leave', () => leaveAsClient(socket));

  socket.on('tilt', (d) => {
    const r = clientRoom(socket);
    if (!r || !d) return;
    io.to(r.host).volatile.emit('tilt', { id: socket.id, x: num(d.x), y: num(d.y), fine: !!d.fine });
  });

  socket.on('action', (d) => {
    const r = clientRoom(socket);
    if (!r || !d || !ACTIONS.has(d.name)) return;
    io.to(r.host).emit('action', { id: socket.id, name: d.name });
  });

  socket.on('disconnect', () => {
    leaveAsClient(socket);
    const code = socket.data.hostOf;
    if (code && rooms.get(code) && rooms.get(code).host === socket.id) closeRoom(code);
  });
});

server.listen(PORT, () => {
  console.log(`Sky Aim server กำลังทำงานที่ http://localhost:${PORT}`);
});
