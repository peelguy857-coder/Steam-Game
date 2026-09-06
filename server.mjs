// THOUSAND DAYS lobby relay: a tiny WebSocket server with no dependencies.
// One process, many lobbies. A lobby has a 6-letter code, one host and up to 7 guests; the
// relay never looks inside game messages — it only forwards them. Run standalone
// (`node tools/relay.mjs`, PORT env) or mount into another http server with `attachRelay`.
import { createServer } from 'node:http';
import { createHash } from 'node:crypto';

const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const MAX_PLAYERS = 8;

export function attachRelay(server, { log = console.log } = {}) {
  const lobbies = new Map();   // code -> { code, seed, host, players: Map<id, client> }
  let nextId = 1;

  const code = () => { let c = ''; for (let i = 0; i < 6; i++) c += CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)]; return lobbies.has(c) ? code() : c; };
  const roster = (L) => [...L.players.values()].map((c) => ({ id: c.id, name: c.name, host: c === L.host }));
  const broadcast = (L, msg, except = null) => { const s = JSON.stringify(msg); for (const c of L.players.values()) if (c !== except) c.send(s); };

  server.on('upgrade', (req, socket) => {
    const key = req.headers['sec-websocket-key'];
    if (!key || !/\/relay\/?$/.test(req.url.split('?')[0])) { socket.destroy(); return; }
    const accept = createHash('sha1').update(key + GUID).digest('base64');
    socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
    const client = { id: nextId++, name: 'Survivor', lobby: null, socket, send(s) { if (!socket.destroyed) socket.write(frame(s)); } };
    let buf = Buffer.alloc(0);
    socket.on('data', (chunk) => {
      buf = Buffer.concat([buf, chunk]);
      for (;;) {
        const f = parseFrame(buf);
        if (!f) break;
        buf = buf.subarray(f.length);
        if (f.op === 8) { socket.end(); break; }
        if (f.op === 9) { socket.write(frame(f.payload, 10)); continue; }
        if (f.op !== 1) continue;
        let msg; try { msg = JSON.parse(f.payload.toString('utf8')); } catch { continue; }
        handle(client, msg);
      }
    });
    socket.on('close', () => leave(client));
    socket.on('error', () => leave(client));
    client.send(JSON.stringify({ t: 'hello', id: client.id }));
  });

  function handle(c, m) {
    if (m.t === 'create') {
      leave(c);
      const L = { code: code(), seed: String(m.seed || ''), difficulty: m.difficulty || 'standard', host: c, players: new Map(), started: false };
      c.name = String(m.name || 'Survivor').slice(0, 20); c.lobby = L; L.players.set(c.id, c); lobbies.set(L.code, L);
      c.send(JSON.stringify({ t: 'lobby', code: L.code, id: c.id, host: true, seed: L.seed, difficulty: L.difficulty, players: roster(L) }));
      log(`[relay] lobby ${L.code} created by ${c.name}`);
    } else if (m.t === 'join') {
      leave(c);
      const L = lobbies.get(String(m.code || '').toUpperCase().trim());
      if (!L) { c.send(JSON.stringify({ t: 'error', error: 'No lobby with that code.' })); return; }
      if (L.players.size >= MAX_PLAYERS) { c.send(JSON.stringify({ t: 'error', error: 'That lobby is full.' })); return; }
      c.name = String(m.name || 'Survivor').slice(0, 20); c.lobby = L; L.players.set(c.id, c);
      c.send(JSON.stringify({ t: 'lobby', code: L.code, id: c.id, host: false, hostId: L.host.id, seed: L.seed, difficulty: L.difficulty, started: L.started, players: roster(L) }));
      broadcast(L, { t: 'peer', id: c.id, name: c.name, joined: true, players: roster(L) }, c);
      log(`[relay] ${c.name} joined ${L.code} (${L.players.size})`);
    } else if (m.t === 'start') {
      const L = c.lobby; if (!L || L.host !== c) return;
      L.started = true; broadcast(L, { t: 'start', seed: L.seed, difficulty: L.difficulty });
    } else if (m.t === 'msg') {
      const L = c.lobby; if (!L) return;
      const out = JSON.stringify({ t: 'msg', from: c.id, d: m.d });
      if (m.to === 'host') { if (L.host !== c) L.host.send(out); }
      else if (m.to === 'all' || m.to == null) { for (const p of L.players.values()) if (p !== c) p.send(out); }
      else { const p = L.players.get(Number(m.to)); if (p) p.send(out); }
    } else if (m.t === 'leave') leave(c);
  }

  function leave(c) {
    const L = c.lobby; if (!L) return;
    c.lobby = null; L.players.delete(c.id);
    if (L.host === c) { broadcast(L, { t: 'closed', reason: 'The host left.' }); for (const p of L.players.values()) p.lobby = null; lobbies.delete(L.code); log(`[relay] lobby ${L.code} closed`); }
    else broadcast(L, { t: 'peer', id: c.id, name: c.name, left: true, players: roster(L) });
  }

  return { lobbies };
}

// ---- WebSocket framing (server side: incoming frames are masked, outgoing are not) --------------
function frame(data, op = 1) {
  const payload = Buffer.isBuffer(data) ? data : Buffer.from(String(data), 'utf8');
  const len = payload.length;
  let head;
  if (len < 126) head = Buffer.from([0x80 | op, len]);
  else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | op; head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x80 | op; head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
  return Buffer.concat([head, payload]);
}
function parseFrame(buf) {
  if (buf.length < 2) return null;
  const op = buf[0] & 0x0f, masked = (buf[1] & 0x80) !== 0;
  let len = buf[1] & 0x7f, off = 2;
  if (len === 126) { if (buf.length < 4) return null; len = buf.readUInt16BE(2); off = 4; }
  else if (len === 127) { if (buf.length < 10) return null; len = Number(buf.readBigUInt64BE(2)); off = 10; }
  const maskLen = masked ? 4 : 0;
  if (buf.length < off + maskLen + len) return null;
  const mask = masked ? buf.subarray(off, off + 4) : null;
  const payload = Buffer.from(buf.subarray(off + maskLen, off + maskLen + len));
  if (mask) for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
  return { op, payload, length: off + maskLen + len };
}

// standalone: `node tools/relay.mjs` (Render/Fly/any host sets PORT)
if (process.argv[1] && /(relay|server)\.mjs$/.test(process.argv[1])) {
  const port = Number(process.env.PORT) || 8787;
  const server = createServer((req, res) => { res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('THOUSAND DAYS relay is up. Connect a game client to /relay.'); });
  attachRelay(server);
  server.listen(port, () => console.log(`[relay] listening on ${port}`));
}
