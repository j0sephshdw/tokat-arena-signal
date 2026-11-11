// Tokat Arena — eşleştirme (signaling) sunucusu.
// Oyun trafiği buradan GEÇMEZ; sadece WebRTC teklif/cevap/ICE mesajlarını taşır
// ve 4 harfli oda kodlarını yönetir. Host telefon listen-server olarak kalır.
//
// Ortam değişkenleri (opsiyonel):
//   PORT           — Render otomatik verir
//   TURN_URLS      — virgülle ayrılmış, örn "turn:global.relay.metered.ca:80,turn:global.relay.metered.ca:443?transport=tcp"
//   TURN_USER, TURN_PASS

const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const rooms = new Map(); // code -> { host: ws, peers: Map<id, ws> }
const ALPHA = "ABCDEFGHJKLMNPRSTUVYZ"; // karışan harfler (I,O,Q...) yok

function iceServers() {
  const list = [
    { urls: ["stun:stun.l.google.com:19302", "stun:stun1.l.google.com:19302"] },
    { urls: ["stun:stun.cloudflare.com:3478"] },
  ];
  if (process.env.TURN_URLS) {
    list.push({
      urls: process.env.TURN_URLS.split(",").map((s) => s.trim()),
      username: process.env.TURN_USER || "",
      credential: process.env.TURN_PASS || "",
    });
  }
  return list;
}

function newCode() {
  for (let tries = 0; tries < 1000; tries++) {
    let c = "";
    for (let i = 0; i < 4; i++) c += ALPHA[Math.floor(Math.random() * ALPHA.length)];
    if (!rooms.has(c)) return c;
  }
  throw new Error("oda kodu üretilemedi");
}

function newId() {
  return 2 + Math.floor(Math.random() * 2147483640);
}

function send(ws, obj) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj));
}

const server = http.createServer((req, res) => {
  res.writeHead(200, { "content-type": "text/plain; charset=utf-8" });
  res.end(`Tokat Arena signaling OK (v2 relay). Açık oda: ${rooms.size}\n`);
});

const wss = new WebSocketServer({ server, maxPayload: 64 * 1024 });

wss.on("connection", (ws) => {
  ws.isAlive = true;
  ws.on("pong", () => (ws.isAlive = true));
  ws.room = null;
  ws.id = 0;
  send(ws, { t: "ice_servers", list: iceServers() });

  ws.on("message", (raw, isBinary) => {
    // RELAY: ikili paket = [hedef id u32 LE][oyun verisi]. Sunucu hedefi gönderen id'siyle değiştirip iletir.
    if (isBinary) {
      const room = rooms.get(ws.room);
      if (!room || raw.length < 4) return;
      const to = raw.readUInt32LE(0);
      const target = to === 1 ? room.host : room.peers.get(to);
      if (!target || target.readyState !== 1) return;
      const buf = Buffer.from(raw);
      buf.writeUInt32LE(ws.id >>> 0, 0);
      target.send(buf, { binary: true });
      return;
    }
    let d;
    try {
      d = JSON.parse(raw.toString());
    } catch {
      return;
    }
    switch (d.t) {
      case "host": {
        if (ws.room) return;
        const code = newCode();
        rooms.set(code, { host: ws, peers: new Map() });
        ws.room = code;
        ws.id = 1;
        send(ws, { t: "room", code });
        console.log(`oda açıldı ${code}`);
        break;
      }
      case "join": {
        const code = String(d.code || "").toUpperCase();
        const room = rooms.get(code);
        if (!room) return send(ws, { t: "error", msg: "Böyle bir oda yok: " + code });
        if (room.peers.size >= 5) return send(ws, { t: "error", msg: "Oda dolu" });
        const id = newId();
        ws.room = code;
        ws.id = id;
        room.peers.set(id, ws);
        send(ws, { t: "joined", code, id });
        send(room.host, { t: "peer", id });
        console.log(`odaya katıldı ${code} <- ${id}`);
        break;
      }
      case "offer":
      case "answer":
      case "cand": {
        const room = rooms.get(ws.room);
        if (!room) return;
        const to = Number(d.to);
        const target = to === 1 ? room.host : room.peers.get(to);
        if (!target) return;
        const out = { ...d, from: ws.id };
        delete out.to;
        send(target, out);
        break;
      }
    }
  });

  ws.on("close", () => {
    const room = rooms.get(ws.room);
    if (!room) return;
    if (ws.id === 1) {
      for (const p of room.peers.values()) send(p, { t: "error", msg: "Host odayı kapattı" });
      rooms.delete(ws.room);
      console.log(`oda kapandı ${ws.room}`);
    } else {
      room.peers.delete(ws.id);
      send(room.host, { t: "leave", id: ws.id });
    }
  });
});

// bağlantıları canlı tut (Render boşta bağlantıyı kesmesin)
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, 25000);

server.listen(PORT, () => console.log(`signaling port ${PORT}`));
