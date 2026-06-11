// ============================================================
// U-NEXT Party - relay server v0.2
// ルーム単位の中継 + ホスト管理 + 作品IDチェック + チャット
// 起動: npm install && node server.js
// ============================================================

const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8765;
const wss = new WebSocketServer({ port: PORT });

// roomId -> {
//   clients: Set<WebSocket>,
//   host: WebSocket | null,   // 最初に参加した人がホスト
//   hostMode: boolean,        // true = ホストの操作のみ同期
//   contentId: string | null, // ルームで見ている作品ID(最初の参加者基準)
// }
const rooms = new Map();

function getRoom(id) {
  if (!rooms.has(id)) {
    rooms.set(id, { clients: new Set(), host: null, hostMode: false, contentId: null });
  }
  return rooms.get(id);
}

// ルーム全員(または自分以外)に送信
function broadcast(room, payload, except = null) {
  const json = JSON.stringify(payload);
  for (const peer of room.clients) {
    if (peer !== except && peer.readyState === peer.OPEN) {
      peer.send(json);
    }
  }
}

// 各メンバーに最新のルーム情報を送る(isHostは人ごとに違うので個別送信)
function sendRoomInfo(room) {
  for (const peer of room.clients) {
    if (peer.readyState !== peer.OPEN) continue;
    peer.send(JSON.stringify({
      type: "room-info",
      members: room.clients.size,
      isHost: peer === room.host,
      hostMode: room.hostMode,
      hostName: room.host?.name ?? null,
    }));
  }
}

function leaveRoom(ws) {
  if (!ws.roomId) return;
  const room = rooms.get(ws.roomId);
  ws.roomId = null;
  if (!room) return;

  room.clients.delete(ws);

  if (room.clients.size === 0) {
    rooms.delete(ws.roomId);
    return;
  }

  // ホストが抜けたら次の人に引き継ぐ
  if (room.host === ws) {
    room.host = room.clients.values().next().value;
    broadcast(room, {
      type: "system",
      text: `ホストが退出したため ${room.host.name} さんが新しいホストになりました`,
    });
  }
  sendRoomInfo(room);
}

wss.on("connection", (ws) => {
  ws.roomId = null;
  ws.name = "ゲスト";

  ws.on("message", (data) => {
    let msg;
    try {
      msg = JSON.parse(data);
    } catch {
      return;
    }

    // ---------- 参加 ----------
    if (msg.type === "join") {
      leaveRoom(ws); // 前のルームから退出
      ws.roomId = msg.room;
      ws.name = String(msg.name || "ゲスト").slice(0, 20);

      const room = getRoom(ws.roomId);
      room.clients.add(ws);

      // 最初の参加者がホスト & ルームの作品IDの基準になる
      if (!room.host) room.host = ws;
      if (!room.contentId && msg.contentId) room.contentId = msg.contentId;

      // 作品IDが違う場合は本人に警告(同期はするが時間がズレる原因になる)
      if (room.contentId && msg.contentId && room.contentId !== msg.contentId) {
        ws.send(JSON.stringify({
          type: "content-mismatch",
          expected: room.contentId,
          got: msg.contentId,
        }));
        broadcast(room, {
          type: "system",
          text: `⚠ ${ws.name} さんは別の作品を開いている可能性があります`,
        }, ws);
      }

      broadcast(room, { type: "system", text: `${ws.name} さんが参加しました` }, ws);
      sendRoomInfo(room);
      console.log(`join: room=${ws.roomId} name=${ws.name} members=${room.clients.size}`);
      return;
    }

    if (!ws.roomId) return;
    const room = rooms.get(ws.roomId);
    if (!room) return;

    // ---------- ホストモードの切り替え(ホストのみ) ----------
    if (msg.type === "set-host-mode") {
      if (ws !== room.host) return;
      room.hostMode = !!msg.enabled;
      broadcast(room, {
        type: "system",
        text: room.hostMode
          ? "ホストモードがONになりました(ホストの操作のみ同期されます)"
          : "ホストモードがOFFになりました(全員が操作できます)",
      });
      sendRoomInfo(room);
      return;
    }

    // ---------- 再生操作(play/pause/seek/tick) ----------
    if (["play", "pause", "seek", "tick"].includes(msg.type)) {
      // ホストモード中はホスト以外の操作を無視
      // (tickはドリフト補正用の定期送信。常にホストだけが送る)
      if ((room.hostMode || msg.type === "tick") && ws !== room.host) return;
      broadcast(room, { ...msg, from: ws.name }, ws);
      return;
    }

    // ---------- チャット ----------
    if (msg.type === "chat") {
      const text = String(msg.text || "").slice(0, 500);
      if (!text) return;
      broadcast(room, { type: "chat", from: ws.name, text }, ws);
      return;
    }

    // ---------- 同期リクエスト(新規参加者→ホストが状態を返す) ----------
    if (msg.type === "sync-request") {
      if (room.host && room.host !== ws && room.host.readyState === room.host.OPEN) {
        room.host.send(JSON.stringify({ type: "sync-request" }));
      }
      return;
    }
    if (msg.type === "sync-state") {
      // ホストからの状態を全員へ
      if (ws !== room.host) return;
      broadcast(room, msg, ws);
      return;
    }
  });

  ws.on("close", () => leaveRoom(ws));
});

console.log(`U-NEXT Party server listening on ws://localhost:${PORT}`);
