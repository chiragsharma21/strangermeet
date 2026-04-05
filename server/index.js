const express = require("express");
const http    = require("http");
const { Server } = require("socket.io");
 
const app    = express();
const server = http.createServer(app);
const io     = new Server(server, {
  cors: { origin: "*", methods: ["GET","POST"] },
});
 
app.get("/", (_req, res) => res.send("OK"));
 
// ── state ─────────────────────────────────────────────────────────────────────
const waiting = [];        // socket IDs waiting for a partner
const pairs   = new Map(); // id → partnerId
const reports = new Map(); // id → report count
const MAX_REPORTS = 3;
 
// ── helpers ───────────────────────────────────────────────────────────────────
function tryMatch() {
  while (waiting.length >= 2) {
    const a = waiting.shift();
    const b = waiting.shift();
    const sa = io.sockets.sockets.get(a);
    const sb = io.sockets.sockets.get(b);
    if (!sa) { if (sb) waiting.unshift(b); continue; }
    if (!sb) { waiting.unshift(a); continue; }
    pairs.set(a, b); pairs.set(b, a);
    io.to(a).emit("matched", { initiator: true  });
    io.to(b).emit("matched", { initiator: false });
  }
}
 
function removeFromWaiting(id) {
  const i = waiting.indexOf(id);
  if (i !== -1) waiting.splice(i, 1);
}
 
// cleanup: unpair both, optionally re-queue the partner automatically
function cleanup(id, requeuePartner = false) {
  const partner = pairs.get(id);
  pairs.delete(id);
  if (partner) {
    pairs.delete(partner);
    io.to(partner).emit("partner_left");
    // re-queue partner so they don't get stuck waiting for user action
    if (requeuePartner && io.sockets.sockets.get(partner)) {
      removeFromWaiting(partner);
      waiting.push(partner);
      io.to(partner).emit("waiting");
    }
  }
  removeFromWaiting(id);
}
 
// ── events ────────────────────────────────────────────────────────────────────
io.on("connection", (socket) => {
  const { id } = socket;
 
  socket.on("find_partner", () => {
    cleanup(id, false);
    waiting.push(id);
    socket.emit("waiting");
    tryMatch();
  });
 
  // WebRTC relay
  ["offer","answer","ice_candidate"].forEach(ev => {
    socket.on(ev, (data) => {
      const p = pairs.get(id);
      if (p) io.to(p).emit(ev, data);
    });
  });
 
  // chat
  socket.on("message", (text) => {
    if (typeof text !== "string" || text.length > 500) return;
    const p = pairs.get(id);
    if (p) io.to(p).emit("message", { text, from:"stranger" });
  });
 
  // skip — re-queue BOTH users so rematching works instantly
  socket.on("skip", () => {
    cleanup(id, true);        // partner also goes back to queue automatically
    waiting.push(id);
    socket.emit("waiting");
    setTimeout(() => tryMatch(), 100); // small delay so both are in queue
  });
 
  // report
  socket.on("report", () => {
    const p = pairs.get(id);
    if (!p) return;
    const c = (reports.get(p) || 0) + 1;
    reports.set(p, c);
    if (c >= MAX_REPORTS) {
      io.to(p).emit("banned");
      io.sockets.sockets.get(p)?.disconnect(true);
      reports.delete(p);
    }
    cleanup(id, false);
    waiting.push(id);
    socket.emit("waiting");
    tryMatch();
  });
 
  socket.on("disconnect", () => cleanup(id, false));
});
 
const PORT = process.env.PORT || 3001;
server.listen(PORT, () => console.log(`server on :${PORT}`));
