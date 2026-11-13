// RandomChatConnectingServer/index.js
require("dotenv").config();
const express = require("express");
const http = require("http");
const cors = require("cors");
const crypto = require("crypto");
const { Server } = require("socket.io");

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
  },
  maxHttpBufferSize: 1e7, // 10 MB buffer for HD images & voice messages
  pingInterval: 4000,
  pingTimeout: 8000,
  transports: ["websocket", "polling"],
});

const PORT = process.env.PORT || 5000;

// ============================================================================
// ⚙️ LIFECYCLE CONSTANTS
// ============================================================================
const SERVER_INSTANCE_ID = `csrv_${Date.now()}_${crypto
  .randomBytes(4)
  .toString("hex")}`;
const QUEUE_MAX_WAIT_MS = 10000; // Strict 10.0s maximum matchmaking queue duration
const QUEUE_HEARTBEAT_EXPIRY_MS = 4500; // Evict waiting user if queue pings stop > 4.5s
const ACTIVE_CHAT_DISCONNECT_TIMEOUT_MS = 10000; // 10s grace for Wi-Fi <-> Cellular switch
const PROTECTED_DIALOG_GRACE_MS = 30000; // 30s grace while picking gallery image, camera, or permissions

function logEvent(category, message, details = null) {
  const ts = new Date().toISOString();
  if (details !== null && details !== undefined) {
    console.log(
      `[${ts}] [CHAT:${category}] ${message} ->`,
      typeof details === "object" ? JSON.stringify(details) : details
    );
  } else {
    console.log(`[${ts}] [CHAT:${category}] ${message}`);
  }
}

// ============================================================================
// 🗂️ IN-MEMORY STORES ONLY (ZERO DATABASE STORAGE)
// ============================================================================
const waitingQueue = new Map(); // emailKey -> queueEntry
const activeRooms = new Map(); // roomId (20-char) -> roomObject
const userToSocket = new Map(); // emailKey -> socket.id
const socketToUser = new Map(); // socket.id -> emailKey
const userToRoom = new Map(); // emailKey -> roomId
const userSessionGen = new Map(); // emailKey -> integer generation counter
const cancelledAttempts = new Set(); // attemptId
const userProtectedUntil = new Map(); // emailKey -> timestamp
const matchingLocks = new Set(); // emailKey (atomic 1-to-1 lock)
const queueTimers = new Map(); // emailKey -> Timeout (10s matchmaking timer)
const disconnectTimers = new Map(); // emailKey -> Timeout

const ID_CHARS =
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
function generateRoomId(length = 20) {
  let result = "";
  const randomBytes = crypto.randomBytes(length);
  for (let i = 0; i < length; i++) {
    result += ID_CHARS[randomBytes[i] % ID_CHARS.length];
  }
  return activeRooms.has(result) ? generateRoomId(length) : result;
}

function generateAttemptId() {
  return `catt_${Date.now()}_${crypto.randomBytes(4).toString("hex")}`;
}

function nextUserGeneration(emailKey) {
  const nextGen = (userSessionGen.get(emailKey) || 0) + 1;
  userSessionGen.set(emailKey, nextGen);
  return nextGen;
}

function clearQueueTimer(emailKey) {
  const timer = queueTimers.get(emailKey);
  if (timer) {
    clearTimeout(timer);
    queueTimers.delete(emailKey);
  }
}

function clearDisconnectTimer(emailKey) {
  const timer = disconnectTimers.get(emailKey);
  if (timer) {
    clearTimeout(timer);
    disconnectTimers.delete(emailKey);
  }
}

function removeFromWaitingQueue(
  emailKey,
  reason = "removed",
  markAttemptCancelled = true
) {
  if (!emailKey) return;
  clearQueueTimer(emailKey);
  const existing = waitingQueue.get(emailKey);
  if (existing) {
    if (markAttemptCancelled && existing.attemptId) {
      cancelledAttempts.add(existing.attemptId);
      setTimeout(() => cancelledAttempts.delete(existing.attemptId), 30000);
    }
    waitingQueue.delete(emailKey);
    logEvent("QUEUE_REMOVE", `Removed ${emailKey} from chat waitingQueue`, {
      attemptId: existing.attemptId,
      reason,
      remainingInQueue: waitingQueue.size,
    });
  }
  matchingLocks.delete(emailKey);
}

function bindUserSocket(email, socket) {
  if (!email || !socket) return null;
  const emailKey = String(email).trim().toLowerCase();

  clearDisconnectTimer(emailKey);

  const prevSocketId = userToSocket.get(emailKey);
  if (prevSocketId && prevSocketId !== socket.id) {
    socketToUser.delete(prevSocketId);
    const oldSock = io.sockets.sockets.get(prevSocketId);
    if (oldSock) {
      try {
        oldSock.disconnect(true);
      } catch (e) {}
    }
  }

  userToSocket.set(emailKey, socket.id);
  socketToUser.set(socket.id, emailKey);

  const qEntry = waitingQueue.get(emailKey);
  if (qEntry) {
    qEntry.socketId = socket.id;
    qEntry.lastPing = Date.now();
  }

  const activeRoomId = userToRoom.get(emailKey);
  if (activeRoomId && activeRooms.has(activeRoomId)) {
    socket.join(activeRoomId);
    const room = activeRooms.get(activeRoomId);
    room.lastHeartbeat = Date.now();
  }

  return emailKey;
}

function validateSignalingParticipant(roomId, socket, explicitEmail = null) {
  if (!roomId || !activeRooms.has(roomId)) return null;
  const room = activeRooms.get(roomId);
  if (!room || room.status !== "active") return null;

  const senderKey = explicitEmail
    ? String(explicitEmail).trim().toLowerCase()
    : socketToUser.get(socket.id);
  if (!senderKey) return null;

  const callerKey = String(room.caller || "").toLowerCase();
  const calleeKey = String(room.callee || "").toLowerCase();

  if (senderKey !== callerKey && senderKey !== calleeKey) return null;
  if (userToRoom.get(senderKey) !== roomId) return null;

  const recipientKey = senderKey === callerKey ? calleeKey : callerKey;
  if (userToRoom.get(recipientKey) !== roomId) return null;

  room.lastHeartbeat = Date.now();

  return {
    room,
    senderKey,
    recipientKey,
    isCaller: senderKey === callerKey,
    recipientSocketId: userToSocket.get(recipientKey),
  };
}

function evaluateCompatibility(userA, userB) {
  const genderA = userA.gender ? String(userA.gender).toLowerCase() : null;
  const genderB = userB.gender ? String(userB.gender).toLowerCase() : null;

  const bHasFilters =
    userB.filterGender ||
    userB.filterLocation ||
    userB.filterAgeMin ||
    userB.filterAgeMax ||
    (Array.isArray(userB.filterMaritalStatus) &&
      userB.filterMaritalStatus.length > 0);

  const aMatchesB =
    !bHasFilters ||
    ((!userB.filterGender ||
      userB.filterGender === "Both" ||
      String(userB.filterGender).toLowerCase() === genderA) &&
      (!userB.filterLocation ||
        (userA.location &&
          String(userA.location).toLowerCase() ===
            String(userB.filterLocation).toLowerCase())) &&
      (!userB.filterMaritalStatus?.length ||
        userB.filterMaritalStatus.includes(userA.maritalStatus)) &&
      (!userB.filterAgeMin ||
        (userA.age && userA.age >= userB.filterAgeMin)) &&
      (!userB.filterAgeMax ||
        (userA.age && userA.age <= userB.filterAgeMax)));

  if (!aMatchesB) {
    return {
      compatible: false,
      reason: `${userA.email} did not match ${userB.email}'s filters`,
    };
  }

  const aHasFilters =
    userA.filterGender ||
    userA.filterLocation ||
    userA.filterAgeMin ||
    userA.filterAgeMax ||
    (Array.isArray(userA.filterMaritalStatus) &&
      userA.filterMaritalStatus.length > 0);

  const bMatchesA =
    !aHasFilters ||
    ((!userA.filterGender ||
      userA.filterGender === "Both" ||
      String(userA.filterGender).toLowerCase() === genderB) &&
      (!userA.filterLocation ||
        (userB.location &&
          String(userB.location).toLowerCase() ===
            String(userA.filterLocation).toLowerCase())) &&
      (!userA.filterMaritalStatus?.length ||
        userA.filterMaritalStatus.includes(userB.maritalStatus)) &&
      (!userA.filterAgeMin ||
        (userB.age && userB.age >= userA.filterAgeMin)) &&
      (!userA.filterAgeMax ||
        (userB.age && userB.age <= userA.filterAgeMax)));

  if (!bMatchesA) {
    return {
      compatible: false,
      reason: `${userB.email} did not match ${userA.email}'s filters`,
    };
  }

  return { compatible: true, reason: "Bidirectional filters matched" };
}

function destroyRoomAndNotifyPartner(roomId, leaverEmail, reason = "ended") {
  if (!roomId || !activeRooms.has(roomId)) return;
  const room = activeRooms.get(roomId);

  room.status = "terminated";
  room.offer = null;
  room.answer = null;
  room.callerCandidates = [];
  room.calleeCandidates = [];
  room.inChatCallOffer = null;
  room.inChatCallAnswer = null;
  room.callCandidates = [];
  activeRooms.delete(roomId);

  const callerKey = String(room.caller || "").toLowerCase();
  const calleeKey = String(room.callee || "").toLowerCase();
  const leaverKey = String(leaverEmail || "").toLowerCase();

  clearDisconnectTimer(callerKey);
  clearDisconnectTimer(calleeKey);

  if (userToRoom.get(callerKey) === roomId) userToRoom.delete(callerKey);
  if (userToRoom.get(calleeKey) === roomId) userToRoom.delete(calleeKey);

  const callerSockId = userToSocket.get(callerKey);
  const calleeSockId = userToSocket.get(calleeKey);
  if (callerSockId) io.sockets.sockets.get(callerSockId)?.leave(roomId);
  if (calleeSockId) io.sockets.sockets.get(calleeSockId)?.leave(roomId);

  const partnerKey = leaverKey === callerKey ? calleeKey : callerKey;
  const partnerEmail = leaverKey === callerKey ? room.callee : room.caller;
  const partnerSocketId = userToSocket.get(partnerKey);

  const durationSec = Math.round(
    (Date.now() - (room.createdAtMs || Date.now())) / 1000
  );

  logEvent("ROOM_DESTROYED", `Chat room ${roomId} terminated & wiped`, {
    roomId,
    caller: room.caller,
    callee: room.callee,
    endedBy: leaverEmail || "system",
    reason,
    durationSec: `${durationSec}s`,
    remainingActiveRooms: activeRooms.size,
  });

  if (!partnerSocketId) return;

  const cleanReason = String(reason || "ended").toLowerCase();
  const isNext = cleanReason === "next";
  const isLost =
    cleanReason === "disconnected" || cleanReason.includes("internet");

  io.to(partnerSocketId).emit("partner_disconnected_requeue", {
    roomId,
    action: cleanReason,
    reason: cleanReason,
    by: leaverEmail,
    timestamp: Date.now(),
  });

  io.to(partnerSocketId).emit("room_ended", {
    roomId,
    status: cleanReason,
    endedBy: leaverEmail,
    endedReason: cleanReason,
    timestamp: Date.now(),
  });

  io.to(partnerSocketId).emit("call_alert", {
    roomId,
    action: cleanReason,
    title: isNext
      ? "Stranger left"
      : isLost
      ? "Connection Lost ⚠️"
      : "User Has Left",
    message: isNext
      ? "User is looking for someone else"
      : isLost
      ? "User has lost the connection"
      : "The user has left the chat",
    by: leaverEmail,
    timestamp: Date.now(),
  });

  logEvent("PARTNER_NOTIFIED", `Notified ${partnerEmail} of chat room end`, {
    roomId,
    reason: cleanReason,
  });
}

// ============================================================================
// ⚡ SOCKET.IO CONNECTION & REAL-TIME ENGINE
// ============================================================================
io.on("connection", (socket) => {
  logEvent("SOCKET_CONNECTED", `New chat socket connected: ${socket.id}`);

  socket.emit("server_hello", {
    serverInstanceId: SERVER_INSTANCE_ID,
    timestamp: Date.now(),
  });

  socket.conn.on("packet", () => {
    const emailKey = socketToUser.get(socket.id);
    if (!emailKey) return;
    const now = Date.now();

    const qEntry = waitingQueue.get(emailKey);
    if (qEntry) {
      qEntry.lastPing = now;
    }

    const rId = userToRoom.get(emailKey);
    if (rId && activeRooms.has(rId)) {
      activeRooms.get(rId).lastHeartbeat = now;
    }
  });

  // 1. REGISTER USER
  socket.on("register_user", (payload) => {
    if (!payload?.email) return;
    const email = String(payload.email).trim();
    const emailKey = bindUserSocket(email, socket);

    if (payload.roomId && !activeRooms.has(payload.roomId)) {
      socket.emit("room_ended", {
        roomId: payload.roomId,
        status: "server_restarted",
        endedReason: "disconnected",
        timestamp: Date.now(),
      });
    }

    logEvent("USER_REGISTERED", `Chat user registered: ${email}`, {
      socketId: socket.id,
      screen: payload.screen || "unknown",
      activeRoomId: userToRoom.get(emailKey) || null,
    });
  });

  // 2. GALLERY / CAMERA / PERMISSION DIALOG PROTECTION FLAG
  socket.on("ad_status", (payload) => {
    const email = payload?.email || socketToUser.get(socket.id);
    if (!email) return;
    const key = String(email).toLowerCase();
    if (payload?.isActive) {
      userProtectedUntil.set(key, Date.now() + PROTECTED_DIALOG_GRACE_MS);
    } else {
      userProtectedUntil.delete(key);
    }
  });

  // 3. JOIN MATCHMAKING QUEUE (Strict 10s Timeout, Atomic 1-to-1 Pairing)
  socket.on("queue_join", (payload) => {
    if (!payload?.email) return;
    const myEmail = String(payload.email).trim();
    const myKey = bindUserSocket(myEmail, socket);
    const now = Date.now();
    const incomingAttemptId = payload.attemptId || generateAttemptId();

    if (cancelledAttempts.has(incomingAttemptId)) return;

    const existingRoomId = userToRoom.get(myKey);
    if (existingRoomId && activeRooms.has(existingRoomId)) {
      const existingRoom = activeRooms.get(existingRoomId);
      const roomAgeMs = now - (existingRoom.createdAtMs || 0);

      if (!payload.forceNewSearch && roomAgeMs < 4500) {
        const myRole =
          String(existingRoom.caller).toLowerCase() === myKey
            ? "caller"
            : "callee";
        const partnerProfile =
          myRole === "caller"
            ? existingRoom.calleeProfile
            : existingRoom.callerProfile;

        socket.join(existingRoomId);
        socket.emit("match_established_sync", {
          roomId: existingRoomId,
          attemptId: incomingAttemptId,
          role: myRole,
          partnerEmail: partnerProfile.email,
          partnerGender: partnerProfile.gender,
          partnerProfile,
          roomData: existingRoom,
        });
        return;
      }

      destroyRoomAndNotifyPartner(existingRoomId, myEmail, "left");
    }

    if (matchingLocks.has(myKey)) return;
    matchingLocks.add(myKey);

    removeFromWaitingQueue(myKey, "new_attempt_started", false);
    const mySessionGen = nextUserGeneration(myKey);

    const myCountry = String(payload.location || "INDIA")
      .trim()
      .toUpperCase();
    const myEntry = {
      attemptId: incomingAttemptId,
      sessionGen: mySessionGen,
      socketId: socket.id,
      email: myEmail,
      uid: payload.uid || null,
      name: payload.name || "Stranger",
      timeZone: payload.timeZone || "Asia/Kolkata",
      mode: payload.mode || "chat",
      age: Number(payload.age) || null,
      gender: payload.gender || null,
      location: myCountry,
      maritalStatus: payload.maritalStatus || null,
      filterGender: payload.filterGender || null,
      filterLocation: payload.filterLocation || null,
      filterMaritalStatus: Array.isArray(payload.filterMaritalStatus)
        ? payload.filterMaritalStatus
        : [],
      filterAgeMin: Number(payload.filterAgeMin) || null,
      filterAgeMax: Number(payload.filterAgeMax) || null,
      isVip: Boolean(payload.isVip),
      joinedAt: now,
      lastPing: now,
    };

    logEvent("QUEUE_JOIN", `🔍 Chat user searching: ${myEmail}`, {
      attemptId: myEntry.attemptId,
      waitingInQueue: waitingQueue.size,
    });

    let matchedPartner = null;

    for (const [candKey, candidate] of waitingQueue.entries()) {
      if (candKey === myKey) continue;
      if (matchingLocks.has(candKey)) continue;
      if (userToRoom.has(candKey)) {
        removeFromWaitingQueue(candKey, "already_in_room", false);
        continue;
      }

      const candSocketId = userToSocket.get(candKey) || candidate.socketId;
      const candSocket = io.sockets.sockets.get(candSocketId);

      if (
        !candSocket ||
        !candSocket.connected ||
        cancelledAttempts.has(candidate.attemptId) ||
        userSessionGen.get(candKey) !== candidate.sessionGen ||
        now - candidate.lastPing > QUEUE_HEARTBEAT_EXPIRY_MS ||
        now - candidate.joinedAt >= QUEUE_MAX_WAIT_MS
      ) {
        removeFromWaitingQueue(candKey, "stale_or_expired_candidate", true);
        continue;
      }

      const check = evaluateCompatibility(myEntry, candidate);
      if (check.compatible) {
        matchingLocks.add(candKey);
        removeFromWaitingQueue(candKey, "matched_1_to_1", false);
        matchedPartner = { ...candidate, socketId: candSocketId };
        break;
      }
    }

    // 🎉 1-TO-1 CHAT MATCH ESTABLISHED
    if (matchedPartner) {
      const partnerKey = matchedPartner.email.toLowerCase();
      removeFromWaitingQueue(myKey, "matched_1_to_1", false);
      removeFromWaitingQueue(partnerKey, "matched_1_to_1", false);

      const mySock = io.sockets.sockets.get(socket.id);
      const partnerSock = io.sockets.sockets.get(matchedPartner.socketId);
      if (
        !mySock ||
        !mySock.connected ||
        !partnerSock ||
        !partnerSock.connected ||
        cancelledAttempts.has(incomingAttemptId) ||
        cancelledAttempts.has(matchedPartner.attemptId)
      ) {
        matchingLocks.delete(myKey);
        matchingLocks.delete(partnerKey);
        return;
      }

      const roomId = generateRoomId(20);
      const waitTimeMs = now - matchedPartner.joinedAt;

      const callerProfile = {
        email: matchedPartner.email,
        uid: matchedPartner.uid || null,
        name: matchedPartner.name || "Stranger",
        gender: matchedPartner.gender || null,
        country: matchedPartner.location || "INDIA",
        timeZone: matchedPartner.timeZone || "Asia/Kolkata",
        isVip: Boolean(matchedPartner.isVip),
      };

      const calleeProfile = {
        email: myEntry.email,
        uid: myEntry.uid || null,
        name: myEntry.name || "Stranger",
        gender: myEntry.gender || null,
        country: myEntry.location || "INDIA",
        timeZone: myEntry.timeZone || "Asia/Kolkata",
        isVip: Boolean(myEntry.isVip),
      };

      const roomObject = {
        roomId,
        mode: "chat",
        caller: matchedPartner.email,
        callee: myEntry.email,
        callerUid: matchedPartner.uid || null,
        calleeUid: myEntry.uid || null,
        callerName: callerProfile.name,
        calleeName: calleeProfile.name,
        callerTimeZone: callerProfile.timeZone,
        calleeTimeZone: calleeProfile.timeZone,
        callerIsVip: callerProfile.isVip,
        calleeIsVip: calleeProfile.isVip,
        callerCountry: callerProfile.country,
        calleeCountry: calleeProfile.country,
        callerProfile,
        calleeProfile,
        activeThemeId: "theme_1",
        offer: null,
        answer: null,
        callerCandidates: [],
        calleeCandidates: [],
        inChatCallOffer: null,
        inChatCallAnswer: null,
        inChatCallAction: null,
        callCandidates: [],
        lastGift: null,
        lastHeartbeat: now,
        createdAt: new Date(now).toISOString(),
        createdAtMs: now,
        callerEnteredInCall: false,
        calleeEnteredInCall: false,
        enteredInCall: false,
        status: "active",
      };

      activeRooms.set(roomId, roomObject);
      userToRoom.set(partnerKey, roomId);
      userToRoom.set(myKey, roomId);

      partnerSock.join(roomId);
      mySock.join(roomId);

      logEvent(
        "MATCH_ESTABLISHED",
        `🎉 1-TO-1 CHAT MATCH: ${matchedPartner.email} (Caller) <-> ${myEntry.email} (Callee)`,
        {
          roomId,
          partnerWaitTimeMs: `${waitTimeMs}ms`,
          activeRoomsTotal: activeRooms.size,
        }
      );

      io.to(matchedPartner.socketId).emit("match_established_sync", {
        roomId,
        attemptId: matchedPartner.attemptId,
        role: "caller",
        partnerEmail: calleeProfile.email,
        partnerGender: calleeProfile.gender,
        partnerProfile: calleeProfile,
        roomData: roomObject,
      });

      io.to(myEntry.socketId).emit("match_established_sync", {
        roomId,
        attemptId: myEntry.attemptId,
        role: "callee",
        partnerEmail: callerProfile.email,
        partnerGender: callerProfile.gender,
        partnerProfile: callerProfile,
        roomData: roomObject,
      });

      matchingLocks.delete(myKey);
      matchingLocks.delete(partnerKey);
      return;
    }

    waitingQueue.set(myKey, myEntry);
    matchingLocks.delete(myKey);

    clearQueueTimer(myKey);
    const expiryTimer = setTimeout(() => {
      const currentEntry = waitingQueue.get(myKey);
      if (
        !currentEntry ||
        currentEntry.attemptId !== incomingAttemptId ||
        currentEntry.sessionGen !== mySessionGen ||
        userToRoom.has(myKey)
      ) {
        return;
      }

      removeFromWaitingQueue(myKey, "10s_server_timeout", true);
      const curSockId = userToSocket.get(myKey) || socket.id;
      io.to(curSockId).emit("queue_expired_10s", {
        email: myEmail,
        attemptId: incomingAttemptId,
        timestamp: Date.now(),
      });
    }, QUEUE_MAX_WAIT_MS);

    queueTimers.set(myKey, expiryTimer);
  });

  // 4. QUEUE PING
  socket.on("queue_ping", (payload) => {
    if (!payload?.email) return;
    const emailKey = bindUserSocket(payload.email, socket);
    const now = Date.now();
    const qEntry = waitingQueue.get(emailKey);
    if (qEntry) qEntry.lastPing = now;
    if (payload.roomId && activeRooms.has(payload.roomId)) {
      activeRooms.get(payload.roomId).lastHeartbeat = now;
    }
    socket.emit("queue_pong", {
      attemptId: payload.attemptId || null,
      timestamp: now,
    });
  });

  // 5. LEAVE QUEUE / ABORT
  socket.on("queue_leave", (payload) => {
    const email = payload?.email || socketToUser.get(socket.id);
    if (!email) return;
    const key = String(email).trim().toLowerCase();

    if (payload?.attemptId) {
      cancelledAttempts.add(payload.attemptId);
    }
    nextUserGeneration(key);
    removeFromWaitingQueue(key, payload?.reason || "queue_leave", true);

    if (payload?.explicitAbort === true) {
      const roomId = payload.roomId || userToRoom.get(key);
      if (roomId) {
        destroyRoomAndNotifyPartner(
          roomId,
          email,
          payload.dropStatus || payload.reason || "left"
        );
      }
    }
  });

  // 6. ENTERED RANDOMCHATSCREEN
  socket.on("entered_incall", (payload) => {
    const { roomId, email, role } = payload || {};
    const key = email ? bindUserSocket(email, socket) : null;
    if (key) removeFromWaitingQueue(key, "entered_incall", false);

    if (!roomId || !activeRooms.has(roomId)) {
      socket.emit("room_ended", {
        roomId,
        status: "left",
        endedReason: "left",
      });
      return;
    }

    const room = activeRooms.get(roomId);
    if (
      String(role).toLowerCase() === "caller" ||
      key === String(room.caller).toLowerCase()
    ) {
      room.callerEnteredInCall = true;
    } else {
      room.calleeEnteredInCall = true;
    }
    room.enteredInCall = true;
    room.lastHeartbeat = Date.now();
    socket.join(roomId);
  });

  // ==========================================================================
  // 💬 RANDOM CHAT & IN-CHAT CALL SIGNALING (0ms INSTANT RELAY)
  // ==========================================================================
  socket.on("join_room", (payload, callback) => {
    const { roomId, email } = payload || {};
    if (!roomId) return;

    const room = activeRooms.get(roomId) || null;
    if (!room || room.status !== "active") {
      if (typeof callback === "function") {
        callback({ exists: false, room: null });
      }
      socket.emit("room_ended", { roomId, status: "left" });
      return;
    }

    const emailKey = email ? bindUserSocket(email, socket) : null;
    const callerKey = String(room.caller || "").toLowerCase();
    const calleeKey = String(room.callee || "").toLowerCase();

    if (emailKey && emailKey !== callerKey && emailKey !== calleeKey) {
      if (typeof callback === "function") {
        callback({ exists: false, room: null });
      }
      return;
    }

    socket.join(roomId);
    if (emailKey) {
      removeFromWaitingQueue(emailKey, "joined_active_room", false);
      userToRoom.set(emailKey, roomId);
      if (emailKey === callerKey) room.callerEnteredInCall = true;
      if (emailKey === calleeKey) room.calleeEnteredInCall = true;
      room.enteredInCall = true;
    }
    room.lastHeartbeat = Date.now();

    if (typeof callback === "function") {
      callback({ exists: true, room });
    }

    socket.emit("room_snapshot", room);

    // Replay buffered Chat WebRTC Offer/Answer/Candidates immediately
    if (emailKey === calleeKey && room.offer) {
      socket.emit("webrtc_offer", { roomId, offer: room.offer });
    }
    if (emailKey === calleeKey && room.callerCandidates.length > 0) {
      room.callerCandidates.forEach((candidate) => {
        socket.emit("webrtc_ice_candidate", {
          roomId,
          role: "caller",
          candidate,
        });
      });
    }
    if (emailKey === callerKey && room.answer) {
      socket.emit("webrtc_answer", { roomId, answer: room.answer });
    }
    if (emailKey === callerKey && room.calleeCandidates.length > 0) {
      room.calleeCandidates.forEach((candidate) => {
        socket.emit("webrtc_ice_candidate", {
          roomId,
          role: "callee",
          candidate,
        });
      });
    }
  });

  socket.on("room_update", (payload) => {
    const { roomId, updates } = payload || {};
    if (!roomId || !updates) return;

    const valid = validateSignalingParticipant(
      roomId,
      socket,
      updates?.endedBy
    );
    if (!valid) return;

    const { room } = valid;
    Object.assign(room, updates, { lastHeartbeat: Date.now() });
    activeRooms.set(roomId, room);

    io.to(roomId).emit("room_snapshot", room);

    if (
      updates.status &&
      ["ended", "next", "left", "disconnected", "background"].includes(
        String(updates.status).toLowerCase()
      )
    ) {
      destroyRoomAndNotifyPartner(
        roomId,
        updates.endedBy || valid.senderKey,
        updates.status
      );
    }
  });

  // Direct Socket Relay for Chat Messages, Voice Messages, Images & In-Chat Call Packets
  socket.on("chat_event", (payload) => {
    const { roomId } = payload || {};
    if (!roomId) return;
    const valid = validateSignalingParticipant(roomId, socket);
    if (!valid || !valid.recipientSocketId) return;
    io.to(valid.recipientSocketId).emit("chat_event", payload);
  });

  socket.on("webrtc_offer", (payload) => {
    const { roomId, offer } = payload || {};
    if (!roomId || !offer) return;
    const valid = validateSignalingParticipant(roomId, socket);
    if (!valid) return;
    valid.room.offer = offer;
    if (valid.recipientSocketId) {
      io.to(valid.recipientSocketId).emit("webrtc_offer", { roomId, offer });
    }
  });

  socket.on("webrtc_answer", (payload) => {
    const { roomId, answer } = payload || {};
    if (!roomId || !answer) return;
    const valid = validateSignalingParticipant(roomId, socket);
    if (!valid) return;
    valid.room.answer = answer;
    if (valid.recipientSocketId) {
      io.to(valid.recipientSocketId).emit("webrtc_answer", { roomId, answer });
    }
  });

  socket.on("webrtc_ice_candidate", (payload) => {
    const { roomId, candidate } = payload || {};
    if (!roomId || !candidate) return;
    const valid = validateSignalingParticipant(roomId, socket);
    if (!valid) return;
    if (valid.isCaller) {
      valid.room.callerCandidates.push(candidate);
    } else {
      valid.room.calleeCandidates.push(candidate);
    }
    if (valid.recipientSocketId) {
      io.to(valid.recipientSocketId).emit("webrtc_ice_candidate", {
        roomId,
        role: valid.isCaller ? "caller" : "callee",
        candidate,
      });
    }
  });

  socket.on("room_heartbeat", (payload) => {
    const { roomId, email } = payload || {};
    if (!roomId || !activeRooms.has(roomId)) return;
    validateSignalingParticipant(roomId, socket, email);
  });

  // 7. EXPLICIT EXIT EVENTS
  const handleExplicitExit = (defaultReason) => (payload) => {
    const email =
      (typeof payload === "object" && payload?.email) ||
      socketToUser.get(socket.id);
    const emailKey = email ? String(email).trim().toLowerCase() : null;

    if (emailKey) {
      nextUserGeneration(emailKey);
      removeFromWaitingQueue(emailKey, defaultReason, true);
    }

    const roomId =
      typeof payload === "string"
        ? payload
        : payload?.roomId || (emailKey ? userToRoom.get(emailKey) : null);

    if (roomId && activeRooms.has(roomId)) {
      const reason =
        (typeof payload === "object" && (payload?.reason || payload?.status)) ||
        defaultReason;
      destroyRoomAndNotifyPartner(roomId, email || emailKey, reason);
    }
  };

  socket.on("leave_room", handleExplicitExit("ended"));
  socket.on("chatend", handleExplicitExit("ended"));
  socket.on("videoend", handleExplicitExit("ended"));
  socket.on("chatnext", handleExplicitExit("next"));
  socket.on("videonext", handleExplicitExit("next"));
  socket.on("chatbackground", handleExplicitExit("background"));
  socket.on("videobackground", handleExplicitExit("background"));
  socket.on("checkInternet", handleExplicitExit("disconnected"));

  // 8. SOCKET DISCONNECT (FORCE-CLOSE & NETWORK LOSS DETECTION)
  socket.on("disconnect", (disconnectReason) => {
    const emailKey = socketToUser.get(socket.id);
    if (!emailKey) return;

    socketToUser.delete(socket.id);
    if (userToSocket.get(emailKey) !== socket.id) return;
    userToSocket.delete(emailKey);

    removeFromWaitingQueue(
      emailKey,
      `socket_disconnect:${disconnectReason}`,
      true
    );

    const activeRoomId = userToRoom.get(emailKey);
    if (!activeRoomId || !activeRooms.has(activeRoomId)) return;

    const room = activeRooms.get(activeRoomId);
    const isProtected = (userProtectedUntil.get(emailKey) || 0) > Date.now();

    const graceMs = isProtected
      ? PROTECTED_DIALOG_GRACE_MS
      : room.enteredInCall
      ? ACTIVE_CHAT_DISCONNECT_TIMEOUT_MS
      : 600;

    clearDisconnectTimer(emailKey);
    const timer = setTimeout(() => {
      disconnectTimers.delete(emailKey);
      const currentSocketId = userToSocket.get(emailKey);
      const currentSocket = currentSocketId
        ? io.sockets.sockets.get(currentSocketId)
        : null;

      if (!currentSocket || !currentSocket.connected) {
        destroyRoomAndNotifyPartner(activeRoomId, emailKey, "disconnected");
      }
    }, graceMs);

    disconnectTimers.set(emailKey, timer);
  });
});

// ============================================================================
// ⏱️ 1-SECOND QUEUE SWEEPER ONLY (ACTIVE ROOMS HAVE UNLIMITED DURATION!)
// ============================================================================
setInterval(() => {
  const now = Date.now();
  for (const [emailKey, entry] of waitingQueue.entries()) {
    const sock = io.sockets.sockets.get(entry.socketId);
    const isSocketDead = !sock || !sock.connected;
    const isPingExpired = now - entry.lastPing > QUEUE_HEARTBEAT_EXPIRY_MS;
    const isTimeExpired = now - entry.joinedAt >= QUEUE_MAX_WAIT_MS;

    if (isSocketDead || isPingExpired || isTimeExpired) {
      const reason = isSocketDead
        ? "socket_dead"
        : isPingExpired
        ? "heartbeat_expired"
        : "10s_timeout";
      const expiredAttemptId = entry.attemptId;
      removeFromWaitingQueue(emailKey, reason, true);

      if (isTimeExpired && sock && sock.connected) {
        sock.emit("queue_expired_10s", {
          email: entry.email,
          attemptId: expiredAttemptId,
          timestamp: now,
        });
      }
    }
  }
}, 1000);

app.get("/", (req, res) => {
  res.status(200).json({
    server: "RandomChatConnectingServer",
    serverInstanceId: SERVER_INSTANCE_ID,
    status: "online",
    waitingInQueue: waitingQueue.size,
    activeRoomsCount: activeRooms.size,
    uptimeSeconds: Math.floor(process.uptime()),
  });
});

app.get("/health", (req, res) => {
  res.status(200).send("OK");
});

server.listen(PORT, "0.0.0.0", () => {
  logEvent(
    "SERVER_START",
    `🚀 RandomChatConnectingServer (${SERVER_INSTANCE_ID}) running on 0.0.0.0:${PORT}`
  );
});