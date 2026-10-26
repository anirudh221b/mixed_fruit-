
const express = require("express");
const http = require("http");
const fs = require("fs");
const path = require("path");
const { WebSocketServer, WebSocket } = require("ws");

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const PORT = 3000;
const SONG_PATH = path.join(__dirname,"song_db","veera.mp3");

app.use(express.static(__dirname));

// -------------------------
// HTTP routes
// -------------------------

app.get("/", (req, res) => {
    res.sendFile(path.join(__dirname, "index.html"));
});

app.get("/hello", (req, res) => {
    res.json({ message: "Hello from Express" });
});

// Stream audio over HTTP
app.get("/stream", (req, res) => {
    if (!fs.existsSync(SONG_PATH)) {
        return res.status(404).send("song.mp3 not found");
    }

    res.setHeader("Content-Type", "audio/mpeg");
    res.setHeader("Cache-Control", "no-cache");

    const stream = fs.createReadStream(SONG_PATH);

    stream.on("error", (error) => {
        console.error("Audio stream error:", error.message);

        if (!res.headersSent) {
            res.status(500).end("Unable to stream audio");
        } else {
            res.destroy(error);
        }
    });

    res.on("close", () => {
        if (!stream.destroyed) {
            stream.destroy();
        }
    });

    stream.pipe(res);
});

// -------------------------
// Shared playback state
// -------------------------

let songState = {
    status: "paused",
    position: 0,
    updatedAt: Date.now()
};

function getCurrentPosition() {
    if (songState.status === "paused") {
        return songState.position;
    }

    return songState.position +
        (Date.now() - songState.updatedAt) / 1000;
}

function getStateMessage() {
    return {
        type: "state",
        status: songState.status,
        position: getCurrentPosition(),
        serverTime: Date.now()
    };
}

function broadcastState() {
    const message = JSON.stringify(getStateMessage());

    for (const client of wss.clients) {
        if (client.readyState === WebSocket.OPEN) {
            client.send(message);
        }
    }

    console.log("Broadcast:", message);
}

// -------------------------
// WebSocket server
// -------------------------

wss.on("connection", (ws) => {
    console.log("Client connected. Total:", wss.clients.size);

    // Send current state only to the newcomer.
    // Do not broadcast a connection event to existing clients.
    ws.send(JSON.stringify(getStateMessage()));

    ws.on("message", (rawMessage) => {
        let data;

        try {
            data = JSON.parse(rawMessage.toString());
        } catch {
            console.log("Ignoring invalid JSON");
            return;
        }

        console.log("Received:", data);

        if (data.type === "play") {
            if (songState.status !== "playing") {
                songState.position = getCurrentPosition();
                songState.status = "playing";
                songState.updatedAt = Date.now();
            }
        } else if (data.type === "pause") {
            if (songState.status !== "paused") {
                songState.position = getCurrentPosition();
                songState.status = "paused";
                songState.updatedAt = Date.now();
            }
        } else if (data.type === "seek") {
            const position = Number(data.position);

            if (!Number.isFinite(position) || position < 0) {
                return;
            }

            songState.position = position;
            songState.updatedAt = Date.now();
        } else {
            return;
        }

        broadcastState();
    });

    ws.on("close", () => {
        console.log("Client disconnected. Total:", wss.clients.size - 1);
    });

    ws.on("error", (error) => {
        console.error("WebSocket error:", error.message);
    });
});

// -------------------------
// Start server
// -------------------------

server.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
});

