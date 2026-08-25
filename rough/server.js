const express = require("express");
const http = require("http");
const fs = require("fs");

const app = express();

app.use(express.static("."));

// -------------------------
// Express HTTP routes
// -------------------------

app.get("/", (req, res) => {
    res.send("Express is working!");
});

app.get("/hello", (req, res) => {
    res.json({
        message: "Hello from Express"
    });
});

app.get("/stream", (req, res) => {
    const stream = fs.createReadStream("./song.mp3");

    res.setHeader("Content-Type", "audio/mpeg");

    stream.pipe(res);
});

// -------------------------
// Create HTTP server
// -------------------------

const server = http.createServer(app);

// -------------------------
// WebSocket server
// -------------------------

const { WebSocketServer, WebSocket } = require("ws");

const wss = new WebSocketServer({ server });

wss.on("connection", (ws) => {
    console.log("Client connected");

    ws.on("message", (message) => {
        const text = message.toString();

        console.log("Received:", text);

        wss.clients.forEach((client) => {
            if (client.readyState === WebSocket.OPEN) {
                client.send(text);
            }
        });
    });

    ws.on("close", () => {
        console.log("Client disconnected");
    });
});

// -------------------------
// Start everything
// -------------------------

server.listen(3000, () => {
    console.log("Server running on http://localhost:3000");
});
