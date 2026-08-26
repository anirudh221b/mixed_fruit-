const express = require("express");
const http = require("http");
const { WebSocketServer, WebSocket } = require("ws");
const fs = require("fs");

const app = express();

app.use(express.static("."));

// ==========================================
// EXPRESS ROUTES
// ==========================================

app.get("/hello", (req, res) => {
    res.json({
        message: "Hello from Express"
    });
});


// ==========================================
// AUDIO STREAM
// ==========================================

app.get("/stream", (req, res) => {

    const stream = fs.createReadStream("./song.mp3");

    res.setHeader("Content-Type", "audio/mpeg");

    stream.pipe(res);
});


// ==========================================
// HTTP SERVER
// ==========================================

const server = http.createServer(app);


// ==========================================
// WEBSOCKET SERVER
// ==========================================

const wss = new WebSocketServer({ server });


// ==========================================
// SHARED SONG STATE
// ==========================================

let songState = {
    status: "paused",
    position: 0,
    updatedAt: Date.now()
};


// ==========================================
// GET CURRENT SONG POSITION
// ==========================================

function getCurrentPosition() {

    if (songState.status === "paused") {
        return songState.position;
    }

    const elapsed =
        (Date.now() - songState.updatedAt) / 1000;

    return songState.position + elapsed;
}


// ==========================================
// BROADCAST STATE TO ALL CLIENTS
// ==========================================

function broadcastState() {

    const state = {
        type: "state",
        status: songState.status,
        position: getCurrentPosition(),
        serverTime: Date.now()
    };

    const message = JSON.stringify(state);

    wss.clients.forEach((client) => {

        if (client.readyState === WebSocket.OPEN) {
            client.send(message);
        }

    });
}


// ==========================================
// WEBSOCKET CONNECTION
// ==========================================

wss.on("connection", (ws) => {

    console.log("Client connected");


    // --------------------------------------
    // Send current state to new client
    // --------------------------------------

    ws.send(JSON.stringify({
        type: "state",
        status: songState.status,
        position: getCurrentPosition(),
        serverTime: Date.now()
    }));


    // --------------------------------------
    // Receive messages
    // --------------------------------------

    ws.on("message", (message) => {

        let data;

        try {
            data = JSON.parse(message.toString());
        }
        catch (error) {
            console.log("Invalid JSON");
            return;
        }

        console.log("Received:", data);


        // ==================================
        // PLAY
        // ==================================

        if (data.type === "play") {

            // Already playing
            if (songState.status === "playing") {
                return;
            }

            songState.position =
                getCurrentPosition();

            songState.status = "playing";

            songState.updatedAt =
                Date.now();

            broadcastState();
        }


        // ==================================
        // PAUSE
        // ==================================

        else if (data.type === "pause") {

            // Already paused
            if (songState.status === "paused") {
                return;
            }

            songState.position =
                getCurrentPosition();

            songState.status = "paused";

            songState.updatedAt =
                Date.now();

            broadcastState();
        }


        // ==================================
        // SEEK
        // ==================================

        else if (data.type === "seek") {

            const position =
                Number(data.position);

            if (
                Number.isNaN(position) ||
                position < 0
            ) {
                return;
            }

            songState.position = position;

            songState.updatedAt =
                Date.now();

            broadcastState();
        }

    });


    // ======================================
    // CLIENT DISCONNECTED
    // ======================================

    ws.on("close", () => {

        console.log("Client disconnected");

    });

});


// ==========================================
// START SERVER
// ==========================================

server.listen(3000, () => {

    console.log(
        "Server running on http://localhost:3000"
    );

});
