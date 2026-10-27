
const express = require("express");
const http = require("http");
const fs = require("fs");
const path = require("path");
const { execFile } = require("child_process");
const { WebSocketServer, WebSocket } = require("ws");

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const PORT = 3000;
const SONG_DIR = path.join(__dirname, "song_db");
const YT_SCRIPT = path.join(__dirname, "yt.sh");
const DEFAULT_SONG = "song.mp3";

// Do not expose song_db through static hosting.
app.use((req, res, next) => {
    if (req.path.startsWith("/song_db/")) {
        return res.sendStatus(404);
    }
    next();
});

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

function getSongPath(filename) {
    if (
        typeof filename !== "string" ||
        path.basename(filename) !== filename ||
        !filename.toLowerCase().endsWith(".mp3")
    ) {
        return null;
    }

    try {
        const fullPath = path.join(SONG_DIR, filename);
        return fs.statSync(fullPath).isFile() ? fullPath : null;
    } catch {
        return null;
    }
}

// -------------------------
// Audio streaming with seeking
// -------------------------

app.get("/stream", (req, res) => {
    const filename = req.query.file || DEFAULT_SONG;
    const songPath = getSongPath(filename);

    if (!songPath) {
        return res.status(404).send("Song not found");
    }

    let fileSize;

    try {
        fileSize = fs.statSync(songPath).size;
    } catch {
        return res.status(404).send("Song not found");
    }

    res.setHeader("Content-Type", "audio/mpeg");
    res.setHeader("Accept-Ranges", "bytes");
    res.setHeader("Cache-Control", "no-cache");

    const range = req.headers.range;

    if (range) {
        const match = /^bytes=(\d*)-(\d*)$/.exec(range);

        if (!match || (!match[1] && !match[2])) {
            res.setHeader("Content-Range", `bytes */${fileSize}`);
            return res.status(416).end();
        }

        let start;
        let end;

        if (!match[1]) {
            const suffixLength = Number(match[2]);

            if (suffixLength <= 0) {
                res.setHeader("Content-Range", `bytes */${fileSize}`);
                return res.status(416).end();
            }

            start = Math.max(0, fileSize - suffixLength);
            end = fileSize - 1;
        } else {
            start = Number(match[1]);
            end = match[2] ? Number(match[2]) : fileSize - 1;
        }

        if (
            !Number.isSafeInteger(start) ||
            !Number.isSafeInteger(end) ||
            start < 0 ||
            end < start ||
            start >= fileSize
        ) {
            res.setHeader("Content-Range", `bytes */${fileSize}`);
            return res.status(416).end();
        }

        end = Math.min(end, fileSize - 1);

        res.status(206);
        res.setHeader(
            "Content-Range",
            `bytes ${start}-${end}/${fileSize}`
        );
        res.setHeader("Content-Length", end - start + 1);

        const stream = fs.createReadStream(songPath, { start, end });
        res.on("close", () => stream.destroy());
        stream.on("error", (error) => {
            console.error("Audio stream error:", error.message);
            if (!res.destroyed) res.destroy(error);
        });

        return stream.pipe(res);
    }

    res.setHeader("Content-Length", fileSize);

    const stream = fs.createReadStream(songPath);
    res.on("close", () => stream.destroy());
    stream.on("error", (error) => {
        console.error("Audio stream error:", error.message);
        if (!res.destroyed) res.destroy(error);
    });

    stream.pipe(res);
});

// -------------------------
// Song listing
// -------------------------

function sendSongList(ws) {
    fs.readdir(SONG_DIR, { withFileTypes: true }, (error, entries) => {
        if (ws.readyState !== WebSocket.OPEN) return;

        if (error) {
            ws.send(JSON.stringify({
                type: "songList",
                songs: [],
                error: "Cannot read the song_db directory."
            }));
            return;
        }

        const songs = entries
            .filter(entry =>
                entry.isFile() &&
                entry.name.toLowerCase().endsWith(".mp3")
            )
            .map(entry => entry.name)
            .sort((a, b) => a.localeCompare(b));

        ws.send(JSON.stringify({ type: "songList", songs }));
    });
}

// -------------------------
// Shared playback state
// -------------------------

let songState = {
    song: DEFAULT_SONG,
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
        song: songState.song,
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
}

function sendToClient(ws, message) {
    if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(message));
    }
}

// -------------------------
// YouTube download
// -------------------------

const activeDownloads = new Set();

function handleAddSong(ws, data) {
    const name = typeof data.name === "string"
        ? data.name.trim()
        : "";

    if (!/^[a-zA-Z0-9][a-zA-Z0-9 _-]{0,79}$/.test(name)) {
        sendToClient(ws, {
            type: "downloadResult",
            success: false,
            message: "Invalid name. Use letters, numbers, spaces, _ or -."
        });
        return;
    }

    let parsedUrl;

    try {
        parsedUrl = new URL(data.url);
    } catch {
        parsedUrl = null;
    }

    const allowedHosts = new Set([
        "youtube.com",
        "www.youtube.com",
        "m.youtube.com",
        "music.youtube.com",
        "youtu.be",
        "www.youtu.be"
    ]);

    if (
        !parsedUrl ||
        parsedUrl.protocol !== "https:" ||
        !allowedHosts.has(parsedUrl.hostname) ||
        !(
            parsedUrl.hostname.endsWith("youtu.be") ||
            parsedUrl.pathname === "/watch" ||
            parsedUrl.pathname.startsWith("/shorts/") ||
            parsedUrl.pathname.startsWith("/live/") ||
            parsedUrl.pathname.startsWith("/embed/")
        )
    ) {
        sendToClient(ws, {
            type: "downloadResult",
            success: false,
            message: "Enter a valid HTTPS YouTube URL."
        });
        return;
    }

    const filename = `${name}.mp3`;
    const outputPath = path.join(SONG_DIR, filename);

    if (activeDownloads.has(filename) || fs.existsSync(outputPath)) {
        sendToClient(ws, {
            type: "downloadResult",
            success: false,
            message: "That filename already exists or is downloading."
        });
        return;
    }

    activeDownloads.add(filename);

    sendToClient(ws, {
        type: "downloadResult",
        success: null,
        message: `Downloading ${filename}...`
    });

    // Keep yt.sh unchanged. Arguments are passed separately.
    execFile(
        YT_SCRIPT,
        [parsedUrl.href, filename],
        {
            cwd: __dirname,
            timeout: 10 * 60 * 1000,
            maxBuffer: 2 * 1024 * 1024
        },
        (error, stdout, stderr) => {
            activeDownloads.delete(filename);

            if (error) {
                console.error("yt.sh failed:", stderr || error.message);
                sendToClient(ws, {
                    type: "downloadResult",
                    success: false,
                    message: "Download failed. Check the server terminal."
                });
                return;
            }

            console.log(stdout);

            if (!getSongPath(filename)) {
                sendToClient(ws, {
                    type: "downloadResult",
                    success: false,
                    message: "The expected MP3 file was not found."
                });
                return;
            }

            sendToClient(ws, {
                type: "downloadResult",
                success: true,
                message: `Downloaded ${filename} successfully!`
            });

            sendSongList(ws);
        }
    );
}

// -------------------------
// WebSocket
// -------------------------

wss.on("connection", (ws) => {
    console.log("Client connected. Total:", wss.clients.size);
    ws.send(JSON.stringify(getStateMessage()));

    ws.on("message", (rawMessage) => {
        let data;

        try {
            data = JSON.parse(rawMessage.toString());
        } catch {
            return;
        }

        if (!data || typeof data.type !== "string") return;

        if (data.type === "listSongs") {
            sendSongList(ws);
            return;
        }

        if (data.type === "addSong") {
            handleAddSong(ws, data);
            return;
        }

        if (data.type === "selectSong") {
            const songPath = getSongPath(data.song);

            if (!songPath) {
                sendToClient(ws, {
                    type: "error",
                    message: "Song not found."
                });
                return;
            }

            songState.song = path.basename(songPath);
            songState.position = 0;
            songState.status = "playing";
            songState.updatedAt = Date.now();

            broadcastState();
            return;
        }

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

            if (!Number.isFinite(position) || position < 0) return;

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

server.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
});

