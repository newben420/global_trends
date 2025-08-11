import https from 'https';
import { Site } from './site';
if (Site.FORCE_FAMILY_4) {
    https.globalAgent.options.family = 4;
}
import express, { Request, Response, NextFunction } from 'express';
import { startEngine, stopEngine } from './engine/terminal';
import http from 'http';
import bodyParser from 'body-parser';
import { Log } from './lib/log';
import { getDateTime } from './lib/date_time';
import { Server } from 'socket.io';
import { GRes } from './lib/res';
import { engine } from 'express-handlebars';
import path from 'path';
import cors from "cors";
import { SocketEngine } from './engine/socket';
const CACHE_PROD = 1000 * 60 * 60 * 24 * 30;

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
    cors: {
        origin: Site.PRODUCTION ? "/" : "*",
    }
});

app.engine('handlebars', engine({
    layoutsDir: path.join(__dirname, 'views/layouts'),
    partialsDir: path.join(__dirname, 'views/partials'),
    defaultLayout: 'main',
}));
app.set('view engine', 'handlebars');
app.set('views', path.join(__dirname, 'views'));

app.use(cors({
    origin: '*',
    optionsSuccessStatus: 204,
}));

app.disable("x-powered-by");
app.disable('etag');

app.use(bodyParser.json({ limit: "35mb" }));

app.use(
    bodyParser.urlencoded({
        extended: true,
        limit: "35mb",
        parameterLimit: 50000,
    })
);

app.use((req: Request, res: Response, next: NextFunction) => {
    if (req.method === "POST" && (!req.body)) {
        res.status(400).send(GRes.err("NO_BODY"));
    }
    else {
        next();
    }
});


app.use(express.static(path.join(Site.ROOT, "public"), {
    maxAge: Site.PRODUCTION ? CACHE_PROD : 0,
}));

app.use((req: Request, res: Response) => {
    res.sendFile(path.join(Site.ROOT, "public", "index.html"));
});

process.on('exit', async (code) => {
    // NOTHING FOR NOW
});

process.on('SIGINT', async () => {
    Log.dev('Process > Received SIGINT.');
    const l = await stopEngine();
    process.exit(0);
});

process.on('SIGTERM', async () => {
    Log.dev('Process > Received SIGTERM.');
    const l = await stopEngine();
    process.exit(0);
});

process.on('uncaughtException', async (err) => {
    Log.dev('Process > Unhandled exception caught.');
    console.log(err);
    if (Site.EXIT_ON_UNCAUGHT_EXCEPTION) {
        const l = await stopEngine();
        process.exit(0);
    }
});

process.on('unhandledRejection', async (err, promise) => {
    Log.dev('Process > Unhandled rejection caught.');
    console.log("Promise:", promise);
    console.log("Reason:", err);
    if (Site.EXIT_ON_UNHANDLED_REJECTION) {
        const l = await stopEngine();
        process.exit(0);
    }
});

Log.flow([Site.TITLE, 'Attempting to start engines.'], 0);
startEngine().then(r => {
    if (r) {
        server.listen(Site.PORT, async () => {
            SocketEngine.initialize(io);
            Log.flow([Site.TITLE, 'Sucessfully started all engines.'], 0);
            Log.flow([Site.TITLE, `Running at http://127.0.0.1:${Site.PORT}`], 0);
        });
    }
    else {
        Log.flow([Site.TITLE, 'Failed to start all engines.'], 0);
        process.exit(0);
    }
});
