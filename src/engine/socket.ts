import { extractJsonFromText } from './../lib/extract_json';
import { Server, DefaultEventsMap, Socket } from 'socket.io';
import { Log } from '../lib/log';
import { Site } from '../site';

export class SocketEngine {
    private static io: Server<DefaultEventsMap, DefaultEventsMap, DefaultEventsMap, any>;

    static initialize = (io: Server<DefaultEventsMap, DefaultEventsMap, DefaultEventsMap, any>) => {
        SocketEngine.io = io;
        SocketEngine.runOnce();
    }

    private static runOnce = () => {
        if (SocketEngine.io) {
            SocketEngine.io.on("connection", (socket) => {
                Log.dev(`Socket client connected with id ${socket.id}.`);

                socket.on("disconnect", () => {
                    Log.dev(`Socket client with id ${socket.id}.`);
                });
            });
        }
    }

    // static broadcastRouteData = (socket: Socket) => {
    //     socket.emit("ROUTE_UPDATE", DataEngine.path.getValue());
    // }
}