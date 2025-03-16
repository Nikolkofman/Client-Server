package bgu.spl.net.impl.stomp;

import bgu.spl.net.srv.Server;

public class StompServer {

    public static void main(String[] args) {
        int port = 7777;  // Default port
        String mode = (args.length > 1) ? args[1].toLowerCase() : "tpc";

        Server<String> server;
        switch (mode) {
            case "reactor":
                server = Server.reactor(
                    Runtime.getRuntime().availableProcessors(), port, 
                    () -> new StompMessagingProtocolImpl(), 
                    () -> new StompEncoderDecoder()
                );
                break;
            default:
                server = Server.threadPerClient(
                    port, 
                    () -> new StompMessagingProtocolImpl(), 
                    () -> new StompEncoderDecoder()
                );
                break;
        }

        server.serve();
    }
}
