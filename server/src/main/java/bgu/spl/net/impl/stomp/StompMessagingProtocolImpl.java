package bgu.spl.net.impl.stomp;

import bgu.spl.net.api.StompMessagingProtocol;
import bgu.spl.net.srv.Connections;
import bgu.spl.net.srv.ConnectionHandler;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import bgu.spl.net.srv.*;

public class StompMessagingProtocolImpl implements StompMessagingProtocol<String> {
    private boolean shouldTerminate = false;
    private Connections<String> connections;
    private int connectionId;
    private String username = "";
    private ConnectionHandler<String> handler;
    private AtomicInteger messageIdCounter = new AtomicInteger(0);

    @Override
    public void start(int connectionId, Connections<String> connections, ConnectionHandler<String> handler) {
        this.connectionId = connectionId;
        this.connections = connections;
        this.handler = handler;
        connections.addClient(connectionId, handler);
    }

    @Override
    public String process(String message) {
        ArrayList<String> parsedMessage = parseMessage(message);
        String command = parsedMessage.get(0);

        switch (command) {
            case "CONNECT":
                return handleLogin(parsedMessage, message);
            case "SUBSCRIBE":
                return handleSubscribe(parsedMessage);
            case "UNSUBSCRIBE":
                return handleUnsubscribe(parsedMessage);
            case "SEND":
                return handleSend(parsedMessage, message);
            case "DISCONNECT":
                return handleLogout(parsedMessage, message);
            default:
                return "ERROR\nmessage: Unknown command\nThe message:\n----\n" + message;
        }
    }

    private String handleLogin(ArrayList<String> parsedMessage, String message) {
        this.username = parsedMessage.get(3);
        String password = parsedMessage.get(4);

        if (User.getInstance().registerOrLogin(username, password)) {
            if (!User.getInstance().isOnline(username)) {
                User.getInstance().setOnlineStatus(username, true);
                return "CONNECTED\nversion:1.2";
            }
            return "ERROR\nmessage: User already logged in\nThe message:\n----\n" + message;
        }
        return "ERROR\nmessage: Incorrect password\nThe message:\n----\n" + message;
    }

    private String handleSubscribe(ArrayList<String> parsedMessage) {
        String channel = parsedMessage.get(1).substring(12);
        String subscriptionId = parsedMessage.get(2).substring(3);
        connections.subscribeClient(this.connectionId, channel, subscriptionId);
        String receiptId = parsedMessage.get(3).substring(8);
        return "RECEIPT\nreceipt-id:" + receiptId;
    }

    private String handleUnsubscribe(ArrayList<String> parsedMessage) {
        String subscriptionId = parsedMessage.get(1).substring(3);
        String channel = connections.findChannelBySubscription(this.connectionId, subscriptionId);
        connections.unsubscribeClient(this.connectionId, channel);
        String receiptId = parsedMessage.get(2).substring(8);
        return "RECEIPT\nreceipt-id:" + receiptId;
    }

    private String handleSend(ArrayList<String> parsedMessage, String message) {
        String destination = parsedMessage.get(1).substring(12);

        if (destination.isEmpty()) {
            System.out.println("ERROR: Destination is empty in SEND command.");
            return "ERROR\nmessage: Destination is empty\nThe message:\n----\n" + message;
        }

        String subscriptionId = connections.getClientSubscriptionId(this.connectionId, destination.substring(1));

        if (subscriptionId.equals("-1")) {
            System.out.println("ERROR: Subscription ID is invalid for connection " + connectionId + " in channel " + destination);
            return "ERROR\nmessage: No subscription found for destination\nThe message:\n----\n" + message;
        }

        try {
            String frameBody = parsedMessage.get(2);
            int messageId = messageIdCounter.getAndIncrement();

            String broadcastMessage = "MESSAGE\nsubscription:" + subscriptionId + 
                                      "\nmessage-id:" + messageId + 
                                      "\ndestination:" + destination + "\n" + frameBody;

            connections.send(destination.substring(1), broadcastMessage);
            return null;
        } catch (Exception e) {
            System.out.println("EXCEPTION in handleSend(): " + e.getMessage());
            e.printStackTrace();
            return "ERROR\nmessage: Internal server error in SEND command\nThe message:\n----\n" + message;
        }
    }

    private String handleLogout(ArrayList<String> parsedMessage, String message) {
        String receiptId = parsedMessage.get(1).substring(8);
        shouldTerminate = message.equals("logout");
        connections.disconnect(connectionId);
        User.getInstance().setOnlineStatus(this.username, false);
        return "RECEIPT\nreceipt-id:" + receiptId;
    }

    private ArrayList<String> parseMessage(String input) {
        int i = 0;
        ArrayList<String> parsedList = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean blankLine = false;

        while (i < input.length()) {
            while (i < input.length() && input.charAt(i) != '\n' && !blankLine) {
                current.append(input.charAt(i));
                i++;
            }
            if (current.length() > 0) {
                parsedList.add(current.toString());
                current.setLength(0);
                i++;
            } else {
                blankLine = true;
                parsedList.add(input.substring(i));
                break;
            }
        }
        return parsedList;
    }

    @Override
    public boolean shouldTerminate() {
        return shouldTerminate;
    }
}
