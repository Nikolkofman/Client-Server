package bgu.spl.net.impl.stomp;

import bgu.spl.net.api.StompMessagingProtocol;

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
    
        if ("CONNECT".equals(command)) {
            return handleLogin(parsedMessage, message); 
        } 
        if ("SUBSCRIBE".equals(command)) {
            handleSubscribe(parsedMessage); 
            return null;
        } 
        if ("UNSUBSCRIBE".equals(command)) {
            handleUnsubscribe(parsedMessage);
            return null;
        } 
        if ("SEND".equals(command)) {
            handleSend(parsedMessage, message);
            return null;
        } 
        if ("DISCONNECT".equals(command)) {
            handleLogout(parsedMessage, message);
            return null;
        } 
    
        return "ERROR\nmessage: Unknown command\nThe message:\n----\n" + message;
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

    private void handleSubscribe(ArrayList<String> parsedMessage) {
        String channel = parsedMessage.get(1).substring(12);
        String subscriptionId = parsedMessage.get(2).substring(3);
        String receiptId = parsedMessage.size() > 3 ? parsedMessage.get(3).substring(8) : null;
    
        connections.subscribeClient(this.connectionId, channel, subscriptionId);
    
        sendReceipt(receiptId);
    }
    

    private void handleUnsubscribe(ArrayList<String> parsedMessage) {
        String subscriptionId = parsedMessage.get(1).substring(3);
        String channel = connections.findChannelBySubscription(this.connectionId, subscriptionId);
        String receiptId = parsedMessage.size() > 2 ? parsedMessage.get(2).substring(8) : null;
    
        connections.unsubscribeClient(this.connectionId, channel);
    
        sendReceipt(receiptId);
    }
    

    private void handleSend(ArrayList<String> parsedMessage, String message) {
        String destination = parsedMessage.get(1).substring(12);
        String receiptId = parsedMessage.size() > 3 ? parsedMessage.get(3).substring(8) : null;
    
        if (destination.isEmpty()) {
            connections.send(connectionId, "ERROR\nmessage: Destination is empty\nThe message:\n----\n" + message);
            return;
        }
    
        String subscriptionId = connections.getClientSubscriptionId(this.connectionId, destination.substring(1));
    
        if (subscriptionId.equals("-1")) {
            connections.send(connectionId, "ERROR\nmessage: No subscription found for destination\nThe message:\n----\n" + message);
            return;
        }
    
        try {
            String frameBody = parsedMessage.get(2);
            int messageId = messageIdCounter.getAndIncrement();
    
            String broadcastMessage = "MESSAGE\nsubscription:" + subscriptionId + 
                                      "\nmessage-id:" + messageId + 
                                      "\ndestination:" + destination + "\n" + frameBody;
    
            connections.send(destination.substring(1), broadcastMessage);
    
            sendReceipt(receiptId); // Ensure receipt is sent if requested
    
        } catch (Exception e) {
            connections.send(connectionId, "ERROR\nmessage: Internal server error in SEND command\nThe message:\n----\n" + message);
        }
    }
    
    private void handleLogout(ArrayList<String> parsedMessage, String message) {
        String receiptId = parsedMessage.size() > 1 ? parsedMessage.get(1).substring(8) : null;
        sendReceipt(receiptId);
        shouldTerminate = true;
        connections.disconnect(connectionId);
        User.getInstance().setOnlineStatus(this.username, false);
    }
    
    

    private ArrayList<String> parseMessage(String input) {
        ArrayList<String> parsedList = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean isBody = false;
    
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
    
            if (c == '\n') {
                if (current.length() > 0) {
                    parsedList.add(current.toString());
                    current.setLength(0);
                } else {
                    isBody = true; 
                }
            } else if (!isBody) {
                current.append(c);
            } else {
                parsedList.add(input.substring(i).trim());
                break;
            }
        }
    
        if (current.length() > 0) {
            parsedList.add(current.toString()); 
        }
    
        return parsedList;
    }
    
    @Override
    public boolean shouldTerminate() {
        return shouldTerminate;
    }


    private void sendReceipt(String receiptId) {
        if (receiptId != null && !receiptId.isEmpty()) {
            connections.send(connectionId, "RECEIPT\nreceipt-id:" + receiptId + "\n");
        }
    }
    
    
}
