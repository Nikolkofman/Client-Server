package bgu.spl.net.srv;

import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionsImpl<T> implements Connections<T> {
    private final AtomicInteger messageCounter = new AtomicInteger();
    private final ConcurrentHashMap<Integer, ConnectionHandler<T>> activeConnections = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LinkedList<Integer>> channelSubscriptions = new ConcurrentHashMap<>();
    private final UserDataBase userDataBase;

    public ConnectionsImpl() {
        this.userDataBase = UserDataBase.getInstance();
    }

    public ConnectionsImpl(UserDataBase userDataBase) {
        this.userDataBase = userDataBase;
    }

    @Override
    public boolean send(int connectionId, T msg) {
        ConnectionHandler<T> handler = activeConnections.get(connectionId);
        if (handler != null) {
            handler.send(msg);
            return true;
        }
        return false;
    }

    @Override
    public void send(String channel, T msg) {
        synchronized (channelSubscriptions) {
            LinkedList<Integer> subscribers = channelSubscriptions.get(channel);
            if (subscribers != null) {
                System.out.println("Forwarding message to channel: " + channel);
                for (int connectionId : subscribers) {
                    System.out.println("➡️ Delivering to connection ID: " + connectionId);
                    send(connectionId, msg);
                }
            } else {
                System.out.println("No subscribers found for channel: " + channel);
            }
        }
    }
    
    

    @Override
    public void disconnect(int connectionId) {
        handleUserLogout(connectionId);
        activeConnections.remove(connectionId);
    }

    private void removeUserFromAllChannels(int connectionId) {
        LinkedList<String> userChannels = userDataBase.getUserSubscriptions(connectionId);
        if (userChannels != null) {
            synchronized (channelSubscriptions) {
                for (String channel : userChannels) {
                    LinkedList<Integer> subscribers = channelSubscriptions.get(channel);
                    if (subscribers != null) {
                        subscribers.remove(Integer.valueOf(connectionId));
                        if (subscribers.isEmpty()) {
                            channelSubscriptions.remove(channel);
                        }
                    }
                }
            }
        }
        userDataBase.resetUserSubscriptions(connectionId);
    }

    @Override
    public boolean isValidLogin(String username, String password) {
        return userDataBase.verifyCredentials(username, password);
    }

    @Override
    public boolean isUserLoggedIn(String username, String password) {
        return userDataBase.isUserOnline(username);
    }

    @Override
    public boolean checkUserExists(String username) {
        return userDataBase.userExists(username);
    }

    @Override
    public void handleUserLogin(int connectionId, String username, String password) {
        if (!userDataBase.userExists(username)) {
            userDataBase.registerUser(username, password);
        }
        userDataBase.assignConnection(connectionId, username);
    }

    @Override
    public void handleUserLogout(int connectionId) {
        removeUserFromAllChannels(connectionId);
        activeConnections.remove(connectionId);
        userDataBase.releaseConnection(connectionId);
    }

    @Override
    public void addSubscription(String channel, int connectionId, int subscriptionId) {
        if (subscriptionId == 0) {
            subscriptionId = generateSubscriptionId(connectionId);
        }
    
        System.out.println("✅ Storing subscription: Connection ID " + connectionId + " -> Channel: " + channel + " -> Subscription ID: " + subscriptionId);
        
        userDataBase.subscribeUser(connectionId, subscriptionId, channel);
    
        synchronized (channelSubscriptions) {
            channelSubscriptions.computeIfAbsent(channel, k -> new LinkedList<>()).add(connectionId);
        }
        
        System.out.println("📡 Updated Subscriptions: " + channelSubscriptions);
    }
    

    private int generateSubscriptionId(int connectionId) {
        int newId = connectionId * 1000 + channelSubscriptions.size();  // Ensure a unique ID
        System.out.println("🆕 Generated new Subscription ID: " + newId);
        return newId;
    }


    


    @Override
    public void removeSubscription(int connectionId, int subscriptionId) {
        String channel = userDataBase.unsubscribeUser(connectionId, subscriptionId);
        if (channel != null) {
            synchronized (channelSubscriptions) {
                LinkedList<Integer> subscribers = channelSubscriptions.get(channel);
                if (subscribers != null) {
                    subscribers.remove(Integer.valueOf(connectionId));
                    if (subscribers.isEmpty()) {
                        channelSubscriptions.remove(channel);
                    }
                }
            }
        }
    }

    @Override
    public void registerConnectionHandler(ConnectionHandler<T> handler, int connectionId) {
        activeConnections.put(connectionId, handler);
    }

    @Override
    public ConnectionHandler<T> retrieveConnectionHandler(int connectionId) {
        return activeConnections.get(connectionId);
    }

    @Override
    public boolean isSubscribedToChannel(String channel, int connectionId) {
        synchronized (channelSubscriptions) {
            LinkedList<Integer> subscribers = channelSubscriptions.get(channel);
            return subscribers != null && subscribers.contains(connectionId);
        }
    }

    @Override
    public LinkedList<Integer> getSubscribersOfChannel(String channel) {
        synchronized (channelSubscriptions) {
            return channelSubscriptions.getOrDefault(channel, new LinkedList<>());
        }
    }

    @Override
    public int generateMessageId() {
        return messageCounter.getAndIncrement();
    }

    @Override
    public Integer fetchSubscriptionId(int connectionId, String channel) {
        System.out.println("🔍 Fetching Subscription ID for connection: " + connectionId + " in channel: " + channel);
        Integer subscriptionId = userDataBase.getSubscriptionId(connectionId, channel);
        
        if (subscriptionId == null) {
            System.out.println("❌ Subscription ID not found for " + channel);
        } else {
            System.out.println("✅ Retrieved Subscription ID: " + subscriptionId);
        }
    
        return subscriptionId;
    }
    
}    
