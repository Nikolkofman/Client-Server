package bgu.spl.net.srv;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.Map;
import java.util.Set;
import java.util.LinkedHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manages connections and subscriptions for STOMP clients.
 */
public class ConnectionsImpl implements Connections<String> {

    // Stores active clients: connectionId → ConnectionHandler
    private final ConcurrentHashMap<Integer, ConnectionHandler<String>> clientHandlers;

    // Stores subscribers per channel: channel → sorted set of connection IDs
    private final ConcurrentHashMap<String, Set<Integer>> subscribersByChannel;

    // Stores client subscriptions: connectionId → (channel → subscriptionId) [Maintains insertion order]
    private final ConcurrentHashMap<Integer, Map<String, String>> subscriptionsByClient;

    // Read-Write Lock for handling client connection updates safely
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private ConnectionsImpl() {
        this.clientHandlers = new ConcurrentHashMap<>();
        this.subscribersByChannel = new ConcurrentHashMap<>();
        this.subscriptionsByClient = new ConcurrentHashMap<>();
    }

    // Singleton instance holder
    private static class Holder {
        private static final ConnectionsImpl INSTANCE = new ConnectionsImpl();
    }

    /**
     * Returns the singleton instance of ConnectionsImpl.
     */
    public static ConnectionsImpl getInstance() {
        return Holder.INSTANCE;
    }

    @Override
    public boolean send(int connectionId, String message) {
        ConnectionHandler<String> handler = clientHandlers.get(connectionId);
        if (handler != null) {
            handler.send(message);
            return true;
        }
        return false;
    }

    @Override
    public void send(String channel, String message) {
        Set<Integer> subscribers = subscribersByChannel.get(channel);
        if (subscribers != null) {
            for (Integer connectionId : subscribers) {
                send(connectionId, message);
            }
        }
    }

    @Override
    public void disconnect(int connectionId) {
        lock.writeLock().lock();
        try {
            clientHandlers.remove(connectionId);

            Map<String, String> userSubscriptions = subscriptionsByClient.remove(connectionId);
            if (userSubscriptions != null) {
                for (String channel : userSubscriptions.keySet()) {
                    subscribersByChannel.computeIfPresent(channel, (key, subscribers) -> {
                        subscribers.remove(connectionId);
                        return subscribers.isEmpty() ? null : subscribers;
                    });
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void addClient(int connectionId, ConnectionHandler<String> handler) {
        lock.writeLock().lock();
        try {
            clientHandlers.put(connectionId, handler);
            subscriptionsByClient.put(connectionId, new LinkedHashMap<>()); // Keeps subscription order
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void subscribeClient(int connectionId, String channel, String subscriptionId) {
        lock.writeLock().lock();
        try {
            subscribersByChannel.computeIfAbsent(channel, k -> new ConcurrentSkipListSet<>()).add(connectionId);

            subscriptionsByClient.computeIfAbsent(connectionId, k -> new LinkedHashMap<>()).putIfAbsent(channel, subscriptionId);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void unsubscribeClient(int connectionId, String channel) {
        lock.writeLock().lock();
        try {
            subscribersByChannel.computeIfPresent(channel, (key, subscribers) -> {
                subscribers.remove(connectionId);
                return subscribers.isEmpty() ? null : subscribers;
            });

            Map<String, String> clientSubscriptions = subscriptionsByClient.get(connectionId);
            if (clientSubscriptions != null) {
                clientSubscriptions.remove(channel);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public String getClientSubscriptionId(int connectionId, String channel) {
        lock.readLock().lock();
        try {
            Map<String, String> clientSubscriptions = subscriptionsByClient.get(connectionId);
            if (clientSubscriptions != null && clientSubscriptions.containsKey(channel)) {
                return clientSubscriptions.get(channel);
            }
            System.out.println("WARNING: No subscription ID found for connection " + connectionId + " on channel " + channel);
            return "-1"; 
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Set<Integer> getChannelSubscribers(String channel) {
        lock.readLock().lock();
        try {
            return subscribersByChannel.get(channel);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public String findChannelBySubscription(int connectionId, String subscriptionId) {
        lock.readLock().lock();
        try {
            Map<String, String> clientSubscriptions = subscriptionsByClient.get(connectionId);
            if (clientSubscriptions != null) {
                for (Map.Entry<String, String> entry : clientSubscriptions.entrySet()) {
                    if (entry.getValue().equals(subscriptionId)) {
                        return entry.getKey();
                    }
                }
            }
            return "";
        } finally {
            lock.readLock().unlock();
        }
    }
}
