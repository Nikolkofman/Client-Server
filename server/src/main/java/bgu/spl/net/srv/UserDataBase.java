package bgu.spl.net.srv;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class UserDataBase {
    private static UserDataBase instance = null;

    private final Map<String, String> userPasswords; // username -> password
    private final Map<String, ConcurrentHashMap<Integer, String>> subscriptions; // username -> (subscriptionId -> channel)
    private final Map<Integer, String> activeUsers; // connectionId -> username

    // Private constructor to prevent external instantiation
    private UserDataBase() {
        userPasswords = new ConcurrentHashMap<>();
        subscriptions = new ConcurrentHashMap<>();
        activeUsers = new ConcurrentHashMap<>();
    }

    // Singleton instance retrieval
    public static synchronized UserDataBase getInstance() {
        if (instance == null) {
            instance = new UserDataBase();
        }
        return instance;
    }

    // Register a new user
    public boolean registerUser(String username, String password) {
        return userPasswords.putIfAbsent(username, password) == null;
    }

    // Verify user login credentials
    public boolean verifyCredentials(String username, String password) {
        return password.equals(userPasswords.get(username));
    }

    // Associate a user with a connection
    public void assignConnection(int connectionId, String username) {
        activeUsers.put(connectionId, username);
    }

    // Remove a user connection
    public void releaseConnection(int connectionId) {
        String username = activeUsers.remove(connectionId);
        if (username != null) {
            subscriptions.remove(username); // Clear subscriptions on logout
        }
    }

    // Check if a user is currently logged in
    public boolean isUserOnline(String username) {
        return activeUsers.containsValue(username);
    }

    // Check if a user exists in the system
    public boolean userExists(String username) {
        return userPasswords.containsKey(username);
    }

    // Retrieve the username associated with a connection ID
    public String getUsername(int connectionId) {
        return activeUsers.get(connectionId);
    }

    // Add a subscription for a user
    public void subscribeUser(int connectionId, int subscriptionId, String channel) {
        String username = activeUsers.get(connectionId);
        if (username != null) {
            subscriptions.computeIfAbsent(username, k -> new ConcurrentHashMap<>()).put(subscriptionId, channel);
            System.out.println("User subscribed to channel: " + channel);
        }
    }

    // Remove a user's subscription and return the unsubscribed channel
    public String unsubscribeUser(int connectionId, int subscriptionId) {
        String username = activeUsers.get(connectionId);
        if (username != null) {
            ConcurrentHashMap<Integer, String> userSubscriptions = subscriptions.get(username);
            if (userSubscriptions != null) {
                return userSubscriptions.remove(subscriptionId);
            }
        }
        return null;
    }

    // Retrieve all channels a user is subscribed to
    public LinkedList<String> getUserSubscriptions(int connectionId) {
        String username = activeUsers.get(connectionId);
        if (username != null) {
            ConcurrentHashMap<Integer, String> userSubscriptions = subscriptions.get(username);
            return userSubscriptions != null ? new LinkedList<>(userSubscriptions.values()) : new LinkedList<>();
        }
        return new LinkedList<>();
    }

    // Retrieve the subscription ID for a specific channel
    public Integer getSubscriptionId(int connectionId, String channel) {
        String username = activeUsers.get(connectionId);
        if (username != null) {
            ConcurrentHashMap<Integer, String> userSubscriptions = subscriptions.get(username);
            if (userSubscriptions != null) {
                return userSubscriptions.entrySet().stream()
                        .filter(entry -> entry.getValue().equals(channel))
                        .map(Map.Entry::getKey)
                        .findFirst()
                        .orElse(null);
            }
        }
        return null;
    }

    // Clear all subscriptions for a user upon logout
    public void resetUserSubscriptions(int connectionId) {
        String username = activeUsers.get(connectionId);
        if (username != null) {
            subscriptions.remove(username);
        }
    }
}
