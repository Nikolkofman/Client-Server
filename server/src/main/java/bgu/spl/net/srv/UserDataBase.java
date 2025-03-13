package bgu.spl.net.srv;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class UserDataBase {
    private static UserDataBase instance = null;

    private final Map<String, String> userPasswords; // username -> password
    private final Map<Integer, ConcurrentHashMap<Integer, String>> subscriptions; // 🔧 Store by connectionId -> (subscriptionId -> channel)
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

    public void releaseConnection(int connectionId) {
        System.out.println("🔄 Releasing connection ID: " + connectionId);
        subscriptions.remove(connectionId);
        activeUsers.remove(connectionId);
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

    public void subscribeUser(int connectionId, int subscriptionId, String channel) {
        subscriptions.computeIfAbsent(connectionId, k -> new ConcurrentHashMap<>()).put(subscriptionId, channel);
        System.out.println(" Connection ID " + connectionId + " subscribed to channel: " + channel + " with Subscription ID: " + subscriptionId);
    }
    

    public String unsubscribeUser(int connectionId, int subscriptionId) {
        ConcurrentHashMap<Integer, String> userSubscriptions = subscriptions.get(connectionId);
        if (userSubscriptions != null) {
            return userSubscriptions.remove(subscriptionId);  
        }
        return null;
    }
    

    public LinkedList<String> getUserSubscriptions(int connectionId) {
        ConcurrentHashMap<Integer, String> userSubscriptions = subscriptions.get(connectionId);
        return userSubscriptions != null ? new LinkedList<>(userSubscriptions.values()) : new LinkedList<>();
    }
    

    public Integer getSubscriptionId(int connectionId, String channel) {
        ConcurrentHashMap<Integer, String> userSubscriptions = subscriptions.get(connectionId);
        if (userSubscriptions != null) {
            return userSubscriptions.entrySet().stream()
                    .filter(entry -> entry.getValue().equals(channel))
                    .map(Map.Entry::getKey)
                    .findFirst()
                    .orElse(null);
        }
        return null;
    }
    

    public void resetUserSubscriptions(int connectionId) {
        subscriptions.remove(connectionId); 
    }
    

    public Map<Integer, ConcurrentHashMap<Integer, String>> getAllSubscriptions() {
        return subscriptions; 
    }
    
    
}
