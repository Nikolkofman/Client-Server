package bgu.spl.net.srv;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class User {
    private static final User INSTANCE = new User();
    private final Map<String, String> userPasswords;
    private final Map<String, Boolean> userStatus;

    private User() {
        userPasswords = new ConcurrentHashMap<>();
        userStatus = new ConcurrentHashMap<>();
    }

    public static User getInstance() {
        return INSTANCE;
    }

    public boolean registerOrLogin(String username, String password) {
        if (isInvalid(username) || isInvalid(password)) {
            System.out.println("Username or password cannot be null or empty");
            return false;
        }

        return userPasswords.computeIfAbsent(username, k -> password).equals(password);
    }

    public void setOnlineStatus(String username, boolean status) {
        if (isInvalid(username)) {
            System.out.println("Username cannot be null or empty");
            return;
        }
        userStatus.put(username, status);
    }

    
    public boolean isOnline(String username) {
        if (isInvalid(username)) {
            System.out.println("Username cannot be null or empty");
            return false;
        }
        return userStatus.getOrDefault(username, false);
    }

    private boolean isInvalid(String value) {
        return value == null || value.trim().isEmpty();
    }
}
