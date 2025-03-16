package bgu.spl.net.srv;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

    
    public class User {
        private static final User INSTANCE = new User();
        private final Map<String, String> userPasswords = new ConcurrentHashMap<>();
        private final Map<String, Boolean> userStatus = new ConcurrentHashMap<>();
    
        private User() {}
    
        public static User getInstance() {
            return INSTANCE;
        }
    
        public boolean registerOrLogin(String username, String password) {
            if (username == null || password == null) {
                System.out.println("ERROR: Username or password cannot be null");
                return false;
            }
    
            return userPasswords.computeIfAbsent(username, k -> password).equals(password);
        }

        public void setOnlineStatus(String username, boolean status) {
            if (username == null) {
                System.out.println("ERROR: Username cannot be null");
                return;
            }
            userStatus.put(username, status);
        }
    
    
        public boolean isOnline(String username) {
            if (username == null) {
                System.out.println("ERROR: Username cannot be null");
                return false;
            }
            return userStatus.getOrDefault(username, false);
        }
    
    }

    