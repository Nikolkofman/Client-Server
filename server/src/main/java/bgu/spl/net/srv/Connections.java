package bgu.spl.net.srv;
import java.util.LinkedList;

public interface Connections<T> {

    void send(String channel, T msg);
    
    boolean send(int connectionId, T msg);

    void disconnect(int connectionId);

    boolean isUserLoggedIn(String username, String password);

    boolean isValidLogin(String username, String password);

    void handleUserLogin(int connectionId, String username, String password);

    void handleUserLogout(int connectionId);

    void addSubscription(String channel, int connectionId, int subscriptionId);

    void removeSubscription(int connectionId, int subscriptionId);

    ConnectionHandler<T> retrieveConnectionHandler(int connectionId);

    void registerConnectionHandler(ConnectionHandler<T> handler, int connectionId);

    boolean isSubscribedToChannel(String channel, int connectionId);

    LinkedList<Integer> getSubscribersOfChannel(String channel);

    int generateMessageId();
    boolean checkUserExists(String username);
    Integer fetchSubscriptionId(int connectionId, String channel);
}