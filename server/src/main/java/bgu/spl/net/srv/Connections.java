package bgu.spl.net.srv;

import java.util.Set;

public interface Connections<T> {

    boolean send(int connectionId, T message);

    void send(String channel, T message);

    void disconnect(int connectionId);

    void addClient(int connectionId, ConnectionHandler<T> handler);

    
    void subscribeClient(int connectionId, String channel, String subscriptionId);

   
    void unsubscribeClient(int connectionId, String channel);

    String getClientSubscriptionId(int connectionId, String channel);

    Set<Integer> getChannelSubscribers(String channel);

    public String findChannelBySubscription(int connectionId, String subscriptionId);
}
