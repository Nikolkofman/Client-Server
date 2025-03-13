package bgu.spl.net.srv;

import java.util.LinkedList;
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
                for (int connectionId : subscribers) {
                    send(connectionId, msg);
                }
            }
        }
    }

 