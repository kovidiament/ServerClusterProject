package edu.yu.cs.fall2019.intro_to_distributed;

import edu.yu.cs.fall2019.intro_to_distributed.stage4.HeartBeatEntry;

import java.net.InetAddress;
import java.net.InetSocketAddress;

public interface stateTeller {
    ZooKeeperPeerServer.ServerState currentState();
    void sendMessageKnownLeader(InetSocketAddress address);
    Vote getCurrentLeader();
    void sendBroadcast(Message.MessageType type, byte[] messageContents);
    void updateEntry(HeartBeatEntry entry, boolean wasGossip, long sourceid);
    void addToGossipList(String str);
    Long getId();
    void sendMessage(Message.MessageType type, byte[] messageContents, InetSocketAddress target);
    void funeralForTheNiftar(long serverid);

}
