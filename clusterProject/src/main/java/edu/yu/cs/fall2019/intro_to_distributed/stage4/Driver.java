package edu.yu.cs.fall2019.intro_to_distributed.stage4;

import edu.yu.cs.fall2019.intro_to_distributed.Vote;
import edu.yu.cs.fall2019.intro_to_distributed.ZooKeeperPeerServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Driver
{
    public static void main(String[] args) throws IOException {
        ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress = new ConcurrentHashMap<>(3);
        peerIDtoAddress.put(0L, new InetSocketAddress("localhost", 9000));
        peerIDtoAddress.put(2L, new InetSocketAddress("localhost", 8020));
        peerIDtoAddress.put(3L, new InetSocketAddress("localhost", 8030));
        peerIDtoAddress.put(4L, new InetSocketAddress("localhost", 8040));
        peerIDtoAddress.put(5L, new InetSocketAddress("localhost", 8050));
        peerIDtoAddress.put(6L, new InetSocketAddress("localhost", 8060));
        peerIDtoAddress.put(7L, new InetSocketAddress("localhost", 8070));
        peerIDtoAddress.put(8L, new InetSocketAddress("localhost", 8080));

        long serverid = Long.parseLong(args[0]);
        InetSocketAddress myself = peerIDtoAddress.get(serverid);
        peerIDtoAddress.remove(serverid);

        if(serverid != 0L)
        {
            ZooKeeperPeerServer server = new ZooKeeperPeerServerImpl(myself.getPort(), 0, serverid, peerIDtoAddress);
            new Thread(server, "Server on port " + server.getMyAddress().getPort()).start();
        }

        else
        {
            Gateway gateway = new Gateway(0, serverid, peerIDtoAddress);
            new Thread(gateway, "Server on port " + gateway.getMyAddress().getPort()).start();
        }

        try
        {
            Thread.sleep(1000);
        }
        catch (Exception e)
        {
        }
        while(true);

    }
}
