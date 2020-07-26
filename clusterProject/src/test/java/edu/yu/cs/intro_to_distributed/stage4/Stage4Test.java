package edu.yu.cs.intro_to_distributed.stage4;

import edu.yu.cs.fall2019.intro_to_distributed.*;
import edu.yu.cs.fall2019.intro_to_distributed.stage4.ClientImpl;
import edu.yu.cs.fall2019.intro_to_distributed.stage4.Gateway;
import edu.yu.cs.fall2019.intro_to_distributed.stage4.HeartBeatEntry;
import edu.yu.cs.fall2019.intro_to_distributed.stage4.ZooKeeperPeerServerImpl;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Stage4Test
{
    @Test
    public void test1() throws IOException {
        //create IDs and addresses
        ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress = new ConcurrentHashMap<>(3);
        peerIDtoAddress.put(0L, new InetSocketAddress("localhost", 9000));
        peerIDtoAddress.put(2L, new InetSocketAddress("localhost", 8020));
        peerIDtoAddress.put(3L, new InetSocketAddress("localhost", 8030));
        peerIDtoAddress.put(4L, new InetSocketAddress("localhost", 8040));
        peerIDtoAddress.put(5L, new InetSocketAddress("localhost", 8050));
        peerIDtoAddress.put(6L, new InetSocketAddress("localhost", 8060));
        peerIDtoAddress.put(7L, new InetSocketAddress("localhost", 8070));
        peerIDtoAddress.put(8L, new InetSocketAddress("localhost", 8080));


        //create servers
        ArrayList<ZooKeeperPeerServerImpl> servers = new ArrayList<>(3);
        for(Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet())
        {
            ConcurrentHashMap<Long, InetSocketAddress> map = new ConcurrentHashMap<>(peerIDtoAddress);
           if(entry.getKey() != 0)//first one never starts even - test tolerance
           {
               map.remove(entry.getKey());
               ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map);
               servers.add(server);
               new Thread(server, "Server on port " + server.getMyAddress().getPort()).start();
           }
           else if(entry.getKey() == 0)
           {
               map.remove(entry.getKey());
               Gateway gateway = new Gateway(0, entry.getKey(), map);

               new Thread(gateway, "Server on port " + gateway.getMyAddress().getPort()).start();
           }
        }

        //wait for threads to start
        try
        {
            Thread.sleep(1000);
        }
        catch (Exception e)
        {
        }
        //print out the leaders and shutdown
        Vote correctLeader = new Vote(8,0);

        ClientImpl testClient = new ClientImpl("localhost", 9010);
        String code ="public class HelloWorld\n" +
                "{  \n" +
                "    public void run()\n" +
                "    { \n" +
                "        System.out.print(\"Hello, World\"); \n" +
                "    } \n" +
                "} ";

        ClientImpl testClient2 = new ClientImpl("localhost", 9010);
        String code2 ="public class HelloWorld\n" +
                "{  \n" +
                "    public void run()\n" +
                "    { \n" +
                "        System.out.print(\"Hello, World #2!\"); \n" +
                "    } \n" +
                "} ";
        ClientImpl.Response response = testClient.compileAndRun(code);
        ClientImpl.Response response2 = testClient2.compileAndRun(code2);
        String expected = "System.err:\n[]\nSystem.out:\n[Hello, World]\n";
        String actual = response.getBody();
        System.out.println("EXPECTED:\nSystem.err:\n[]\nSystem.out:\n[Hello, World]");
        System.out.println("ACTUAL:\n"+actual);
        Assert.assertEquals(actual, expected);
        String expected2 = "System.err:\n[]\nSystem.out:\n[Hello, World #2!]\n";
        String actual2 = response2.getBody();
        System.out.println("EXPECTED:\nSystem.err:\n[]\nSystem.out:\n[Hello, World #2!]");
        System.out.println("ACTUAL:\n"+actual2);
        Assert.assertEquals(actual2, expected2);



        for (ZooKeeperPeerServer server : servers)
        {
            Vote leader1 = server.getCurrentLeader();
            Assert.assertEquals(leader1,correctLeader);
            System.out.println("Server on port " + server.getMyAddress().getPort() + " whose ID is " + server.getId() +
                    " has the following ID as its leader: " + leader1.getCandidateID() + " and its state is " + server.getPeerState().name());
            //server.shutdown();

        }

        try{
            servers.get(6).shutdown();
        }
        catch(Exception e)
        {

        }
        servers.remove(6);
        try
        {
            Thread.sleep(20000);
        }
        catch (Exception e)
        {
        }

        Vote correctLeader2 = new Vote(7,0);

        for (ZooKeeperPeerServerImpl server : servers)
        {
            ConcurrentHashMap<Long, HeartBeatEntry> heartbeatTable  = server.getHeartbeatTable();
            Assert.assertFalse(heartbeatTable.containsKey(8L));
            System.out.println("Server "+server.getId()+" removed dead server from table");
        }
        for (ZooKeeperPeerServer server : servers)
        {
            Vote leader2 = server.getCurrentLeader();
            Assert.assertEquals(leader2, correctLeader2);
            System.out.println("Server on port " + server.getMyAddress().getPort() + " whose ID is " + server.getId() +
                    " has the following ID as its leader: " + leader2.getCandidateID() + " and its state is " + server.getPeerState().name());
            server.shutdown();

        }


    }



}
