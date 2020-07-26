package edu.yu.cs.fall2019.intro_to_distributed;

import edu.yu.cs.fall2019.intro_to_distributed.stage4.HeartBeatEntry;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class HeartbeatSender implements Runnable
{
    stateTeller server;
    long myID;
    long seqNum;
    ConcurrentHashMap<Long, HeartBeatEntry> heartbeatTable;
    ConcurrentHashMap<Long, InetSocketAddress> peerIdToAddress;

    public HeartbeatSender(stateTeller teller, long id, ConcurrentHashMap<Long, HeartBeatEntry> heartbeatTable, ConcurrentHashMap<Long, InetSocketAddress> peerIdToAddress)
    {
        this.server = teller;
        this.myID = id;
        this.heartbeatTable = heartbeatTable;
        this.peerIdToAddress = peerIdToAddress;
    }
    volatile boolean shutdown = false;
    public void setShutdown()
    {
        this.shutdown = true;
    }

    @Override
    public void run()
    {
        Random rand = new Random();
       while(!shutdown)
       {
           List<Long> idKeys = new ArrayList(peerIdToAddress.keySet());
           long nextGuy = (long)rand.nextInt()%(idKeys.size()+1);
           InetSocketAddress nextGuyAddress = peerIdToAddress.get(nextGuy);
           byte[] contents = new String("HEARTBEAT,"+myID+","+seqNum).getBytes();
           server.sendBroadcast(Message.MessageType.WORK, contents);

           seqNum++;




           try {
              Thread.sleep(2000);
           } catch (InterruptedException e) {
               e.printStackTrace();
           }


           String gossipToSend = "GOSSIP,"+server.getId()+",";
           for(Map.Entry<Long, HeartBeatEntry> entry: heartbeatTable.entrySet())
           {
               HeartBeatEntry h = entry.getValue();
               gossipToSend += h.getServerID()+"-"+h.getSeqNum()+":";
           }


           while(!peerIdToAddress.containsKey(nextGuy))
           {
               idKeys = new ArrayList(peerIdToAddress.keySet());
               nextGuy= rand.nextLong()%(idKeys.size());

           }



           server.sendMessage(Message.MessageType.COMPLETED_WORK, gossipToSend.getBytes(), peerIdToAddress.get(nextGuy));

       }


    }
}
