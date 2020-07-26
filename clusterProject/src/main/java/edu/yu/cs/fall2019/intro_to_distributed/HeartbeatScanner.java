package edu.yu.cs.fall2019.intro_to_distributed;


import edu.yu.cs.fall2019.intro_to_distributed.stage4.HeartBeatEntry;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HeartbeatScanner implements Runnable
{
    ConcurrentHashMap<Long, HeartBeatEntry> heartbeatTable;
    ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress;
    stateTeller server;
    public HeartbeatScanner(ConcurrentHashMap<Long, HeartBeatEntry> heartbeatTable, ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress, stateTeller teller)
    {
        this.heartbeatTable = heartbeatTable;
        this.peerIDtoAddress = peerIDtoAddress;
        this.server = teller;
    }
    volatile boolean shutdown = false;
    public void setShutdown()
    {
        shutdown = true;
    }
    @Override
    public void run()
    {
	    try{
		Thread.sleep(5000);
		}
		catch(InterruptedException e)
		{}
        while(!shutdown)
        {

            int count = 0;
            for(Map.Entry<Long, HeartBeatEntry> entry : heartbeatTable.entrySet())
            {

                if(System.nanoTime() - entry.getValue().getTime() > 10000000000L)
                {


                    peerIDtoAddress.remove(entry.getKey());
                    server.funeralForTheNiftar(entry.getKey());

                }
            }
        }
    }
}
