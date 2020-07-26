package edu.yu.cs.fall2019.intro_to_distributed;

import edu.yu.cs.fall2019.intro_to_distributed.stage4.HeartBeatEntry;

import java.net.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class UDPMessageReceiver implements Runnable
{
    private static final int MAXLENGTH = 4096;
    private final InetSocketAddress myAddress;
    private final int myPort;
    private LinkedBlockingQueue<Message> incomingMessages;
    private volatile boolean shutdown = false;
    private ConcurrentHashMap<Long,InetSocketAddress> peerIDtoAddress;
    private stateTeller teller;

    public UDPMessageReceiver(LinkedBlockingQueue<Message> incomingMessages, InetSocketAddress myAddress, int myPort, ConcurrentHashMap<Long,InetSocketAddress> peerIDtoAddress, stateTeller teller)
    {
        this.incomingMessages = incomingMessages;
        this.myAddress = myAddress;
        this.myPort = myPort;
        this.peerIDtoAddress = peerIDtoAddress;
        this.teller = teller;
    }

    public void shutdown()
    {
        this.shutdown = true;
    }
    public void respondKnowLeader(DatagramPacket packet)
    {
        String hostname = packet.getAddress().getHostName();
        int port = packet.getPort();
        InetSocketAddress address = new InetSocketAddress(hostname, port);
        teller.sendMessageKnownLeader(address);
    }

    @Override
    public void run()
    {
        //create the socket
        DatagramSocket socket = null;
        try
        {
            socket = new DatagramSocket(this.myAddress);
            socket.setSoTimeout(3000);
        }
        catch(Exception e)
        {
            System.err.println("failed to create receiving socket");
            e.printStackTrace();
        }
        //loop
        while (!this.shutdown)
        {
            try
            {
                DatagramPacket packet = new DatagramPacket(new byte[MAXLENGTH], MAXLENGTH);
                socket.receive(packet); // Receive packet from a client
                Message received = new Message(packet.getData());
                String[] messageInfo = new String(received.getMessageContents()).split(",");
                if(messageInfo[0].equals("HEARTBEAT"))
                {

                    long id = Long.parseLong(messageInfo[1]);
                    if(!peerIDtoAddress.containsKey(id)) continue;
                    long seqnum = Long.parseLong(messageInfo[2]);
                    HeartBeatEntry entry = new HeartBeatEntry(seqnum, id);
                    teller.updateEntry(entry, false, -1);
                }
                else
                    if(messageInfo[0].equals("GOSSIP"))
                {
                    /*
                    Gossip message structure is GOSSIP,sourceID,serverid-seqnum:serverid-seqnum etc
                     */

                    long sourceID = Long.parseLong(messageInfo[1]);
                    DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                    Date date = new Date();
                    String time = dateFormat.format(date);
                    String forTheGossipList = "\nNew Gossip Entry From Server "+sourceID+" \nReceived at "+time+"\n";

                    String gossipString = messageInfo[2];
                    String[] gossipByEntry = messageInfo[2].split(":");//update entry for each, then make big string, have thing of old gossip
                    for(int i = 0; i < gossipByEntry.length; i++)
                    {
                        String[] splitEntry = gossipByEntry[i].split("-");
                        long id = Long.parseLong(splitEntry[0]);
                        long seqNum = Long.parseLong(splitEntry[1]);
                        HeartBeatEntry entry = new HeartBeatEntry(seqNum, id);
                        teller.updateEntry(entry, true, sourceID);
                        String addOn = "ServerID: "+id+" Sequence Number: " + seqNum+"\n";
                        forTheGossipList += addOn;
                    }

                    teller.addToGossipList(forTheGossipList);

                }
                else
                {
                    ZooKeeperPeerServer.ServerState state = teller.currentState();
                    switch (state)
                    {
                        case LEADING:
                         respondKnowLeader(packet);
                            break;
                  
                        default:
                            this.incomingMessages.put(received);
                    }


                }

            }
            catch(SocketTimeoutException ste)
            {
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
        //cleanup
        if(socket != null)
        {
            socket.close();
        }
    }
}
