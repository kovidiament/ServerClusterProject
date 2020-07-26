package edu.yu.cs.fall2019.intro_to_distributed;

import java.net.DatagramSocket;
import java.net.DatagramPacket;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class UDPMessageSender implements Runnable
{
    private LinkedBlockingQueue<Message> outgoingMessages;
    private volatile boolean shutdown = false;
    private ConcurrentHashMap<Long,InetSocketAddress> peerIDtoAddress;
    private stateTeller teller;
    public UDPMessageSender(LinkedBlockingQueue<Message> outgoingMessages, ConcurrentHashMap<Long,InetSocketAddress> peerIDtoAddress, stateTeller teller)
    {
        this.outgoingMessages = outgoingMessages;
        this.peerIDtoAddress = peerIDtoAddress;
        this.teller = teller;
    }
    public void shutdown()
    {
        this.shutdown = true;
    }
    @Override
    public void run()
    {
        while (!this.shutdown)
        {
            try
            {
                Message messageToSend = this.outgoingMessages.poll(2, TimeUnit.SECONDS);
                if(messageToSend != null)
                {
                    DatagramSocket socket = new DatagramSocket();
                    byte[] payload = messageToSend.getNetworkPayload();
                    DatagramPacket sendPacket = new DatagramPacket(payload, payload.length, new InetSocketAddress(messageToSend.getReceiverHost(),messageToSend.getReceiverPort()));
                    socket.send(sendPacket);
                    socket.close();
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }
}