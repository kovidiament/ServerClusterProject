package edu.yu.cs.fall2019.intro_to_distributed;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TCPSender implements Runnable
{
    private LinkedBlockingQueue<Message> messages;
    private static volatile boolean shutdown = false;
    private ConcurrentHashMap<Long,InetSocketAddress> peerIDtoAddress;
    private stateTeller teller;

    public TCPSender(LinkedBlockingQueue<Message> outgoingMessages, ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress, stateTeller teller)
    {
        this.messages = outgoingMessages;
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
        while(!this.shutdown)
        {
            Message messageToSend = null;
            try {
                messageToSend = this.messages.poll(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try
            {

                if(messageToSend != null)
                {
                    byte[] messageBytes = messageToSend.getNetworkPayload();
                    String contents = new String(messageBytes);

                    Socket sendSocket = new Socket(messageToSend.getReceiverHost(), messageToSend.getReceiverPort());
                    OutputStream socketOutputStream = sendSocket.getOutputStream();
                    socketOutputStream.write(messageBytes);
                    socketOutputStream.flush();
                    socketOutputStream.close();
                    sendSocket.close();
                }
            }
            catch (Exception e)
            {
                ZooKeeperPeerServer.ServerState state = teller.currentState();
                InetSocketAddress address = new InetSocketAddress(messageToSend.getReceiverHost(), messageToSend.getReceiverPort());
                switch (teller.currentState())
                {
                    case FOLLOWING:
                        break;
                    case OBSERVER:
                        
                            redo(messageToSend);
                        
                       
                        break;
                    case LOOKING:
                        messages.offer(messageToSend);
                        break;
                    case LEADING:
                       /* if(!peerIDtoAddress.containsValue(address)) //oops! he's dead. send to different slave.
                        {
                            redoAsLeader(messageToSend);
                        }
                        else
                        {
                            messages.offer(messageToSend); //lets try that again...
                        }*/
                       break;


                }
            }
        }

    }
    public void redo(Message messageToSend)
    {
        Vote leader = teller.getCurrentLeader();
	while(!peerIDtoAddress.contains(leader.getCandidateID()))
	{
		leader = teller.getCurrentLeader();
	}

        InetSocketAddress newLeader = peerIDtoAddress.get(leader.getCandidateID());
        Message message = new Message(messageToSend.getMessageType(), messageToSend.getMessageContents(), messageToSend.getSenderHost(), messageToSend.getSenderPort(), newLeader.getHostString(), newLeader.getPort());
        messages.offer(message);
    }
    public void redoAsLeader(Message messageToSend)
    {
        int intendedReceiver = messageToSend.getReceiverPort();
        if(intendedReceiver == 8010)//meant for gateway. why an exception? who knows
        {
            messages.offer(messageToSend);//let's try that again...
        }
        else
        {
            ConcurrentHashMap.KeySetView<Long, InetSocketAddress> keys = peerIDtoAddress.keySet();
            for(long key: keys) { //choose a new slave for it - spin the wheel!
                if (key != teller.getCurrentLeader().getCandidateID() && peerIDtoAddress.get(key).getPort() != 8010)
                {
                    //getCurrentLeader here being me but whatever.
                    //also making sure its not the gateway.
                    InetSocketAddress newWorker = peerIDtoAddress.get(key);
                    Message message = new Message(messageToSend.getMessageType(), messageToSend.getMessageContents(), messageToSend.getSenderHost(), messageToSend.getSenderPort(), newWorker.getHostString(), newWorker.getPort());
                    messages.offer(message);
                }
            }
        }

    }
}
