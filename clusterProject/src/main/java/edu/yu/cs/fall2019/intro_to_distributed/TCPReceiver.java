package edu.yu.cs.fall2019.intro_to_distributed;

import edu.yu.cs.fall2019.intro_to_distributed.stage4.Gateway;

import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TCPReceiver implements Runnable
{
    private static final int MAXLENGTH = 4096;
    private LinkedBlockingQueue<Message> messages;
    private static volatile boolean shutdown = false;
    int listeningPort;
    private ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress;
    private stateTeller teller;

    public TCPReceiver(LinkedBlockingQueue<Message> incomingMessages, int myPort, ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress, stateTeller teller)
    {
        this.messages = incomingMessages;
        this.listeningPort = myPort;
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
        try
        {
            ServerSocket server = new ServerSocket(listeningPort);
            while(!shutdown)
            {
                Socket socket = server.accept();
                byte[] receivedBytes = Util.readAllBytes(socket.getInputStream());
                Message received = new Message(receivedBytes);

                this.messages.offer(received);
                socket.close();

            }
        }
        catch (Exception e)
        {
           
           
        }



    }
}
