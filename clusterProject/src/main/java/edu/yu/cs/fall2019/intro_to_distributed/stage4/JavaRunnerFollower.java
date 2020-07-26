package edu.yu.cs.fall2019.intro_to_distributed.stage4;

import edu.yu.cs.fall2019.intro_to_distributed.JavaRunnerImpl;
import edu.yu.cs.fall2019.intro_to_distributed.Message;
import edu.yu.cs.fall2019.intro_to_distributed.ZooKeeperPeerServer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class JavaRunnerFollower implements Runnable{
    private LinkedBlockingQueue<Message> incomingMessages;
    private ZooKeeperPeerServerImpl myPeerServer;
    private volatile boolean shutdown = false;
    JavaRunnerImpl runner;
    private ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress;

    public JavaRunnerFollower(ZooKeeperPeerServerImpl server, LinkedBlockingQueue<Message> messages, ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress) throws IOException
    {
        this.myPeerServer = server;
        this.incomingMessages = messages;
        this.peerIDtoAddress = peerIDtoAddress;
        this.runner = new JavaRunnerImpl();
    }
    public void shutdown(){this.shutdown = true;}

    @Override
    public void run()
    {
        while(!shutdown)
        {

            try
            {
                if(this.myPeerServer.getPeerState() != ZooKeeperPeerServer.ServerState.LEADING)
                {
                    String code;
                    Message nextJob = incomingMessages.take();
                    String payload = new String(nextJob.getMessageContents());
                    String[] splitPayload = payload.split("DELIMIT", 2);
                    String inetAddress = splitPayload[0];
                    String codeBody = splitPayload[1];
                    InputStream codeStream = new ByteArrayInputStream(codeBody.getBytes());
                    String responsePayload;
                    try
                    {
                        responsePayload = runner.compileAndRun(codeStream);
                        code = "200";
                    }
                    catch (Exception e)
                    {
                        responsePayload = e.getMessage();
                        code = "400";
                    }
                    String completeResponse = inetAddress+"DELIMIT"+code+"DELIMIT"+responsePayload;
                    if(this.myPeerServer.getPeerState() == ZooKeeperPeerServer.ServerState.LEADING)
                    {
                        incomingMessages.offer(nextJob);
                    }

                       else{
                        myPeerServer.sendTCPMessage(Message.MessageType.COMPLETED_WORK, completeResponse.getBytes(), peerIDtoAddress.get(myPeerServer.getCurrentLeader().getCandidateID()));

                    }

                }
                else{
                    shutdown();
                }

            }

            catch (InterruptedException e) {}


        }
    }


}
