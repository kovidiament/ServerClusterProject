package edu.yu.cs.fall2019.intro_to_distributed.stage4;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.fall2019.intro_to_distributed.*;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
//

public class ZooKeeperPeerServerImpl implements ZooKeeperPeerServer, stateTeller
{
    long reqID = 0;
    private final InetSocketAddress myAddress;
    private final int myPort;
    private volatile ServerState state;
    private  volatile boolean shutdown;
    private LinkedBlockingQueue<Message> outgoingMessages;
    private LinkedBlockingQueue<Message> incomingMessages;
    private LinkedBlockingQueue<Message> outgoingTCP;
    private LinkedBlockingQueue<Message> incomingTCP;
    private LinkedBlockingQueue<String> outgoingWork;
    private Long id; private long peerEpoch;
    private volatile Vote currentLeader;
    private ConcurrentHashMap<Long,InetSocketAddress> peerIDtoAddress;
    private UDPMessageSender senderWorker;
    private UDPMessageReceiver receiverWorker;
    private TCPSender TCPDaemonSender;
    private TCPReceiver TCPDaemonReceiver;
    private HttpServer server;
    private ConcurrentHashMap<String, HttpExchange> exchangeHashMap;
    private ConcurrentHashMap<Long, ConcurrentHashMap<Long, String>> unfinishedStuffByWorker; //map server id to a map of requestnumber->request
    private ConcurrentHashMap<Long, Long> reqIdToWorker;
    private ConcurrentHashMap<Long, HeartBeatEntry> heartBeatTableHashMap;
    private JavaRunnerFollower JavaFollower;
    private final ExecutorService responsePool;
    private ArrayList<String> allGossipEverReceived;
    private HeartbeatSender send;
    private HeartbeatScanner scan;

    public ZooKeeperPeerServerImpl(int myPort, long peerEpoch, Long id, ConcurrentHashMap<Long,InetSocketAddress> peerIDtoAddress)
    {
        this.shutdown = false;
        this.myPort = myPort;
        this.peerEpoch = peerEpoch;
        this.id = id;
        this.peerIDtoAddress = peerIDtoAddress;

            this.state = ServerState.LOOKING;

        this.unfinishedStuffByWorker = new ConcurrentHashMap<Long, ConcurrentHashMap<Long, String>>();
        reqIdToWorker = new ConcurrentHashMap<Long, Long>();
        for(Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet())
        {
           unfinishedStuffByWorker.put(entry.getKey(), new ConcurrentHashMap<Long, String>());
        }
        this.myAddress = new InetSocketAddress("localhost", myPort);
        this.outgoingMessages = new LinkedBlockingQueue<Message>();
        this.incomingMessages = new LinkedBlockingQueue<Message>();
        this.outgoingTCP = new LinkedBlockingQueue<Message>();
        this.incomingTCP = new LinkedBlockingQueue<Message>();
        this.senderWorker = new UDPMessageSender(this.outgoingMessages, peerIDtoAddress, this);
        this.receiverWorker = new UDPMessageReceiver(this.incomingMessages, this.myAddress, this.myPort, peerIDtoAddress, this);
        this.TCPDaemonReceiver = new TCPReceiver(this.incomingTCP, myPort, peerIDtoAddress, this);
        this.TCPDaemonSender = new TCPSender(this.outgoingTCP, peerIDtoAddress, this);
        this.exchangeHashMap = new ConcurrentHashMap<String, HttpExchange>();
        this.heartBeatTableHashMap = new ConcurrentHashMap<Long, HeartBeatEntry>();
        for(Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet())
        {
           long entryid = entry.getKey();
           if(entryid != this.id)
           {
               heartBeatTableHashMap.put(entryid, new HeartBeatEntry(0, entryid));
           }
        }
        this.outgoingWork = new LinkedBlockingQueue<String>();
        this.responsePool = Executors.newCachedThreadPool();
        this.allGossipEverReceived = new ArrayList<String>();
        try {
            this.JavaFollower = new JavaRunnerFollower(this, incomingTCP, this.peerIDtoAddress);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void addToGossipList(String str)
    {
        allGossipEverReceived.add(str);
    }
    public ConcurrentHashMap<Long, HeartBeatEntry> getHeartbeatTable()
    {
        return this.heartBeatTableHashMap;
    }

    @Override
    public void sendMessageKnownLeader(InetSocketAddress target)
    {
        String notification = this.getCurrentLeader().getCandidateID() + "," + this.state +"," + this.id+"," + this.peerEpoch;
        Message message = new Message(Message.MessageType.ELECTION, notification.getBytes(), this.myAddress.getHostString(), this.myPort, target.getHostString(), target.getPort());
        outgoingMessages.offer(message);

    }
    @Override
    public void updateEntry(HeartBeatEntry entry, boolean wasGossip, long sourceid)
    {
        long id = entry.serverID;
        if(id != this.getId().longValue())
        {
            HeartBeatEntry oldEntry = heartBeatTableHashMap.get(id);
            if(oldEntry == null)
            {
                heartBeatTableHashMap.put(id, entry);
            }
            else if(oldEntry.getSeqNum() < entry.getSeqNum())
            {
                heartBeatTableHashMap.put(id, entry);
                if(wasGossip)
                {
                    DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                    Date date = new Date();
                    String time = dateFormat.format(date);
                    System.out.println(this.getId()+": updated "+ entry.serverID +" sequence number to "+entry.getSeqNum()+" based on message from "+sourceid+" at time "+time);
                }
            }
        }

    }


    @Override
    public void shutdown()
    {
        this.shutdown = true;
        this.senderWorker.shutdown();
        this.receiverWorker.shutdown();
        this.TCPDaemonSender.shutdown();
        this.TCPDaemonReceiver.shutdown();
        this.scan.setShutdown();
        this.send.setShutdown();
    }

    @Override
    public void run()
    {
        //step 1: create and run thread that sends broadcast messages

       Util.startAsDaemon(senderWorker, "UDP sender thread for "+this.myAddress.toString());
        // step 2: create and run thread that listens for messages sent to this server
       Util.startAsDaemon(receiverWorker, "UDP receiving thread for "+this.myAddress.toString());
        // step 3: main server loop
        Util.startAsDaemon(TCPDaemonReceiver, "TCP sender thread for "+this.myAddress.toString());
        Util.startAsDaemon(TCPDaemonSender, "TCP receiver thread for "+this.myAddress.toString());
         scan = new HeartbeatScanner(heartBeatTableHashMap, peerIDtoAddress, this);
         send = new HeartbeatSender(this, this.id, heartBeatTableHashMap, peerIDtoAddress);
        Util.startAsDaemon(send, "heartbeats");
        Util.startAsDaemon(scan, "heartbeat scanner");
        boolean started = false;
        try
        {
            startServer();
		boolean javaRunnerStarted = false;

             while(!shutdown)
             {
                 switch (getPeerState())
                 {
                     case LOOKING:
                         setCurrentLeader(lookForLeader());
			            if(currentLeader.getCandidateID() == this.id)
			            {
				            this.state = ServerState.LEADING;
                            System.out.println(id+": switching from LOOKING to LEADING");
				             started = true;

				            giveResults();
				            roundRobinAlloc();
			            }
			            else
                        {
                            System.out.println(id+": switching from LOOKING to FOLLOWING");
                        }
                         break;
                     case FOLLOWING:
                        if(!javaRunnerStarted)
			            {
				         javaRunnerStarted = true;
				         new Thread(JavaFollower).start();
			            }
                         while(getPeerState() == ServerState.FOLLOWING)
                         {
                             if(shutdown)
                             {break;}
                             continue;
                         }
                         System.out.println(id+": switching from FOLLOWING to LOOKING");

                     case LEADING:
                        while(getPeerState() == ServerState.LEADING)
                         {
                             if(shutdown)
                             {break;}
                             continue;
                         }
			
                 }
             }
             senderWorker.shutdown();
             receiverWorker.shutdown();
             TCPDaemonReceiver.shutdown();
             TCPDaemonSender.shutdown();

        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(1);
        }
    }
    private void startServer() throws IOException //create multithreaded http request server, spinning off handler threads
    {
        server = HttpServer.create(new InetSocketAddress(this.myPort+1), 0);
        server.createContext("/gossip", new GossipHandler());
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
    }
    @Override
    public void funeralForTheNiftar(long serverid)
    {

        this.heartBeatTableHashMap.remove(serverid);
        System.out.println(id+": no heartbeat from server "+serverid+" - server failed");
       if(this.state == ServerState.LEADING)
       {

           ConcurrentHashMap<Long, String> unfinishedWork = unfinishedStuffByWorker.get(serverid);
           for(Map.Entry<Long, String> entry : unfinishedWork.entrySet())
           {
               if(!outgoingWork.contains(entry.getValue()))
               {
                   outgoingWork.offer(entry.getValue());
               }
           }
       }
        if(this.currentLeader.getCandidateID() == serverid)
        {
           

		
			this.state = ServerState.LOOKING;
		
               
                //System.out.println("new leader: "+this.currentLeader.getCandidateID());
          
        }


    }
    private void giveResults() throws IOException, InterruptedException {

        Thread resultsThread = new Thread( () ->
        {
            while(!shutdown)
            {

                Message result = null;
                try {
                    result = incomingTCP.take();

                    String checkWhoFrom = new String(result.getMessageContents());
                    if(checkWhoFrom.split("DELIMIT").length == 2)
                    {

                       outgoingWork.offer(checkWhoFrom);//tcp is coming from two ways. if it's from gateway, it's work, otherwise its a result.
                        continue;
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
               byte[] contents = result.getMessageContents();
                //remove from unfinished jobs:
                unfinishedStuffByWorker.get(reqIdToWorker.get(Long.parseLong(new String(contents).split("DELIMIT")[0]))).remove(Long.parseLong(new String(contents).split("DELIMIT")[0]));
                InetSocketAddress gateway = peerIDtoAddress.get(0L);
                Message toGateway = new Message(Message.MessageType.COMPLETED_WORK, contents, myAddress.getHostName(), myAddress.getPort(), gateway.getHostName(), gateway.getPort());

                outgoingTCP.offer(toGateway);
            }


        }
        );
        resultsThread.start();
    }
    private void roundRobinAlloc() //loop through address map, stamping work with their addresses and sending to queue
    {


       Thread robinAllocThread = new Thread(() -> {
           int nextUp = 0;
           while(!shutdown)
           {
               List<Long> idKeys = new ArrayList(peerIDtoAddress.keySet());
               idKeys.remove(0);
               String job = null; //get next piece of code
               try {
                   job = outgoingWork.take();

               } catch (InterruptedException e) {
                   e.printStackTrace();
               }

               if(nextUp == idKeys.size())  {
                   nextUp = 0;
               }
               InetSocketAddress workerUpTo;
               long id = idKeys.get(nextUp);
             try{
                 if(peerIDtoAddress.get(id).equals(this.myAddress))  {
                     nextUp++;
                 }
                  workerUpTo = peerIDtoAddress.get(id);//circle through address map
             }
             catch (NullPointerException e)
             {
                 if(!outgoingWork.contains(job))
                 {
                     outgoingWork.offer(job);
                 }
                 continue;
             }



               sendTCPMessage(Message.MessageType.WORK, job.getBytes(), workerUpTo); //send to TCPSender queue
               long reqID = Long.parseLong(job.split("DELIMIT")[0]);
               reqIdToWorker.put(reqID, id);
               ConcurrentHashMap<Long, String>  jobsThisWorkerIsInMiddleOf = unfinishedStuffByWorker.get(id);
               jobsThisWorkerIsInMiddleOf.put(reqID, job);
               nextUp++;
           }
       }

       );
       robinAllocThread.start();
    }

    @Override
    public ZooKeeperPeerServer.ServerState currentState()
    {
        return this.getPeerState();
    }

    private Vote lookForLeader () throws InterruptedException {
        ZooKeeperLeaderElection election = new ZooKeeperLeaderElection(this,this.incomingMessages);
        return election.lookForLeader();
    }

    @Override
    public void setCurrentLeader(Vote v) {

        this.currentLeader = v;

    }

    @Override
    public Vote getCurrentLeader() {
        return this.currentLeader;
    }

    @Override
    public void sendMessage(Message.MessageType type, byte[] messageContents, InetSocketAddress target) throws IllegalArgumentException
    {
        if(!peerIDtoAddress.containsValue(target))
        {
            throw new IllegalArgumentException("invalid target address");
        }
        Message message = new Message(type, messageContents, this.myAddress.getHostString(), this.myPort, target.getHostString(), target.getPort());
        outgoingMessages.offer(message);
    }
    public void sendTCPMessage(Message.MessageType type, byte[] messageContents, InetSocketAddress target) //send message to TCPSender queue
    {
      String contents = new String(messageContents);

        Message message = new Message(type, messageContents, this.myAddress.getHostString(), this.myPort, target.getHostString(), target.getPort());
        outgoingTCP.offer(message);

    }


    @Override
    public void sendBroadcast(Message.MessageType type, byte[] messageContents)
    {
        for(Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet())
        {
            if(!entry.getKey().equals(this.id))
            {
                Message message = new Message(type, messageContents, this.myAddress.getHostString(), this.myPort, entry.getValue().getHostString(), entry.getValue().getPort());
                outgoingMessages.offer(message);
            }
        }

    }

    @Override
    public ServerState getPeerState() {
        return this.state;
    }

    @Override
    public void setPeerState(ServerState newState) {
            this.state = newState;
    }

    @Override
    public Long getId() {
        return this.id;
    }

    @Override
    public long getPeerEpoch() {
        return this.peerEpoch;
    }

    @Override
    public InetSocketAddress getMyAddress() {
        return this.myAddress;
    }

    @Override
    public int getMyPort() {
        return this.myPort;
    }

    @Override
    public InetSocketAddress getPeerByID(long id) {
        return peerIDtoAddress.get(id);
    }

    @Override
    public int getQuorumSize() {
      return peerIDtoAddress.size()/2;
    }

    class GossipHandler implements HttpHandler {
        public void handle(HttpExchange exchange) throws IOException
        {

            String responseBody = getGossipResponse();
            exchange.sendResponseHeaders(200, responseBody.length());
            OutputStream outputWriter = exchange.getResponseBody();

            outputWriter.write(responseBody.getBytes());
            outputWriter.close();


        }
    }
    private String getGossipResponse()
    {
        String gossipToSend = "";
        int size = allGossipEverReceived.size();
        for(int i = 0; i < size; i++)
        {
            String str = allGossipEverReceived.get(i);
            gossipToSend += str;
        }


        return gossipToSend;

    }

}
