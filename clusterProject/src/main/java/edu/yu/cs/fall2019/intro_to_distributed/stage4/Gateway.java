package edu.yu.cs.fall2019.intro_to_distributed.stage4;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.fall2019.intro_to_distributed.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
/////////GATEWAY IS ID ZERO////////////
import static edu.yu.cs.fall2019.intro_to_distributed.ZooKeeperPeerServer.ServerState.OBSERVER;

public class Gateway implements Runnable, stateTeller {
    long reqID = 0;
    private final InetSocketAddress myAddress;
    private volatile InetSocketAddress leaderAddress;
    private final int myPort = 9000;
    private volatile ZooKeeperPeerServer.ServerState state;
    private static volatile boolean shutdown;
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
    private ConcurrentHashMap<Long, HttpExchange> exchangeHashMap;
    private ConcurrentHashMap<Long, HeartBeatEntry> heartBeatTableHashMap;
    private ConcurrentHashMap<Long, String> unfinishedWorkByRequestNumber;
    private final ExecutorService responsePool;
    private ArrayList<String> allGossipEverReceived;

    public Gateway(long peerEpoch, Long id, ConcurrentHashMap<Long,InetSocketAddress> peerIDtoAddress)
    {
        this.peerEpoch = peerEpoch;
        this.id = id;
        this.peerIDtoAddress = peerIDtoAddress;
        this.myAddress = new InetSocketAddress("localhost", myPort);
        this.outgoingMessages = new LinkedBlockingQueue<Message>();
        this.incomingMessages = new LinkedBlockingQueue<Message>();
        this.outgoingTCP = new LinkedBlockingQueue<Message>();
        this.incomingTCP = new LinkedBlockingQueue<Message>();
        this.senderWorker = new UDPMessageSender(this.outgoingMessages, peerIDtoAddress, this);
        this.receiverWorker = new UDPMessageReceiver(this.incomingMessages, this.myAddress, this.myPort, peerIDtoAddress, this);
        this.TCPDaemonReceiver = new TCPReceiver(this.incomingTCP, myPort, peerIDtoAddress, this);
        this.TCPDaemonSender = new TCPSender(this.outgoingTCP, peerIDtoAddress, this);
        this.exchangeHashMap = new ConcurrentHashMap<Long, HttpExchange>();
        this.outgoingWork = new LinkedBlockingQueue<String>();
        this.responsePool = Executors.newCachedThreadPool();
        this.heartBeatTableHashMap = new ConcurrentHashMap<Long, HeartBeatEntry>();
        this.allGossipEverReceived = new ArrayList<String>();
        this.unfinishedWorkByRequestNumber = new ConcurrentHashMap<Long, String>();

    }

    @Override
    public void addToGossipList(String str)
    {
        allGossipEverReceived.add(str);
    }
    @Override
    public ZooKeeperPeerServer.ServerState currentState()
    {
        return OBSERVER;
    }
    public void shutdown()
    {
        this.shutdown = true;
        this.senderWorker.shutdown();
        this.receiverWorker.shutdown();
        this.TCPDaemonSender.shutdown();
        this.TCPDaemonReceiver.shutdown();
    }
    private synchronized long nextReqID()
    {
        reqID++;
        return reqID;
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
        Util.startAsDaemon(new HeartbeatSender(this, this.id, heartBeatTableHashMap, peerIDtoAddress), "heartbeats");
        Util.startAsDaemon(new HeartbeatScanner(heartBeatTableHashMap, peerIDtoAddress, this), "heartbeat scanner");
        boolean serverStarted = false;
        try
        {


                setCurrentLeader(lookForLeader());


            while(!shutdown)
            {

                if(!serverStarted)
                {
                    startServer();
                    sendWorkToLeader();
                    giveResults();
                    serverStarted = true;

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
        server = HttpServer.create(new InetSocketAddress(9010), 0);
        server.createContext("/compileandrun", new MyHandler());
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
        HttpServer server2 = HttpServer.create(new InetSocketAddress(this.myPort+1), 0);
        server2.createContext("/gossip", new GossipHandler());
        server2.setExecutor(Executors.newCachedThreadPool());
        server2.start();
        HttpServer server3 = HttpServer.create(new InetSocketAddress(this.myPort+2), 0);
        server3.createContext("/roles", new RolesPrinter());
        server3.setExecutor(Executors.newCachedThreadPool());
        server3.start();
    }
    class RolesPrinter implements HttpHandler {
        public void handle(HttpExchange exchange) throws IOException
        {

            String responseBody = printAllRoles();
            exchange.sendResponseHeaders(200, responseBody.length());
            OutputStream outputWriter = exchange.getResponseBody();
            outputWriter.write(responseBody.getBytes());
            outputWriter.close();


        }
    }
    public String printAllRoles()
    {
        String rolesList = "";
        for(Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet())
        {

            if(!entry.getKey().equals(this.currentLeader.getCandidateID()))
            {
                rolesList += "Server ID: "+entry.getKey() + " Role: Following\n";
            }
            else{
                rolesList += "Server ID: "+entry.getKey() + " Role: Leading\n";
            }
        }
        return rolesList;
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
    class MyHandler implements HttpHandler
    {
        public void handle(HttpExchange exchange) throws IOException
        {
            InputStream codeStream = exchange.getRequestBody();
            byte[] codeBytes = Util.readAllBytes(codeStream);
            long request = nextReqID();
            String messagePayload = Long.toString(request);
            String codeString = new String(codeBytes);
            messagePayload = messagePayload+"DELIMIT"+codeString;
            //message to worker: InetAddress.tostring that request maps to, DELIMIT, code itself
            exchangeHashMap.put(request, exchange);
            //map inetstring to httpexchange
            outgoingWork.offer(messagePayload);

        }
    }
    @Override
    public void sendMessageKnownLeader(InetSocketAddress target)
    {
        String notification = this.getCurrentLeader().getCandidateID() + "," + this.state +"," + this.id+"," + this.peerEpoch;
        Message message = new Message(Message.MessageType.ELECTION, notification.getBytes(), this.myAddress.getHostString(), this.myPort, target.getHostString(), target.getPort());
        outgoingMessages.offer(message);

    }
    private void sendWorkToLeader() //loop through address map, stamping work with their addresses and sending to queue
    {


        Thread workSender = new Thread(() -> {
            while(!shutdown)
            {



                String job = null;
                try {
                    job = outgoingWork.take();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                long reqID = Long.parseLong(job.split("DELIMIT")[0]);
                unfinishedWorkByRequestNumber.put(reqID, job);

                sendTCPMessage(Message.MessageType.WORK, job.getBytes(), this.leaderAddress); //send to TCPSender queue


            }
        }

        );
        workSender.start();
    }

    @Override
    public void funeralForTheNiftar(long serverid)  {

        this.heartBeatTableHashMap.remove(serverid);
        if(this.currentLeader.getCandidateID() == serverid)
            {
                try{

                    setCurrentLeader(lookForLeader());

                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
            }



    }


    private void giveResults() throws IOException, InterruptedException {
        Thread resultsThread = new Thread( () ->
        {
            while(!shutdown)
            {
                try{
                    Message result = incomingTCP.take();
                    String payload = new String(result.getMessageContents());
                    String[] split = payload.split("DELIMIT");
                    unfinishedWorkByRequestNumber.remove(Long.parseLong(split[0]));
                    HttpExchange exchange = exchangeHashMap.get(Long.parseLong(split[0]));
                    int code = Integer.parseInt(split[1]);
                    String responseBody = split[2];

                    responsePool.execute( () ->{
                                try{
                                    exchange.sendResponseHeaders(code, responseBody.length());
                                    OutputStream outputWriter = exchange.getResponseBody();
                                    outputWriter.write(responseBody.getBytes());
                                    outputWriter.close();
                                }
                                catch (IOException e)
                                { e.printStackTrace();}
                            }
                    );
                }
                catch (InterruptedException e)
                { e.printStackTrace();}
            }
        }
        );
        resultsThread.start();
    }

    private Vote lookForLeader () throws InterruptedException
    {
        ZooKeeperLeaderElectionGatewayVersion election = new ZooKeeperLeaderElectionGatewayVersion(this,this.incomingMessages);
        return election.lookForLeader();
    }


    public void setCurrentLeader(Vote v) {

        this.currentLeader = v;
        this.leaderAddress = this.peerIDtoAddress.get(v.getCandidateID());

    }

    public Vote getCurrentLeader() {
        return this.currentLeader;
    }

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


    public Long getId() {
        return this.id;
    }


    public long getPeerEpoch() {
        return this.peerEpoch;
    }


    public InetSocketAddress getMyAddress() {
        return this.myAddress;
    }


    public int getMyPort() {
        return this.myPort;
    }


    public InetSocketAddress getPeerByID(long id) {
        return peerIDtoAddress.get(id);
    }


    public int getQuorumSize() {
        return peerIDtoAddress.size()/2;
    }


}
