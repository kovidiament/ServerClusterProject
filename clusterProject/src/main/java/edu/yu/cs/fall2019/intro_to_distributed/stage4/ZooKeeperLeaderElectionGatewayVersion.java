package edu.yu.cs.fall2019.intro_to_distributed.stage4;

import edu.yu.cs.fall2019.intro_to_distributed.ElectionNotification;
import edu.yu.cs.fall2019.intro_to_distributed.Message;
import edu.yu.cs.fall2019.intro_to_distributed.Vote;
import edu.yu.cs.fall2019.intro_to_distributed.ZooKeeperPeerServer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static edu.yu.cs.fall2019.intro_to_distributed.ZooKeeperPeerServer.ServerState.OBSERVER;

public class ZooKeeperLeaderElectionGatewayVersion
{


    private LinkedBlockingQueue<Message> incomingMessages;
    private Gateway myGateway;
    private long proposedLeader, proposedEpoch;
    HashMap<Long, Vote> votes = new HashMap<Long, Vote>();
    /**
     * time to wait once we believe we've reached the end of leader election.
     **/
    private final static int finalizeWait = 200;
    /**
     * Upper bound on the amount of time between two consecutive notification checks.
     * This impacts the amount of time to get the system up again after long partitions. Currently 60 seconds.
     */
    private final static int maxNotificationInterval = 60000;


    public ZooKeeperLeaderElectionGatewayVersion(Gateway server, LinkedBlockingQueue<Message> incomingMessages) {
        this.incomingMessages = incomingMessages;
        this.myGateway = server;
        this.proposedLeader = server.getId();
        this.proposedEpoch = server.getPeerEpoch();
    }

    public synchronized Vote getVote() {
        return new Vote(this.proposedLeader, this.proposedEpoch);
    }
    public void sendNotifications(ElectionNotification n)
    {
        String notification = n.leader + "," + n.state.getChar() +"," + n.sid+"," + n.peerEpoch;
        myGateway.sendBroadcast(Message.MessageType.ELECTION, notification.getBytes());
    }

    public synchronized Vote lookForLeader() throws InterruptedException {

            return lookForLeaderAsObserver();

    }

    public synchronized Vote lookForLeaderAsObserver() throws InterruptedException
    {
        Vote resultingLeader = null;
        boolean done = false;
        //send initial notifications to other peers to get things started
        ElectionNotification startup = new ElectionNotification(myGateway.getId(), OBSERVER, myGateway.getId(), myGateway.getPeerEpoch());
        sendNotifications(startup);
        //Loop, exchanging notifications with other servers until we find a leader
        while (!done) {    // Remove next notification from queue, timing out after 2 times the termination time
            long timeout = finalizeWait;
            Message nextNotification = incomingMessages.poll(timeout, TimeUnit.MILLISECONDS);

            // if no notifications received...

            while(nextNotification == null)
            {
                // ...resend notifications to prompt a reply from others...
                ElectionNotification newNotification = new ElectionNotification(this.proposedLeader, OBSERVER, myGateway.getId(),this.proposedEpoch);
                sendNotifications(newNotification);

                // ..and implement exponential backoff when notifications not received...
                long tmpTimeOut = timeout*2;
                timeout = (tmpTimeOut < maxNotificationInterval? tmpTimeOut : maxNotificationInterval);
                nextNotification = incomingMessages.poll(timeout, TimeUnit.MILLISECONDS);
            }
            String[] messageInfo = new String(nextNotification.getMessageContents()).split(",");
            long messageLeader = Long.parseLong(messageInfo[0]);
            ZooKeeperPeerServer.ServerState messageState = ZooKeeperPeerServer.ServerState.getServerState(messageInfo[1].charAt(0));
            long messageSid = Long.parseLong(messageInfo[2]);
            long messageEpoch = Long.parseLong(messageInfo[3]);

            if(myGateway.getPeerByID(messageSid) == null)
            {
                continue;
            }
            switch (messageState)
            {

                case LOOKING://if the sender is also looking
                    // if the received message has a vote for a leader which supersedes mine,
                    if(newVoteSupersedesCurrent(messageLeader, messageEpoch, this.proposedLeader, this.proposedEpoch))
                    {
                        // change my vote and tell all my peers what my new vote is...
                        this.proposedEpoch = messageEpoch;
                        this.proposedLeader = messageLeader;
                        ElectionNotification newNotification = new ElectionNotification(this.proposedLeader, OBSERVER, myGateway.getId(),this.proposedEpoch);
                        sendNotifications(newNotification);
                    }
                    // ...while keeping track of the votes I received and who I received them from
                    Vote theNewVote = new Vote(messageLeader, messageEpoch);
                    votes.put(messageSid, theNewVote);

                    // if I have enough votes to declare a leader:
                    if(haveEnoughVotes(votes, new Vote(this.proposedLeader, this.proposedEpoch)))
                    {
                        // check if there are any new votes for a higher ranked possible leader before I declare a leader. If so, continue in my election Loop
                        boolean higherVoteExists = false;
                        Message nextMessage = incomingMessages.poll(finalizeWait, TimeUnit.MILLISECONDS);
                        while(nextMessage != null)
                        {
                            String[] message = new String(nextMessage.getMessageContents()).split(",");
                            long senderLeader = Long.parseLong(message[0]);
                            ZooKeeperPeerServer.ServerState senderState = ZooKeeperPeerServer.ServerState.getServerState(message[1].charAt(0));
                            long senderID = Long.parseLong(message[2]);
                            long senderEpoch = Long.parseLong(message[3]);
                            if(myGateway.getPeerByID(senderID) == null)
                            {
                                nextMessage = incomingMessages.poll(finalizeWait, TimeUnit.MILLISECONDS);
                            }
                            else{
                                if(newVoteSupersedesCurrent(senderID, senderEpoch, this.proposedLeader, this.proposedEpoch))
                                {
                                    higherVoteExists = true;
                                    incomingMessages.put(nextMessage);
                                    break;
                                }
                                else{
                                    nextMessage = incomingMessages.poll(finalizeWait, TimeUnit.MILLISECONDS);
                                }
                            }
                        }
                        if(!higherVoteExists)
                        {


                            ElectionNotification acceptResult = new ElectionNotification(this.proposedLeader, OBSERVER, myGateway.getId(),this.proposedEpoch);
                            return acceptElectionWinner(acceptResult);


                        }

                    }
                    break;


                case FOLLOWING:
                case LEADING: //if the sender is following a leader already or thinks it is the leader
                    Vote flVote = new Vote(messageLeader, messageEpoch, messageState);
                    votes.put(messageSid, flVote);
                    // IF: see if the sender's vote allows me to reach a conclusion based on the election epoch that I'm in,
                    // i.e. gives the majority to some peer among the set of votes in my epoch.
                    if(haveEnoughVotes(votes, flVote) && flVote.getPeerEpoch() >= this.proposedEpoch)
                    {
                        this.proposedLeader = flVote.getCandidateID();
                        this.proposedEpoch = flVote.getPeerEpoch();
                        // if so, accept the election winner. I don't count who voted for who, since
                        // as I receive them I will automatically change my vote to the highest sid, as will everyone else.
                        // As, once someone declares a winner, we are done. We are not worried about / accounting for misbehaving peers.


                            ElectionNotification acceptResult = new ElectionNotification(this.proposedLeader, OBSERVER, myGateway.getId(),this.proposedEpoch);
                            return acceptElectionWinner(acceptResult);

                    }
                    // ELSE: if n is from a later election epoch and/or there are not enough votes in my epoch...
                    // ...before joining their established ensemble, verify that a majority are following the same leader.from that epoch
                    // if so, accept their leader. If not, keep looping on the election loop.
            }
        }

        return resultingLeader;
    }


    private Vote acceptElectionWinner(ElectionNotification n) {
        incomingMessages.clear();
        return new Vote(n.leader, n.peerEpoch, n.state);
    }

    /* * We return true if one of the following three cases hold:
     * * 1- New epoch is higher
     * * 2- New epoch is the same as current epoch, but server id is higher.
     * */
    protected boolean newVoteSupersedesCurrent(long newId, long newEpoch, long curId, long curEpoch) {
        return (newEpoch > curEpoch) || ((newEpoch == curEpoch) && (newId > curId));
    }

    /**
     * Termination predicate. Given a set of votes, determines if have sufficient to declare the end of the election round.
     * * I don't count who voted for who, since as I receive them I will automatically change my vote to the highest sid, as will everyone else
     */
    protected boolean haveEnoughVotes(Map<Long, Vote> votes, Vote vote) {
        int inFavor = 0;
        for (Map.Entry<Long, Vote> entry : votes.entrySet()) {
            if (vote.equals(entry.getValue())) {
                inFavor++;
            }
        }
        return this.myGateway.getQuorumSize() <= inFavor;
    }

}
