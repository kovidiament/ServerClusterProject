package edu.yu.cs.fall2019.intro_to_distributed.stage4;

public class HeartBeatEntry
{
    long seqNum;
    long serverID;
    long time;
    public  HeartBeatEntry(long seqNum, long serverID)
    {
        this.time = System.nanoTime();
        this.serverID = serverID;
        this.seqNum = seqNum;
    }
    public String toString()
    {
        return "ServerID: "+serverID+" SeqNum: "+seqNum+" NanoTime: "+time;
    }
    public long getSeqNum(){return seqNum;}
    public long getServerID(){return serverID;}
    public long getTime(){return time;}
}
