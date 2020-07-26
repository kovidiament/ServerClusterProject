package edu.yu.cs.fall2019.intro_to_distributed.stage4;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

public class ClientImpl implements Client
{
    String host;
    int portNum;
    public ClientImpl(String hostName, int hostPort) throws MalformedURLException
    {
        this.host = hostName;
        this.portNum = hostPort;
    }
    @Override
    public Response compileAndRun(String src) throws IOException {
        String address = "http://"+host+":"+portNum+"/compileandrun";
        URL theURL = new URL(address);
        HttpURLConnection serverConn = (HttpURLConnection)theURL.openConnection();
        serverConn.setRequestMethod("GET");
        serverConn.setDoOutput(true);
        DataOutputStream writer = new DataOutputStream(serverConn.getOutputStream());
        writer.writeBytes(src);
        writer.flush();
        writer.close();
        StringBuffer response = new StringBuffer();
        int responseCode = serverConn.getResponseCode();
        if(responseCode == 200)
        {
            BufferedReader in = new BufferedReader(new InputStreamReader(serverConn.getInputStream()));
            String inputLine;
            while((inputLine = in.readLine()) != null)
            {
                response.append(inputLine+"\n");
            }
            in.close();
        }
        else{
            BufferedReader errorIn = new BufferedReader(new InputStreamReader(serverConn.getErrorStream()));
            String inputErrorLine;
            while((inputErrorLine = errorIn.readLine()) != null)
            {
                response.append(inputErrorLine+"\n");
            }
            errorIn.close();
        }
        Response serverResponse = new Response(responseCode, response.toString());
        return serverResponse;

    }
}
