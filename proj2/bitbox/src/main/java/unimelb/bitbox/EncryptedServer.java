package unimelb.bitbox;

import unimelb.bitbox.util.Configuration;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

public class EncryptedServer extends Thread{
    ConnectionCounter connectionCounter;
    int port;
    private HashMap<String,Thread> connections;
    private ClientMaster clientMaster;
    private UdpClient udpClient;
    private Boolean mode;
    EncryptedServer(ConnectionCounter connectionCounter, HashMap<String,Thread> connections, ClientMaster clientMaster){
        this.connectionCounter = connectionCounter;
        this.port = Integer.parseInt(Configuration.getConfiguration().get("clientPort").trim());
        this.connections = connections;
        this.clientMaster = clientMaster;
        this.mode = false;
        start();
    }
    EncryptedServer(ConnectionCounter connectionCounter, HashMap<String,Thread> connections, UdpClient udpClient){
        this.connectionCounter = connectionCounter;
        this.port = Integer.parseInt(Configuration.getConfiguration().get("clientPort").trim());
        this.connections = connections;
        this.udpClient = udpClient;
        this.mode = true;
        start();
    }


    @Override
    public void run(){
        try {
            System.out.println("EncryptedMaster is created and running");
            System.out.println("Waiting for client connection..");
            ServerSocket serverSkt = new ServerSocket(port);
            while (true) {
                Socket client = serverSkt.accept();
                if(mode){
                    EncryptedServerThread serverThread = new EncryptedServerThread(client,connectionCounter,connections,udpClient);
                    serverThread.start();
                }else{
                    EncryptedServerThread serverThread = new EncryptedServerThread(client,connectionCounter,connections,clientMaster);
                    serverThread.start();
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
