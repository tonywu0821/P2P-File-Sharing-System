package unimelb.bitbox;

import unimelb.bitbox.util.Configuration;
import unimelb.bitbox.util.Document;
import unimelb.bitbox.util.FileSystemManager;
import unimelb.bitbox.util.FileSystemManager.FileSystemEvent;
import unimelb.bitbox.util.HostPort;

import java.util.ArrayList;
import java.util.HashMap;

public class UdpClient extends Thread{
    private FileSystemManager fileSystemManager;
    private ArrayList<ClientThreadUdp> clientThreads;
    private ArrayList<Document> peersNeedToConnect;
    private ConnectionCounter connectionCounter;
    private HashMap<String,Thread> connections;

    UdpClient(FileSystemManager fileSystemManager, ConnectionCounter connectionCounter, HashMap<String,Thread> connections){
        this.fileSystemManager = fileSystemManager;
        this.clientThreads = new ArrayList<ClientThreadUdp>();
        this.connectionCounter = connectionCounter;
        this.peersNeedToConnect = parsePeers(Configuration.getConfiguration().get("peers").trim());
        this.connections = connections;
        start();
    }

    void processEvent(FileSystemEvent fileSystemEvent) {
        synchronized (clientThreads) {
            for (ClientThreadUdp clientThread : clientThreads) {
                clientThread.processEvent(fileSystemEvent);
            }
        }
    }
    public void createNewConnection(Document peer){
        System.out.println("createNewConnection");
        ClientThreadUdp clientThread = new ClientThreadUdp(fileSystemManager,peer,connectionCounter,clientThreads,connections);
        synchronized (clientThreads) {
            clientThreads.add(clientThread);
        }
    }
    public void run() {
        System.out.println("ClientUdpMaster is created and running");

        for(Document peer : peersNeedToConnect){
            ClientThreadUdp clientThread = new ClientThreadUdp(fileSystemManager,peer,connectionCounter, clientThreads,connections);
            synchronized (clientThreads) {
                clientThreads.add(clientThread);
            }
        }


    }

    private ArrayList<Document> parsePeers(String peersStr){
        String[] peers = peersStr.split(",");
        ArrayList<Document> peerList = new ArrayList<Document>();
        for(String peer : peers){
            String host = peer.split(":")[0];
            int port = Integer.parseInt(peer.split(":")[1]);
            HostPort hostPort = new HostPort(host, port);
            peerList.add(hostPort.toDoc());
        }
        return peerList;
    }
}
