package unimelb.bitbox;

import unimelb.bitbox.util.*;
import unimelb.bitbox.util.FileSystemManager.FileSystemEvent;
import java.util.ArrayList;
import java.util.HashMap;


/** A thread manage several client threads.
 * @author Tong-Ing Wu, Lih-Ren Chaung.
 */

public class ClientMaster extends Thread {
    private FileSystemManager fileSystemManager;
    private ArrayList<ClientThread> clientThreads;
    private ArrayList<Document> peersNeedToConnect;
    private ConnectionCounter connectionCounter;
    private HashMap<String,Thread> connections;

    ClientMaster(FileSystemManager fileSystemManager, ConnectionCounter connectionCounter, HashMap<String,Thread> connections){
        this.fileSystemManager = fileSystemManager;
        this.clientThreads = new ArrayList<ClientThread>();
        this.connectionCounter = connectionCounter;
        this.peersNeedToConnect = parsePeers(Configuration.getConfiguration().get("peers").trim());
        this.connections = connections;
        start();
    }

    void processEvent(FileSystemEvent fileSystemEvent) {
        synchronized (clientThreads) {
            for (ClientThread clientThread : clientThreads) {
                clientThread.processEvent(fileSystemEvent);
            }
        }
    }

    public void run() {
        System.out.println("ClientMaster is created and running");

        for(Document peer : peersNeedToConnect){
            ClientThread clientThread = new ClientThread(fileSystemManager,peer,connectionCounter, clientThreads,connections);
            synchronized (clientThreads) {
                clientThreads.add(clientThread);
            }
        }


    }

    void createNewConnection(Document peer){
        System.out.println("createNewConnection");
        ClientThread clientThread = new ClientThread(fileSystemManager,peer,connectionCounter, clientThreads,connections);;
        synchronized (clientThreads) {
            clientThreads.add(clientThread);
        }
    }

    private ArrayList<Document> parsePeers(String peersStr){
        String[] peers = peersStr.split(",");
        ArrayList<Document> peerList = new ArrayList<Document>();
        for(String peer : peers){
            String host = peer.split(":")[0].trim();
            int port = Integer.parseInt(peer.split(":")[1].trim());
            HostPort hostPort = new HostPort(host, port);
            peerList.add(hostPort.toDoc());
        }
        return peerList;
    }
}
