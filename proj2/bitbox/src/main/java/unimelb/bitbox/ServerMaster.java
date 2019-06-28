package unimelb.bitbox;

import unimelb.bitbox.util.*;
import unimelb.bitbox.util.FileSystemManager.FileSystemEvent;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

/** A thread manage several server threads.
 * @author Tong-Ing Wu, Lih-Ren Chaung.
 */

public class ServerMaster extends Thread {
    private FileSystemManager fileSystemManager;
    private ArrayList<ServerThread> serverThreads;
    private ConnectionCounter connectionCounter;
    private int port;
    private HashMap<String,Thread> connections;
    ServerMaster(FileSystemManager fileSystemManager, ConnectionCounter connectionCounter, HashMap<String,Thread> connections){
        this.fileSystemManager = fileSystemManager;
        this.serverThreads = new ArrayList<ServerThread>();
        this.connectionCounter = connectionCounter;
        this.port = Integer.parseInt(Configuration.getConfiguration().get("port").trim());
        this.connections = connections;
        start();
    }
    void processEvent(FileSystemEvent fileSystemEvent){
        synchronized (serverThreads) {
            for (ServerThread serverThread : serverThreads) {
                serverThread.processEvent(fileSystemEvent);
            }
        }
    }
    @Override
    public void run(){
        try{
            System.out.println("ServerMaster is created and running");
            System.out.println("Waiting for client connection..");
            ServerSocket serverSkt = new ServerSocket(port);
            // Wait for connections.
            while (true) {
                Socket client = serverSkt.accept();
                // Start a new thread for a connection
                ServerThread serverThread = new ServerThread(fileSystemManager,client,connectionCounter, serverThreads,connections);
                synchronized (serverThreads) {
                    serverThreads.add(serverThread);
                }


            }
        } catch(IOException e){
            e.printStackTrace();
        }
    }
}


