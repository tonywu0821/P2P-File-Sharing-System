package unimelb.bitbox;

import unimelb.bitbox.util.Configuration;
import unimelb.bitbox.util.Document;
import unimelb.bitbox.util.FileSystemManager;
import unimelb.bitbox.util.FileSystemManager.FileSystemEvent;
import unimelb.bitbox.util.HostPort;

import java.io.*;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.*;
import java.util.logging.Logger;

import static unimelb.bitbox.Protocol.*;

/** A thread is connected by the other peer.
 * @author Tong-Ing Wu, Lih-Ren Chaung.
 */

public class ServerThread extends Thread{
    private static Logger log = Logger.getLogger(FileSystemManager.class.getName());
    private FileSystemManager fileSystemManager;
    private BlockingQueue<FileSystemEvent> fileSystemEvents = new ArrayBlockingQueue<>(1024);
    private ConnectionCounter connectionCounter;
    private Socket client;
    private int syncInterval;
    private ArrayList<ServerThread> serverThreads;
    private boolean flag;
    private HashMap<String,Thread> connections;

    ServerThread(FileSystemManager fileSystemManager, Socket client, ConnectionCounter connectionCounter,
                 ArrayList<ServerThread> serverThreads, HashMap<String,Thread> connections){
        this.fileSystemManager = fileSystemManager;
        this.connectionCounter = connectionCounter;
        this.client = client;
        this.syncInterval = Integer.parseInt(Configuration.getConfiguration().get("syncInterval").trim());
        this.serverThreads = serverThreads;
        this.flag = true;
        this.connections = connections;
        start();
    }

    void processEvent(FileSystemEvent fileSystemEvent) {
        try {
            this.fileSystemEvents.put(fileSystemEvent);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void monitor(BufferedWriter out){
        while (flag) {
            try {
                FileSystemEvent fileSystemEvent = fileSystemEvents.take();
                Document command = fileSystemEventParser(fileSystemEvent);
                if(command == null) continue;
                log.info("Server Thread is sending:" + command.toJson());
                out.write(command.toJson() + "\n");
                out.flush();
            } catch (InterruptedException | IOException e) {
                //e.printStackTrace();
                break;
            }
        }
        System.out.println("monitorThread is closed");
    }

    private void synEventManager(FileSystemManager fileSystemManager){
        while(flag){
            try{
                log.info("synEvent has been generated");
                for(FileSystemEvent fileSystemEvent : fileSystemManager.generateSyncEvents()){
                    fileSystemEvents.put(fileSystemEvent);
                }
                Thread.sleep(syncInterval*1000);
            } catch (InterruptedException e){
                //e.printStackTrace();
                break;
            }
        }
        System.out.println("synThread is closed");
    }

    public void terminate() throws IOException {
        this.flag = false;
        System.out.println("call terminate");
        //this.interrupt();
        client.close();
    }


    @Override
    public void run(){
        System.out.println("A ServerThread is created and running");

        String incommingHost;
        int incommingPort;
        String hashMapKey = null ;
        while(true) {
            try {
                BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream(), "UTF-8"));
                BufferedWriter out = new BufferedWriter(new OutputStreamWriter(client.getOutputStream(), "UTF-8"));

                if (connectionCounter.isFull()) {
                    try {
                        connectionRefuse(out, connectionCounter, "connection limit reached");
                        client.close();
                        System.out.println("The server closes the connection because it reach connection limit. (code:1)");
                        break;
                    } catch (IOException e) {
                        e.printStackTrace();
                        client.close();
                        System.out.println("The server closes the connection. (code:2)");
                        break;
                    }
                }
                try {
                    String handShakeRequestStr = in.readLine();
                    if (handShakeRequestStr == null) {
                        client.close();
                        System.out.println("The server closes the connection because the client is disconnected (code:3)");
                        break;
                    }
                    Document handShakeRequest = Document.parse(handShakeRequestStr);
                    if (!isValidCommand(handShakeRequest)) {
                        invalidCommandResponse(out, "is not a valid command");
                        client.close();
                        System.out.println("The server closes the connection because an invalid command was received (code:4)");
                        break;
                    }

                    if (!handShakeRequest.getString("command").equals("HANDSHAKE_REQUEST")) {
                        System.out.println("invalid command2" + handShakeRequest.toJson());
                        invalidCommandResponse(out, "not a HANDSHAKE_REQUEST");
                        client.close();
                        System.out.println("The server closes the connection because an invalid command was received. (code:5)");
                        break;
                    }
                    Document hostPort = (Document) handShakeRequest.get("hostPort");
                    if (connectionCounter.isConnected(hostPort)) {
                        connectionRefuse(out, connectionCounter, "already connected");
                        client.close();
                        System.out.println("The server closes the connection the client was already connected. (code:6)");
                        break;
                    } else {
                        handShakeResponse(out);
                        connectionCounter.addIncommingConnection(hostPort);
                        incommingPort = hostPort.get("port") instanceof Integer ? hostPort.getInteger("port"): (int) (long) hostPort.get("port");
                        incommingHost = hostPort.getString("host");
                        hashMapKey = incommingHost + ":" + String.valueOf(incommingPort);
                        synchronized (connections) {
                            connections.put(hashMapKey, this);
                        }
                        Thread monitorThread = new Thread(() -> monitor(out));
                        Thread synThread = new Thread(() -> synEventManager(fileSystemManager));
                        monitorThread.start();
                        synThread.start();

                        try {
                            String newCommandStr = null;
                            while ((newCommandStr = in.readLine()) != null) {
                                Document newCommand = Document.parse(newCommandStr);
                                //System.out.println("newCommand:" + newCommand.toJson());
                                if (isValidCommand(newCommand)) {
                                    switch (newCommand.getString("command")) {
                                        case "FILE_CREATE_REQUEST": {
                                            processFileCreateRequest(newCommand, fileSystemManager, out);
                                            break;
                                        }
                                        case "FILE_CREATE_RESPONSE": {
                                            processFileCreateResponse(newCommand, fileSystemManager, out);
                                            break;
                                        }
                                        case "FILE_DELETE_REQUEST": {
                                            processFileDeleteRequest(newCommand, fileSystemManager, out);
                                            break;
                                        }
                                        case "FILE_DELETE_RESPONSE": {
                                            processFileDeleteResponse(newCommand, fileSystemManager, out);
                                            break;
                                        }
                                        case "FILE_MODIFY_REQUEST": {
                                            processFileModifyRequest(newCommand, fileSystemManager, out);
                                            break;
                                        }
                                        case "FILE_MODIFY_RESPONSE": {
                                            processFileModifyResponse(newCommand, fileSystemManager, out);
                                            break;
                                        }
                                        case "DIRECTORY_CREATE_REQUEST": {
                                            processDirectoryCreateRequest(newCommand, fileSystemManager, out);
                                            break;
                                        }
                                        case "DIRECTORY_CREATE_RESPONSE": {
                                            processDirectoryCreateResponse(newCommand, fileSystemManager, out);
                                            break;
                                        }
                                        case "DIRECTORY_DELETE_REQUEST": {
                                            processDirectoryDeleteRequest(newCommand, fileSystemManager, out);
                                            break;
                                        }
                                        case "DIRECTORY_DELETE_RESPONSE": {
                                            processDirectoryDeleteResponse(newCommand, fileSystemManager, out);
                                            break;
                                        }
                                        case "FILE_BYTES_REQUEST": {
                                            processFileBytesRequest(newCommand, fileSystemManager, out);
                                            break;
                                        }
                                        case "FILE_BYTES_RESPONSE": {
                                            processFileBytesResponse(newCommand, fileSystemManager, out);
                                            break;
                                        }
                                        default: {
                                            invalidCommandResponse(out, "not right time(handShake)");
                                            client.close();
                                            connectionCounter.removeIncommingConnection(hostPort);
                                            flag = false;
                                            monitorThread.interrupt();
                                            synThread.interrupt();
                                            System.out.println("The server closes the connection because an invalid command was received. (code:7)");
                                            break;
                                        }
                                    }
                                } else {
                                    //System.out.println("invalid command4" + newCommand.toJson());
                                    invalidCommandResponse(out, "invalid command");
                                    client.close();
                                    connectionCounter.removeIncommingConnection(hostPort);
                                    monitorThread.interrupt();
                                    synThread.interrupt();
                                    System.out.println("The server closes the connection because an invalid command was received. (code:8)");
                                    break;

                                }
                            }
                            client.close();
                            connectionCounter.removeIncommingConnection(hostPort);
                            flag = false;
                            monitorThread.interrupt();
                            synThread.interrupt();
                            System.out.println("The server closes the connection because the client is disconnected (code:9)");

                        } catch (IOException | NoSuchAlgorithmException e) {
                            //e.printStackTrace();
                            client.close();
                            connectionCounter.removeIncommingConnection(hostPort);
                            flag = false;
                            monitorThread.interrupt();
                            synThread.interrupt();
                            System.out.println("The server closes the connection(code:10)");
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    client.close();
                    System.out.println("The server closes the connection. (code:11)");
                    break;
                }
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("The server closes the connection. (code:12)");
                break;
            }
            break;
        }

        synchronized (connections) {
            if(hashMapKey != null) {
                connections.remove(hashMapKey);
            }
        }

        synchronized (serverThreads) {
            serverThreads.remove(this);
        }
        System.out.println("The server has been safely closed");
    }
}

