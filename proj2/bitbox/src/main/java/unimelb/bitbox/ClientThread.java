package unimelb.bitbox;

import unimelb.bitbox.util.Configuration;
import unimelb.bitbox.util.Document;
import unimelb.bitbox.util.FileSystemManager;
import unimelb.bitbox.util.FileSystemManager.FileSystemEvent;

import java.io.*;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import static unimelb.bitbox.Protocol.*;


/** A thread connect to the other peer.
 * @author Tong-Ing Wu, Lih-Ren Chaung.
 */

public class ClientThread extends Thread {
    private static Logger log = Logger.getLogger(FileSystemManager.class.getName());
    private FileSystemManager fileSystemManager;
    private BlockingQueue<FileSystemEvent> fileSystemEvents = new ArrayBlockingQueue<>(1024);
    private Document peer;
    private ConnectionCounter connectionCounter;
    private int syncInterval;
    private ArrayList<ClientThread> clientThreads;
    private Queue<Document> peersQueue;
    private HashSet<String> visited;
    private boolean flag;
    private HashMap<String,Thread> connections;
    private Socket toPeer;

    ClientThread(FileSystemManager fileSystemManager, Document peer, ConnectionCounter connectionCounter,
                 ArrayList<ClientThread> clientThreads,HashMap<String,Thread> connections){
        this.fileSystemManager = fileSystemManager;
        this.peer = peer;
        this.connectionCounter = connectionCounter;
        this.syncInterval = Integer.parseInt(Configuration.getConfiguration().get("syncInterval").trim());
        this.clientThreads = clientThreads;
        this.peersQueue =  new LinkedList<Document>();
        this.visited = new HashSet<String>();
        this.flag = true;
        this.connections = connections;
        start();
    }

    void processEvent(FileSystemEvent fileSystemEvent){
        try {
            this.fileSystemEvents.put(fileSystemEvent);
        } catch (InterruptedException e) {
            //e.printStackTrace();
        }
    }

    @Override
    public void run(){
        String localHost = Configuration.getConfiguration().get("advertisedName").trim();
        String localPort = Configuration.getConfiguration().get("port").trim();
        visited.add(localHost + ":" +localPort);

        connectToPeer(peer);
    }

    public void terminate() throws IOException {
        this.flag = false;
        System.out.println("call terminate");
        //this.interrupt();
        toPeer.close();
    }

    private void monitor(BufferedWriter out){
        while (flag) {
            try {
                FileSystemEvent fileSystemEvent = fileSystemEvents.take();
                Document command = fileSystemEventParser(fileSystemEvent);
                if(command == null) continue;
                log.info("clientThread is sending:" + command.toJson());
                out.write(command.toJson() + "\n");
                out.flush();
            } catch (InterruptedException | IOException e) {
                //e.printStackTrace();
                break;
            }
        }
        System.out.println("monitor is closed");
    }

    private void synEventManager(FileSystemManager fileSystemManager){
        while(flag){
            try{
                log.info("synEvents has been generated");
                for(FileSystemEvent fileSystemEvent : fileSystemManager.generateSyncEvents()){
                    fileSystemEvents.put(fileSystemEvent);
                }
                Thread.sleep(syncInterval*1000);
            } catch (InterruptedException e){
                //e.printStackTrace();
                break;
            }
        }
        System.out.println("synEventManager is closed");
    }

    private void connectToPeer(Document hostPort) {
        int counter = 5;

        String host = hostPort.getString("host");
        int port = hostPort.get("port") instanceof Integer ? hostPort.getInteger("port"): (int) (long) hostPort.get("port");
        String hashMapKey = host + ":" + port;
        while (counter > 0) {
            System.out.println( (counter-1) + " attempts left");
            try {
                System.out.println("connecting to " + host + ":" + port);
                String visitedPeerStr = host + ":" + Integer.toString(port);
                visited.add(visitedPeerStr);

                toPeer = new Socket(host, port);

                BufferedReader in = new BufferedReader(new InputStreamReader(toPeer.getInputStream(), "UTF-8"));
                BufferedWriter out = new BufferedWriter(new OutputStreamWriter(toPeer.getOutputStream(), "UTF-8"));

                // sending HANDSHAKE_REQUEST
                handShakeRequest(out);

                // reading HANDSHAKE_RESPONSE
                String handShakeRespondStr = in.readLine();
                if(handShakeRespondStr == null) break ;

                Document handShakeRespond = Document.parse(handShakeRespondStr);
                //System.out.println("client received command: " + handShakeRespond.toJson());
                if (!isValidCommand(handShakeRespond)) {
                    //System.out.println("throws an invalid command"+handShakeRespond.toJson());
                    String message = "not a valid command.";
                    invalidCommandResponse(out, message);
                    toPeer.close();
                    System.out.println("The client closes the connection because receive invalid command! (code:1)");
                    break;
                }

                if (handShakeRespond.getString("command").equals("HANDSHAKE_RESPONSE")) {
                    System.out.println("HANDSHAKE SUCCESS");
                    connectionCounter.addConnection(hostPort);
                    synchronized (connections){
                        connections.put(hashMapKey,this);
                    }
                }
                else if(handShakeRespond.getString("command").equals("CONNECTION_REFUSED")){
                    //Do BFS
                    System.out.println("CONNECTION_REFUSED:"+handShakeRespond.toJson());
                    ArrayList<Document> peers = (ArrayList<Document>) handShakeRespond.get("peers");
                    for(int i = 0; i < peers.size(); i++){
                        Document peer = peers.get(i);
                        String key = peer.getString("host") + ":" + String.valueOf(peer.get("port"));
                        if(!connectionCounter.isConnected(peer)
                                && !visited.contains(key)){
                            peersQueue.offer(peer);
                        }
                    }
                    if(peersQueue.size() == 0) break;
                    Document nextHostPort = peersQueue.poll();
                    connectToPeer(nextHostPort);

                } else {
                    String message = "not a handshake response";
                    invalidCommandResponse(out, message);
                    toPeer.close();
                    System.out.println("The client closes the connection because it's not a HANDSHAKE_RESPONSE (code:2)");
                }

                Thread monitorThread = new Thread(() -> monitor(out));
                Thread synThread = new Thread(() -> synEventManager(fileSystemManager));
                monitorThread.start();
                synThread.start();

                try{
                    String newCommandStr = null;
                    while ((newCommandStr = in.readLine()) != null && flag) {
                        Document newCommand = Document.parse(newCommandStr);
                        //System.out.println("newCommand:" + newCommand.toJson());
                        if (isValidCommand(newCommand)) {
                            switch (newCommand.getString("command")) {
                                case "FILE_CREATE_REQUEST": {
                                    processFileCreateRequest(newCommand,fileSystemManager,out);
                                    break;
                                }
                                case "FILE_CREATE_RESPONSE": {
                                    processFileCreateResponse(newCommand,fileSystemManager,out);
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
                                    processFileModifyRequest(newCommand,fileSystemManager,out);
                                    break;
                                }
                                case "FILE_MODIFY_RESPONSE": {
                                    processFileModifyResponse(newCommand,fileSystemManager,out);
                                    break;
                                }
                                case "DIRECTORY_CREATE_REQUEST": {
                                    processDirectoryCreateRequest(newCommand,fileSystemManager,out);
                                    break;
                                }
                                case "DIRECTORY_CREATE_RESPONSE": {
                                    processDirectoryCreateResponse(newCommand,fileSystemManager,out);
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
                                    toPeer.close();
                                    System.out.println("The client close the connection(code:3)");
                                    connectionCounter.removeConnection(hostPort);
                                    flag = false;
                                    monitorThread.interrupt();
                                    synThread.interrupt();
                                    break;
                                }
                            }
                        } else {
                            invalidCommandResponse(out, "invalid command");
                            toPeer.close();
                            connectionCounter.removeConnection(hostPort);
                            flag = false;
                            monitorThread.interrupt();
                            synThread.interrupt();
                            System.out.println("The client closes the connection.(code:4)");
                            break;
                        }
                    }
                    toPeer.close();
                    connectionCounter.removeConnection(hostPort);
                    flag = false;
                    monitorThread.interrupt();
                    synThread.interrupt();
                    System.out.println("The client closes the connection.(code:5)");
                    break;
                } catch (IOException | NoSuchAlgorithmException e) {
                    //e.printStackTrace();
                    toPeer.close();
                    connectionCounter.removeConnection(hostPort);
                    flag = false;
                    monitorThread.interrupt();
                    synThread.interrupt();
                    System.out.println("The client closes the connection.(code:6)");
                    break;
                }
            } catch (IOException e){
                //e.printStackTrace();
                counter -= 1;
                System.out.println("will try to reconnect the peer after 5 sec");
                try{
                    Thread.sleep(5000);
                } catch (InterruptedException e2){
                    //e2.printStackTrace();
                }
            }
        }

        synchronized (connections){
            connections.remove(hashMapKey);
        }
        System.out.println("The client's connection hashMap has been removed");

        synchronized (clientThreads){
            clientThreads.remove(this);
        }
        System.out.println("The client has been safely closed");
    }

}
