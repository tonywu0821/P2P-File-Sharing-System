package unimelb.bitbox;

import unimelb.bitbox.util.Configuration;
import unimelb.bitbox.util.Document;
import unimelb.bitbox.util.FileSystemManager;

import java.io.IOException;
import java.net.*;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import static unimelb.bitbox.Protocol.*;

public class ClientThreadUdp extends Thread {
    private static Logger log = Logger.getLogger(FileSystemManager.class.getName());
    private FileSystemManager fileSystemManager;
    private BlockingQueue<FileSystemManager.FileSystemEvent> fileSystemEvents = new ArrayBlockingQueue<>(1024);
    private Document peer;
    private ConnectionCounter connectionCounter;
    private int syncInterval;
    private ArrayList<ClientThreadUdp> clientThreads;
    private Queue<Document> peersQueue;
    private HashSet<String> visited;
    private boolean flag;
    private DatagramSocket socket;
    private int udpRetries = Integer.parseInt(Configuration.getConfiguration().get("udpRetries").trim());
    private int udpTimeout = Integer.parseInt(Configuration.getConfiguration().get("udpTimeout").trim());
    private HashMap<String,FileWriter> fileWriterHashMap;
    private HashMap<String,Thread> connections;
    ClientThreadUdp(FileSystemManager fileSystemManager, Document peer, ConnectionCounter connectionCounter,
                    ArrayList<ClientThreadUdp> clientThreads,HashMap<String,Thread> connections){
        this.fileSystemManager = fileSystemManager;
        this.peer = peer;
        this.connectionCounter = connectionCounter;
        this.syncInterval = Integer.parseInt(Configuration.getConfiguration().get("syncInterval").trim());
        this.clientThreads = clientThreads;
        this.peersQueue = new LinkedList<>();
        this.visited = new HashSet<>();
        this.flag = true;
        this.fileWriterHashMap = new HashMap<>();
        this.connections = connections;
        start();
    }

    @Override
    public void run(){
        System.out.println("an udp client thread is created");
        String localHost = Configuration.getConfiguration().get("advertisedName").trim();
        String localPort = Configuration.getConfiguration().get("port").trim();
        visited.add(localHost + ":" + localPort);
        try {
            this.socket = new DatagramSocket();
        } catch (SocketException e) {
            // to do remove itself from udpThreads
            e.printStackTrace();
        }
        connectToPeer(peer);
    }

    void processEvent(FileSystemManager.FileSystemEvent fileSystemEvent){
        try {
            this.fileSystemEvents.put(fileSystemEvent);
        } catch (InterruptedException e) {
            //e.printStackTrace();
        }
    }

    public void terminate() throws IOException {
        System.out.println("terminate is called");
        String host = peer.getString("host");
        int port = peer.get("port") instanceof Integer
                ? peer.getInteger("port"): (int) (long) peer.get("port");
        InetAddress address = InetAddress.getByName(host);
        byte[] buffer = invalidCommand("close the connection").getBytes();
        DatagramPacket response = new DatagramPacket(buffer,buffer.length,address,port);
        socket.send(response);
        socket.close();
    }

    private void monitor(DatagramSocket out){
        System.out.println("monitor is running");
        while (flag) {
            try {
                FileSystemManager.FileSystemEvent fileSystemEvent = fileSystemEvents.take();
                Document command = fileSystemEventParser(fileSystemEvent);
                if(command == null) continue;

                byte buffer [] = command.toJson().getBytes();
                String host = peer.getString("host");
                int port = peer.get("port") instanceof Integer ? peer.getInteger("port"): (int) (long) peer.get("port");
                InetAddress address = InetAddress.getByName(host);
                DatagramPacket request = new DatagramPacket(buffer,buffer.length,address,port);
                out.send(request);
                System.out.println("sending: " + command.toJson());
                System.out.println("(To:"+ " host:" + host
                        +" port:" + port + " address:" + address+")");

            } catch (InterruptedException | IOException e) {
                //e.printStackTrace();
                break;
            }
        }
        System.out.println("monitor is closed");
    }

    private void synEventManager(FileSystemManager fileSystemManager){
        System.out.println("synEventManager is running");
        while(flag){
            try{
                log.info("synEvents has been generated");
                for(FileSystemManager.FileSystemEvent fileSystemEvent : fileSystemManager.generateSyncEvents()){
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
        int counter = udpRetries;

        String host = hostPort.getString("host");
        int port = hostPort.get("port") instanceof Integer
                ? hostPort.getInteger("port")
                : (int) (long) hostPort.get("port");
        String hashMapKey = host + ":" + String.valueOf(port);

        while (counter > 0) {
            System.out.println((counter - 1) + " attempts left");
            try {
                InetAddress address = InetAddress.getByName(host);
                System.out.println("connect to port:" + port + " host:" + host + " address:" + address);
                byte buffer[] = handShakeRequestUdp().getBytes();
                DatagramPacket request = new DatagramPacket(buffer, buffer.length, address, port);
                socket.send(request);

                //socket.setSoTimeout(udpTimeout*1000);
                socket.setSoTimeout(5000);
                // recieve data until timeout

                buffer = new byte[20480];
                DatagramPacket handShakeResponsePacket = new DatagramPacket(buffer, buffer.length);

                socket.receive(handShakeResponsePacket);
                //String rcvd = "rcvd from " + handShakeResponsePacket.getAddress() + ", " + handShakeResponsePacket.getPort() + ": " + new String(handShakeResponsePacket.getData(), 0, handShakeResponsePacket.getLength());
                //System.out.println(rcvd);
                String responseStr = new String(buffer, 0, handShakeResponsePacket.getLength());
                //String responseStr = new String(handShakeResponsePacket.getData(),0,handShakeResponsePacket.getLength());
                if (responseStr == null) break;
                System.out.println(responseStr);

                Document handShakeResponse = Document.parse(responseStr);
                if (!isValidCommand(handShakeResponse)) {
                    //System.out.println("throws an invalid command"+handShakeRespond.toJson());
                    String message = "not a valid command.";
                    buffer = invalidCommand(message).getBytes();
                    request = new DatagramPacket(buffer, buffer.length, address, port);
                    socket.send(request);
                    System.out.println("The client closes the connection because receive an invalid command! (code:1)");
                    break;
                }

                if (handShakeResponse.getString("command").equals("HANDSHAKE_RESPONSE")) {
                    System.out.println("HANDSHAKE SUCCESS");
                    connectionCounter.addConnection(hostPort);
                    synchronized (connections) {
                        connections.put(hashMapKey, this);
                    }
                    peer = hostPort;

                } else if (handShakeResponse.getString("command").equals("CONNECTION_REFUSED")) {
                    //Do BFS
                    System.out.println("CONNECTION_REFUSED:" + handShakeResponse.toJson());
                    ArrayList<Document> peers = (ArrayList<Document>) handShakeResponse.get("peers");
                    for (int i = 0; i < peers.size(); i++) {
                        Document peer = peers.get(i);
                        String key = peer.getString("host") + ":" + String.valueOf(peer.get("port"));
                        if (!connectionCounter.isConnected(peer)
                                && !visited.contains(key)) {
                            peersQueue.offer(peer);
                        }
                    }
                    if (peersQueue.size() == 0) break;
                    Document nextHostPort = peersQueue.poll();
                    connectToPeer(nextHostPort);
                } else {
                    String message = "not a handshake response";
                    System.out.println("The client closes the connection because it's not a HANDSHAKE_RESPONSE (code:2)");
                    buffer = invalidCommand(message).getBytes();
                    DatagramPacket response = new DatagramPacket(buffer, buffer.length, address, port);
                    socket.send(response);
                    socket.close();
                    flag = false;
                    break;
                }

                Thread monitorThread = new Thread(() -> monitor(socket));
                Thread synThread = new Thread(() -> synEventManager(fileSystemManager));
                monitorThread.start();
                synThread.start();
                try {
                    boolean tolerate = true;

                    while (flag) {
                        buffer = new byte[20480];
                        DatagramPacket response = new DatagramPacket(buffer, buffer.length);
                        socket.setSoTimeout(udpTimeout * 1000);

                        socket.receive(response);

                        String newCommandStr = new String(buffer, 0, response.getLength());

                        int inCommingPort = response.getPort();
                        InetAddress inCommingAddress = response.getAddress();
                        String inCommingHost = response.getAddress().getHostName();
                        System.out.println("udp client receive command:" + newCommandStr);
                        System.out.println("(From:" + " host:" + inCommingHost
                                + " port:" + inCommingPort + " address:" + inCommingAddress + ")");

                        if (newCommandStr == null) break;

                        Document newCommand = Document.parse(newCommandStr);

                        if (isValidCommand(newCommand)) {
                            switch (newCommand.getString("command")) {
                                case "FILE_CREATE_REQUEST": {
                                    if (inCommingPort == port && inCommingHost.equals(host) && address.equals(inCommingAddress)) {
                                        processFileCreateRequestUdp(newCommand, socket, fileSystemManager,
                                                inCommingAddress, port, fileWriterHashMap);
                                        tolerate = false;
                                    }
                                    break;
                                }
                                case "FILE_CREATE_RESPONSE": {
                                    if (inCommingPort == port && inCommingHost.equals(host) && address.equals(inCommingAddress)) {
                                        processFileCreateResponseUdp(newCommand, socket, fileSystemManager,
                                                inCommingAddress, port);
                                    }
                                    break;
                                }
                                case "FILE_DELETE_REQUEST": {
                                    if (inCommingPort == port && inCommingHost.equals(host) && address.equals(inCommingAddress)) {
                                        processFileDeleteRequestUdp(newCommand, socket, fileSystemManager,
                                                inCommingAddress, port);
                                        tolerate = false;
                                    }
                                    break;
                                }
                                case "FILE_DELETE_RESPONSE": {
                                    if (inCommingPort == port && inCommingHost.equals(host) && address.equals(inCommingAddress)) {
                                        processFileDeleteResponseUdp(newCommand,socket,fileSystemManager,
                                                inCommingAddress,port);
                                    }
                                    break;
                                }
                                case "FILE_MODIFY_REQUEST": {
                                    if (inCommingPort == port && inCommingHost.equals(host) && address.equals(inCommingAddress)) {
                                        processFileModifyRequestUdp(newCommand, socket, fileSystemManager,
                                                inCommingAddress, port, fileWriterHashMap);
                                        tolerate = false;
                                    }
                                    break;
                                }
                                case "FILE_MODIFY_RESPONSE": {
                                    if (inCommingPort == port && inCommingHost.equals(host) && address.equals(inCommingAddress)) {
                                        processFileModifyResponseUdp(newCommand,socket,fileSystemManager,
                                                inCommingAddress,port);
                                    }
                                    break;
                                }
                                case "DIRECTORY_CREATE_REQUEST": {
                                    if (inCommingPort == port && inCommingHost.equals(host) && address.equals(inCommingAddress)) {
                                        processDirectoryCreateRequestUdp(newCommand, socket, fileSystemManager,
                                                inCommingAddress, port);
                                        tolerate = false;
                                    }
                                    break;
                                }
                                case "DIRECTORY_CREATE_RESPONSE": {
                                    if (inCommingPort == port && inCommingHost.equals(host) && address.equals(inCommingAddress)) {
                                        processDirectoryCreateResponseUdp(newCommand,socket,fileSystemManager,
                                                inCommingAddress,port);
                                    }
                                    break;
                                }
                                case "DIRECTORY_DELETE_REQUEST": {
                                    if (inCommingPort == port && inCommingHost.equals(host) && address.equals(inCommingAddress)) {
                                        processDirectoryDeleteRequestUdp(newCommand, socket, fileSystemManager,
                                                inCommingAddress, port);
                                        tolerate = false;

                                    }
                                    break;
                                }
                                case "DIRECTORY_DELETE_RESPONSE": {
                                    if (inCommingPort == port && inCommingHost.equals(host) && address.equals(inCommingAddress)) {
                                        processDirectoryDeleteResponseUdp(newCommand,socket,fileSystemManager,
                                                inCommingAddress,port);
                                    }
                                    break;
                                }
                                case "FILE_BYTES_REQUEST": {
                                    if (inCommingPort == port && inCommingHost.equals(host) && address.equals(inCommingAddress)) {
                                        processFileBytesRequestUdp(newCommand, socket, fileSystemManager, inCommingAddress, port);
                                        tolerate = false;
                                    }
                                    break;
                                }
                                case "FILE_BYTES_RESPONSE": {
                                    if (inCommingPort == port && inCommingHost.equals(host) && address.equals(inCommingAddress)) {
                                        String pathName = newCommand.getString("pathName");
                                        if (fileWriterHashMap.containsKey(pathName)) {
                                            fileWriterHashMap.get(pathName).addCommand(newCommand);
                                        }
                                    }
                                    break;
                                }
                                case "HANDSHAKE_RESPONSE": {
                                    if (inCommingPort == port && inCommingHost.equals(host) && address.equals(inCommingAddress)) {
                                        if (!tolerate) {
                                            buffer = invalidCommand("already send a handShakeRespond").getBytes();
                                            response = new DatagramPacket(buffer, buffer.length, address, port);
                                            socket.send(response);
                                            socket.close();
                                            flag = false;
                                        }
                                    }
                                    break;
                                }
                                case "INVALID_PROTOCOL": {
                                    if (inCommingPort == port && inCommingHost.equals(host) && address.equals(inCommingAddress)) {
                                        socket.close();
                                        flag = false;
                                    }
                                    break;
                                }
                                default: {
                                    if (inCommingPort == port && inCommingHost.equals(host) && address.equals(inCommingAddress)) {
                                        buffer = invalidCommand("not a valid command (1)").getBytes();
                                        response = new DatagramPacket(buffer, buffer.length, address, port);
                                        socket.send(response);
                                        socket.close();
                                        flag = false;
                                        break;
                                    }
                                    break;
                                }
                            }
                        } else {
                            if (address.equals(inCommingAddress)) {
                                buffer = invalidCommand("not a valid command (2)").getBytes();
                                response = new DatagramPacket(buffer, buffer.length, address, port);
                                socket.send(response);
                                socket.close();
                                flag = false;
                            }
                            break;
                        }
                        break;
                    }
                    socket.close();
                    flag = false;
                    connectionCounter.removeConnection(hostPort);
                    monitorThread.interrupt();
                    synThread.interrupt();

                } catch (IOException | NoSuchAlgorithmException e) {
                    socket.close();
                    connectionCounter.removeConnection(hostPort);
                    flag = false;
                    monitorThread.interrupt();
                    synThread.interrupt();
                    break;
                }
                socket.close();
                connectionCounter.removeConnection(hostPort);
                flag = false;
                monitorThread.interrupt();
                synThread.interrupt();
                break;

            } catch (IOException e){
                counter -= 1;
                System.out.println("will try to reconnect the peer soon");
            }
        }

        synchronized (connections){
            connections.remove(hashMapKey);
        }

        synchronized (clientThreads){
            clientThreads.remove(this);
        }

        System.out.println("The udpClient has been safely closed");
    }
}

