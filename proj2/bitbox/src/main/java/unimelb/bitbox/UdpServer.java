package unimelb.bitbox;

import unimelb.bitbox.util.*;
import unimelb.bitbox.util.FileSystemManager;
import unimelb.bitbox.util.FileSystemManager.FileSystemEvent;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import static unimelb.bitbox.Protocol.*;

public class UdpServer extends Thread{
    private static Logger log = Logger.getLogger(FileSystemManager.class.getName());
    private FileSystemManager fileSystemManager;
    private ConnectionCounter connectionCounter;
    private BlockingQueue<FileSystemEvent> fileSystemEvents = new ArrayBlockingQueue<>(1024);
    private Boolean flag = true;
    private int syncInterval;
    private DatagramSocket socket;
    private HashMap<String,FileWriter> fileWriterHashMap;
    private HashMap<String,Thread> connections;
    private HashMap<String,Boolean> tolerateMap;
    UdpServer(FileSystemManager fileSystemManager ,ConnectionCounter connectionCounter,HashMap<String,Thread> connections) throws SocketException {
        this.fileSystemManager = fileSystemManager;
        this.connectionCounter = connectionCounter;
        this.syncInterval = Integer.parseInt(Configuration.getConfiguration().get("syncInterval").trim());
        int udpPort = Integer.parseInt(Configuration.getConfiguration().get("udpPort").trim());
        this.socket = new DatagramSocket(udpPort);
        this.fileWriterHashMap = new HashMap<>();
        this.connections = connections;
        this.tolerateMap = new HashMap<>();
        this.start();
    }


    void terminate(Document inCommingConnection) throws IOException {
        Document udpConnection = connectionCounter.getUdpConnectionByInComming(inCommingConnection);
        int port = udpConnection.get("port") instanceof Integer
                ? udpConnection.getInteger("port")
                : (int) (long) udpConnection.get("port");
        String host = udpConnection.getString("host");
        InetAddress address = InetAddress.getByName(host);
        byte buff[] = invalidCommand("Bit box client ask me to disconnect").getBytes();
        DatagramPacket disconnection = new DatagramPacket(buff,buff.length,address,port);
        socket.send(disconnection);

        //connectionCounter.removeUdpConnection(udpConnection);
        //String hashMapKey = connectionCounter.getInCommingConnectionStrByUdp(udpConnection);
        //synchronized (connections){
        //    connections.remove(hashMapKey);
        //}
    }

    void processEvent(FileSystemEvent fileSystemEvent) {
        try {
            this.fileSystemEvents.put(fileSystemEvent);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void monitor(DatagramSocket out){
        System.out.println("monitor is started!!");
        while (flag) {
            try {
                FileSystemEvent fileSystemEvent = fileSystemEvents.take();
                Document command = fileSystemEventParser(fileSystemEvent);
                if(command == null) continue;
                byte buffer [] = command.toJson().getBytes();
                ArrayList<Document> connections = connectionCounter.getUdpConnections();
                //if (connections == null) continue;
                for(Document connection:connections){
                    String host = connection.getString("host");
                    int port = connection.get("port") instanceof Integer ? connection.getInteger("port"): (int) (long) connection.get("port");
                    InetAddress address = InetAddress.getByName(host);
                    DatagramPacket request = new DatagramPacket(buffer,buffer.length,address,port);
                    out.send(request);
                    System.out.println("sending: " + command.toJson());
                    System.out.println("(To:"+ " host:" + host
                            +" port:" + port + " address:" + address+")");
                }
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
                break;
            }
        }
        System.out.println("monitorThread is closed");
    }

    private void synEventManager(FileSystemManager fileSystemManager){
        System.out.println("synEventManager is started!!");
        while(flag){
            try {
                log.info("synEvent has been generated");
                System.out.println("synEventManager is generating synEvent!");
                for(FileSystemEvent fileSystemEvent : fileSystemManager.generateSyncEvents()){
                    fileSystemEvents.put(fileSystemEvent);
                }
                Thread.sleep(syncInterval*1000);
            } catch (InterruptedException e){
                e.printStackTrace();
                break;
            }
        }
        System.out.println("synThread is closed");
    }

    @Override
    public void run() {

        System.out.println("Udp Server is started!!");
        Thread monitorThread = new Thread(() -> monitor(socket));
        Thread synThread = new Thread(() -> synEventManager(fileSystemManager));

        monitorThread.start();
        synThread.start();

        while (true){
            byte buffer[] = new byte[20480];
            DatagramPacket request = new DatagramPacket(buffer, buffer.length);
            try {
                socket.receive(request);
                String requestStr = new String(buffer, 0, request.getLength());
                Document newCommand = Document.parse(requestStr);

                InetAddress inCommingAddress = request.getAddress();
                String host = inCommingAddress.getHostAddress();
                int port = request.getPort();
                System.out.println("host:"+host+" port:" +port);
                HostPort hostPort = new HostPort(host, port);
                String tolerateKey = host + ":" + String.valueOf(port);
                System.out.println("Udp Server receive:" + requestStr);
                System.out.println("(From:"+ " host:" + host +" port:" + port +")");

                if (isValidCommand(newCommand)) {
                    switch (newCommand.getString("command")){
                        case "FILE_CREATE_REQUEST": {
                            if(connectionCounter.isConnectedUdp(hostPort.toDoc())){
                                tolerateMap.put(tolerateKey,false);
                                System.out.println("YYYYY");
                                processFileCreateRequestUdp(newCommand,socket,fileSystemManager,
                                        inCommingAddress,port,fileWriterHashMap);
                            }
                            break;
                        }
                        case "FILE_CREATE_RESPONSE": {
                            if(connectionCounter.isConnectedUdp(hostPort.toDoc())){
                                processFileCreateResponseUdp(newCommand,socket,fileSystemManager,
                                        inCommingAddress,port);
                            }
                            break;
                        }
                        case "FILE_DELETE_REQUEST": {
                            if(connectionCounter.isConnectedUdp(hostPort.toDoc())){
                                tolerateMap.put(tolerateKey,false);
                                processFileDeleteRequestUdp(newCommand, socket, fileSystemManager,
                                        inCommingAddress,  port);
                            }
                            break;
                        }
                        case "FILE_DELETE_RESPONSE": {
                            if(connectionCounter.isConnectedUdp(hostPort.toDoc())){
                                processFileDeleteResponseUdp(newCommand,socket,fileSystemManager,
                                        inCommingAddress,port);
                            }
                            break;
                        }
                        case "FILE_MODIFY_REQUEST": {
                            if(connectionCounter.isConnectedUdp(hostPort.toDoc())){
                                tolerateMap.put(tolerateKey,false);
                                processFileModifyRequestUdp(newCommand,socket,fileSystemManager,
                                        inCommingAddress,port,fileWriterHashMap);
                            }
                            break;
                        }
                        case "FILE_MODIFY_RESPONSE": {
                            if(connectionCounter.isConnectedUdp(hostPort.toDoc())){
                                processFileModifyResponseUdp(newCommand,socket,fileSystemManager,
                                        inCommingAddress,port);
                            }
                            break;
                        }
                        case "DIRECTORY_CREATE_REQUEST": {
                            if(connectionCounter.isConnectedUdp(hostPort.toDoc())){
                                tolerateMap.put(tolerateKey,false);
                                processDirectoryCreateRequestUdp(newCommand, socket, fileSystemManager,
                                        inCommingAddress, port);
                            }
                            break;
                        }
                        case "DIRECTORY_CREATE_RESPONSE": {
                            if(connectionCounter.isConnectedUdp(hostPort.toDoc())){
                                processDirectoryCreateResponseUdp(newCommand,socket,fileSystemManager,
                                        inCommingAddress,port);
                            }
                            break;
                        }
                        case "DIRECTORY_DELETE_REQUEST": {
                            if(connectionCounter.isConnectedUdp(hostPort.toDoc())){
                                tolerateMap.put(tolerateKey,false);
                                processDirectoryDeleteRequestUdp(newCommand, socket, fileSystemManager,
                                        inCommingAddress, port);
                            }
                            break;
                        }
                        case "DIRECTORY_DELETE_RESPONSE": {
                            if(connectionCounter.isConnectedUdp(hostPort.toDoc())){
                                processDirectoryDeleteResponseUdp(newCommand,socket,fileSystemManager,
                                        inCommingAddress,port);
                            }
                            break;
                        }
                        case "FILE_BYTES_REQUEST": {
                            if(connectionCounter.isConnectedUdp(hostPort.toDoc())){
                                tolerateMap.put(tolerateKey,false);
                                processFileBytesRequestUdp(newCommand,socket,fileSystemManager,inCommingAddress,port);
                            }
                            break;
                        }
                        case "FILE_BYTES_RESPONSE": {
                            if(connectionCounter.isConnectedUdp(hostPort.toDoc())){
                                String pathName = newCommand.getString("pathName");
                                if(fileWriterHashMap.containsKey(pathName)){
                                    fileWriterHashMap.get(pathName).addCommand(newCommand);
                                }
                            }
                            break;
                        }
                        case "HANDSHAKE_REQUEST":{
                            Document newConnection = (Document) newCommand.get("hostPort");
                            System.out.println("hostPort:" + newConnection.toJson());
                            if(connectionCounter.isConnectedUdp(hostPort.toDoc()) | !connectionCounter.isFull()){
                                if(tolerateMap.containsKey(tolerateKey)){
                                    System.out.println("contain tolerateKey");
                                    if(!tolerateMap.get(tolerateKey)){
                                        System.out.println("send too many HANDSHAKE_REQUEST!");
                                        byte buff[] = invalidCommand("send too many HANDSHAKE_REQUEST!").getBytes();
                                        DatagramPacket disconnection = new DatagramPacket(buff, buff.length, inCommingAddress, port);
                                        socket.send(disconnection);
                                        connectionCounter.removeUdpConnection(hostPort.toDoc());
                                        String hashMapKey = connectionCounter.getInCommingConnectionStrByUdp(hostPort.toDoc());
                                        synchronized (connections) {
                                            connections.remove(hashMapKey);
                                        }
                                        break;
                                    }
                                }
                                System.out.println("no contain tolerateKey");
                                tolerateMap.put(tolerateKey,true);
                                String newConnectionHost = newConnection.getString("host");
                                int newConnectionPort = newConnection.get("port") instanceof Integer
                                        ? newConnection.getInteger("port")
                                        : (int) (long) newConnection.get("port");
                                String hashMapKey = newConnectionHost + ":" + String.valueOf(newConnectionPort);
                                synchronized (connections){
                                    connections.put(hashMapKey,this);
                                    System.out.println("hashMapKey:" + hashMapKey);
                                }
                                connectionCounter.addUdpConnection(hostPort.toDoc(),newConnection);
                                buffer = handShakeResponseUdp().getBytes();
                                DatagramPacket response = new DatagramPacket(buffer,buffer.length,request.getAddress(),request.getPort());
                                socket.send(response);
                                System.out.println("sending:"+handShakeResponseUdp());
                            } else {
                                buffer = connectionRefuseUdp(connectionCounter,"connection limit reached").getBytes();
                                DatagramPacket response = new DatagramPacket(buffer,buffer.length,request.getAddress(),request.getPort());
                                socket.send(response);
                            }
                            break;
                        }
                        case "INVALID_PROTOCOL":{
                            if(connectionCounter.isConnectedUdp(hostPort.toDoc())) {
                                connectionCounter.removeUdpConnection(hostPort.toDoc());
                                String hashMapKey = connectionCounter.getInCommingConnectionStrByUdp(hostPort.toDoc());
                                synchronized (connections) {
                                    connections.remove(hashMapKey);
                                }
                            }
                            break;
                        }
                        default: {
                            break;
                        }
                    }
                } else {
                    if(connectionCounter.isConnectedUdp(hostPort.toDoc())) {
                        System.out.println("sending invalid");
                        byte buff[] = invalidCommand("invalid command!").getBytes();
                        DatagramPacket disconnection = new DatagramPacket(buff, buff.length, inCommingAddress, port);
                        socket.send(disconnection);
                        connectionCounter.removeUdpConnection(hostPort.toDoc());
                        String hashMapKey = connectionCounter.getInCommingConnectionStrByUdp(hostPort.toDoc());
                        synchronized (connections) {
                            connections.remove(hashMapKey);
                        }
                    }
                }
            } catch (IOException | NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

        }
    }

}
