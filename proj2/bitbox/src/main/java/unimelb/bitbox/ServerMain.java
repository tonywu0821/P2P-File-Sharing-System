package unimelb.bitbox;

import unimelb.bitbox.util.*;
import unimelb.bitbox.util.FileSystemManager.FileSystemEvent;

import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.logging.Logger;


public class ServerMain implements FileSystemObserver {
    private static Logger log = Logger.getLogger(ServerMain.class.getName());
    protected FileSystemManager fileSystemManager;
    private ClientMaster clientMaster;
    private ServerMaster serverMaster;
    private Boolean mode;
    private UdpServer udpServer;
    private UdpClient udpClient;

    ServerMain() throws NumberFormatException, IOException, NoSuchAlgorithmException {
        fileSystemManager = new FileSystemManager(Configuration.getConfigurationValue("path").trim(), this);
        HashMap<String, Thread> connections = new HashMap<>();

        if(Configuration.getConfigurationValue("mode").trim().equals("udp")){
            mode = true;
            System.out.println("mode is udp");
        }
        else {
            mode = false;
            System.out.println("mode is tcp");
        }
        synchronized (fileSystemManager) {
            ConnectionCounter connectionCounter;
            EncryptedServer encryptedServer;
            if(mode){
                connectionCounter = new ConnectionCounter();
                udpServer = new UdpServer(fileSystemManager, connectionCounter, connections);
                udpClient = new UdpClient(fileSystemManager, connectionCounter, connections);
                encryptedServer = new EncryptedServer(connectionCounter, connections,udpClient);
            }
            else {
                connectionCounter = new ConnectionCounter();
                clientMaster = new ClientMaster(fileSystemManager, connectionCounter, connections);
                serverMaster = new ServerMaster(fileSystemManager, connectionCounter, connections);
                encryptedServer = new EncryptedServer(connectionCounter, connections,clientMaster);
            }
        }
    }

    @Override
    public void processFileSystemEvent(FileSystemEvent fileSystemEvent) {
        // TODO: process events
        if(mode){
            udpServer.processEvent(fileSystemEvent);
            udpClient.processEvent(fileSystemEvent);
        }
        else {
            clientMaster.processEvent(fileSystemEvent);
            serverMaster.processEvent(fileSystemEvent);
        }
    }
}
