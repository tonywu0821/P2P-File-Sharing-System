package unimelb.bitbox;

import unimelb.bitbox.util.*;
import unimelb.bitbox.util.FileSystemManager.FileSystemEvent;

import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Logger;


public class ServerMain implements FileSystemObserver {
    private static Logger log = Logger.getLogger(ServerMain.class.getName());
    protected FileSystemManager fileSystemManager;
    private ClientMaster clientMaster;
    private ServerMaster serverMaster;
    private ConnectionCounter connectionCounter;


    public ServerMain() throws NumberFormatException, IOException, NoSuchAlgorithmException {
        fileSystemManager = new FileSystemManager(Configuration.getConfigurationValue("path"), this);
        synchronized (fileSystemManager) {
            connectionCounter = new ConnectionCounter();
            clientMaster = new ClientMaster(fileSystemManager, connectionCounter);
            serverMaster = new ServerMaster(fileSystemManager, connectionCounter);
        }
    }

    @Override
    public void processFileSystemEvent(FileSystemEvent fileSystemEvent) {
        // TODO: process events
        clientMaster.processEvent(fileSystemEvent);
        serverMaster.processEvent(fileSystemEvent);
    }

}
