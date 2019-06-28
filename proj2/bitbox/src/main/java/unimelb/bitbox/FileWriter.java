package unimelb.bitbox;

import unimelb.bitbox.util.Document;
import unimelb.bitbox.util.FileSystemManager;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class FileWriter extends Thread{
    private FileSystemManager fileSystemManager;
    private DatagramSocket socket;
    private InetAddress address;
    private int port;
    private Document fileDescriptor;
    private String pathName;
    private String md5;
    private int blockSize;
    private long lastModified;
    private long fileSize;
    private long position;
    private long remainingSize;
    private BlockingQueue<Document> fileBytesResponses = new ArrayBlockingQueue<>(1024);
    private Boolean flag = true;
    FileWriter(DatagramSocket socket, String pathName, Document fileDescriptor, InetAddress address, int port ,
               FileSystemManager fileSystemManager, int blockSize){
        this.socket = socket;
        this.pathName = pathName;
        this.fileDescriptor = fileDescriptor;
        this.md5 = fileDescriptor.getString("md5");
        this.lastModified = fileDescriptor.getLong("lastModified");
        this.fileSize  = fileDescriptor.getLong("fileSize");
        this.address = address;
        this.port = port;
        this.fileSystemManager = fileSystemManager;
        this.blockSize = blockSize;
        this.position = 0;
        this.remainingSize = fileSize;
        start();
    }

    public void run(){
        try {
            System.out.println("fileWriterIsRunning!");
            System.out.println("blockSize:" + blockSize + " remainingSize:" +remainingSize + "position:" + position);
            if (remainingSize > blockSize) {
                //System.out.println("1");
                fileBytesRequest(position, blockSize);
            } else {
                //System.out.println("2");
                fileBytesRequest(position, remainingSize);
            }
            while (flag) {
                Thread.sleep(200);
                int waitingTimes = 0;
                while (fileBytesResponses.size() == 0) {
                    waitingTimes += 1;
                    //System.out.println("fileWriterIsWatting!" + waitingTimes);
                    if (waitingTimes % 50 == 0) {
                        if (remainingSize > blockSize) {
                            //System.out.println("3");
                            fileBytesRequest(position, blockSize);
                        } else {
                            //System.out.println("4");
                            fileBytesRequest(position, remainingSize);
                        }
                    }

                    if(waitingTimes > 500){
                        flag = false;
                        break;
                    }
                    Thread.sleep(200);
                }
                if (flag) {
                    Document command = fileBytesResponses.take();
                    waitingTimes = 0;
                    fileWriteBytes(command);
                }
            }
            System.out.println("cancel file loader of "+ pathName + " because there is no response for a long time!");
            fileSystemManager.cancelFileLoader(pathName);
        } catch (InterruptedException | IOException | NoSuchAlgorithmException e){
            try {
                System.out.println("cancel file loader of "+ pathName + " because there some errors happen!");
                fileSystemManager.cancelFileLoader(pathName);
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            e.printStackTrace();
        }
    }

    private void fileWriteBytes(Document command) throws IOException, NoSuchAlgorithmException {
        Document fileDescriptor = (Document) command.get("fileDescriptor");
        long fileSize = fileDescriptor.getLong("fileSize");
        String md5 = fileDescriptor.getString("md5");
        long lastModified = fileDescriptor.getLong("lastModified");
        String pathName = command.getString("pathName");
        long position = command.getLong("position");
        long length = command.getLong("length");

        if (this.position == position && this.fileSize == fileSize && this.md5.equals(md5)
                && this.lastModified == lastModified) {
            if (command.getBoolean("status")) {
                String content = command.getString("content");
                byte[] readBytes = Base64.getDecoder().decode(content);
                ByteBuffer src = ByteBuffer.wrap(readBytes);
                //writing the file.
                fileSystemManager.writeFile(pathName, src, position);
                this.position += length;
                this.remainingSize = this.fileSize - this.position;
                System.out.println("remainingSize:"+ remainingSize + "position:" +position);
                if (this.remainingSize > 0) {
                    if (this.remainingSize > blockSize) {
                        fileBytesRequest(this.position, blockSize);
                    } else {
                        fileBytesRequest(this.position, this.remainingSize);
                    }
                } else {
                    if (fileSystemManager.checkWriteComplete(pathName)) {
                        System.out.println("file " + pathName + " is done ");
                        flag = false;
                    } else {
                        System.out.println("file " + pathName + " had some error and been cancel");
                        fileSystemManager.cancelFileLoader(pathName);
                        flag = false;
                    }
                }
            } else {
                System.out.println("file " + pathName + "'s byte request has some problems.");
                fileSystemManager.cancelFileLoader(pathName);
                flag = false;
            }
        }
    }

    private void fileBytesRequest(long position, long length) throws IOException {
        Document fileBytesRequest = new Document();
        fileBytesRequest.append("command", "FILE_BYTES_REQUEST");
        fileBytesRequest.append("fileDescriptor", fileDescriptor);
        fileBytesRequest.append("pathName", pathName);
        fileBytesRequest.append("position", position);
        fileBytesRequest.append("length", length);

        byte buffer[] = fileBytesRequest.toJson().getBytes();
        DatagramPacket request = new DatagramPacket(buffer,buffer.length,address,port);
        socket.send(request);
    }

    public void terminate(){
        flag = false;
    }

    public void addCommand(Document command){
        try {
            fileBytesResponses.put(command);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

