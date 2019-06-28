package unimelb.bitbox;

import unimelb.bitbox.util.Configuration;
import unimelb.bitbox.util.Document;
import unimelb.bitbox.util.FileSystemManager;
import unimelb.bitbox.util.FileSystemManager.FileSystemEvent;
import unimelb.bitbox.util.HostPort;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;

/**
 * High level protocol and some functions for handling potential risks.
 * @author Tong-Ing Wu, Lih-Ren Chaung.
 */


public class Protocol {

    private static int blockSize = Integer.parseInt(Configuration.getConfiguration().get("blockSize"));
    private static String root = Configuration.getConfigurationValue("path");

    // A parser that can parse fileSystemEvent to Document.
    public static Document fileSystemEventParser(FileSystemEvent fileSystemEvent){
        String event = fileSystemEvent.event.name();
        switch(event){
            case "FILE_CREATE": {
                String md5 = fileSystemEvent.fileDescriptor.md5;
                long lastModified = fileSystemEvent.fileDescriptor.lastModified;
                long fileSize = fileSystemEvent.fileDescriptor.fileSize;

                Document fileDescriptor = new Document();
                fileDescriptor.append("md5", md5);
                fileDescriptor.append("lastModified", lastModified);
                fileDescriptor.append("fileSize", fileSize);

                Document fileCreateRequest = new Document();
                fileCreateRequest.append("command", "FILE_CREATE_REQUEST");
                fileCreateRequest.append("fileDescriptor", fileDescriptor);
                fileCreateRequest.append("pathName", fileSystemEvent.pathName);
                return fileCreateRequest;
            }
            case "FILE_DELETE": {
                String md5 = fileSystemEvent.fileDescriptor.md5;
                long lastModified = fileSystemEvent.fileDescriptor.lastModified;
                long fileSize = fileSystemEvent.fileDescriptor.fileSize;

                Document fileDescriptor = new Document();
                fileDescriptor.append("md5", md5);
                fileDescriptor.append("lastModified", lastModified);
                fileDescriptor.append("fileSize", fileSize);

                Document fileDeleteRequest = new Document();
                fileDeleteRequest.append("command", "FILE_DELETE_REQUEST");
                fileDeleteRequest.append("fileDescriptor", fileDescriptor);
                fileDeleteRequest.append("pathName", fileSystemEvent.pathName);
                return fileDeleteRequest;
            }
            case "FILE_MODIFY":{
                String md5 = fileSystemEvent.fileDescriptor.md5;
                long lastModified = fileSystemEvent.fileDescriptor.lastModified;
                long fileSize = fileSystemEvent.fileDescriptor.fileSize;

                Document fileDescriptor = new Document();
                fileDescriptor.append("md5",md5);
                fileDescriptor.append("lastModified",lastModified);
                fileDescriptor.append("fileSize",fileSize);

                Document fileModifyRequest = new Document();
                fileModifyRequest.append("command","FILE_MODIFY_REQUEST");
                fileModifyRequest.append("fileDescriptor",fileDescriptor);
                fileModifyRequest.append("pathName",fileSystemEvent.pathName);
                return fileModifyRequest;
            }
            case "DIRECTORY_CREATE":{
                Document directCreateRequest = new Document();
                directCreateRequest.append("command","DIRECTORY_CREATE_REQUEST");
                directCreateRequest.append("pathName",fileSystemEvent.pathName);
                return directCreateRequest;
            }
            case "DIRECTORY_DELETE":{
                Document directDeleteRequest = new Document();
                directDeleteRequest.append("command","DIRECTORY_DELETE_REQUEST");
                directDeleteRequest.append("pathName",fileSystemEvent.pathName);
                return directDeleteRequest;
            }
        }
        return null;
    }

    //////////////////////
    // High Level Protocol
    //////////////////////


    public static void invalidCommandResponse(BufferedWriter output, String message) throws IOException {
        //to do sending
        Document invalidCommandResponse = new Document();
        invalidCommandResponse.append("command","INVALID_PROTOCOL");
        invalidCommandResponse.append("message",message);

        output.write(invalidCommandResponse.toJson() + "\n");
        output.flush();
    }

    public static void handShakeRequest(BufferedWriter output) throws IOException {
        String localHost = Configuration.getConfiguration().get("advertisedName");
        int localPort = Integer.parseInt(Configuration.getConfiguration().get("port"));
        HostPort localHostPort = new HostPort(localHost, localPort);

        Document handShakeRequest = new Document();
        handShakeRequest.append("command","HANDSHAKE_REQUEST");
        handShakeRequest.append("hostPort",localHostPort.toDoc());

        output.write(handShakeRequest.toJson() + "\n");
        output.flush();

        //System.out.println("sending HANDSHAKE_REQUEST: " + handShakeRequest.toJson());
    }

    public static void handShakeResponse(BufferedWriter output) throws IOException {
        String localHost = Configuration.getConfiguration().get("advertisedName");
        int localPort = Integer.parseInt(Configuration.getConfiguration().get("port"));
        HostPort localHostPort = new HostPort(localHost, localPort);

        Document handShakeResponse = new Document();
        handShakeResponse.append("command", "HANDSHAKE_RESPONSE");
        handShakeResponse.append("hostPort", localHostPort.toDoc());

        output.write(handShakeResponse.toJson() + "\n");
        output.flush();

        //System.out.println("sending HANDSHAKE_RESPONSE: " + handShakeResponse.toJson());
    }

    public static void connectionRefuse(BufferedWriter output,ConnectionCounter connectionCounter,String message) throws IOException {
        Document newCommand = new Document();
        newCommand.append("command", "CONNECTION_REFUSED");
        newCommand.append("message", message);
        newCommand.append("peers", connectionCounter.getConnections());

        output.write(newCommand.toJson() + "\n");
        output.flush();

        //System.out.println("sending CONNECTION_REFUSED: " + newCommand.toJson());
    }

    public static void fileDeleteResponse(BufferedWriter output, String pathName, Document fileDescriptor,String message,Boolean status) throws IOException {
        Document fileDeleteResponse = new Document();
        fileDeleteResponse.append("command","FILE_DELETE_RESPONSE");
        fileDeleteResponse.append("fileDescriptor",fileDescriptor);
        fileDeleteResponse.append("pathName",pathName);
        fileDeleteResponse.append("message",message);
        fileDeleteResponse.append("status",status);

        output.write(fileDeleteResponse.toJson() + "\n");
        output.flush();

        //System.out.println("sending FILE_DELETE_RESPONSE: " + fileDeleteResponse.toJson());
    }

    public static void fileCreateResponse(BufferedWriter output, String pathName, Document fileDescriptor, String message, boolean status) throws IOException {
        Document fileCreateResponse = new Document();
        fileCreateResponse.append("command","FILE_CREATE_RESPONSE");
        fileCreateResponse.append("fileDescriptor",fileDescriptor);
        fileCreateResponse.append("pathName",pathName);
        fileCreateResponse.append("message",message);
        fileCreateResponse.append("status",status);

        output.write(fileCreateResponse.toJson() + "\n");
        output.flush();

        //System.out.println("sending FILE_CREATE_RESPONSE: " + fileCreateResponse.toJson());
    }

    public static void fileBytesRequest(BufferedWriter output,String pathName,Document fileDescriptor,long position, long length) throws IOException {
        Document fileBytesRequest = new Document();
        fileBytesRequest.append("command", "FILE_BYTES_REQUEST");
        fileBytesRequest.append("fileDescriptor", fileDescriptor);
        fileBytesRequest.append("pathName", pathName);
        fileBytesRequest.append("position", position);
        fileBytesRequest.append("length", length);

        output.write(fileBytesRequest.toJson() + "\n");
        output.flush();

        //System.out.println("sending FILE_BYTES_REQUEST: " + fileBytesRequest.toJson());
    }

    public static void fileBytesResponse(BufferedWriter output, Document fileDescriptor, String pathName, long position, long length , String encodedContent,String message,Boolean status) throws IOException {
        Document fileBytesResponse = new Document();
        fileBytesResponse.append("command", "FILE_BYTES_RESPONSE");
        fileBytesResponse.append("fileDescriptor", fileDescriptor);
        fileBytesResponse.append("pathName", pathName);
        fileBytesResponse.append("position", position);
        fileBytesResponse.append("length", length);
        fileBytesResponse.append("content", encodedContent);
        fileBytesResponse.append("message", message);
        fileBytesResponse.append("status", status);

        output.write(fileBytesResponse.toJson() + "\n");
        output.flush();

        //System.out.println("sending FILE_BYTES_RESPONSE: " + fileBytesResponse.toJson());
    }

    public static void fileModifyResponse(BufferedWriter output, String pathName, Document fileDescriptor, String message, boolean status) throws IOException {
        Document fileModifyResponse = new Document();
        fileModifyResponse.append("command","FILE_MODIFY_RESPONSE");
        fileModifyResponse.append("fileDescriptor",fileDescriptor);
        fileModifyResponse.append("pathName",pathName);
        fileModifyResponse.append("message",message);
        fileModifyResponse.append("status",status);

        output.write(fileModifyResponse.toJson() + "\n");
        output.flush();

        //System.out.println("sending FILE_MODIFY_RESPONSE: " + fileModifyResponse.toJson());
    }

    public static void directoryOptionResponse(BufferedWriter output, String  commandName, String pathName, String message, boolean status) throws IOException {
        Document response = new Document();
        response.append("command", commandName);
        response.append("pathName",pathName);
        response.append("message",message);
        response.append("status",status);

        output.write(response.toJson() + "\n");
        output.flush();

        //System.out.println("sending " + commandName + ": " + response.toJson());
    }


    ///////////////////
    // Helper Functions
    ///////////////////


    // A function can justify if a command is valid.
    public static boolean isValidCommand(Document command){
        if(command.containsKey("command") && command.get("command") instanceof String){
            switch(command.getString("command")){
                case "INVALID_PROTOCOL":
                    if(command.containsKey("message") && command.get("message") instanceof String){
                        return true;
                    }
                    System.out.println("INVALID_PROTOCOL is wrong");
                    return false;

                case "CONNECTION_REFUSED":
                    if(command.containsKey("message") && command.containsKey("peers")
                            && command.get("message") instanceof String
                            && command.get("peers") instanceof ArrayList){
                        return true;
                    }
                    System.out.println("INVALID_PROTOCOL is wrong");
                    return false;

                case "HANDSHAKE_REQUEST":
                    if(command.containsKey("hostPort") && command.get("hostPort") instanceof Document){
                        return true;
                    }
                    System.out.println("HANDSHAKE_REQUEST is wrong");
                    return false;

                case "HANDSHAKE_RESPONSE":
                    if(command.containsKey("hostPort") && command.get("hostPort") instanceof Document){
                        return true;
                    }
                    System.out.println("HANDSHAKE_RESPONSE is wrong");
                    return false;

                case "FILE_CREATE_REQUEST":
                    if(command.containsKey("fileDescriptor") && command.containsKey("pathName")
                            && command.get("fileDescriptor") instanceof Document
                            && command.get("pathName") instanceof String ){
                        if(isValidFileDescriptor((Document)command.get("fileDescriptor"))){
                            return true;
                        }
                        return false;
                    }
                    System.out.println("FILE_CREATE_REQUEST is wrong");
                    return false;

                case "FILE_CREATE_RESPONSE":
                    if(command.containsKey("fileDescriptor") && command.containsKey("pathName")
                            && command.containsKey("message") && command.containsKey("status")
                            && command.get("fileDescriptor") instanceof Document
                            && command.get("pathName") instanceof String
                            && command.get("message") instanceof String
                            && command.get("status") instanceof Boolean) {
                        if(isValidFileDescriptor((Document)command.get("fileDescriptor"))){
                            return true;
                        }
                        return false;
                    }
                    System.out.println("FILE_CREATE_REQUEST is wrong");
                    return false;

                case "FILE_BYTES_REQUEST":
                    if(command.containsKey("fileDescriptor") && command.containsKey("pathName")
                            && command.containsKey("position") && command.containsKey("length")
                            && command.get("fileDescriptor") instanceof Document
                            && command.get("pathName") instanceof String
                            && command.get("position") instanceof Long
                            && command.get("length") instanceof Long) {
                        if(isValidFileDescriptor((Document)command.get("fileDescriptor"))){
                            return true;
                        }
                        return false;
                    }
                    System.out.println("FILE_CREATE_REQUEST is wrong");
                    return false;
                case "FILE_BYTES_RESPONSE":
                    if(command.containsKey("fileDescriptor") && command.containsKey("pathName")
                            && command.containsKey("position") && command.containsKey("length")
                            && command.containsKey("content") && command.containsKey("message")
                            && command.containsKey("status")
                            && command.get("fileDescriptor") instanceof Document
                            && command.get("pathName") instanceof String
                            && command.get("position") instanceof Long
                            && command.get("length") instanceof Long
                            && command.get("content") instanceof String
                            && command.get("message") instanceof String
                            && command.get("status") instanceof Boolean) {
                        if(isValidFileDescriptor((Document)command.get("fileDescriptor"))){
                            return true;
                        }
                        return false;
                    }
                    System.out.println("FILE_BYTES_RESPONSE is wrong");
                    return false;
                case "FILE_DELETE_REQUEST":
                    if(command.containsKey("fileDescriptor") && command.containsKey("pathName")
                            && command.get("fileDescriptor") instanceof Document
                            && command.get("pathName") instanceof String ){
                        if(isValidFileDescriptor((Document)command.get("fileDescriptor"))){
                            return true;
                        }
                        return false;
                    }
                case "FILE_DELETE_RESPONSE":
                    if(command.containsKey("fileDescriptor") && command.containsKey("pathName")
                            && command.containsKey("message") && command.containsKey("status")
                            && command.get("fileDescriptor") instanceof Document
                            && command.get("pathName") instanceof String
                            && command.get("message") instanceof String
                            && command.get("status") instanceof Boolean) {
                        if(isValidFileDescriptor((Document)command.get("fileDescriptor"))){
                            return true;
                        }
                        return false;
                    }
                    System.out.println("FILE_DELETE_RESPONSE is wrong");
                    return false;

                case "FILE_MODIFY_REQUEST":
                    if(command.containsKey("fileDescriptor") && command.containsKey("pathName")
                            && command.get("fileDescriptor") instanceof Document
                            && command.get("pathName") instanceof String ){
                        if(isValidFileDescriptor((Document)command.get("fileDescriptor"))){
                            return true;
                        }
                        return false;
                    }
                    System.out.println("FILE_MODIFY_REQUEST is wrong");
                    return false;

                case "FILE_MODIFY_RESPONSE":
                    if(command.containsKey("fileDescriptor") && command.containsKey("pathName")
                            && command.containsKey("message") && command.containsKey("status")
                            && command.get("fileDescriptor") instanceof Document
                            && command.get("pathName") instanceof String
                            && command.get("message") instanceof String
                            && command.get("status") instanceof Boolean) {
                        if(isValidFileDescriptor((Document)command.get("fileDescriptor"))){
                            return true;
                        }
                        return false;
                    }
                    System.out.println("FILE_MODIFY_RESPONSE is wrong");
                    return false;

                case "DIRECTORY_CREATE_REQUEST":
                    if(command.containsKey("pathName") && command.get("pathName") instanceof String) {
                        return true;
                    }
                    System.out.println("DIRECTORY_CREATE_REQUEST is wrong");
                    return false;

                case "DIRECTORY_CREATE_RESPONSE":
                    if(command.containsKey("pathName") && command.containsKey("message")
                            && command.containsKey("status")
                            && command.get("pathName") instanceof String
                            && command.get("message") instanceof String
                            && command.get("status") instanceof Boolean) {
                        return true;
                    }
                    System.out.println("DIRECTORY_CREATE_RESPONSE is wrong");
                    return false;

                case "DIRECTORY_DELETE_REQUEST":
                    if(command.containsKey("pathName") && command.get("pathName") instanceof String) {
                        return true;
                    }
                    System.out.println("DIRECTORY_DELETE_REQUEST is wrong");
                    return false;

                case "DIRECTORY_DELETE_RESPONSE":
                    if(command.containsKey("pathName") && command.containsKey("message")
                            && command.containsKey("status")
                            && command.get("pathName") instanceof String
                            && command.get("message") instanceof String
                            && command.get("status") instanceof Boolean) {
                        return true;
                    }
                    System.out.println("DIRECTORY_DELETE_RESPONSE is wrong");
                    return false;
            }
        }
        return false;
    }

    //A helper function for isValidCommand which can justify if the fileDescriptor field is valid.
    private static boolean isValidFileDescriptor(Document fileDescriptor){
        if(fileDescriptor.containsKey("md5") && fileDescriptor.containsKey("lastModified")
                && fileDescriptor.containsKey("fileSize")
                && fileDescriptor.get("md5") instanceof String
                && fileDescriptor.get("lastModified") instanceof Long
                && fileDescriptor.get("fileSize") instanceof Long){
            return true;
        }
        return false;
    }



    /////////////////////////////////////
    // Functions used to process commands
    /////////////////////////////////////

    /**
     * The function will process the FileCreateRequest command.
     * @param newCommand The new Command received from other peer.
     * @param fileSystemManager The fileSystemManager which is monitoring the share file folder.
     * @param out The BufferedWriter used to write JsonCommand to other peer.
     */
    public static void processFileCreateRequest(Document newCommand, FileSystemManager fileSystemManager, BufferedWriter out) throws IOException, NoSuchAlgorithmException {
        //System.out.println("command:FILE_CREATE_REQUEST"+newCommand.toJson());
        Document fileDescriptor = (Document) newCommand.get("fileDescriptor");
        String pathName = newCommand.getString("pathName");
        String md5 = fileDescriptor.getString("md5");
        long lastModified = fileDescriptor.getLong("lastModified");
        long fileSize = fileDescriptor.getLong("fileSize");

        if(fileSystemManager.isSafePathName(pathName)) {
            if(!fileSystemManager.fileNameExists(pathName)) {
                boolean sucess = false;
                try {
                    sucess = fileSystemManager.createFileLoader(pathName, md5, fileSize, lastModified);
                } catch (NoSuchAlgorithmException | IOException e) {
                    String realPathName = separatorsToSystem(pathName);
                    String fullPathName = root+ FileSystems.getDefault().getSeparator() + realPathName + fileSystemManager.loadingSuffix;
                    File file = new File(fullPathName);
                    if(file.isFile()) {
                        if (file.delete()) {
                            System.out.println("the redundant file(A file isn't being load but ends with loadingSuffix))(code:1)");
                            processFileCreateRequest(newCommand,fileSystemManager,out);
                        }
                    }
                }
                if (sucess) {
                    if (!fileSystemManager.checkShortcut(pathName)) {
                        fileCreateResponse(out, pathName, fileDescriptor, "file loader is ready", true);
                        long position = 0;
                        if (fileSize > blockSize) {
                            fileBytesRequest(out, pathName, fileDescriptor, position, blockSize);
                        } else {
                            fileBytesRequest(out, pathName, fileDescriptor, position, fileSize);
                        }

                    } else {
                        String message = "has shortcut";
                        fileCreateResponse(out, pathName, fileDescriptor, message, false);
                        if (fileSystemManager.cancelFileLoader(pathName)) {
                            //System.out.println("successfully cancel file loader!");
                        }
                    }
                } else {
                    String message = "file loader fails because the file is being transferred by other ";
                    fileCreateResponse(out, pathName, fileDescriptor, message, false);
                }
            } else {
                String message = "fileName already exists";
                fileCreateResponse(out, pathName, fileDescriptor, message, false);
            }
        } else {
            String message = "not a safe path name";
            fileCreateResponse(out, pathName, fileDescriptor, message, false);
        }

    }


    /**
     * The function will process the FileCreateResponse.
     * @param newCommand The new Command received from other peer.
     * @param fileSystemManager The fileSystemManager which is monitoring the share file folder.
     * @param out The BufferedWriter used to write JsonCommand to other peer.
     */
    public static void processFileCreateResponse(Document newCommand, FileSystemManager fileSystemManager, BufferedWriter out){
        //System.out.println("command:FILE_CREATE_RESPONSE"+newCommand.toJson());
        if (!newCommand.getBoolean("status")) {
        // do nothing or request again
        }
    }

    /**
     * The function will process the FileDeleteRequest.
     * @param newCommand The new Command received from other peer.
     * @param fileSystemManager The fileSystemManager which is monitoring the share file folder.
     * @param out The BufferedWriter used to write JsonCommand to other peer.
     */
    public static void processFileDeleteRequest(Document newCommand, FileSystemManager fileSystemManager, BufferedWriter out) throws IOException {
        //System.out.println("command:FILE_DELETE_REQUEST"+newCommand.toJson());
        Document fileDescriptor = (Document) newCommand.get("fileDescriptor");
        String pathName = newCommand.getString("pathName");
        String md5 = fileDescriptor.getString("md5");
        long lastModified = fileDescriptor.getLong("lastModified");

        if (fileSystemManager.isSafePathName(pathName)) {
            if (fileSystemManager.fileNameExists(pathName)) {
                if (fileSystemManager.deleteFile(pathName, lastModified, md5)) {
                    fileDeleteResponse(out, pathName, fileDescriptor, "deletion is successful ", true);
                } else {
                    fileDeleteResponse(out, pathName, fileDescriptor, "md5 or lastModified is not ok", false);
                }
            } else {
                fileDeleteResponse(out, pathName, fileDescriptor, "pathname does not exist", false);
            }
        } else {
            fileDeleteResponse(out, pathName, fileDescriptor, "pathname is not safe", false);
        }
    }
    /**
     * The function will process the FileDeleteResponse.
     * @param newCommand The new Command received from other peer.
     * @param fileSystemManager The fileSystemManager which is monitoring the share file folder.
     * @param out The BufferedWriter used to write JsonCommand to other peer.
     */
    public static void processFileDeleteResponse(Document newCommand, FileSystemManager fileSystemManager, BufferedWriter out) {
        //System.out.println("command:FILE_DELETE_RESPONSE"+newCommand.toJson());
        if (!newCommand.getBoolean("status")) {
         // do nothing or request again
        }
    }
    /**
     * The function will process the FileModifyRequest.
     * @param newCommand The new Command received from other peer.
     * @param fileSystemManager The fileSystemManager which is monitoring the share file folder.
     * @param out The BufferedWriter used to write JsonCommand to other peer.
     */
    public static void processFileModifyRequest(Document newCommand, FileSystemManager fileSystemManager, BufferedWriter out) throws IOException, NoSuchAlgorithmException {
        //System.out.println("command:FILE_MODIFY_REQUEST" + newCommand.toJson());
        Document fileDescriptor = (Document) newCommand.get("fileDescriptor");
        String pathName = newCommand.getString("pathName");
        String md5 = fileDescriptor.getString("md5");
        long lastModified = fileDescriptor.getLong("lastModified");
        long fileSize = fileDescriptor.getLong("fileSize");

        if (fileSystemManager.isSafePathName(pathName)) {
            if (fileSystemManager.fileNameExists(pathName)) {
                if (!fileSystemManager.fileNameExists(pathName, md5)) {
                    boolean success = false;
                    try {
                        success = fileSystemManager.modifyFileLoader(pathName, md5, lastModified);
                    } catch (IOException e) {
                        String realPathName = separatorsToSystem(pathName);
                        String fullPathName = root+ FileSystems.getDefault().getSeparator() + realPathName + fileSystemManager.loadingSuffix;
                        File file = new File(fullPathName);
                        if(file.isFile()) {
                            if(file.delete()){
                                System.out.println("the redundant file(A file isn't being loaded but ends with loadingSuffix))(code:2)");
                                processFileModifyRequest(newCommand,fileSystemManager,out);
                            }
                        }
                    }
                    if (success) {
                        if (!fileSystemManager.checkShortcut(pathName)) {
                            fileModifyResponse(out, pathName, fileDescriptor, "file loader is ready", true);
                            long position = 0;
                            if (fileSize > blockSize) {
                                fileBytesRequest(out, pathName, fileDescriptor, position, blockSize);
                            } else {
                                fileBytesRequest(out, pathName, fileDescriptor, position, fileSize);
                            }
                        } else {
                            fileModifyResponse(out, pathName, fileDescriptor, "have shortcut", false);
                            if (fileSystemManager.cancelFileLoader(pathName)) {
                                System.out.println("successfully cancel the file loader!");
                            }
                        }
                    } else {
                        fileModifyResponse(out, pathName, fileDescriptor, "file loader creation is failed because the file is being transferred by other", false);
                    }
                } else {
                    fileModifyResponse(out, pathName, fileDescriptor, "file already exists with matching content", false);
                }
            } else {
                fileModifyResponse(out, pathName, fileDescriptor, "File (pathname) does not exist", false);
            }
        } else {
            fileModifyResponse(out, pathName, fileDescriptor, "Path is unsafe!", false);
        }
    }


    /**
     * A function will process FileModifyResponse.
     * @param newCommand The new Command received from other peer.
     * @param fileSystemManager The fileSystemManager which is monitoring the share file folder.
     * @param out The BufferedWriter used to write JsonCommand to other peer.
     */
    public static void processFileModifyResponse(Document newCommand, FileSystemManager fileSystemManager, BufferedWriter out){
        //System.out.println("command:FILE_MODIFY_RESPONSE"+newCommand.toJson());
        if (!newCommand.getBoolean("status")) {
        // do nothing or request again
        }
    }
    /**
     * A function will process DirectoryCreateRequest.
     * @param newCommand The new Command received from other peer.
     * @param fileSystemManager The fileSystemManager which is monitoring the share file folder.
     * @param out The BufferedWriter used to write JsonCommand to other peer.
     */
    public static void processDirectoryCreateRequest(Document newCommand, FileSystemManager fileSystemManager, BufferedWriter out) throws IOException {
        //System.out.println("command:DIRECTORY_CREATE_REQUEST" + newCommand.toJson());
        String pathName = newCommand.getString("pathName");
        String responseName = "DIRECTORY_CREATE_RESPONSE";

        if (fileSystemManager.isSafePathName(pathName)) {
            if (!fileSystemManager.dirNameExists(pathName)) {
                if (fileSystemManager.makeDirectory(pathName)) {
                    directoryOptionResponse(out, responseName, pathName, "directory created", true);
                } else {
                    directoryOptionResponse(out, responseName, pathName, "there was a problem creating the directory", false);
                }
            } else { // target directory already exist)
                directoryOptionResponse(out, responseName, pathName, "pathname already exists", false);
            }
        } else { // path not valid
            directoryOptionResponse(out, responseName, pathName, "unsafe pathname given", false);
        }
    }
    /**
     * A function will process DirectoryCreateResponse.
     * @param newCommand The new Command received from other peer.
     * @param fileSystemManager The fileSystemManager which is monitoring the share file folder.
     * @param out The BufferedWriter used to write JsonCommand to other peer.
     */
    public static void processDirectoryCreateResponse(Document newCommand, FileSystemManager fileSystemManager,BufferedWriter out){
        //System.out.println("command:DIRECTORY_CREATE_RESPONSE"+newCommand.toJson());
        if (!newCommand.getBoolean("status")) {
         // do nothing or request again
        }
    }
    /**
     * A function will process DirectoryDeleteRequest.
     * @param newCommand The new Command received from other peer.
     * @param fileSystemManager The fileSystemManager which is monitoring the share file folder.
     * @param out The BufferedWriter used to write JsonCommand to other peer.
     */
    public static void processDirectoryDeleteRequest(Document newCommand, FileSystemManager fileSystemManager,BufferedWriter out) throws IOException {
        //System.out.println("command:DIRECTORY_DELETE_REQUEST"+newCommand.toJson());
        String pathName = newCommand.getString("pathName");
        String responseName = "DIRECTORY_DELETE_RESPONSE";

        if (fileSystemManager.isSafePathName(pathName)) {
            if (!fileSystemManager.dirNameExists(pathName)) {
                directoryOptionResponse(out, responseName, pathName, "pathname does not exist", false);
            } else {
                if (fileSystemManager.deleteDirectory(pathName)) {
                    directoryOptionResponse(out, responseName, pathName, "directory deleted", true);
                } else {
                    directoryOptionResponse(out, responseName, pathName, "there was a problem deleting the directory", false);
                }
            }
        } else {
            directoryOptionResponse(out, responseName, pathName, "unsafe pathname given", false);
        }

    }
    /**
     * A function will process DirectoryDeleteResponse.
     * @param newCommand The new Command received from other peer.
     * @param fileSystemManager The fileSystemManager which is monitoring the share file folder.
     * @param out The BufferedWriter used to write JsonCommand to other peer.
     */
    public static void processDirectoryDeleteResponse (Document newCommand, FileSystemManager fileSystemManager, BufferedWriter out){
        //System.out.println("command:DIRECTORY_DELETE_RESPONSE"+newCommand.toJson());
        if (!newCommand.getBoolean("status")) {
            //  do nothing or request again
        }
    }
    /**
     * A function will process FileBytesRequest. It will create a new thread to deal with fileReadBytes.
     * @param newCommand The new Command received from other peer.
     * @param fileSystemManager The fileSystemManager which is monitoring the share file folder.
     * @param out The BufferedWriter used to write JsonCommand to other peer.
     */
    public static void processFileBytesRequest(Document newCommand, FileSystemManager fileSystemManager, BufferedWriter out){
        //System.out.println("command:FILE_BYTES_REQUEST"+newCommand.toJson());
        Thread readThread = new Thread(() -> {
            try {
                fileReadBytes(newCommand, out, fileSystemManager);
            } catch (IOException | NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        });
        readThread.start();
    }
    /**
     * A function will process DirectoryDeleteResponse.
     * @param newCommand The new Command received from other peer.
     * @param fileSystemManager The fileSystemManager which is monitoring the share file folder.
     * @param out The BufferedWriter used to write JsonCommand to other peer.
     */
    public static void processFileBytesResponse(Document newCommand, FileSystemManager fileSystemManager, BufferedWriter out){
        //System.out.println("command:FILE_BYTES_RESPONSE"+newCommand.toJson());
        Thread writeThread = new Thread(() -> {
            try {
                fileWriteBytes(newCommand, out, fileSystemManager);
            } catch (IOException | NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        });
        writeThread.start();
    }
    /**
     * A function will read bytes from the file and encode it by Base64 before sending a fileBytesResponse.
     * @param command The new Command received from other peer.
     * @param fileSystemManager The fileSystemManager which is monitoring the share file folder.
     * @param output The BufferedWriter used to write JsonCommand to other peer.
     */
    private static void fileReadBytes(Document command, BufferedWriter output, FileSystemManager fileSystemManager) throws IOException, NoSuchAlgorithmException {
        Document fileDescriptor = (Document) command.get("fileDescriptor");
        String md5 = fileDescriptor.getString("md5");
        String pathName = command.getString("pathName");
        long position = command.getLong("position");
        long length = command.getLong("length");
        long lastModified = fileDescriptor.getLong("lastModified");
        ByteBuffer buffer = fileSystemManager.readFile(md5, position, length);
        if(buffer != null){
            String encodedContent = new String(Base64.getEncoder().encode(buffer.array()));
            String message = "successful read";
            fileBytesResponse(output, fileDescriptor, pathName, position, length, encodedContent, message, true);
        } else {
            String message = "unsuccessful read";
            String encodedContent = "";
            fileBytesResponse(output, fileDescriptor, pathName, position, length, encodedContent, message, false);
            fileSystemManager.deleteFile(pathName,lastModified,md5);
        }
    }

    /**
     * A function will write bytes to the file from encoded decoded content.
     * It will send a fileBytesRequest after writing if it is necessary.
     * @param command The new Command received from other peer.
     * @param fileSystemManager The fileSystemManager which is monitoring the share file folder.
     * @param output The BufferedWriter used to write JsonCommand to other peer.
     */
    private static void fileWriteBytes(Document command, BufferedWriter output, FileSystemManager fileSystemManager) throws IOException, NoSuchAlgorithmException {
        Document fileDescriptor = (Document) command.get("fileDescriptor");
        String pathName = command.getString("pathName");
        long fileSize = fileDescriptor.getLong("fileSize");
        long position = command.getLong("position");
        long length = command.getLong("length");
        if (command.getBoolean("status")) {
            String content = command.getString("content");
            //System.out.println("FILE_BYTES_RESPONSE Status is true");
            byte[] readBytes = Base64.getDecoder().decode(content);
            ByteBuffer src = ByteBuffer.wrap(readBytes);
            //writing the file.
            fileSystemManager.writeFile(pathName, src, position);
            position += length; // update the transferred file size
            fileSize -= position; // update the remaining size
            if (fileSize > 0) {
                if (fileSize > blockSize) {
                    fileBytesRequest(output, pathName, fileDescriptor, position, blockSize);
                } else {
                    fileBytesRequest(output, pathName, fileDescriptor, position, fileSize);
                }
            } else {
                if (fileSystemManager.checkWriteComplete(pathName)) {
                    System.out.println("file " + pathName + " is done ");
                } else {
                    fileSystemManager.cancelFileLoader(pathName);
                }
            }
        } else {
            fileSystemManager.cancelFileLoader(pathName);
        }
    }

    //code from FileSystemManager
    private static String separatorsToSystem(String res) {
        if (res==null) return null;
        if (File.separatorChar=='\\') {
            // From Windows to Linux/Mac
            return res.replace('/', File.separatorChar);
        } else {
            // From Linux/Mac to Windows
            return res.replace('\\', File.separatorChar);
        }
    }

    //functions that are not used.
    /*
    public static void directCreateRequest(BufferedWriter output, FileSystemEvent fileSystemEvent) throws IOException {
        Document directCreateRequest = new Document();
        directCreateRequest.append("command","DIRECTORY_CREATE_REQUEST");
        directCreateRequest.append("pathName",fileSystemEvent.pathName);

        output.write(directCreateRequest.toJson() + "\n");
        output.flush();

        //System.out.println("sending DIRECTORY_CREATE_REQUEST: "  + directCreateRequest.toJson());
    }
    */
    /*
    public static void directDeleteRequest(BufferedWriter output, FileSystemEvent fileSystemEvent) throws IOException {
        Document directDeleteRequest = new Document();
        directDeleteRequest.append("command","DIRECTORY_DELETE_REQUEST");
        directDeleteRequest.append("pathName",fileSystemEvent.pathName);

        output.write(directDeleteRequest.toJson() + "\n");
        output.flush();

        //System.out.println("sending DIRECTORY_DELETE_REQUEST: " + directDeleteRequest.toJson());
    }
    */
    /*
    public static void fileModifyRequest(BufferedWriter output, FileSystemEvent fileSystemEvent) throws IOException {
        String md5 = fileSystemEvent.fileDescriptor.md5;
        long lastModified = fileSystemEvent.fileDescriptor.lastModified;
        long fileSize = fileSystemEvent.fileDescriptor.fileSize;

        Document fileDescriptor = new Document();
        fileDescriptor.append("md5",md5);
        fileDescriptor.append("lastModified",lastModified);
        fileDescriptor.append("fileSize",fileSize);

        Document fileModifyRequest = new Document();
        fileModifyRequest.append("command","FILE_MODIFY_REQUEST");
        fileModifyRequest.append("fileDescriptor",fileDescriptor);
        fileModifyRequest.append("pathName",fileSystemEvent.pathName);

        output.write(fileModifyRequest.toJson() + "\n");
        output.flush();

        //System.out.println("sending FILE_MODIFY_REQUEST: " + fileModifyRequest.toJson());
    }
    */
    /*
    public static void fileCreateRequest(BufferedWriter output, FileSystemEvent fileSystemEvent) throws IOException {
        String md5 = fileSystemEvent.fileDescriptor.md5;
        long lastModified = fileSystemEvent.fileDescriptor.lastModified;
        long fileSize = fileSystemEvent.fileDescriptor.fileSize;

        Document fileDescriptor = new Document();
        fileDescriptor.append("md5",md5);
        fileDescriptor.append("lastModified",lastModified);
        fileDescriptor.append("fileSize",fileSize);

        Document fileCreateRequest = new Document();
        fileCreateRequest.append("command","FILE_CREATE_REQUEST");
        fileCreateRequest.append("fileDescriptor",fileDescriptor);
        fileCreateRequest.append("pathName",fileSystemEvent.pathName);

        output.write(fileCreateRequest.toJson() + "\n");
        output.flush();

        //System.out.println("sending FILE_CREATE_REQUEST: " + fileCreateRequest.toJson());
    }
    */
    /* old version of  processFileModifyRequest
    public static void processFileModifyRequest(Document newCommand, FileSystemManager fileSystemManager, BufferedWriter out) {
        //System.out.println("command:FILE_MODIFY_REQUEST" + newCommand.toJson());
        Document fileDescriptor = (Document) newCommand.get("fileDescriptor");
        String pathName = newCommand.getString("pathName");
        String md5 = fileDescriptor.getString("md5");
        long lastModified = fileDescriptor.getLong("lastModified");
        long fileSize = fileDescriptor.getLong("fileSize");
        try {
            if (fileSystemManager.isSafePathName(pathName)) {
                if (fileSystemManager.fileNameExists(pathName)) {
                    if (!fileSystemManager.fileNameExists(pathName, md5)) {
                        if (fileSystemManager.modifyFileLoader(pathName, md5, lastModified)) {
                            if (!fileSystemManager.checkShortcut(pathName)) {
                                fileModifyResponse(out, pathName, fileDescriptor, "file loader is ready", true);
                                long position = 0;
                                if (fileSize > blockSize) {
                                    fileBytesRequest(out, pathName, fileDescriptor, position, blockSize);
                                } else {
                                    fileBytesRequest(out, pathName, fileDescriptor, position, fileSize);
                                }
                            } else {
                                fileModifyResponse(out, pathName, fileDescriptor, "have shortcut", false);
                                if (fileSystemManager.cancelFileLoader(pathName)) {
                                    System.out.println("successfully cancel the file loader!");
                                }
                            }
                        } else {
                            fileModifyResponse(out, pathName, fileDescriptor, "file loader creation is failed because the file is being transferred by other", false);
                        }
                    } else {
                        fileModifyResponse(out, pathName, fileDescriptor, "file already exists with matching content", false);
                    }
                } else {
                    fileModifyResponse(out, pathName, fileDescriptor, "File (pathname) does not exist", false);
                }
            } else {
                fileModifyResponse(out, pathName, fileDescriptor, "Path is unsafe!", false);
            }
        } catch (IOException | NoSuchAlgorithmException e){
            e.printStackTrace();
            System.out.println("modifyFileLoader fai2");
        }
    }
    */
    /* old version of processFileCreateRequest
    public static void processFileCreateRequest(Document newCommand, FileSystemManager fileSystemManager, BufferedWriter out){
        //System.out.println("command:FILE_CREATE_REQUEST"+newCommand.toJson());
        Document fileDescriptor = (Document) newCommand.get("fileDescriptor");
        String pathName = newCommand.getString("pathName");
        String md5 = fileDescriptor.getString("md5");
        long lastModified = fileDescriptor.getLong("lastModified");
        long fileSize = fileDescriptor.getLong("fileSize");
        try {
            if (fileSystemManager.isSafePathName(pathName)) {
                if (!fileSystemManager.fileNameExists(pathName)) {
                    if (fileSystemManager.createFileLoader(pathName, md5, fileSize, lastModified)) {
                        if (!fileSystemManager.checkShortcut(pathName)) {

                            fileCreateResponse(out, pathName, fileDescriptor, "file loader is ready", true);
                            long position = 0;
                            if (fileSize > blockSize) {
                                fileBytesRequest(out, pathName, fileDescriptor, position, blockSize);
                            } else {
                                fileBytesRequest(out, pathName, fileDescriptor, position, fileSize);
                            }

                        } else {
                            String message = "has shortcut";
                            fileCreateResponse(out, pathName, fileDescriptor, message, false);
                            if (fileSystemManager.cancelFileLoader(pathName)) {
                                //System.out.println("successfully cancel file loader!");
                            }
                        }
                    } else {
                        String message = "file loader fails because the file is being transferred by other ";
                        fileCreateResponse(out, pathName, fileDescriptor, message, false);
                    }
                } else {
                    String message = "fileName already exists";
                    fileCreateResponse(out, pathName, fileDescriptor, message, false);
                }
            } else {
                String message = "not a safe path name";
                fileCreateResponse(out, pathName, fileDescriptor, message, false);
            }
        } catch (IOException | NoSuchAlgorithmException e) {
            System.out.println("creadte FileLoader fail");
        }
    }
    */

}
