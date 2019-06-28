package unimelb.bitbox;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.jce.spec.OpenSSHPublicKeySpec;
import unimelb.bitbox.util.Configuration;
import unimelb.bitbox.util.Document;
import unimelb.bitbox.util.HostPort;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.net.Socket;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;
import java.util.HashMap;


public class EncryptedServerThread extends Thread {
    private ConnectionCounter connectionCounter;
    private HashMap<String, Thread> connections;
    private Socket client;
    private ClientMaster clientMaster;
    private UdpClient udpclient;
    private Boolean mode;
    private HashMap<String, String> identityToKey;
    private String aesKeyStr = "5v8y/B?D(G+KbPeS";

    EncryptedServerThread(Socket client, ConnectionCounter connectionCounter, HashMap<String, Thread> connections,
                          ClientMaster clientMaster) {
        this.connectionCounter = connectionCounter;
        this.connections = connections;
        this.client = client;
        this.clientMaster = clientMaster;
        this.mode = true;
        this.identityToKey = getIdentityToKey();
    }

    EncryptedServerThread(Socket client, ConnectionCounter connectionCounter, HashMap<String, Thread> connections,
                          UdpClient udpclient) {
        this.connectionCounter = connectionCounter;
        this.connections = connections;
        this.client = client;
        this.udpclient = udpclient;
        this.mode = false;
        this.identityToKey = getIdentityToKey();
    }

    private String encryptAES(String original) throws InvalidKeyException, NoSuchPaddingException, NoSuchAlgorithmException, UnsupportedEncodingException, BadPaddingException, IllegalBlockSizeException {
        Key aesKey = new SecretKeySpec(aesKeyStr.getBytes(), "AES");

        Cipher cipher = Cipher.getInstance("AES");
        cipher.init(Cipher.ENCRYPT_MODE, aesKey);
        byte[] encrypted = cipher.doFinal(original.getBytes("UTF-8"));

        //System.out.println(original);
        //System.out.println("encrypted byte length: "+encrypted.length);
        //String base64A = new String(Base64.getEncoder().encode(encrypted));
        //System.out.println("AES+BASE64 String: "+base64A);

        return new String(Base64.getEncoder().encode(encrypted));
    }

    private String decryptAES(String encrytedStr) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException, BadPaddingException, IllegalBlockSizeException {
        byte[] base64 = Base64.getDecoder().decode(encrytedStr.getBytes());
        Key aesKey = new SecretKeySpec(aesKeyStr.getBytes(), "AES");
        Cipher cipher = Cipher.getInstance("AES");
        cipher.init(Cipher.DECRYPT_MODE, aesKey);
        String decryptedMessage = new String(cipher.doFinal(base64));
        return decryptedMessage;
    }

    private String encryptRSA(String aesKeyStr, String identity) throws NoSuchProviderException, NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, NoSuchPaddingException, BadPaddingException, IllegalBlockSizeException {
        String publicKeyRsa = identityToKey.get(identity);
        Security.addProvider(new BouncyCastleProvider());
        byte publicKeyByte[] = Base64.getDecoder().decode(publicKeyRsa);
        OpenSSHPublicKeySpec publicKeySpec = new OpenSSHPublicKeySpec(publicKeyByte);
        KeyFactory kf = KeyFactory.getInstance("RSA", "BC");
        PublicKey publicKey = kf.generatePublic(publicKeySpec);
        Cipher cipher = Cipher.getInstance("RSA/ECB/PKCS1Padding");
        cipher.init(Cipher.ENCRYPT_MODE, publicKey);
        byte encryptedAes[] = cipher.doFinal(aesKeyStr.getBytes());

        return new String(Base64.getEncoder().encode(encryptedAes));
    }

    private String authResponse(String identity) throws NoSuchPaddingException, NoSuchAlgorithmException, IllegalBlockSizeException, BadPaddingException, NoSuchProviderException, InvalidKeyException, InvalidKeySpecException {
        Document authResponse = new Document();
        if (identityToKey.containsKey(identity)) {
            System.out.println("has identity");
            authResponse.append("command", "AUTH_RESPONSE");
            authResponse.append("AES128", encryptRSA(aesKeyStr, identity));
            authResponse.append("status", true);
            authResponse.append("message", "public key found");
        } else {
            System.out.println(identityToKey.keySet());
            System.out.println("do not have identity");
            authResponse.append("command", "AUTH_RESPONSE");
            authResponse.append("status", false);
            authResponse.append("message", "public key not found");
        }
        System.out.println("authResponse:" + authResponse.toJson());
        return authResponse.toJson();

    }

    private String listPeersResponse() throws NoSuchPaddingException, BadPaddingException, NoSuchAlgorithmException, IllegalBlockSizeException, UnsupportedEncodingException, InvalidKeyException {
        Document listPeersResponse = new Document();
        listPeersResponse.append("command", "LIST_PEERS_RESPONSE");
        listPeersResponse.append("peers", connectionCounter.getConnections());
        //System.out.println("listPeersResponse:" + listPeersResponse.toJson());
        Document encryptedListPeersResponse = new Document();
        encryptedListPeersResponse.append("payload", encryptAES(listPeersResponse.toJson()));
        System.out.println("encryptedListPeersResponse" + encryptedListPeersResponse.toJson());
        return encryptedListPeersResponse.toJson();
    }

    private String connectPeerResponse(String host, int port, Boolean status, String message) throws NoSuchPaddingException, BadPaddingException, NoSuchAlgorithmException, IllegalBlockSizeException, UnsupportedEncodingException, InvalidKeyException {
        Document connectPeerResponse = new Document();
        connectPeerResponse.append("command", "CONNECT_PEER_RESPONSE");
        connectPeerResponse.append("host", host);
        connectPeerResponse.append("port", port);
        connectPeerResponse.append("status", status);
        connectPeerResponse.append("message", message);
        Document encryptedConnectPeerResponse = new Document();
        encryptedConnectPeerResponse.append("payload", encryptAES(connectPeerResponse.toJson()));
        System.out.println("encryptedConnectPeerResponse" + encryptedConnectPeerResponse.toJson());
        return encryptedConnectPeerResponse.toJson();
    }

    private String disconnectPeerResponse(String host, int port, Boolean status, String message) throws NoSuchPaddingException, BadPaddingException, NoSuchAlgorithmException, IllegalBlockSizeException, UnsupportedEncodingException, InvalidKeyException {
        Document disconnectionPeerResponse = new Document();
        disconnectionPeerResponse.append("command", "DISCONNECT_PEER_RESPONSE");
        disconnectionPeerResponse.append("host", host);
        disconnectionPeerResponse.append("port", port);
        disconnectionPeerResponse.append("status", status);
        disconnectionPeerResponse.append("message", message);
        Document encryptedDisconnectPeerResponse = new Document();
        encryptedDisconnectPeerResponse.append("payload", encryptAES(disconnectionPeerResponse.toJson()));
        System.out.println("encryptedDisconnectPeerResponse" + encryptedDisconnectPeerResponse.toJson());
        return encryptedDisconnectPeerResponse.toJson();
    }


    @Override
    public void run() {
        try {
            System.out.println("Encrypted Server Thread is created and running");

            BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream(), "UTF-8"));
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(client.getOutputStream(), "UTF-8"));
            String newCommandStr;

            System.out.println("Waiting for client input...");
            while (true) {
                newCommandStr = in.readLine();
                if (newCommandStr == null) break;
                System.out.println("client:" + newCommandStr);
                Document newCommand = Document.parse(newCommandStr);
                if (isValidCommand(newCommand)) { // 待新增
                    switch (newCommand.getString("command")) {
                        case "AUTH_REQUEST": {
                            //System.out.println("client identity:" + newCommand.getString("identity"));
                            out.write(authResponse(newCommand.getString("identity")) + "\n");
                            out.flush();
                            break;
                        }
                        default: {
                            //System.out.println("4");
                            client.close();
                            break;
                        }
                    }
                } else {
                    //System.out.println("5");
                    client.close();
                    break;
                }
                newCommandStr = in.readLine();
                //System.out.println("1");
                if(newCommandStr == null) break;
                //System.out.println("2");
                Document encryptedCommand = Document.parse(newCommandStr);
                //System.out.println("3");
                String encryptedSir = encryptedCommand.getString("payload");
                if (encryptedSir == null) break;
                System.out.println("encryptedCommand" + encryptedCommand.toJson());
                Document decryptedCommand = Document.parse(decryptAES(encryptedSir));
                System.out.println("decryptedCommand" + decryptedCommand.toJson());
                if(!isValidCommand(decryptedCommand)) break;
                //System.out.println("4");
                switch (decryptedCommand.getString("command")) {
                    case "LIST_PEERS_REQUEST": {
                        out.write(listPeersResponse() + "\n");
                        out.flush();
                        break;
                    }
                    case "DISCONNECT_PEER_REQUEST": {
                        System.out.println("DISCONNECT_PEER_REQUEST");
                        String host = decryptedCommand.getString("host");
                        int port = decryptedCommand.get("port") instanceof Integer
                                ? decryptedCommand.getInteger("port")
                                : (int) (long) decryptedCommand.get("port");
                        HostPort hostPort = new HostPort(host, port);
                        String hashMapKey = host + ":" + port;
                        if (!connections.containsKey(hashMapKey)) {
                            out.write(disconnectPeerResponse(host, port, false, "not connected with this peer") + "\n");
                            out.flush();
                            break;
                        }
                        if (mode) { // tcp mode
                            synchronized (connections) {
                                if (connections.get(hashMapKey) instanceof ClientThread) {
                                    System.out.println("is a client thread");
                                    ((ClientThread) connections.get(hashMapKey)).terminate();
                                } else {
                                    System.out.println("is a server thread");
                                    ((ServerThread) connections.get(hashMapKey)).terminate();
                                }
                            }
                        } else { // udp mode
                            synchronized (connections) {
                                if (connections.get(hashMapKey) instanceof ClientThreadUdp) {
                                    System.out.println("is a udpclient thread");
                                    ((ClientThreadUdp) connections.get(hashMapKey)).terminate();
                                } else {
                                    System.out.println("is a udpserver thread");
                                    //System.out.println("hashMapKey:" + hashMapKey);
                                    //
                                    ((UdpServer) connections.get(hashMapKey)).terminate(hostPort.toDoc());
                                    //
                                    connectionCounter.removeUdpConnectionByIncomming(hostPort.toDoc());
                                    connections.remove(hashMapKey);
                                }
                            }
                        }
                        Thread.sleep(1000);
                        if (!connections.containsKey(hashMapKey)) {
                            out.write(disconnectPeerResponse(host, port, true, "suceesful to disconnect") + "\n");
                            out.flush();
                        } else {
                            System.out.println("why? bug 2? ");
                            out.write(disconnectPeerResponse(host, port, false, "failed to disconnect") + "\n");
                            out.flush();
                        }
                        System.out.println("connection stats after:" + connections.containsKey(hashMapKey));
                        break;
                    }
                    case "CONNECT_PEER_REQUEST": {
                        System.out.println("CONNECT_PEER_REQUEST");

                        String host = decryptedCommand.getString("host");
                        int port = decryptedCommand.get("port") instanceof Integer
                                ? decryptedCommand.getInteger("port")
                                : (int) (long) decryptedCommand.get("port");
                        HostPort hostPort = new HostPort(host, port);
                        if (connectionCounter.isConnected(hostPort.toDoc())) {
                            System.out.println("already connected");
                            out.write(connectPeerResponse(host, port, false, "already connected") + "\n");
                            out.flush();
                            break; //?
                        }
                        if (mode) { // tcp
                            System.out.println("connection status before: " + connectionCounter.isConnected(hostPort.toDoc()));
                            clientMaster.createNewConnection(hostPort.toDoc());
                            Thread.sleep(1000);
                            System.out.println("connection status after: " + connectionCounter.isConnected(hostPort.toDoc()));

                        } else { // udp
                            System.out.println("connection status before:" + connectionCounter.isConnected(hostPort.toDoc()));
                            udpclient.createNewConnection(hostPort.toDoc());
                            Thread.sleep(1000);
                            System.out.println("connection status after:" + connectionCounter.isConnected(hostPort.toDoc()));
                        }
                        if (connectionCounter.isConnected(hostPort.toDoc())) {
                            out.write(connectPeerResponse(host, port, true, "connection is successful") + "\n");
                            out.flush();
                        } else {
                            System.out.println("why? bug? 1");
                            out.write(connectPeerResponse(host, port, false, "connection is failed") + "\n");
                            out.flush();
                        }
                        break;
                    }
                }
                client.close();
                break;
            }
        } catch (IOException | InterruptedException | NoSuchPaddingException | BadPaddingException | NoSuchAlgorithmException | IllegalBlockSizeException | InvalidKeyException | NoSuchProviderException | InvalidKeySpecException e) {
            e.printStackTrace();
        }
        System.out.println("Encrypted Server Thread has safely left");
    }

    private Boolean isValidCommand(Document command) {
        if (command.containsKey("command") && command.get("command") instanceof String) {
            switch (command.getString("command")) {
                case "AUTH_REQUEST": {
                    if (command.containsKey("identity") && command.get("identity") instanceof String) {
                        return true;
                    }
                    return false;
                }
                case "LIST_PEERS_REQUEST": {
                    return true;
                }
                case "CONNECT_PEER_REQUEST": {
                    if (command.containsKey("host") && command.get("host") instanceof String
                            && command.containsKey("port")) {
                        return true;
                    }
                    return false;
                }
                case "DISCONNECT_PEER_REQUEST": {
                    if (command.containsKey("host") && command.get("host") instanceof String
                            && command.containsKey("port")) {
                        return true;
                    }
                    return false;
                }
            }
        }
        return false;
    }
    public HashMap<String, String> getIdentityToKey () {
        String authorizedKeys = Configuration.getConfiguration().get("authorized_keys");
        String[] keysAndIdentities = authorizedKeys.trim().split(",");
        HashMap<String, String> hashMap = new HashMap<>();
        for (String keyAndIdentity : keysAndIdentities) {
            String[] keyComponents = keyAndIdentity.trim().split(" ");
            System.out.println("put_key:" + keyComponents[2] + "value:" + keyComponents[1]);
            hashMap.put(keyComponents[2], keyComponents[1]);
        }
        return hashMap;
    }
}
