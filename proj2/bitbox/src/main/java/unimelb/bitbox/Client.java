package unimelb.bitbox;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.kohsuke.args4j.Option;
import unimelb.bitbox.util.Document;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.net.Socket;
import java.security.*;
import java.util.Base64;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

public class Client {

    private static String aesKeyStr;

    public static void main(String[] args) throws IOException {

        Security.addProvider(new BouncyCastleProvider());
        JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider("BC");
        PEMParser pemParser = new PEMParser(new FileReader("id_rsa"));
        Object privateObject = pemParser.readObject();
        PEMKeyPair pemKeyPair = (PEMKeyPair) privateObject;
        PrivateKey privateKey = converter.getPrivateKey(pemKeyPair.getPrivateKeyInfo());
        CmdLineArgs argsBean = new CmdLineArgs();
        CmdLineParser parser = new CmdLineParser(argsBean);

        try {

            parser.parseArgument(args);

            String commandStr = argsBean.getCommand();
            int serverPort = Integer.parseInt(argsBean.getServer().split(":")[1]);
            String serverHost = argsBean.getServer().split(":")[0];
            int peerPort = 0;
            String peerHost = null;
            String identity = argsBean.getIdentity();

            if (!commandStr.equals("list_peers") && !commandStr.equals("connect_peer")
                    && !commandStr.equals("disconnect_peer")) {
                System.out.println(commandStr + "is not a valid command");
                return;
            }

            if (commandStr.equals("disconnect_peer") || commandStr.equals("connect_peer")) {
                if (argsBean.getPeer() == null) {
                    System.out.println("you need to provide -p peerHost:peerPort");
                    return;
                }
                peerPort = Integer.parseInt(argsBean.getPeer().split(":")[1]);
                peerHost = argsBean.getServer().split(":")[0];
            }

            /*
            //String commandStr = "list_peers";
            //String commandStr = "connect_peer";
            String commandStr = "disconnect_peer";
            int serverPort = 3001;
            String serverHost = "10.0.0.231";
            int peerPort = 8116;
            String peerHost = "10.0.0.231";
            String identity = "your_email@example.com";
            */
            Socket toPeer = new Socket(serverHost, serverPort);
            BufferedReader in = new BufferedReader(new InputStreamReader(toPeer.getInputStream(), "UTF-8"));
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(toPeer.getOutputStream(), "UTF-8"));
            String inputStr;
            Document newCommand;

            switch (commandStr){
                case "list_peers":{
                    sendAuthRequest(out,identity);
                    inputStr = in.readLine();
                    System.out.println("server say:" + inputStr);
                    newCommand = Document.parse(inputStr);
                    if(!newCommand.getString("command").equals("AUTH_RESPONSE")) break;
                    if(!newCommand.getBoolean("status")){
                        System.out.println();
                        break;
                    }
                    aesKeyStr = decryptRSA(newCommand.getString("AES128"),privateKey);
                    sendEncryptedListPeerRequest(out);
                    inputStr = in.readLine();
                    newCommand = Document.parse(inputStr);
                    System.out.println("server says(encrypted):" + inputStr);
                    System.out.println("server says(original):" + decryptAES(newCommand.getString("payload")));
                    break;
                }
                case "connect_peer":{
                    sendAuthRequest(out,identity);
                    inputStr = in.readLine();
                    System.out.println("server say:" + inputStr);
                    newCommand = Document.parse(inputStr);
                    if(!newCommand.getString("command").equals("AUTH_RESPONSE")) break;
                    if(!newCommand.getBoolean("status")) break;
                    aesKeyStr = decryptRSA(newCommand.getString("AES128"),privateKey);
                    sendEncryptedConnectPeerRequest(out,peerHost,peerPort);
                    inputStr = in.readLine();
                    newCommand = Document.parse(inputStr);
                    System.out.println("server says(encrypted):" + inputStr);
                    System.out.println("server says(original):" + decryptAES(newCommand.getString("payload")));
                    break;
                }
                case "disconnect_peer":{
                    sendAuthRequest(out,identity);
                    inputStr = in.readLine();
                    System.out.println("server say:" + inputStr);
                    newCommand = Document.parse(inputStr);
                    if(!newCommand.getString("command").equals("AUTH_RESPONSE")) break;
                    if(!newCommand.getBoolean("status")) break;
                    aesKeyStr = decryptRSA(newCommand.getString("AES128"),privateKey);
                    sendEncryptedDisconnectPeerRequest(out,peerHost,peerPort);
                    inputStr = in.readLine();
                    newCommand = Document.parse(inputStr);
                    System.out.println("server says(encrypted):" + inputStr);
                    System.out.println("server says(original):" + decryptAES(newCommand.getString("payload")));
                    break;
                }
            }
            toPeer.close();
        } catch(NoSuchAlgorithmException | BadPaddingException | NoSuchPaddingException | IllegalBlockSizeException | InvalidKeyException | CmdLineException e ){
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
        }
    }

    private static void sendEncryptedListPeerRequest(BufferedWriter out) throws NoSuchPaddingException, BadPaddingException, NoSuchAlgorithmException, IllegalBlockSizeException, IOException, InvalidKeyException {
        Document listPeerRequest = new Document();
        listPeerRequest.append("command","LIST_PEERS_REQUEST");
        Document encryptedRequest = new Document();
        encryptedRequest.append("payload",encryptAES(listPeerRequest.toJson()));
        out.write(encryptedRequest.toJson() + "\n");
        out.flush();
    }
    private static void sendEncryptedConnectPeerRequest(BufferedWriter out, String host, int port) throws NoSuchPaddingException, BadPaddingException, NoSuchAlgorithmException, IllegalBlockSizeException, IOException, InvalidKeyException {
        Document connectPeerRequest = new Document();
        connectPeerRequest.append("command","CONNECT_PEER_REQUEST");
        connectPeerRequest.append("host",host);
        connectPeerRequest.append("port",port);
        Document encryptedRequest = new Document();
        encryptedRequest.append("payload",encryptAES(connectPeerRequest.toJson()));
        out.write(encryptedRequest.toJson() + "\n");
        out.flush();
    }
    private static void sendEncryptedDisconnectPeerRequest(BufferedWriter out, String host, int port) throws IOException, NoSuchAlgorithmException, BadPaddingException, NoSuchPaddingException, IllegalBlockSizeException, InvalidKeyException {
        Document disconnectPeerRequest = new Document();
        disconnectPeerRequest.append("command","DISCONNECT_PEER_REQUEST");
        disconnectPeerRequest.append("host",host);
        disconnectPeerRequest.append("port",port);
        Document encryptedRequest = new Document();
        encryptedRequest.append("payload",encryptAES(disconnectPeerRequest.toJson()));
        out.write(encryptedRequest.toJson() + "\n");
        out.flush();
    }
    private static void sendAuthRequest(BufferedWriter out,String identity) throws IOException {
        Document AuthRequest = new Document();
        AuthRequest.append("command","AUTH_REQUEST");
        AuthRequest.append("identity",identity);
        out.write(AuthRequest.toJson() + "\n");
        out.flush();
    }

    private static String decryptAES(String encrytedStr) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException, BadPaddingException, IllegalBlockSizeException {
        byte[] base64 = Base64.getDecoder().decode(encrytedStr.getBytes());
        Key aesKey = new SecretKeySpec(aesKeyStr.getBytes(), "AES");
        Cipher cipher = Cipher.getInstance("AES");
        cipher.init(Cipher.DECRYPT_MODE, aesKey);
        return new String(cipher.doFinal(base64));
    }

    private static String encryptAES(String original) throws InvalidKeyException, NoSuchPaddingException, NoSuchAlgorithmException, UnsupportedEncodingException, BadPaddingException, IllegalBlockSizeException {
        Key aesKey = new SecretKeySpec(aesKeyStr.getBytes(), "AES");

        Cipher cipher = Cipher.getInstance("AES");
        cipher.init(Cipher.ENCRYPT_MODE, aesKey);
        byte[] encrypted = cipher.doFinal(original.getBytes("UTF-8"));

        //System.out.println("original:"+original);
        //System.out.println("encrypted byte length: "+encrypted.length);
        //System.out.println("AES+BASE64 String: "+base64A);

        return new String(Base64.getEncoder().encode(encrypted));
    }

    private static String decryptRSA(String encryptedStr, PrivateKey privateKey) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException, BadPaddingException, IllegalBlockSizeException {
        byte decodeBase64 [] = Base64.getDecoder().decode(encryptedStr);
        Cipher cipher = Cipher.getInstance("RSA/ECB/PKCS1Padding");
        cipher.init(Cipher.DECRYPT_MODE, privateKey);
        //System.out.println(("rsa encode str(AES):"+ encryptedStr));
        //System.out.println(("rsa decode str(AES):"+new String(cipher.doFinal(decodeBase64))));

        return new String(cipher.doFinal(decodeBase64));
    }
}

class CmdLineArgs {

    @Option(required = true, name = "-c", usage = "-c command e.q.: -c list_peers")
    private String command;

    @Option(required = true, name = "-s", usage = "-s serverHost:serverPort e.q.: -s localhost:3000")
    private String server;

    @Option(required = false, name = "-p", usage = "-p peerHost:peerPort e.q.: -p localhost:3000")
    private String peer;

    @Option(required = true, name = "-i", usage = "-i identify e.q.: -i example@gmail.com")
    private String identity;

    String getCommand() {return command; }

    String getServer() { return server; }

    String getPeer() { return peer; }

    String getIdentity() { return identity; }

}

