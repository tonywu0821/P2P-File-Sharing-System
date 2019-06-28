package unimelb.bitbox;

import unimelb.bitbox.util.Configuration;
import unimelb.bitbox.util.Document;

import java.net.DatagramPacket;
import java.util.ArrayList;
import java.util.HashMap;

/** A helper class can count the number of incomming connections and provides some necessary
 * functions such as isFull() and isConnected
 * @author Tong-Ing Wu, Lih-Ren Chaung.
 */

public class ConnectionCounter {
    private final int maxIncommingConnections;
    private HashMap<String,Document> conncetionHashMap;
    private int incommingConnections = 0;
    private HashMap<String,Document> incommingConncetionHashMap;
    private HashMap<String,Document> udpClientHashMap;
    private HashMap<String,Document> udpToIncommingHashMap;
    private HashMap<String,Document> incommingToUdpHashMap;
    ConnectionCounter(){
        this.maxIncommingConnections = Integer.parseInt(Configuration.getConfiguration()
                .get("maximumIncommingConnections").trim());
        this.conncetionHashMap = new HashMap<>();
        this.incommingConncetionHashMap = new HashMap<>();
        this.udpClientHashMap = new HashMap<>();
        this.udpToIncommingHashMap = new HashMap<>();
        this.incommingToUdpHashMap = new HashMap<>();
    }
    ArrayList<Document> getConnections(){
        ArrayList<Document> connections = new ArrayList<>();
        synchronized(this) {
            connections.addAll(conncetionHashMap.values());
        }
        return connections;
    }

    ArrayList<Document> getUdpConnections(){
        ArrayList<Document> connections = new ArrayList<>();
        synchronized(this) {
            connections.addAll(udpClientHashMap.values());
        }
        return connections;
    }

    String getInCommingConnectionStrByUdp(Document udpConnection){
        String key = udpConnection.getString("host") + ":" + String.valueOf(udpConnection.get("port"));
        synchronized (this) {
            if(!udpToIncommingHashMap.containsKey(key)){return null;}
            Document connection = udpToIncommingHashMap.get(key);
            return connection.getString("host") + ":" + String.valueOf(connection.get("port"));
        }
    }

    Document getUdpConnectionByInComming(Document inCommingConnection){
        String key = inCommingConnection.getString("host") + ":" + String.valueOf(inCommingConnection.get("port"));
        synchronized (this){
            if(!incommingToUdpHashMap.containsKey(key)){return null;}
            return incommingToUdpHashMap.get(key);
        }
    }


    boolean isFull(){
        synchronized(this) {
            return !(incommingConnections < maxIncommingConnections);
        }
    }
    void addConnection(Document connection){
        String key = connection.getString("host") + ":" + String.valueOf(connection.get("port"));
        synchronized(this) {
            conncetionHashMap.put(key,connection);
        }
    }
    void addIncommingConnection(Document connection){
        String key = connection.getString("host") + ":" + String.valueOf(connection.get("port"));
        synchronized(this) {
            conncetionHashMap.put(key,connection);
            incommingConncetionHashMap.put(key,connection);
            incommingConnections += 1;
        }
    }

    void addUdpConnection(Document udpConnection,Document newConnection){
        String key = udpConnection.getString("host") + ":" + String.valueOf(udpConnection.get("port"));
        String key2 = newConnection.getString("host") + ":" + String.valueOf(newConnection.get("port"));
        synchronized(this) {
            addIncommingConnection(newConnection);
            udpClientHashMap.put(key,udpConnection);
            udpToIncommingHashMap.put(key,newConnection);
            incommingToUdpHashMap.put(key2,udpConnection);
        }
    }

    void removeUdpConnection(Document udpConnection){
        String key = udpConnection.getString("host") + ":" + String.valueOf(udpConnection.get("port"));
        synchronized(this) {
            Document incommingConnection = udpToIncommingHashMap.get(key);
            String key2 = incommingConnection.getString("host") + ":" + String.valueOf(incommingConnection.get("port"));
            incommingToUdpHashMap.remove(key2);
            udpToIncommingHashMap.remove(key);
            removeIncommingConnection(incommingConnection);
            udpClientHashMap.remove(key);
        }
    }

    void removeUdpConnectionByIncomming(Document incommingConnection){
        String key = incommingConnection.getString("host") + ":" + String.valueOf(incommingConnection.get("port"));
        synchronized (this){
            removeIncommingConnection(incommingConnection);
            Document udpConnection = incommingToUdpHashMap.get(key);
            //System.out.println("key1:"+key);
            String key2 = udpConnection.getString("host") + ":" + String.valueOf(udpConnection.get("port"));
            //System.out.println("key2:"+key2);
            udpClientHashMap.remove(key2);
            udpToIncommingHashMap.remove(key2);
        }
    }

    void removeConnection(Document connection){
        String key = connection.getString("host") + ":" + String.valueOf(connection.get("port"));
        synchronized(this) {
            conncetionHashMap.remove(key);
        }
    }

    void removeIncommingConnection(Document connection){
        String key = connection.getString("host") + ":" + String.valueOf(connection.get("port"));
        synchronized(this) {
            conncetionHashMap.remove(key);
            incommingConncetionHashMap.remove(key);
            incommingConnections -= 1;
        }
    }

    int getnConnectionNum(){
        synchronized(this) {
            return conncetionHashMap.size();
        }
    }

    Boolean isConnected(Document connection){
        String key = connection.getString("host") + ":" + String.valueOf(connection.get("port"));
        synchronized(this) {
            return conncetionHashMap.containsKey(key);
        }
    }

    Boolean isConnectedUdp(Document connection){
        String key = connection.getString("host") + ":" + String.valueOf(connection.get("port"));
        synchronized(this) {
            return udpClientHashMap.containsKey(key);
        }
    }
}
