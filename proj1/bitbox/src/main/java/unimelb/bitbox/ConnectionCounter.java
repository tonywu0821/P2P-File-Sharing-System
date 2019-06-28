package unimelb.bitbox;

import unimelb.bitbox.util.Configuration;
import unimelb.bitbox.util.Document;

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

    ConnectionCounter(){
        this.maxIncommingConnections = Integer.parseInt(Configuration.getConfiguration()
                .get("maximumIncommingConnections"));
        this.conncetionHashMap = new HashMap<String, Document>();
    }
    ArrayList<Document> getConnections(){
        ArrayList<Document> connections = new ArrayList<Document>();
        synchronized(this) {
            connections.addAll(conncetionHashMap.values());
        }
        return connections;
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
            incommingConnections += 1;
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
}
