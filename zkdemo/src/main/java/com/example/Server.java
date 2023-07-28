package com.example;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.json.JSONObject;

import java.util.List;

class MessageParsedTuple {
    String[] parts;
    String finalString;

    public MessageParsedTuple(String[] parts, String finalString) {
        this.parts = parts;
        this.finalString = finalString;
    }
}

class NodeLeader {
    public String connData;
    public int sequence_data;
    public int zk_node;

    public NodeLeader(String connData, int sequence_data, int zk_node) {
        this.connData = connData;
        this.sequence_data = sequence_data;
        this.zk_node = zk_node;
    }
}

public class Server {
    private static String hostPort;
    private static MyHashMap myMap = new MyHashMap();
    private static Map<String, SocketChannel> requestMap = new HashMap<String, SocketChannel>();
    private static Selector selector;
    private static ServerSocketChannel serverSocket;
    private static ZKClientManagerImpl zkmanager = new ZKClientManagerImpl();
    private static ConsistentHashing partitioner = new ConsistentHashing();
    private static Map<String, SocketChannel> nodeMap = new HashMap<String, SocketChannel>();
    private static String partitionId;
    private static boolean isLeader=false;
    private static List<String> replicas = Collections.synchronizedList(new ArrayList<String>());
    private static String partitionLeaderNode;
    private static CommitLog commitLog;
    private static String DELIM = "<EOM>";

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        // Get server host IP, port and partition id from command line
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        hostPort = host + ":" + String.valueOf(port);
        partitionId = args[2];

        // Commit log
        commitLog = new CommitLog("commitlog-" + hostPort + ".txt");

        // Add server to consistent hashing table
        addNodeToPartitioner();

        // Create ZNodes for server
        createZnodes();

        // Run background threads for adding server to cluster, reconcile keys 
        // in the consistent hashing ring and replicate logs to other replicas
        new Thread(() -> addNodeToCluster()).start();
        new Thread(() -> runReconciliation()).start();
        new Thread(() -> replicate()).start();

        // Create socket selector and a socket for server
        selector = Selector.open();
        serverSocket = ServerSocketChannel.open();

        // Make server socket non-blocking
        serverSocket.bind(new InetSocketAddress("localhost", port));
        serverSocket.configureBlocking(false);

        // Register server socket to selector
        // Server socket can only ACCEPT, client sockets are READ
        serverSocket.register(selector, SelectionKey.OP_ACCEPT);

        while (true) {
            // Get all sockets ready to connect or send message
            selector.select();
            Set<SelectionKey> selectedKeys = selector.selectedKeys();
            Iterator<SelectionKey> iter = selectedKeys.iterator();
            
            while (iter.hasNext()) {
                SelectionKey key = iter.next();

                // Client ready to connect to server.
                // Accept and register the client socket.
                if (key.isAcceptable()) {
                    SocketChannel client = serverSocket.accept();
                    register(selector, client);
                }

                // Client socket ready to send messages to server
                if (key.isReadable()) {
                    SocketChannel client = (SocketChannel) key.channel();
                    List<String> msgs = getMessages(client);
                    for (String msg : msgs) {
                        handleRequest(msg, client);
                    }
                }

                iter.remove();
            }
        }
    }

    public static MessageParsedTuple split(String str, String delim) {
        // Split str by delimiter
        // Update str to point to last part after splitting
        try {
            String[] parts = str.split(delim);
            str = parts[parts.length-1];

            return new MessageParsedTuple(parts, str);
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        return null;
    }

    public static void register(Selector selector, SocketChannel client)
      throws IOException {
        // Register socket with selector
        // Need to make non-blocking
        client.configureBlocking(false);
        client.register(selector, SelectionKey.OP_READ);
    }

    public static List<String> getMessages(SocketChannel client) throws IOException {
        // Parse messages from client socket
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        String remainder = "";
        List<String> all_msgs = new ArrayList<String>();

        while(true) {
            // Read r bytes from client socket into buffer
            int r = client.read(buffer);

            if (r > 0) {
                // Convert to string
                String msg = new String(buffer.array(), 0, r);

                // Update msg by adding the in-complete message
                // from last invocation to current invocation.
                msg = remainder + msg;
                MessageParsedTuple parsedTuple = split(msg, DELIM);

                String[] parts = parsedTuple.parts;
                msg = parsedTuple.finalString;

                // Add all complete messages into list
                all_msgs.addAll(Arrays.asList(parts));

                // Update remainder to point to incomplete message.
                remainder = msg;
                buffer.clear();
            }
            else if (r == 0) {
                // No more data to send
                break;
            }
            else {
                // Client closed connection
                client.close();
                System.out.println("Not accepting client messages anymore");
                break;
            }
        }

        return all_msgs;
    }

    public static void sendMessage(String request, String nodeHostPort) {
        // Send message to nodeHostPort
        try {
            SocketChannel socket;

            // Reuse socket to send message
            if (nodeMap.containsKey(nodeHostPort)) {
                socket = nodeMap.get(nodeHostPort);
            }
            else {
                // Socket being used for the 1st time
                String[] ipPort = nodeHostPort.split(":");
                String ip = ipPort[0];
                int port = Integer.parseInt(ipPort[1]);
                socket = SocketChannel.open(new InetSocketAddress(ip, port));
                register(selector, socket);
                nodeMap.put(nodeHostPort, socket);
            }

            // Write to socket
            ByteBuffer buffer = ByteBuffer.wrap(request.getBytes());
            
            // If socket got closed before writing
            int num_retries = 5;
            int ret = 0;

            while(ret < num_retries) {
                int m = socket.write(buffer);

                if (m == -1) {
                    // Recreate socket and add again after 2 seconds.
                    socket.close();

                    TimeUnit.SECONDS.sleep(2);

                    String[] ipPort = nodeHostPort.split(":");
                    String ip = ipPort[0];
                    int port = Integer.parseInt(ipPort[1]);
                    socket = SocketChannel.open(new InetSocketAddress(ip, port));
                    register(selector, socket);
                    nodeMap.put(nodeHostPort, socket);
                }
                else {
                    break;
                }
                ret += 1;
            }

        } catch (Exception e) {
            e.printStackTrace();
        } 
    }

    public static void handleRequest(String request, SocketChannel client) {
        // Handle client request

        System.out.println(request);
        JSONObject obj = new JSONObject(request);

        String op = obj.getString("operator");
        String data = obj.getString("data");
        String request_id = obj.getString("request_id");
        long timestamp = obj.getLong("timestamp");
        int request_type = obj.getInt("request_type");

        try {
            if (request_type == 1) {
                // Response given by another server
                // Forward to client if required
                if (requestMap.containsKey(request_id)) {
                    client = requestMap.get(request_id);
                    String clientMsg = obj.toString() + "<EOM>";
                    client.write(ByteBuffer.wrap(clientMsg.getBytes()));
                }
            }
            else {
                if (op.equals("PUT")) {
                    String[] dataParts = data.split(":");

                    String key = dataParts[0];
                    String val = dataParts[1];

                    synchronized(partitioner) {
                        String partition = partitioner.getNext(key, false);

                        if (partition.equals(partitionId)) {
                            writeLog(request);
                            int seq = commitLog.getSequence();
                            updateSequence(seq);

                            myMap.insert(key, val, timestamp);

                            obj.put("request_type", 1);
                            obj.put("data", "OK");
                            obj.put("node", hostPort);

                            String clientMsg = obj.toString() + "<EOM>";
                            client.write(ByteBuffer.wrap(clientMsg.getBytes()));
                        }
                        else {
                            String node = getLeaderForPartition(partition);

                            requestMap.put(request_id, client);
                            sendMessage(request + "<EOM>", node);
                        }
                    }
                }

                else if (op.equals("GET")) {
                    String key = data;

                    synchronized(partitioner) {
                        String partition = partitioner.getNext(key, false);

                        if (partition.equals(partitionId)) {
                            String val = myMap.get(key);

                            obj.put("request_type", 1);
                            obj.put("data", val);
                            obj.put("node", hostPort);

                            String clientMsg = obj.toString() + "<EOM>";
                            client.write(ByteBuffer.wrap(clientMsg.getBytes()));
                        }
                        else {
                            String node = getLeaderForPartition(partition);
                            
                            requestMap.put(request_id, client);
                            sendMessage(request + "<EOM>", node);
                        }
                    }
                }

                else if (op.equals("RECONCILE-KEYS")) {
                    synchronized(partitioner) {
                        int nodeHash = partitioner.getHash(data);

                        Set<String> keys = myMap.getKeys();
                        Set<String> toDelete = new HashSet<String>();

                        for (String k : keys) {
                            if (partitioner.getNextKey(k, false) == nodeHash) {
                                toDelete.add(k);
                            }
                        }

                        String response = "";

                        for (String s :  toDelete) {
                            JSONObject jsonObj = new JSONObject();

                            UUID uuid = UUID.randomUUID();
                            String val = myMap.get(s);
                            long ts = myMap.getTimestamp(s);

                            jsonObj.put("operator", "PUT");
                            jsonObj.put("request_id", uuid.toString());
                            jsonObj.put("data", s + ":" + val);
                            jsonObj.put("request_type", 0);
                            jsonObj.put("timestamp", ts);

                            response += jsonObj.toString() + "<EOM>";
                            myMap.delete(s);
                        }

                        client.write(ByteBuffer.wrap(response.getBytes()));
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void createZnodes() {
        try {
            byte[] data = "Hello".getBytes();

            zkmanager.create("/sequence", data, true, false);
            zkmanager.create("/replicas", data, true, false);
            zkmanager.create("/leader", data, true, false);

            zkmanager.create("/sequence/" + hostPort, "-1".getBytes(), false, false);
            zkmanager.create("/replicas/" + partitionId, data, true, false);
            zkmanager.create("/replicas/" + partitionId + "/" + hostPort + "_", data, false, true);
            zkmanager.create("/leader/" + partitionId, hostPort.getBytes(), true, false);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void addNodeToPartitioner() {
        try {
            synchronized(partitioner) {
                partitioner.insert(partitionId);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String getLeaderNode(List<String> replicas) {
        try {
            PriorityQueue<NodeLeader> pq = new PriorityQueue<>(new Comparator<NodeLeader>() {
                public int compare(NodeLeader a, NodeLeader b) {
                    if (a.sequence_data == b.sequence_data) {
                        return a.zk_node - b.zk_node;
                    }
                    else if (a.sequence_data < b.sequence_data) {
                        return 1;
                    }
                    return -1;
                }
            });

            for (String replica : replicas) {
                String[] reps = replica.split("_");

                String seq = zkmanager.getZNodeData("/sequence/" + reps[0], false);
                int sequence = Integer.parseInt(seq);
                
                NodeLeader l = new NodeLeader(reps[0], sequence, Integer.parseInt(reps[1]));
                pq.add(l);
            }

            NodeLeader leader = pq.peek();
            return leader.connData;

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public static void addOtherPartitions() {
        try {
            synchronized(partitioner) {
                List<String> partIds = zkmanager.getZNodeChildren("/replicas");
                
                for (String part : partIds) {
                    if (!part.equals(partitionId)) {
                        partitioner.insert(part);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void addReplicas() {
        try {
            replicas = zkmanager.getZNodeChildren("/replicas/" + partitionId);
            partitionLeaderNode = getLeaderNode(replicas);

            if (partitionLeaderNode.equals(hostPort)) {
                zkmanager.update("/leader/" + partitionId, hostPort.getBytes());
                isLeader = true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void addNodeToCluster() {
        while(true) {
            try {
                addOtherPartitions();
                addReplicas();

                TimeUnit.SECONDS.sleep(1);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void reconcileKeys() {
        try {
            if (isLeader) {
                synchronized(partitioner) {
                    String partition = partitioner.getNext(partitionId, true);

                    if (!partition.equals(partitionId)) {
                        String nextNode = getLeaderForPartition(partition);

                        JSONObject jsonObj = new JSONObject();
                        UUID uuid = UUID.randomUUID();

                        jsonObj.put("operator", "RECONCILE-KEYS");
                        jsonObj.put("request_id", uuid.toString());
                        jsonObj.put("data", partitionId);
                        jsonObj.put("request_type", 0);
                        jsonObj.put("timestamp", System.currentTimeMillis());

                        sendMessage(jsonObj.toString() + "<EOM>", nextNode);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void runReconciliation() {
        try {
            while(true) {
                reconcileKeys();
                TimeUnit.SECONDS.sleep(5);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void replicate() {
        try {
            while(true) {
                if (isLeader) {
                    for (String replica : replicas) {
                        String[] reps = replica.split("_");
                        replica = reps[0];

                        if (!replica.equals(hostPort)) {
                            String seq = zkmanager.getZNodeData("/sequence/" + replica, false);
                            int seqLong = Integer.parseInt(seq);

                            List<String> logsToSend = getLogs(seqLong+1);

                            String msg = "";
                            for (String log : logsToSend) {
                                msg += log + "<EOM>";
                            }

                            sendMessage(msg, replica);
                        }
                    }
                }

                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void writeLog(String msg) {
        try {
            commitLog.log(msg);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static List<String> getLogs(int start) {
        try {
            return commitLog.readLines(start);
            
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public static void updateSequence(int sequence) {
        try {
            zkmanager.update("/sequence/" + hostPort, Integer.toString(sequence).getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String getLeaderForPartition(String pid) {
        String leader = null;
        try {
            leader = zkmanager.getZNodeData("/leader/" + pid, false);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return leader;
    }
}
