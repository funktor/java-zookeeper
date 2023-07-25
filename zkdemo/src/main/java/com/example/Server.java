package com.example;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.json.JSONObject;

import java.util.List;

class MessageParsedTuple {
    List<String> parts;
    String finalString;

    public MessageParsedTuple(List<String> parts, String finalString) {
        this.parts = parts;
        this.finalString = finalString;
    }
}

public class Server {
    private static ZKClientManagerImpl zkmanager = new ZKClientManagerImpl();
    private static String hostPort;
    private static ConsistentHashing partitioner = new ConsistentHashing();
    private static MyHashMap myMap = new MyHashMap();
    private static Map<String, SocketChannel> nodeMap = new HashMap<String, SocketChannel>();
    private static Map<String, SocketChannel> requestMap = new HashMap<String, SocketChannel>();
    private static Selector selector;
    private static ServerSocketChannel serverSocket;

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        hostPort = host + ":" + String.valueOf(port);

        synchronized(partitioner) {
            partitioner.insert(hostPort);
        }

        byte[] data = hostPort.getBytes();

        zkmanager.create("/partition", "Hello".getBytes(), true);
        zkmanager.create("/partition/" + hostPort, data, false);

        new Thread(() -> addOtherPartitions()).start();

        selector = Selector.open();
        serverSocket = ServerSocketChannel.open();

        serverSocket.bind(new InetSocketAddress("localhost", port));
        serverSocket.configureBlocking(false);
        serverSocket.register(selector, SelectionKey.OP_ACCEPT);

        while (true) {
            selector.select();
            Set<SelectionKey> selectedKeys = selector.selectedKeys();
            Iterator<SelectionKey> iter = selectedKeys.iterator();
            
            while (iter.hasNext()) {

                SelectionKey key = iter.next();

                if (key.isAcceptable()) {
                    SocketChannel client = serverSocket.accept();
                    register(selector, client);
                }

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

    private static MessageParsedTuple split(String str, String delim) {
        List<String> parts = new ArrayList<String>();

        while(true) {
            int pos = str.indexOf(delim);
            if (pos >= 0) {
                String sub = str.substring(0, pos);
                if (sub.length() > 0) {
                    parts.add(sub);
                }
                str = str.substring(pos+delim.length());
            }
            else {
                break;
            }
        }

        return new MessageParsedTuple(parts, str);
    }

    private static List<String> getMessages(SocketChannel client) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        String remainder = "";
        String delim = "<EOM>";
        List<String> all_msgs = new ArrayList<String>();

        while(true) {
            int r = client.read(buffer);

            if (r > 0) {
                String msg = new String(buffer.array(), 0, r);
                System.out.println(msg);

                msg = remainder + msg;
                MessageParsedTuple parsedTuple = split(msg, delim);

                List<String> parts = parsedTuple.parts;
                msg = parsedTuple.finalString;

                all_msgs.addAll(parts);
                remainder = msg;
                buffer.clear();
            }
            else if (r == 0) {
                break;
            }
            else {
                client.close();
                System.out.println("Not accepting client messages anymore");
                break;
            }
        }

        return all_msgs;
    }

    private static void register(Selector selector, SocketChannel client)
      throws IOException {
        client.configureBlocking(false);
        client.register(selector, SelectionKey.OP_READ);
    }

    private static void addOtherPartitions() {
        while(true) {
            try {
                synchronized(partitioner) {
                    List<String> partitions = zkmanager.getZNodeChildren("/partition");
                    
                    for (String partition : partitions) {
                        if (partition != hostPort) {
                            partitioner.insert(partition);
                        }
                    }
                }

                TimeUnit.SECONDS.sleep(1);

            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    } 

    private static void handleRequest(String request, SocketChannel client) {
        System.out.println(request);
        JSONObject obj = new JSONObject(request);

        String op = obj.getString("operator");
        String data = obj.getString("data");
        String request_id = obj.getString("request_id");
        long timestamp = obj.getLong("timestamp");
        int request_type = obj.getInt("request_type");

        try {
            if (request_type == 1) {
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
                        partitioner.print();
                        String node = partitioner.getNext(key);

                        if (node.equals(hostPort)) {
                            myMap.insert(key, val, timestamp);

                            obj.put("request_type", 1);
                            obj.put("data", "OK");
                            obj.put("node", hostPort);

                            String clientMsg = obj.toString() + "<EOM>";
                            client.write(ByteBuffer.wrap(clientMsg.getBytes()));
                        }
                        else {
                            SocketChannel socket;

                            if (nodeMap.containsKey(node)) {
                                socket = nodeMap.get(node);
                            }
                            else {
                                String[] ipPort = node.split(":");
                                String ip = ipPort[0];
                                int port = Integer.parseInt(ipPort[1]);
                                socket = SocketChannel.open(new InetSocketAddress(ip, port));
                                register(selector, socket);
                                nodeMap.put(node, socket);
                            }

                            requestMap.put(request_id, client);
                            String requestMsg = request + "<EOM>";
                            ByteBuffer buffer = ByteBuffer.wrap(requestMsg.getBytes());
                            socket.write(buffer);
                        }
                    }
                }

                else if (op.equals("GET")) {
                    String key = data;

                    synchronized(partitioner) {
                        String node = partitioner.getNext(key);

                        if (node.equals(hostPort)) {
                            String val = myMap.get(key);

                            obj.put("request_type", 1);
                            obj.put("data", val);
                            obj.put("node", hostPort);

                            String clientMsg = obj.toString() + "<EOM>";
                            client.write(ByteBuffer.wrap(clientMsg.getBytes()));
                        }
                        else {
                            SocketChannel socket;
                            
                            if (nodeMap.containsKey(node)) {
                                socket = nodeMap.get(node);
                            }
                            else {
                                String[] ipPort = node.split(":");
                                String ip = ipPort[0];
                                int port = Integer.parseInt(ipPort[1]);
                                socket = SocketChannel.open(new InetSocketAddress(ip, port));
                                register(selector, socket);
                                nodeMap.put(node, socket);
                            }

                            String requestMsg = request + "<EOM>";
                            ByteBuffer buffer = ByteBuffer.wrap(requestMsg.getBytes());
                            socket.write(buffer);
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
