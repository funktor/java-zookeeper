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
    private static Map<String, String> myMap = new HashMap<String, String>();
    private static Map<String, SocketChannel> nodeMap = new HashMap<String, SocketChannel>();

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        hostPort = host + ":" + String.valueOf(port);
        partitioner.insert(hostPort);

        byte[] data = hostPort.getBytes();

        zkmanager.create("/partition", "Hello".getBytes(), true);
        zkmanager.create("/partition/" + hostPort, data, false);

        new Thread(() -> addOtherPartitions()).start();

        Selector selector = Selector.open();
        ServerSocketChannel serverSocket = ServerSocketChannel.open();
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
                    register(selector, serverSocket);
                }

                if (key.isReadable()) {
                    SocketChannel client = (SocketChannel) key.channel();
                    List<String> msgs = getMessages(client);
                    for (String msg : msgs) {
                        System.out.println(msg);
                        msg += "<EOM>";
                        client.write(ByteBuffer.wrap(msg.getBytes()));
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
        ByteBuffer buffer = ByteBuffer.allocate(5);
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

    private static void register(Selector selector, ServerSocketChannel serverSocket)
      throws IOException {
 
        SocketChannel client = serverSocket.accept();
        client.configureBlocking(false);
        client.register(selector, SelectionKey.OP_READ);
    }

    private static void addOtherPartitions() {
        while(true) {
            try {
                partitioner.clear();
                List<String> partitions = zkmanager.getZNodeChildren("/partition");
                
                for (String partition : partitions) {
                    if (partition != hostPort) {
                        partitioner.insert(partition);
                    }
                }

                TimeUnit.SECONDS.sleep(1);

            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    } 

    private static void handleRequest(String request, SocketChannel client) {
        JSONObject obj = new JSONObject(request);
        String op = obj.getString("operator");

        if (op.equals("PUT")) {
            String data = obj.getString("data");
            String[] dataParts = data.split(":");

            String key = dataParts[0];
            String val = dataParts[1];

            String node = partitioner.getNext(key);

            try {
                if (node.equals(hostPort)) {
                    myMap.put(key, val);
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
                        nodeMap.put(node, socket);
                    }

                    String requestMsg = request + "<EOM>";
                    ByteBuffer buffer = ByteBuffer.wrap(requestMsg.getBytes());
                    socket.write(buffer);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        else if (op.equals("GET")) {
            String key = obj.getString("data");
            String node = partitioner.getNext(key);

            try {
                if (node.equals(hostPort)) {
                    String val = myMap.get(key) + "<EOM>";
                    ByteBuffer buffer = ByteBuffer.wrap(val.getBytes());
                    client.write(buffer);
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
                        nodeMap.put(node, socket);
                    }

                    String requestMsg = request + "<EOM>";
                    ByteBuffer buffer = ByteBuffer.wrap(requestMsg.getBytes());
                    socket.write(buffer);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            
        }
    }
}
