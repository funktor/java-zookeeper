package com.example;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Scanner;
import java.util.UUID;

import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class Client {
    private static SocketChannel socket;
    private static Client instance;

    public static Client start() {
        if (instance == null)
            instance = new Client();

        return instance;
    }

    public static void stop() throws IOException {
        socket.close();
    }

    private Client() {
        try {
            socket = SocketChannel.open(new InetSocketAddress("localhost", 5001));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private MessageParsedTuple split(String str, String delim) {
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

    public void getMessage() {
        ByteBuffer buffer = ByteBuffer.allocate(5);
        String remainder = "";
        String delim = "<EOM>";

        try {
            while(true) {
                int r = socket.read(buffer);

                if (r > 0) {
                    String msg = new String(buffer.array(), 0, r);
                    msg = remainder + msg;
                    MessageParsedTuple parsedTuple = split(msg, delim);

                    List<String> parts = parsedTuple.parts;
                    msg = parsedTuple.finalString;

                    for (String in_msg : parts) {
                        JSONObject jsonObject = new JSONObject(in_msg);
                        System.out.println("response : " + jsonObject.getString("data"));
                        System.out.println("response-node : " + jsonObject.getString("node"));
                    }

                    remainder = msg;
                    buffer.clear();
                }
                else {
                    remainder = "";
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void sendMessage(String msg) {
        ByteBuffer buffer = ByteBuffer.wrap(msg.getBytes());
        try {
            socket.write(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Scanner in = new Scanner(System.in);
        Client client = start();

        new Thread(() -> client.getMessage()).start();

        try {
            while(true) {
                String input = in.nextLine();
                String[] inputs = input.split(":");

                JSONObject jsonObj = new JSONObject();

                UUID uuid = UUID.randomUUID();

                if (inputs.length == 1) {
                    jsonObj.put("operator", "GET");
                }
                else {
                    jsonObj.put("operator", "PUT");
                }

                jsonObj.put("request_id", uuid.toString());
                jsonObj.put("data", input);
                jsonObj.put("request_type", 0);
                jsonObj.put("timestamp", System.currentTimeMillis());

                String msg = jsonObj.toString() + "<EOM>";
                client.sendMessage(msg);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            in.close();
        }
    }
}
