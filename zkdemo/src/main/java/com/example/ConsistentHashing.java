package com.example;

import java.util.TreeMap;

public class ConsistentHashing {
    TreeMap<Integer, String> keyValueMap = new TreeMap<Integer, String>();

    public int getHash(String value) {
        return value.hashCode();
    }

    public void insert(String value) {
        int key = getHash(value);
        keyValueMap.put(key, value);
    }

    public String getNext(String value) {
        int key = getHash(value);
        return keyValueMap.higherEntry(key).getValue();
    }

    public void delete(String value) {
        int key = getHash(value);
        if (keyValueMap.containsKey(key)) {
            keyValueMap.remove(key);
        }
    }

    public void clear() {
        keyValueMap.clear();
    }

    public boolean exists(String value) {
        int key = getHash(value);
        return keyValueMap.containsKey(key);
    }
}
