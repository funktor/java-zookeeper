package com.example;

import java.util.TreeMap;

import org.apache.commons.codec.digest.MurmurHash3;

import java.util.Map;

public class ConsistentHashing {
    TreeMap<Integer, String> keyValueMap = new TreeMap<Integer, String>();

    public int getHash(String value) {
        return MurmurHash3.hash32x86(value.getBytes());
    }

    public void insert(String value) {
        int key = getHash(value);
        keyValueMap.put(key, value);
    }

    public String getNext(String value) {
        int key = getHash(value);

        if (keyValueMap.containsKey(key)) {
            return keyValueMap.get(key);
        }

        Map.Entry<Integer, String> entry = keyValueMap.higherEntry(key);

        if (entry == null) {
            return keyValueMap.firstEntry().getValue();
        }

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

    public void print() {
        for (int k : keyValueMap.keySet()) {
            System.out.println(k + " : " + keyValueMap.get(k));
        }
    }
}
