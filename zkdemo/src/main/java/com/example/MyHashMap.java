package com.example;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

class MapObject {
    String value;
    long timestamp;

    public MapObject(String value, long timestamp) {
        this.value = value;
        this.timestamp = timestamp;
    }
}

public class MyHashMap {
    private static Map<String, MapObject> myMap = new HashMap<String, MapObject>();
    
    public void insert(String key, String value, long ts) {
        if (!myMap.containsKey(key) || myMap.get(key).timestamp < ts) {
            MapObject obj = new MapObject(value, ts);
            myMap.put(key, obj);
        }
    }

    public String get(String key) {
        if (myMap.containsKey(key)) {
            return myMap.get(key).value;
        }
        return "NOT FOUND";
    }

    public long getTimestamp(String key) {
        if (myMap.containsKey(key)) {
            return myMap.get(key).timestamp;
        }
        return -1;
    }

    public Set<String> getKeys() {
        return myMap.keySet();
    }

    public void delete(String key) {
        if (myMap.containsKey(key)) {
            myMap.remove(key);
        }
    }
}
