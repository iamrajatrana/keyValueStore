package io.cred.model;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Attribute {
    private final String name;
    private final String datatype;
    // key - values {can store values only for disk storage. }
    private ConcurrentHashMap<String, Object> idValueMap;

    // secondary index: value - keys mapping
    private ConcurrentHashMap<Object, List<String>> valueIdMap;

    public Attribute(String name, Object value) {
        this.name = name;
        this.datatype = value.getClass().getName();
        this.idValueMap = new ConcurrentHashMap<>();
        this.valueIdMap = new ConcurrentHashMap<>();
    }

    public String getName() {
        return name;
    }

    public String getDatatype() {
        return datatype;
    }

    public ConcurrentHashMap<String, Object> getIdValueMap() {
        return idValueMap;
    }


    public ConcurrentHashMap<Object, List<String>> getValueIdMap() {
        return valueIdMap;
    }

}
