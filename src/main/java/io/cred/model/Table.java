package io.cred.model;

import io.cred.exception.AttributeDoesNotExistsException;
import io.cred.exception.KeyAlreadyExistsException;
import io.cred.exception.KeyDoesNotExistsException;
import io.cred.util.ValidationUtil;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Table {

    private String name;
    private ConcurrentSkipListSet<String> keys = new ConcurrentSkipListSet<>();
    private ConcurrentHashMap<String, Attribute> attributesMap = new ConcurrentHashMap<>();
    private static final Map<String, Object> keyLocks = new ConcurrentHashMap<>();
    private final static String READLOCK = "readLock-";
    private final static String WRITELOCK = "writeLock-";


    public Table(String name) {
        this.name = name;
    }

    public long size() {
        return keys.size();
    }

    public void insertRecord(String key, List<Entry> entries) {
        if (this.keys.contains(key))
            throw new KeyAlreadyExistsException("Key {} already exists " + key);

        String lockKey = WRITELOCK + key;
        synchronized (keyLocks.computeIfAbsent(lockKey, (k) -> new Object())) {
            if (this.keys.contains(key))
                throw new KeyAlreadyExistsException("Key {} already exists " + key);
            try {
                if (ValidationUtil.validate(this, entries)) {
                    entries.forEach(e -> {
                        keys.add(key);
                        Attribute attribute = this.attributesMap.get(e.getName());
                        if (attribute == null) {
                            attribute = new Attribute(e.getName(), e.getValue());
                            this.attributesMap.putIfAbsent(e.getName(), attribute);
                        }
                        attribute.getIdValueMap().putIfAbsent(key, e.getValue());
                        if (attribute.getValueIdMap().get(e.getValue()) == null) {
                            attribute.getValueIdMap().get(e.getValue());
                            attribute.getValueIdMap().putIfAbsent(e.getValue(), new ArrayList<>());
                        }
                        attribute.getValueIdMap().get(e.getValue()).add(key);
                    });
                }
            } finally {
                keyLocks.remove(lockKey);
            }
        }
    }

    public ResultSet fetchRecord(List<String> keys, String[] columns) {
        if (Objects.isNull(keys)) {
            return new ResultSet();
        }

        String lockKey = READLOCK + keys.stream().toString();
        synchronized (keyLocks.computeIfAbsent(lockKey, (k) -> new Object())) {
            ResultSet resultSet = new ResultSet();

            try {
                keys.stream().forEach(key -> {
                    this.getAttributesMap().get(key);
                    List<Entry> entries = new ArrayList<>();
                    Arrays.stream(columns).forEach(column -> {
                                if (this.attributesMap.containsKey(column)) {
                                    Object o = this.attributesMap.get(column).getIdValueMap().get(key);
                                    entries.add(new Entry(column, o));
//                                    resultSet.addRow(new Row(key, entries));
                                }
                            }
                    );
                    resultSet.addRow(new Row(key, entries));


//                    this.attributesMap.forEach((attributeName, attribute) -> {
//                        Object o = attribute.getIdValueMap().get(key);
//                        entries.add(new Entry(attributeName, o));
//                    });
                });
//
                return resultSet;
            } finally {
                keyLocks.remove(lockKey);
            }
        }
    }

    public void deleteRecord(String key) {
        if (!this.keys.contains(key)) {
            throw new KeyDoesNotExistsException("Key {}  does not exists" + key);
        }

        String lockKey = WRITELOCK + key;
        synchronized (keyLocks.computeIfAbsent(lockKey, (k) -> new Object())) {
            try {
                this.attributesMap.forEach((attributeName, attribute) -> {
                    attribute.getIdValueMap().remove(key);
                });
                this.keys.remove(key);
            } finally {
                keyLocks.remove(lockKey);
            }
        }
    }

    public List<String> searchBySecondaryIndex(Entry entry) {
        if (!this.getAttributesMap().containsKey(entry.getName())) {
            throw new AttributeDoesNotExistsException("Attribute {} does not exists in table {}" + entry.getName() + " " + this.getName());
        }

        String lockKey = READLOCK + entry.getName() + entry.getValue();
        synchronized (keyLocks.computeIfAbsent(lockKey, (k) -> new Object())) {

            try {
                Attribute attribute = this.getAttributesMap().get(entry.getName());
                List<String> keys = attribute.getValueIdMap().get(entry.getValue());
                return keys;
            } catch (RuntimeException e) {
                System.out.println(e.getMessage());
            } finally {
                keyLocks.remove(lockKey);
            }
        }
        return new ArrayList<>();
    }

    public ResultSet searchBySecondaryIndex(Entry entry, String[] columns) {
        if (!this.getAttributesMap().containsKey(entry.getName())) {
            throw new AttributeDoesNotExistsException("Attribute {} does not exists in table {}" + entry.getName() + " " + this.getName());
        }

        String lockKey = READLOCK + entry.getName() + entry.getValue();
        synchronized (keyLocks.computeIfAbsent(lockKey, (k) -> new Object())) {

            try {
                Attribute attribute = this.getAttributesMap().get(entry.getName());
                List<String> keys = attribute.getValueIdMap().get(entry.getValue());
                return this.fetchRecord(keys, columns);
            } catch (RuntimeException e) {
                System.out.println(e.getMessage());
            } finally {
                keyLocks.remove(lockKey);
            }
        }
        return new ResultSet();
    }

    public String getDataTypeIfAttributeExists(String attributeName) {
        if (attributesMap.containsKey(attributeName)) {
            return attributesMap.get(attributeName).getDatatype();
        }
        return null;
    }

    public ConcurrentHashMap<String, Attribute> getAttributesMap() {
        return attributesMap;
    }

    public void addAttribute(Attribute attribute) {
        this.attributesMap.putIfAbsent(attribute.getName(), attribute);
    }

    public String getName() {
        return name;
    }

}
