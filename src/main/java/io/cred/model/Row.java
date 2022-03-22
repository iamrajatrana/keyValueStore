package io.cred.model;

import java.util.List;

public class Row {
    private String key;
    private List<Entry> entries;
    private long createdAtMillis;
//    private Metadata metadata;   We can save row related metadata here, like available column map for given row.

    public Row(String key, List<Entry> entries) {
        this.key = key;
        this.entries = entries;
        this.createdAtMillis = System.currentTimeMillis();
    }

    @Override
    public String toString() {
        return "\nRow : " +
               "key='" + key + '\'' +
               ", entries=" + entries;
    }
}
