package io.cred.model;

public class Entry {
    String name;
    Object value;

    public Entry(String name, Object value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Entry{" + name + '\'' + value +
               '}';
    }
}
