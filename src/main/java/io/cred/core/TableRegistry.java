package io.cred.core;

import io.cred.model.Table;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class TableRegistry {
    private static ConcurrentHashMap<String, Table> tables = new ConcurrentHashMap<>();

    public static TableRegistry of() {
        return new TableRegistry();
    }

    public void register(Table table) {
        tables.putIfAbsent(table.getName(), table);
    }

    public static Optional<Table> getTable(String tableName) {
        if(tables.containsKey(tableName)) {
            return Optional.of(tables.get(tableName));
        }
        return Optional.empty();
    }

    public static boolean isPresent(String tableName) {
        return tables.containsKey(tableName);
    }

    public static long tablesCount() {
        return tables.size();
    }

    public static void clear() {
        tables = new ConcurrentHashMap<>();
    }

}
