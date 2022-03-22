package io.cred.core;

import io.cred.IKeyStore;
import io.cred.exception.TableDoesNotExistsException;
import io.cred.model.Entry;
import io.cred.model.ResultSet;
import io.cred.model.Table;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// Benefits
// This key store is storing data based on columnar and not row wise.
// This helps in requirement with lot of columns and for range based data because of better data locality
// it will fetch only the required columns opposite to row wise storage and help to make lean queries
// it can further be improved by merging/compressing common values and have refernce of the keys

//Drawbacks
// Will take more computing resource to write data, as we need write data in each column store.

public class ColumnKeyStore implements IKeyStore {

    private final TableRegistry tableRegistry = TableRegistry.of();
    private final ReentrantReadWriteLock tableLock = new ReentrantReadWriteLock();

    public ColumnKeyStore() {
    }

    @Override
    public boolean createTable(String name) {
        this.tableLock.writeLock().lock();
        try {
            if (!tableRegistry.isPresent(name)) {
                Table table = new Table(name);
                tableRegistry.register(table);
                return true;
            }
        } finally {
            this.tableLock.writeLock().unlock();
        }
        return false;
    }

    @Override
    public void put(String tableName, String key, Entry value) {
        Optional<Table> table = this.tableRegistry.getTable(tableName);
        if (table.isPresent()) {
            table.get().insertRecord(key, Arrays.asList(value));
        } else {
            throw new TableDoesNotExistsException("Table {} does not exists " + tableName);
        }
    }

    @Override
    public void putAll(String tableName, String key, List<Entry> values) {
        Optional<Table> table = this.tableRegistry.getTable(tableName);
        if (table.isPresent()) {
            table.get().insertRecord(key, values);
        } else {
            throw new TableDoesNotExistsException("Table {} does not exists " + tableName);
        }
    }

    @Override
    public ResultSet get(String tableName, String key, String[] columns) {
        Optional<Table> table = this.tableRegistry.getTable(tableName);
        if (table.isPresent()) {
            return table.get().fetchRecord(Arrays.asList(key), columns);
        } else {
            throw new TableDoesNotExistsException("Table {} does not exists " + tableName);
        }
    }

    @Override
    public void delete(String tableName, String key) {
        Optional<Table> table = this.tableRegistry.getTable(tableName);
        if (table.isPresent()) {
            table.get().deleteRecord(key);
        } else {
            throw new TableDoesNotExistsException("Table {} does not exists " + tableName);
        }
    }

    @Override
    public ResultSet search(String tableName, Entry entry, String[] columns) {
        Optional<Table> table = this.tableRegistry.getTable(tableName);
        if (table.isPresent()) {
            return table.get().searchBySecondaryIndex(entry, columns);
        } else {
            throw new TableDoesNotExistsException("Table {} does not exists " + tableName);
        }
    }

    @Override
    public List<String> search(String tableName, Entry entry) {
        Optional<Table> table = this.tableRegistry.getTable(tableName);
        if (table.isPresent()) {
            return table.get().searchBySecondaryIndex(entry);
        } else {
            throw new TableDoesNotExistsException("Table {} does not exists " + tableName);
        }
    }

}
