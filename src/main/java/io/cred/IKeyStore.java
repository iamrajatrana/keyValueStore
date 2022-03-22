package io.cred;

import io.cred.model.Entry;
import io.cred.model.ResultSet;

import java.util.List;

// For smart Client - we can
//    1. find hash of key
//    2. using consistent hashing, can identify correct node e,g 0-5000: node1, 5001-10001: node2, 10001:0 node3
//    3. This can also be done directly from each node that will redirect request to correct node.
//            Benefit: Can have dumb clients. Isolate logic from client.


// Improvements
// 1. Make get query paginated
// 2. Line 30

public interface IKeyStore {

    boolean createTable(String name);

    // Currently support insert only not upsert. Can modify to support update
    void put(String tableName, String key, Entry value);

    // Currently support insert only not upsert. Can modify to support update
    void putAll(String tableName, String key, List<Entry> value);

    // get required columns only as there can be many column and end user will require only few columns basis usecase
    // if still user need to get all rows, without passing required columns, we need to store some key metadata DS, that help in identifying
    // only the non null values, instead of doing full search
    ResultSet get(String tableName, String Key, String [] columns);

    void delete(String tableName, String key);

    // Seach all relevent keys for secondary index search
    List<String> search(String tableName, Entry entry);

    // Secondary Search: fetch required columns only as there can be many column and end user will require only few columns basis usecase
    ResultSet search(String tableName, Entry entry, String [] columns);

}
