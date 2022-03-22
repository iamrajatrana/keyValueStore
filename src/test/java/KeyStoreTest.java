import helper.Task;
import io.cred.IKeyStore;
import io.cred.core.ColumnKeyStore;
import io.cred.core.TableRegistry;
import io.cred.exception.AttributeDoesNotExistsException;
import io.cred.exception.InvalidDatatypeException;
import io.cred.exception.TableDoesNotExistsException;
import io.cred.model.Entry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class KeyStoreTest {

    @BeforeEach
    public void cleanup() {
        TableRegistry.clear();
    }

    @Test
    public void itShouldCreateANewTable() {
        boolean isCreated = new ColumnKeyStore().createTable("cities");
        assertTrue(isCreated);
        assertTrue(TableRegistry.isPresent("cities"));
    }

    @Test
    public void itShouldFailForCreatingExistingTable() {
        boolean isCreated = new ColumnKeyStore().createTable("cities");
        boolean isDuplicateCreated = new ColumnKeyStore().createTable("cities");
        assertTrue(isCreated);
        assertFalse(isDuplicateCreated);
        assertTrue(TableRegistry.isPresent("cities"));
        assertTrue(TableRegistry.tablesCount() == 1);
    }

    @Test
    public void itShouldFailIfTableDoesNotEXists() {
        assertFalse(TableRegistry.isPresent("cities"));
    }

    @Test
    public void itShouldThrowIfTableDoesNotExistsWhileInsertAllCommand() {
        IKeyStore keyStore = new ColumnKeyStore();
        List<Entry> entries1 = Arrays.asList(new Entry("latitude", -6.0), new Entry("longitude", 106.0));
        assertThrows(TableDoesNotExistsException.class, () -> keyStore.putAll("cities", "jakarta", entries1));
    }

    @Test
    public void itShouldThrowIfTableDoesNotExistsWhileInsertOneCommand() {
        IKeyStore keyStore = new ColumnKeyStore();
        assertThrows(TableDoesNotExistsException.class, () -> keyStore.put("cities", "jakarta", new Entry("latitude", -6.0)));
    }

    @Test
    public void itShouldCreateSingleRowWithPut() {
        IKeyStore keyStore = new ColumnKeyStore();
        keyStore.createTable("cities");
        keyStore.put("cities", "jakarta", new Entry("latitude", -6.0));
        System.out.println(keyStore.get("cities", "jakarta", new String[]{"latitude", "longitude"}));
        assertTrue(TableRegistry.getTable("cities").get().size() == 1);

    }

    @Test
    public void itShouldCreateSingleRowWithPutAll() {
        IKeyStore keyStore = new ColumnKeyStore();
        keyStore.createTable("cities");
        List<Entry> entries = Arrays.asList(new Entry("latitude", -6.0), new Entry("longitude", 106.0));
        keyStore.putAll("cities", "jakarta", entries);
        System.out.println(keyStore.get("cities", "jakarta", new String[]{"latitude", "longitude"}));
        assertTrue(TableRegistry.getTable("cities").get().size() == 1);

    }

    @Test
    public void itShouldCreateMultipleRowsWithPutAll() {
        IKeyStore keyStore = new ColumnKeyStore();
        keyStore.createTable("cities");
        List<Entry> entries1 = Arrays.asList(new Entry("latitude", -6.0), new Entry("longitude", 106.0));
        List<Entry> entries2 = Arrays.asList(new Entry("latitude", -6.0), new Entry("sea_evel", 5000));

        keyStore.putAll("cities", "jakarta", entries1);
        keyStore.putAll("cities", "bangalore", entries2);

        System.out.println(keyStore.get("cities", "jakarta", new String[]{"latitude", "longitude"}));
        assertTrue(TableRegistry.getTable("cities").get().size() == 2);

    }

    @Test
    public void itShouldThrowIfDifferentDataTypeIsUsedForGivenAttribute() {
        IKeyStore keyStore = new ColumnKeyStore();
        keyStore.createTable("cities");
        List<Entry> entries1 = Arrays.asList(new Entry("latitude", -6.0), new Entry("longitude", 106.0));
        List<Entry> entries2 = Arrays.asList(new Entry("latitude", -6.3), new Entry("longitude", "10.2"));
        List<Entry> entries3 = Arrays.asList(new Entry("latitude", -6.3), new Entry("longitude", true));

        keyStore.putAll("cities", "jakarta", entries1);

        // throws Invalid Datatype {} for longitude)
        assertThrows(InvalidDatatypeException.class, () -> keyStore.putAll("cities", "maldives", entries2));
        assertThrows(InvalidDatatypeException.class, () -> keyStore.putAll("cities", "maldives", entries3));

        System.out.println(keyStore.get("cities", "jakarta", new String[]{"latitude"}));
        assertTrue(TableRegistry.tablesCount() == 1);
    }



    @Test
    public void itShouldFindSingleKeyFromSecordaryIndexForGivenValue() {
        IKeyStore keyStore = new ColumnKeyStore();
        keyStore.createTable("cities");
        List<Entry> entries1 = Arrays.asList(new Entry("latitude", -6.0), new Entry("longitude", 106.0));
        List<Entry> entries2 = Arrays.asList(new Entry("latitude", -6.3), new Entry("longitude", 10.2));
        List<Entry> entries3 = Arrays.asList(new Entry("latitude", -12.3), new Entry("pollution_level", "moderate"));
        List<Entry> entries4 = Arrays.asList(new Entry("pollution_level", "moderate"), new Entry("radiations", 1));

        keyStore.putAll("cities", "jakarta", entries1);
        keyStore.putAll("cities", "maldives", entries2);
        keyStore.putAll("cities", "bangalore", entries3);
        keyStore.putAll("cities", "delhi", entries4);


        System.out.println("Search 1 :: " + keyStore.search("cities", new Entry("latitude", -6.0), new String[]{"latitude", "longitude"}));
        System.out.println("Search 2 :: " + keyStore.search("cities", new Entry("pollution_level", "high"), new String[]{"latitude", "longitude"}));
        System.out.println("Search 3 :: " + keyStore.search("cities", new Entry("pollution_level", "moderate"), new String[]{"pollution_level", "latitude", "longitude"}));
        System.out.println("Search 4 :: " + keyStore.search("cities", new Entry("radiations", 1), new String[]{"pollution_level", "longitude"}));

        assertTrue(TableRegistry.tablesCount() == 1);
        assertTrue(TableRegistry.getTable("cities").get().size() == 4);

    }

    @Test
    public void itShouldFindKeysFromSecordaryIndexForGivenValue() {
        IKeyStore keyStore = new ColumnKeyStore();
        keyStore.createTable("cities");
        List<Entry> entries1 = Arrays.asList(new Entry("latitude", -6.0), new Entry("longitude", 106.0));
        List<Entry> entries2 = Arrays.asList(new Entry("latitude", -6.0), new Entry("longitude", 10.2));
        List<Entry> entries3 = Arrays.asList(new Entry("latitude", -12.3), new Entry("pollution_level", "moderate"));
        List<Entry> entries4 = Arrays.asList(new Entry("pollution_level", "moderate"), new Entry("radiations", 1));

        keyStore.putAll("cities", "jakarta", entries1);
        keyStore.putAll("cities", "maldives", entries2);
        keyStore.putAll("cities", "bangalore", entries3);
        keyStore.putAll("cities", "delhi", entries4);


        System.out.println("Search 1 :: " + keyStore.search("cities", new Entry("latitude", -6.0)));
        System.out.println("Search 2 :: " + keyStore.search("cities", new Entry("radiations", 1)));
        System.out.println("Search 3 :: " + keyStore.search("cities", new Entry("radiations", 2)));

        assertThrows(AttributeDoesNotExistsException.class, () -> keyStore.search("cities", new Entry("radiation1", 2)));

        assertTrue(TableRegistry.tablesCount() == 1);
        assertTrue(TableRegistry.getTable("cities").get().size() == 4);

    }

    @Test
    public void itShouldThrowsErrorIfAttributeDoesNotExistsInSecordaryIndex() {
        IKeyStore keyStore = new ColumnKeyStore();
        keyStore.createTable("cities");
        List<Entry> entries1 = Arrays.asList(new Entry("latitude", -6.0), new Entry("longitude", 106.0));
        keyStore.putAll("cities", "jakarta", entries1);
        assertThrows(AttributeDoesNotExistsException.class, () -> keyStore.search("cities", new Entry("pollution_level", -6.0)));
    }


    @Test
    public void itShouldCreateSingleKeyOnCreationFromMultipleThreads() throws ExecutionException, InterruptedException {
        IKeyStore keyStore = new ColumnKeyStore();
        keyStore.createTable("cities");
        List<Entry> entries = Arrays.asList(new Entry("latitude", -6.0), new Entry("longitude", 106.0));

        CountDownLatch latch = new CountDownLatch(3);
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.submit(new Task(keyStore, latch, "jakarta", entries));
        executorService.submit(new Task(keyStore, latch, "jakarta", entries));
        executorService.submit(new Task(keyStore, latch, "jakarta", entries));

        latch.await();

        System.out.println(TableRegistry.getTable("cities").get().size());
        assertTrue(TableRegistry.getTable("cities").get().size() == 1);

    }

    @Test
    public void itShouldCreate1000KeysUsingMultiThreading() throws ExecutionException, InterruptedException {
        IKeyStore keyStore = new ColumnKeyStore();
        keyStore.createTable("cities");
        List<Entry> entries = Arrays.asList(new Entry("latitude", -6.0), new Entry("longitude", 106.0));

        CountDownLatch latch = new CountDownLatch(1000);
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        IntStream.range(1, 1001).forEach(i -> {
            executorService.submit(new Task(keyStore, latch, "jakarta_" + i, entries));
        });

        latch.await();
        System.out.println(TableRegistry.getTable("cities").get().size());
        assertTrue(TableRegistry.getTable("cities").get().size() == 1000);

    }

    @Test
    public void itShouldCreate1KeyUsingMultiThreading() throws ExecutionException, InterruptedException {
        IKeyStore keyStore = new ColumnKeyStore();
        keyStore.createTable("cities");
        List<Entry> entries = Arrays.asList(new Entry("latitude", -6.0), new Entry("longitude", 106.0));

        CountDownLatch latch = new CountDownLatch(1000);
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        IntStream.range(1, 1001).forEach(i -> {
            executorService.submit(new Task(keyStore, latch, "jakarta", entries));
        });

        latch.await();
        System.out.println(TableRegistry.getTable("cities").get().size());
        assertTrue(TableRegistry.getTable("cities").get().size() == 1);

    }

}
