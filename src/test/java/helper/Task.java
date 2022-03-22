package helper;

import io.cred.IKeyStore;
import io.cred.exception.KeyAlreadyExistsException;
import io.cred.model.Entry;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

public class Task implements Callable {

             IKeyStore keyStore;
             CountDownLatch latch;
             String key;
            List<Entry> entries;
             public Task(IKeyStore keyStore, CountDownLatch latch, String key,List<Entry> entries) {
                  this.keyStore = keyStore;
                  this.latch = latch;
                  this.key = key;
                  this.entries = entries;
             }

             @Override
             public Object call() throws KeyAlreadyExistsException {
                 try {
                     keyStore.putAll("cities", key, entries);
//                     System.out.println("Created key {} " + key);
                     return null;
                 }finally {
                    latch.countDown();
                 }
             }
         }