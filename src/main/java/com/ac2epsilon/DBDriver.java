package com.ac2epsilon;


import com.sleepycat.je.*;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;

import java.io.*;
import java.security.NoSuchAlgorithmException;

/**
 * Created by ac2 on 14.09.16.
 */
public class DBDriver {

    //NOTE! CLEAN_CACHE_INTERVAL MUST BE <= CACHE_AGE_TIMEOUT
    //OTHERWISE ADD ADDITIONAL CHECK OF ENTITY AGE TO getCacheEntity
    public static long CLEAN_CACHE_INTERVAL = 1000 * 60 * 60 * 6;
    public static int CACHE_AGE_TIMEOUT = 1000 * 60 * 60 * 7;
    private static Environment dbEnv;
    private static EntityStore store;
    private static PrimaryIndex<String, CacheEntry> pkFileName;

    public void openDB(String db) throws DatabaseException {
        if (pkFileName != null)
            return;
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        //if not set, many locks are not aquiried
//        envConfig.setTxnTimeout(0, SECONDS);
        dbEnv = new Environment(new File(db), envConfig);
        StoreConfig storConf = new StoreConfig();
        storConf.setAllowCreate(true);
        storConf.setTransactional(true);
        store = new EntityStore(dbEnv, "Store", storConf);
        pkFileName = store
                .getPrimaryIndex(String.class, CacheEntry.class);
    }

    public void closeDB() {
        try {
            store.close();
            dbEnv.cleanLog();
            dbEnv.close();
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    private File getCacheEntryFile(CacheEntry ce) {
        File result = null;
        File targetDir = new File(System.getProperty("catalina.base"),
                "/temp/cache/" + ce.path);
        if (!targetDir.exists())
            targetDir.mkdirs();
        result = new File(targetDir.getPath(), ce.file);
        return result;
    }

    public void putCacheEntry(String fileNmae, String mime, byte[] data)
            throws NoSuchAlgorithmException, DatabaseException {
        int count = 0;
        boolean done = false;
        CacheEntry ce =
                new CacheEntry(fileNmae, mime, data);
        do {
            Transaction tx = dbEnv.beginTransaction(null, null);
//            tx.setTxnTimeout(0, SECONDS);
             pkFileName.putNoReturn(tx, ce);
             tx.commit();
             done = true;
            count++;
        } while (!done && (count < 7));
        if(done){
            File targetFile = getCacheEntryFile(ce);
            if (!targetFile.exists() || targetFile.length() == 0) {
                try {
                    OutputStream os = new FileOutputStream(targetFile);
                    os.write(data);
                    os.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
            }
        } else System.out.println("DROP put() BY LockException "+pkFileName);
    }

    public void putCacheEntry(String fileName, byte[] data)
            throws NoSuchAlgorithmException, DatabaseException {
        putCacheEntry(fileName, "", data);
    }

    public CacheEntry getCacheEntry(String remoteLocation)
            throws DatabaseException {
        boolean done = false;
        int count = 0;
        CacheEntry ce = null;
        do {
            Transaction tx = dbEnv.beginTransaction(null, null);
//            tx.setTxnTimeout(0, TimeUnit.SECONDS);
              ce = pkFileName.get(tx, remoteLocation, LockMode.DEFAULT);
//dont update last access timestamp, we dont use its value anywhere
//		      ce.setLastHit(System.currentTimeMillis());
//			  pkRemoteLocation.put(tx, ce); // проставляем попадание
              tx.commit();
              done = true;
            count++;
        } while (!done && (count < 7));
        if(ce != null){
            //@todo cache current ms
            if(System.currentTimeMillis() - ce.lastHit > CACHE_AGE_TIMEOUT){
                deleteCacheEntry(ce);
                ce = null;
            }
        }
        if (!done)
            System.out.println("DROP get() BY LockException "+remoteLocation);
        return ce;
    }
    public void deleteCacheEntry(CacheEntry ce) {
        int count = 0;
        boolean done = false;
        do{
            try {
                Transaction tx = dbEnv.beginTransaction(null, null);
//            tx.setTxnTimeout(0, TimeUnit.SECONDS);
                pkFileName.delete(tx, ce.fileName);
                tx.commit();
                done = true;
            } catch (DatabaseException e) {
                e.printStackTrace();
            }
            count++;
        } while (!done && (count < 7));
        if(done)
            getCacheEntryFile(ce).delete();
        if (!done)
            System.out.println("DROP delete() BY LockException "+ce.fileName);
    }

    // @todo avoid double getCacheEntryFile invoke during cache request
    public long getDataLength(CacheEntry ce) {
        long result = -1;
        File cacheFile = getCacheEntryFile(ce);
        if (!cacheFile.exists() || !cacheFile.isFile())
            return result;
        // наличие файла проверено, 0L обозначает 0 байт а не отсутствие файла
        result = cacheFile.length();
        return result;
    }
/* great method to response directly to remote host, but needs to  
/*    public void copyDataToStream(CacheEntry ce, HttpServletRequest request,
                                 HttpServletResponse response) throws IOException{
        File file = getCacheEntryFile(ce);
        if(!file.exists()){
            throw new FileNotFoundException("Cache file for "+ce.getRemoteLocation()+" not found!");
        }
        if (Boolean.TRUE==request.getAttribute("org.apache.tomcat.sendfile.support")) {
            Utilities.tomcatNativeFileCopy(request, file);
        } else {
            Utilities.transferViaChannel(file, response.getOutputStream());
        }
    }
*/
    //NB! this method doesnt guarantee, that cache file is present!
    public boolean isFileCached(String fileName) {
        boolean result = false;
        boolean done = false;
        int count = 0;
        do {
            try {
              Transaction tx = dbEnv.beginTransaction(null, null);
//            tx.setTxnTimeout(0, TimeUnit.SECONDS);
              result = pkFileName.contains(tx, fileName, LockMode.DEFAULT);
              tx.commit();
              done = true;
            } catch (DatabaseException dbe) {
                dbe.printStackTrace();
            }
            count++;
        } while (!done && (count < 7));
        if (!done)
            System.out.println("DROP contains() BY LockException "+fileName);
        return result;
    }

    public void removeUseless() {
        System.out.println("Start cache cleaning......................");
        String result = "";
        int cnt = 0;
        int cntOver = 0;
        int count = 0;
        boolean done = false;
        long t1 = System.currentTimeMillis();
        long keys = 0;
        do{
            try {
                Transaction tx = dbEnv.beginTransaction(null, null);
//            tx.setTxnTimeout(1000, TimeUnit.SECONDS);
                EntityCursor<CacheEntry> cursor = pkFileName.entities(tx, null);
                long current = System.currentTimeMillis();
                CacheEntry ce;
                while ((ce = cursor.next()) != null) {
                    cntOver ++;
                    if ((current - ce.lastUpdate) > CACHE_AGE_TIMEOUT) {
                        getCacheEntryFile(ce).delete();
                        cursor.delete();
                        cnt++;
                    }
                }
                keys = pkFileName.count();
                cursor.close();
                tx.commit();
                done = true;
            } catch (DatabaseException ex) {
                System.out.println("cache cleaning interrupted");
            }
            count++;
        } while (!done && (count < 7));
        if (!done)
            System.out.println("DROP removeUseless BY LockException");
        result += "CleanUP: "
                + cnt
                + " records removed, iteretaed over "
                +cntOver+"  final count: "
                +keys;
        result += "\nTime of work: " + (System.currentTimeMillis() - t1);
        System.out.println(result);
    }


    public static String getStats() throws DatabaseException {
        String result = "";
        result += "Cached Records=" + pkFileName.count();
        return result;
    }
}
