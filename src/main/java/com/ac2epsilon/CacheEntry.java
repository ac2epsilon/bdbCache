package com.ac2epsilon;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by ac2 on 14.09.16.
 * exposes BerkeleyDB cache entry to manage files on the disk
 * we don't use Beans-ish style of getters here
 */
public class CacheEntry {
    static final long MAX_FILE_SIZE = 1024*1024*10;
    public String fileName = "";
    public String mimeType = "";
    public long fileSize = 0;
    public long lastHit = 0;
    public long lastUpdate = 0;
    public String md5 = "";
    public String path = "";
    public String file = "";
    public CacheEntry(String fileName, String mime, byte[] data)
            throws NoSuchAlgorithmException {
        if (data.length>MAX_FILE_SIZE) throw new CacheFileTooBigException();
        if (fileName.isEmpty()) throw new CacheEmptyFileNameException();
        this.fileName=fileName;
        this.fileSize=data.length;
        this.lastUpdate = System.currentTimeMillis();
        this.lastHit = this.lastUpdate;
        BigInteger bint = new BigInteger(MessageDigest.getInstance("MD5").digest(data));
        StringBuffer blats = new StringBuffer();
        for (byte b:bint.toByteArray())
            blats.append(Integer.toHexString(0xFF & b));
        this.md5 = blats.toString();
        this.path = md5.substring(0, 2);
        this.file = md5.substring(2);
    }
}
