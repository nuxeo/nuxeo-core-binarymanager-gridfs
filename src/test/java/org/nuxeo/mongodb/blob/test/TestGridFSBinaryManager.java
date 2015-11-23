package org.nuxeo.mongodb.blob.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.core.api.Blobs;
import org.nuxeo.ecm.core.blob.binary.Binary;
import org.nuxeo.ecm.core.blob.binary.BinaryGarbageCollector;
import org.nuxeo.ecm.core.blob.binary.BinaryManagerStatus;
import org.nuxeo.ecm.core.storage.mongodb.blob.GridFSBinaryManager;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.runtime.test.runner.RuntimeFeature;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.MongoServerSelectionException;

@RunWith(FeaturesRunner.class)
@Features(RuntimeFeature.class)
public class TestGridFSBinaryManager {

    protected static final String CONTENT = "this is a file au caf\u00e9";

    protected static final String CONTENT_MD5 = "d25ea4f4642073b7f218024d397dbaef";

    protected static final String CONTENT2 = "abc";

    protected static final String CONTENT2_MD5 = "900150983cd24fb0d6963f7d28e17f72";

    protected static final String CONTENT3 = "defg";

    protected static final String CONTENT3_MD5 = "025e4da7edac35ede583f5e8d51aa7ec";

    protected GridFSBinaryManager binaryManager;

    protected GridFSBinaryManager getBinaryManager() throws IOException {
        if (binaryManager == null) {
            binaryManager = new GridFSBinaryManager();
            Map<String, String> config = new HashMap<>();

            config.put("server", Framework.getProperty("nuxeo.mongodb.server", "localhost"));
            config.put("dbname", Framework.getProperty("nuxeo.mongodb.dbname", "nuxeo"));
            config.put("bucket", Framework.getProperty("nuxeo.mongodb.gridfs.bucket", "nxblobs"));
            binaryManager.initialize("repo", config);
        }
        return binaryManager;
    }

    protected Set<String> listObjects() throws IOException {

        Set<String> res = new HashSet<>();

        try (DBCursor cursor = getBinaryManager().getGridFS().getFileList()) {
            while (cursor.hasNext()) {
                String digest = (String) cursor.next().get("filename");
                res.add(digest);
            }
        }
        return res;
    }

    protected void removeAllObjects() throws IOException {
        for (String digest : listObjects()) {
            removeObject(digest);
        }
    }

    protected void removeObject(String digest) throws IOException {
        getBinaryManager().getGridFS().remove(new BasicDBObject("filename", digest));
    }

    @Before
    public void setUp() throws Exception {
        binaryManager = getBinaryManager();
        try {
            removeAllObjects();
        } catch (MongoServerSelectionException e) {
            Assume.assumeNoException("Tests ignored in case Mongo server is not reachable.", e);
        }
    }

    @Test
    public void testStoreFile() throws Exception {

        // store binary
        byte[] bytes = CONTENT.getBytes("UTF-8");
        Binary binary = binaryManager.getBinary(Blobs.createBlob(CONTENT));
        assertNotNull(binary);

        System.out.println(binary.getDigestAlgorithm());
        System.out.println(binary.getDigest());

        // check binary is here
        binary = binaryManager.getBinary(CONTENT_MD5);
        assertNotNull(binary);
        assertEquals(bytes.length, binary.getLength());

        assertEquals(CONTENT, toString(binary.getStream()));

        // check that there is only one entry
        assertEquals(1, listObjects().size());

        // store again
        binaryManager.getBinary(Blobs.createBlob(CONTENT));

        // check that there is still only one entry
        assertEquals(1, listObjects().size());
    }

    /**
     * NOTE THAT THIS TEST WILL REMOVE ALL FILES IN THE BUCKET!!!
     */
    @Test
    public void testBinaryManagerGC() throws Exception {
        // store binary
        byte[] bytes = CONTENT.getBytes("UTF-8");
        Binary binary = binaryManager.getBinary(Blobs.createBlob(CONTENT));
        assertNotNull(binary);
        assertEquals(Collections.singleton(CONTENT_MD5), listObjects());

        // get binary
        binary = binaryManager.getBinary(CONTENT_MD5);
        assertNotNull(binary);
        assertEquals(bytes.length, binary.getLength());
        assertEquals(CONTENT, toString(binary.getStream()));

        // another binary we'll GC
        binaryManager.getBinary(Blobs.createBlob(CONTENT2));

        // another binary we'll keep
        binaryManager.getBinary(Blobs.createBlob(CONTENT3));

        assertEquals(new HashSet<>(Arrays.asList(CONTENT_MD5, CONTENT2_MD5, CONTENT3_MD5)), listObjects());

        // GC in non-delete mode
        BinaryGarbageCollector gc = binaryManager.getGarbageCollector();
        assertFalse(gc.isInProgress());
        gc.start();
        assertTrue(gc.isInProgress());
        gc.mark(CONTENT_MD5);
        gc.mark(CONTENT3_MD5);
        assertTrue(gc.isInProgress());
        gc.stop(false);
        assertFalse(gc.isInProgress());
        BinaryManagerStatus status = gc.getStatus();
        assertEquals(2, status.numBinaries);
        assertEquals(bytes.length + 4, status.sizeBinaries);
        assertEquals(1, status.numBinariesGC);
        assertEquals(3, status.sizeBinariesGC);
        assertEquals(new HashSet<>(Arrays.asList(CONTENT_MD5, CONTENT2_MD5, CONTENT3_MD5)), listObjects());

        // real GC
        gc = binaryManager.getGarbageCollector();
        gc.start();
        gc.mark(CONTENT_MD5);
        gc.mark(CONTENT3_MD5);
        gc.stop(true);
        status = gc.getStatus();
        assertEquals(2, status.numBinaries);
        assertEquals(bytes.length + 4, status.sizeBinaries);
        assertEquals(1, status.numBinariesGC);
        assertEquals(3, status.sizeBinariesGC);
        assertEquals(new HashSet<>(Arrays.asList(CONTENT_MD5, CONTENT3_MD5)), listObjects());

        // another GC after not marking content3
        gc = binaryManager.getGarbageCollector();
        gc.start();
        gc.mark(CONTENT_MD5);
        gc.stop(true);
        status = gc.getStatus();
        assertEquals(1, status.numBinaries);
        assertEquals(bytes.length, status.sizeBinaries);
        assertEquals(1, status.numBinariesGC);
        assertEquals(4, status.sizeBinariesGC);
        assertEquals(Collections.singleton(CONTENT_MD5), listObjects());
    }

    protected static String toString(InputStream stream) throws IOException {
        return IOUtils.toString(stream, "UTF-8");
    }

}
