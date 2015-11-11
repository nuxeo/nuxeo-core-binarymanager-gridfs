/*
 * (C) Copyright 2015 Nuxeo SA (http://nuxeo.com/) and contributors.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * Contributors:
 *     tiry
 */
package org.nuxeo.ecm.core.storage.mongodb.blob;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.nuxeo.common.utils.SizeUtils;
import org.nuxeo.ecm.core.api.Blob;
import org.nuxeo.ecm.core.api.NuxeoException;
import org.nuxeo.ecm.core.blob.BlobManager;
import org.nuxeo.ecm.core.blob.BlobProvider;
import org.nuxeo.ecm.core.blob.binary.AbstractBinaryManager;
import org.nuxeo.ecm.core.blob.binary.Binary;
import org.nuxeo.ecm.core.blob.binary.BinaryBlobProvider;
import org.nuxeo.ecm.core.blob.binary.BinaryGarbageCollector;
import org.nuxeo.ecm.core.blob.binary.BinaryManager;
import org.nuxeo.ecm.core.blob.binary.BinaryManagerStatus;
import org.nuxeo.ecm.core.model.Document;
import org.nuxeo.runtime.api.Framework;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

/**
 * Implements the {@link BinaryManager} & {@link BlobProvider} interface using MongoDB GridFS. This implementation does
 * not use local caching. This implementation may not always be ideal regarding Streaming because of the usage of
 * {@link Binary} interface that exposes a {@link File}.
 *
 * @since 7.10
 */
public class GridFSBinaryManager extends AbstractBinaryManager implements BlobProvider {

    protected MongoClient client;

    protected GridFS gridFS;

    // protected File cachedir;

    // protected FileCache fileCache;

    protected String bucketName;

    protected long cacheSize = 1024 * 1024 * 25;

    @Override
    public void initialize(String blobProviderId, Map<String, String> properties) throws IOException {
        super.initialize(blobProviderId, properties);
        String server = properties.get("server");
        String dbname = properties.get("dbname");
        bucketName = properties.get("bucket");
        if (bucketName == null) {
            bucketName = "fs";
        }
        String cacheSizeStr = properties.get("cacheSize");
        if (cacheSizeStr != null) {
            cacheSize = SizeUtils.parseSizeInBytes(cacheSizeStr);
        }

        if (server.startsWith("mongodb://")) {
            client = new MongoClient(new MongoClientURI(server));
        } else {
            client = new MongoClient(new ServerAddress(server));
        }
        gridFS = new GridFS(client.getDB(dbname), bucketName);

        garbageCollector = new GridFSBinaryGarbageCollector();
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
    }

    protected class GridFSBinary extends Binary {

        private static final long serialVersionUID = 1L;

        protected final long length;

        protected GridFSBinary(String digest, long length, String blobProviderId) {
            super(digest, blobProviderId);
            this.length = length;
        }

        @Override
        public long getLength() {
            return length;
        }

        @Override
        public InputStream getStream() {
            GridFSDBFile dbFile = gridFS.findOne(digest);
            return dbFile.getInputStream();
        }

        @Override
        public File getFile() {
            // this API is a pain to implement on GridFS !
            if (file == null || !file.exists()) {
                try {
                    file = File.createTempFile("nuxeo-gridfs-", ".bin");
                    GridFSDBFile dbFile = gridFS.findOne(digest);
                    IOUtils.copy(dbFile.getInputStream(), new FileOutputStream(file));
                    Framework.trackFile(file, this);
                } catch (IOException e) {
                    throw new NuxeoException("Unable to extract file from GridFS Stream", e);
                }
            }
            return file;
        }
    }

    @Override
    protected Binary getBinary(InputStream stream) throws IOException {

        GridFSInputFile gFile = gridFS.createFile(stream, true);
        gFile.save();
        String digest = gFile.getMD5();
        long length = gFile.getLength();

        // check if the file already existed ?
        GridFSDBFile existingFile = gridFS.findOne(digest);
        if (existingFile == null) {
            gFile.setFilename(digest);
            gFile.save();
        } else {
            gridFS.remove(gFile);
        }

        return new GridFSBinary(digest, length, blobProviderId);

    }

    @Override
    public Binary getBinary(String digest) {
        GridFSDBFile dbFile = gridFS.findOne(digest);
        if (dbFile != null) {
            return new GridFSBinary(digest, dbFile.getLength(), blobProviderId);
        }
        return null;
    }

    @Override
    public Blob readBlob(BlobManager.BlobInfo blobInfo) throws IOException {
        // just delegate to avoid copy/pasting code
        return new BinaryBlobProvider(this).readBlob(blobInfo);
    }

    @Override
    public String writeBlob(Blob blob, Document doc) throws IOException {
        // just delegate to avoid copy/pasting code
        return new BinaryBlobProvider(this).writeBlob(blob, doc);
    }

    @Override
    public boolean supportsWrite() {
        return true;
    }

    public class GridFSBinaryGarbageCollector implements BinaryGarbageCollector {

        protected BinaryManagerStatus status;

        protected volatile long startTime;

        protected static final String MARK_KEY_PREFIX = "gc-mark-key-";

        protected String msKey;

        @Override
        public String getId() {
            return "gridfs:" + bucketName;
        }

        @Override
        public BinaryManagerStatus getStatus() {
            return status;
        }

        @Override
        public boolean isInProgress() {
            return startTime != 0;
        }

        @Override
        public void mark(String digest) {
            GridFSDBFile dbFile = gridFS.findOne(digest);
            if (dbFile != null) {
                DBObject meta = new BasicDBObject();
                meta.put(msKey, true);
                dbFile.setMetaData(meta);
                dbFile.save();
                status.numBinaries += 1;
                status.sizeBinaries += dbFile.getLength();
            }
        }

        @Override
        public void start() {

            if (startTime != 0) {
                throw new RuntimeException("Already started");
            }
            startTime = System.currentTimeMillis();
            status = new BinaryManagerStatus();

            msKey = MARK_KEY_PREFIX + System.currentTimeMillis();
        }

        @Override
        public void stop(boolean delete) {
            BasicDBObject query = new BasicDBObject("metadata." + msKey, new BasicDBObject("$exists", false));
            List<GridFSDBFile> files = gridFS.find(query);
            for (GridFSDBFile file : files) {
                status.numBinariesGC += 1;
                status.sizeBinariesGC += file.getLength();
                if (delete) {
                    gridFS.remove(file);
                }
            }
            startTime = 0;
        }
    }

    public GridFS getGridFS() {
        return gridFS;
    }
}
