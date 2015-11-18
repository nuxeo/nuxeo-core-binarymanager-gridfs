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

package org.nuxeo.ecm.core.storage.mongodb;

import java.net.UnknownHostException;

import org.apache.commons.lang3.StringUtils;
import org.nuxeo.ecm.core.api.NuxeoException;
import org.nuxeo.runtime.api.Framework;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;
import com.mongodb.gridfs.GridFS;

/**
 * Helper to initialize the MongoDB client and GridFS client
 *
 * @since 7.10
 */

public class MongoDBClientFactory {

    protected String server;

    protected String dbname;

    private String bucket;

    protected MongoClient client;

    public MongoDBClientFactory() {
    }

    public MongoDBClientFactory(String server, String dbname, String bucket) {
        this();
        this.server = server;
        this.dbname = dbname;
        this.bucket = bucket;
    }

    protected String getServer() {
        if (StringUtils.isBlank(server)) {
            server = Framework.getProperty("nuxeo.mongodb.server");
        }
        return server;
    }

    protected String getDBName() {
        if (StringUtils.isBlank(dbname)) {
            dbname = Framework.getProperty("nuxeo.mongodb.dbname");
        }
        return dbname;
    }

    protected String getBucket() {
        if (StringUtils.isBlank(bucket)) {
            bucket = Framework.getProperty("nuxeo.mongodb.gridfs.bucket");
            // Fallback on a default name
            if (StringUtils.isBlank(bucket)) {
                bucket = "fs";
            }
        }

        return bucket;
    }

    public void dispose() {
        if (client != null) {
            client.close();
        }
    }

    public MongoClient initClient() throws UnknownHostException {
        if (client == null) {
            if (getServer().startsWith("mongodb://")) {
                client = new MongoClient(new MongoClientURI(getServer()));
            } else {
                client = new MongoClient(new ServerAddress(getServer()));
            }
        }
        return client;
    }

    public MongoClient getClient() {
        if (client == null) {
            try {
                initClient();
            } catch (UnknownHostException e) {
                throw new NuxeoException("Unable to init MongoDB client", e);
            }
        }
        return client;
    }

    public DB getDB() {
        return getClient().getDB(getDBName());
    }

    public GridFS instanciateGridFS() {
        return new GridFS(getDB(), getBucket());
    }
}
