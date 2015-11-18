# About

This Nuxeo plugin provides an implementation of the `BinaryManager` that replies on MongoDB GridFS.

The goal is to store Nuxeo Blobs directly inside GridFS.

This is particularly interesting when using MongoDB as the main repository backend (using DBS) since this allows to have Structures + Meta-data and the Binaries inside the same MongoDB replicaSet.

# Building

    mvn clean install

##Deploying

**Deploy Bundle**

Deploy the jar in `$NUXEO_HOME/nxserver/bundles/`.

**Tell Nuxeo to use the GridFS BinaryManager**

Create a file named `gridfs-binarymanager-config.xml` in `$NUXEO_HOME/nxserver/config/`

    <component name="default-repository-gridfs-extension">
      <require>default-repository-config</require>
      <extension target="org.nuxeo.ecm.core.blob.BlobManager" point="configuration">
        <blobprovider name="default">
          <class>org.nuxeo.ecm.core.storage.mongodb.blob.GridFSBinaryManager</class>
          <property name="server">localhost</property>
          <property name="dbname">nuxeo</property>
          <property name="bucket">nxblobs</property>
       </blobprovider>
      </extension>
    </component>

Where :

 - `server`: is the DNS name of your MongoDB server, default is the MongoDB server used for Document Store if any (i.e. `nuxeo.mongodb.server`)
 - `dbname`: is the name of the MongoDB database to use , default is the one used for Document Store if any (i.e. `nuxeo.mongodb.dbname`)
 - `bucket`: is the GridFS bucket name (i.e. `nuxeo.mongodb.gridfs.bucket`, and fallback is `fs`)

# Licensing

[GNU Lesser General Public License (LGPL) v2.1](http://www.gnu.org/licenses/lgpl-2.1.html)

# About Nuxeo

Nuxeo dramatically improves how content-based applications are built, managed and deployed, making customers more agile, innovative and successful. Nuxeo provides a next generation, enterprise ready platform for building traditional and cutting-edge content oriented applications. Combining a powerful application development environment with
SaaS-based tools and a modular architecture, the Nuxeo Platform and Products provide clear business value to some of the most recognizable brands including Verizon, Electronic Arts, Netflix, Sharp, FICO, the U.S. Navy, and Boeing. Nuxeo is headquartered in New York and Paris.
 More information is available at [www.nuxeo.com](http://www.nuxeo.com).




