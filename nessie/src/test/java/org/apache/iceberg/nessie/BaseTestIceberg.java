/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.nessie;

import com.dremio.nessie.api.ContentsApi;
import com.dremio.nessie.api.TreeApi;
import com.dremio.nessie.client.NessieClient;
import com.dremio.nessie.client.NessieClient.AuthType;
import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.error.NessieNotFoundException;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.Reference;
import java.io.File;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.StructType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.types.Types.NestedField.required;

public abstract class BaseTestIceberg {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseTestIceberg.class);

  protected static File tempDir;
  protected NessieCatalog catalog;
  protected NessieClient client;
  protected TreeApi tree;
  protected ContentsApi contents;
  protected Configuration hadoopConfig;
  protected final String branch;

  @BeforeClass
  public static void create() throws Exception {
    tempDir = java.nio.file.Files.createTempDirectory(
        "test",
        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxrwxrwx")))
        .toFile();
  }

  public BaseTestIceberg(String branch) {
    this.branch = branch;
  }

  private void resetData() throws NessieConflictException, NessieNotFoundException {
    for (Reference r : tree.getAllReferences()) {
      if (r instanceof Branch) {
        tree.deleteBranch(r.getName(), r.getHash());
      } else {
        tree.deleteTag(r.getName(), r.getHash());
      }
    }
    tree.createEmptyBranch("main");
  }

  @Before
  public void beforeEach() throws NessieConflictException, NessieNotFoundException {
    String path = "http://localhost:19121/api/v1";
    String username = "test";
    String password = "test123";
    this.client = new NessieClient(AuthType.NONE, path, username, password);
    tree = client.getTreeApi();
    contents = client.getContentsApi();

    resetData();

    try {
      tree.createEmptyBranch(branch);
    } catch (Exception e) {
      // ignore, already created. Cant run this in BeforeAll as quarkus hasn't disabled auth
    }

    hadoopConfig = new Configuration();
    hadoopConfig.set(NessieClient.CONF_NESSIE_URL, path);
    hadoopConfig.set(NessieClient.CONF_NESSIE_USERNAME, username);
    hadoopConfig.set(NessieClient.CONF_NESSIE_PASSWORD, password);
    hadoopConfig.set(NessieClient.CONF_NESSIE_REF, branch);
    hadoopConfig.set(NessieClient.CONF_NESSIE_AUTH_TYPE, "NONE");
    hadoopConfig.set("fs.defaultFS", tempDir.toURI().toString());
    hadoopConfig.set("fs.file.impl",
                     org.apache.hadoop.fs.LocalFileSystem.class.getName()
    );
    catalog = new NessieCatalog(hadoopConfig);
  }

  protected Table createTable(TableIdentifier tableIdentifier, int count) {
    try {
      return catalog.createTable(tableIdentifier, schema(count));
    } catch (Throwable t) {
      LOGGER.error("unable to do create " + tableIdentifier.toString(), t);
      throw t;
    }
  }

  protected void createTable(TableIdentifier tableIdentifier) {
    Schema schema = new Schema(StructType.of(required(1, "id", LongType.get()))
                                         .fields());
    catalog.createTable(tableIdentifier, schema).location();
  }

  protected static Schema schema(int count) {
    List<Types.NestedField> fields = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      fields.add(required(i, "id" + i, Types.LongType.get()));
    }
    return new Schema(Types.StructType.of(fields).fields());
  }

  void createBranch(String name, String hash) throws NessieNotFoundException, NessieConflictException {
    if (hash == null) {
      tree.createEmptyBranch(name);
    } else {
      tree.createNewBranch(name, hash);
    }
  }

  @After
  public void afterEach() throws Exception {
    catalog.close();
    client.close();
    catalog = null;
    client = null;
    hadoopConfig = null;
  }

  @AfterClass
  public static void destroy() throws Exception {
    tempDir.delete();
  }

  static String getContent(NessieCatalog catalog, TableIdentifier tableIdentifier) {
    Table table = catalog.loadTable(tableIdentifier);
    BaseTable baseTable = (BaseTable) table;
    TableOperations ops = baseTable.operations();
    NessieTableOperations icebergOps = (NessieTableOperations) ops;
    return icebergOps.currentMetadataLocation();
  }

}
