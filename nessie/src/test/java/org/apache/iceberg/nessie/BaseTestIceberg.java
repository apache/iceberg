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

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.api.ContentsApi;
import org.projectnessie.api.TreeApi;
import org.projectnessie.client.NessieClient;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.jaxrs.NessieJaxRsExtension;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.types.Types.NestedField.required;

public abstract class BaseTestIceberg {
  @RegisterExtension
  static NessieJaxRsExtension server = new NessieJaxRsExtension();

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseTestIceberg.class);

  @TempDir
  public Path temp;

  protected NessieCatalog catalog;
  protected NessieClient client;
  protected TreeApi tree;
  protected ContentsApi contents;
  protected Configuration hadoopConfig;
  protected final String branch;
  private String uri;

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
    tree.createReference(Branch.of("main", null));
  }

  @BeforeEach
  public void beforeEach() throws IOException {
    String port = System.getProperty("quarkus.http.test-port", "19120");
    uri = server.getURI().toString();
    this.client = NessieClient.builder().withUri(uri).build();
    tree = client.getTreeApi();
    contents = client.getContentsApi();

    resetData();

    try {
      tree.createReference(Branch.of(branch, null));
    } catch (Exception e) {
      // ignore, already created. Cant run this in BeforeAll as quarkus hasn't disabled auth
    }

    hadoopConfig = new Configuration();
    catalog = initCatalog(branch);
  }

  NessieCatalog initCatalog(String ref) {
    NessieCatalog newCatalog = new NessieCatalog();
    newCatalog.setConf(hadoopConfig);
    newCatalog.initialize("nessie", ImmutableMap.of("ref", ref,
        CatalogProperties.URI, uri,
        "auth-type", "NONE",
        CatalogProperties.WAREHOUSE_LOCATION, temp.toUri().toString()
    ));
    return newCatalog;
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
    Schema schema = new Schema(StructType.of(required(1, "id", LongType.get())).fields());
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
    tree.createReference(Branch.of(name, hash));
  }

  @AfterEach
  public void afterEach() throws Exception {
    catalog.close();
    client.close();
    catalog = null;
    client = null;
    hadoopConfig = null;
  }

  static String metadataLocation(NessieCatalog catalog, TableIdentifier tableIdentifier) {
    Table table = catalog.loadTable(tableIdentifier);
    BaseTable baseTable = (BaseTable) table;
    TableOperations ops = baseTable.operations();
    NessieTableOperations icebergOps = (NessieTableOperations) ops;
    return icebergOps.currentMetadataLocation();
  }
}
