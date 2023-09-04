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

import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.ext.NessieApiVersion;
import org.projectnessie.client.ext.NessieApiVersions;
import org.projectnessie.client.ext.NessieClientFactory;
import org.projectnessie.client.ext.NessieClientUri;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.jaxrs.ext.NessieJaxRsExtension;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.inmemory.InmemoryBackendTestFactory;
import org.projectnessie.versioned.storage.testextension.NessieBackend;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.PersistExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(PersistExtension.class)
@NessieBackend(InmemoryBackendTestFactory.class)
@NessieApiVersions // test all versions
public abstract class BaseTestIceberg {

  @NessiePersist static Persist persist;

  @RegisterExtension
  static NessieJaxRsExtension server = NessieJaxRsExtension.jaxRsExtension(() -> persist);

  private static final Logger LOG = LoggerFactory.getLogger(BaseTestIceberg.class);

  @TempDir public Path temp;

  protected NessieCatalog catalog;
  protected NessieApiV1 api;
  protected String apiVersion;
  protected Configuration hadoopConfig;
  protected final String branch;
  private String initialHashOfDefaultBranch;
  protected String uri;

  public BaseTestIceberg(String branch) {
    this.branch = branch;
  }

  private void resetData() throws NessieConflictException, NessieNotFoundException {
    Branch defaultBranch = api.getDefaultBranch();
    for (Reference r : api.getAllReferences().get().getReferences()) {
      if (r instanceof Branch && !r.getName().equals(defaultBranch.getName())) {
        api.deleteBranch().branch((Branch) r).delete();
      }
      if (r instanceof Tag) {
        api.deleteTag().tag((Tag) r).delete();
      }
    }

    // Reset default branch "main", if necessary
    if (!defaultBranch.getHash().equals(initialHashOfDefaultBranch)) {
      api.assignBranch()
          .assignTo(Branch.of(defaultBranch.getName(), initialHashOfDefaultBranch))
          .branch(defaultBranch)
          .assign();
    }
  }

  @BeforeEach
  public void beforeEach(NessieClientFactory clientFactory, @NessieClientUri URI nessieUri)
      throws IOException {
    this.uri = nessieUri.toASCIIString();
    this.api = clientFactory.make();
    this.apiVersion = clientFactory.apiVersion() == NessieApiVersion.V2 ? "2" : "1";

    Branch defaultBranch = api.getDefaultBranch();
    initialHashOfDefaultBranch = defaultBranch.getHash();
    if (!branch.equals(defaultBranch.getName())) {
      createBranch(branch, initialHashOfDefaultBranch);
    }

    hadoopConfig = new Configuration();
    catalog = initCatalog(branch);
  }

  NessieCatalog initCatalog(String ref) {
    return initCatalog(ref, null);
  }

  NessieCatalog initCatalog(String ref, String hash) {
    NessieCatalog newCatalog = new NessieCatalog();
    newCatalog.setConf(hadoopConfig);
    ImmutableMap.Builder<String, String> options =
        ImmutableMap.<String, String>builder()
            .put("ref", ref)
            .put(CatalogProperties.URI, uri)
            .put("auth-type", "NONE")
            .put(CatalogProperties.WAREHOUSE_LOCATION, temp.toUri().toString())
            .put("client-api-version", apiVersion);
    if (null != hash) {
      options.put("ref.hash", hash);
    }
    newCatalog.initialize("nessie", options.buildOrThrow());
    return newCatalog;
  }

  protected Table createTable(TableIdentifier tableIdentifier, int count) {
    try {
      createMissingNamespaces(tableIdentifier);
      return catalog.createTable(tableIdentifier, schema(count));
    } catch (Throwable t) {
      LOG.error("unable to do create " + tableIdentifier.toString(), t);
      throw t;
    }
  }

  protected void createTable(TableIdentifier tableIdentifier) {
    createMissingNamespaces(tableIdentifier);
    Schema schema = new Schema(StructType.of(required(1, "id", LongType.get())).fields());
    catalog.createTable(tableIdentifier, schema).location();
  }

  protected Table createTable(TableIdentifier tableIdentifier, Schema schema) {
    createMissingNamespaces(tableIdentifier);
    return catalog.createTable(tableIdentifier, schema);
  }

  protected void createMissingNamespaces(TableIdentifier tableIdentifier) {
    createMissingNamespaces(catalog, tableIdentifier);
  }

  protected static void createMissingNamespaces(
      NessieCatalog catalog, TableIdentifier tableIdentifier) {
    createMissingNamespaces(catalog, tableIdentifier.namespace());
  }

  protected static void createMissingNamespaces(NessieCatalog catalog, Namespace namespace) {
    List<String> elements = Lists.newArrayList();
    for (int i = 0; i < namespace.length(); i++) {
      elements.add(namespace.level(i));
      try {
        catalog.createNamespace(Namespace.of(elements.toArray(new String[0])));
      } catch (AlreadyExistsException ignore) {
        // ignore
      }
    }
  }

  protected static Schema schema(int count) {
    List<Types.NestedField> fields = Lists.newArrayList();
    for (int i = 0; i < count; i++) {
      fields.add(required(i, "id" + i, Types.LongType.get()));
    }
    return new Schema(Types.StructType.of(fields).fields());
  }

  void createBranch(String name) throws NessieNotFoundException, NessieConflictException {
    createBranch(name, catalog.currentHash());
  }

  void createBranch(String name, String hash)
      throws NessieNotFoundException, NessieConflictException {
    createBranch(name, hash, "main");
  }

  void createBranch(String name, String hash, String sourceRef)
      throws NessieNotFoundException, NessieConflictException {
    api.createReference().reference(Branch.of(name, hash)).sourceRefName(sourceRef).create();
  }

  @AfterEach
  public void afterEach() throws Exception {
    resetData();

    try {
      if (catalog != null) {
        catalog.close();
      }
      api.close();
    } finally {
      catalog = null;
      api = null;
      hadoopConfig = null;
    }
  }

  static String metadataLocation(NessieCatalog catalog, TableIdentifier tableIdentifier) {
    Table table = catalog.loadTable(tableIdentifier);
    BaseTable baseTable = (BaseTable) table;
    TableOperations ops = baseTable.operations();
    NessieTableOperations icebergOps = (NessieTableOperations) ops;
    return icebergOps.currentMetadataLocation();
  }

  static String writeRecordsToFile(
      Table table, Schema schema, String filename, List<Record> records) throws IOException {
    String fileLocation =
        table.location().replace("file:", "") + String.format("/data/%s.avro", filename);
    try (FileAppender<Record> writer =
        Avro.write(Files.localOutput(fileLocation)).schema(schema).named("test").build()) {
      for (Record rec : records) {
        writer.add(rec);
      }
    }
    return fileLocation;
  }

  static DataFile makeDataFile(Table icebergTable, String fileLocation) {
    return DataFiles.builder(icebergTable.spec())
        .withRecordCount(3)
        .withPath(fileLocation)
        .withFileSizeInBytes(Files.localInput(fileLocation).getLength())
        .build();
  }
}
