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
package org.apache.iceberg.data;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.functions.MaskAlphanum;
import org.apache.iceberg.functions.UnknownFunction;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadTableResponseParser;
import org.apache.iceberg.rest.restrictions.ReadRestrictions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * End-to-end coverage for read restrictions: a REST server attaches {@link ReadRestrictions} to
 * loadTable, the response crosses the wire as JSON, and the generic reader enforces them.
 *
 * <p>The unit tests in {@code TestReadRestrictionsApplier} cover the applier in isolation. This
 * test exists because every failure mode in the surrounding plumbing is fail-open: if restrictions
 * are dropped anywhere between the response and the reader, queries silently return unmasked rows
 * rather than failing. Each test below asserts on the records a caller actually receives.
 */
public class TestReadRestrictionsEndToEnd {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "email", Types.StringType.get()),
          optional(3, "country", Types.StringType.get()));

  private static final Namespace NAMESPACE = Namespace.of("restrictions");
  private static final TableIdentifier TABLE_IDENT = TableIdentifier.of(NAMESPACE, "events");

  private static final Record TEMPLATE = GenericRecord.create(SCHEMA);

  private static final List<Record> ROWS =
      ImmutableList.of(
          TEMPLATE.copy(ImmutableMap.of("id", 1L, "email", "alice@example.com", "country", "US")),
          TEMPLATE.copy(ImmutableMap.of("id", 2L, "email", "bob@example.com", "country", "CA")),
          TEMPLATE.copy(ImmutableMap.of("id", 3L, "email", "carol@example.com", "country", "US")));

  private static InMemoryCatalog backend;

  @BeforeAll
  public static void createBackendTable() throws IOException {
    // read-only tests share one table; only the restrictions attached to loadTable differ. A test
    // that evolves the schema creates its own table so it cannot disturb the others, since JUnit
    // does not guarantee execution order.
    backend = new InMemoryCatalog();
    backend.initialize(
        "backend", ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, "memory://warehouse"));
    backend.createNamespace(NAMESPACE);
    createTableWithRows(TABLE_IDENT);
  }

  private static Table createTableWithRows(TableIdentifier identifier) throws IOException {
    Table table = backend.createTable(identifier, SCHEMA);
    OutputFile out = table.io().newOutputFile(table.location() + "/data/" + UUID.randomUUID());
    table.newAppend().appendFile(FileHelpers.writeDataFile(table, out, ROWS)).commit();
    return table;
  }

  @Test
  public void masksAreEnforcedThroughTheRestCatalog() throws IOException {
    ReadRestrictions restrictions =
        ReadRestrictions.of(null, ImmutableList.of(new MaskAlphanum(2)));

    try (RESTCatalog catalog = restCatalogReturning(restrictions)) {
      List<Record> records = readAll(catalog.loadTable(TABLE_IDENT));

      assertThat(records)
          .extracting(record -> record.getField("id"), record -> record.getField("email"))
          .containsExactlyInAnyOrder(
              tuple(1L, "xxxxx@xxxxxxx.xxx"),
              tuple(2L, "xxx@xxxxxxx.xxx"),
              tuple(3L, "xxxxx@xxxxxxx.xxx"));
    }
  }

  @Test
  public void rowFilterIsEnforcedThroughTheRestCatalog() throws IOException {
    ReadRestrictions restrictions =
        ReadRestrictions.of(Expressions.equal("country", "US"), ImmutableList.of());

    try (RESTCatalog catalog = restCatalogReturning(restrictions)) {
      List<Record> records = readAll(catalog.loadTable(TABLE_IDENT));

      assertThat(records)
          .extracting(record -> record.getField("id"))
          .containsExactlyInAnyOrder(1L, 3L);
    }
  }

  @Test
  public void rowFilterIsEvaluatedOnOriginalValuesBeforeMasksApply() throws IOException {
    // If the mask ran first, "alice@example.com" would already be masked and the filter would
    // match nothing.
    ReadRestrictions restrictions =
        ReadRestrictions.of(
            Expressions.equal("email", "alice@example.com"), ImmutableList.of(new MaskAlphanum(2)));

    try (RESTCatalog catalog = restCatalogReturning(restrictions)) {
      List<Record> records = readAll(catalog.loadTable(TABLE_IDENT));

      assertThat(records)
          .extracting(record -> record.getField("id"), record -> record.getField("email"))
          .containsExactly(tuple(1L, "xxxxx@xxxxxxx.xxx"));
    }
  }

  @Test
  public void projectionForAColumnThatIsNotReadDoesNotFailTheScan() throws IOException {
    // Per spec, projections referencing columns that are not being read do not apply, so selecting
    // only "id" must succeed even though the server masks "email".
    ReadRestrictions restrictions =
        ReadRestrictions.of(null, ImmutableList.of(new MaskAlphanum(2)));

    try (RESTCatalog catalog = restCatalogReturning(restrictions)) {
      List<Record> records =
          Lists.newArrayList(
              IcebergGenerics.read(catalog.loadTable(TABLE_IDENT)).select("id").build());

      assertThat(records)
          .extracting(record -> record.getField("id"))
          .containsExactlyInAnyOrder(1L, 2L, 3L);
    }
  }

  @Test
  public void unrecognizedActionFailsClosedRatherThanReturningRawValues() throws IOException {
    ReadRestrictions restrictions =
        ReadRestrictions.of(null, ImmutableList.of(new UnknownFunction(2, "xxx-not-real")));

    try (RESTCatalog catalog = restCatalogReturning(restrictions)) {
      Table table = catalog.loadTable(TABLE_IDENT);

      assertThatThrownBy(() -> IcebergGenerics.read(table).build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Cannot bind unknown function 'xxx-not-real'");
    }
  }

  @Test
  public void loadTableFailsWhenAProjectionNamesAFieldIdTheTableNeverHad() throws IOException {
    ReadRestrictions restrictions =
        ReadRestrictions.of(null, ImmutableList.of(new MaskAlphanum(999)));

    try (RESTCatalog catalog = restCatalogReturning(restrictions)) {
      // validated where the restrictions are attached to the table, so this fails at load rather
      // than surfacing later as a skipped projection
      assertThatThrownBy(() -> catalog.loadTable(TABLE_IDENT))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("unknown field ids")
          .hasMessageContaining("999");
    }
  }

  @Test
  public void loadTableSucceedsWhenAProjectionNamesASinceDroppedColumn() throws IOException {
    // own table: dropping a column here must not affect the tests sharing the fixture
    TableIdentifier evolved = TableIdentifier.of(NAMESPACE, "evolved");
    createTableWithRows(evolved).updateSchema().deleteColumn("country").commit();

    ReadRestrictions restrictions =
        ReadRestrictions.of(null, ImmutableList.of(new MaskAlphanum(3)));

    try (RESTCatalog catalog = restCatalogReturning(restrictions)) {
      // field 3 is real, just absent from the current schema, so the table still loads
      Table table = catalog.loadTable(evolved);

      assertThat(readAll(table)).hasSize(3);
    }
  }

  /**
   * Builds a REST catalog whose loadTable response carries the given restrictions. The rewritten
   * response is round-tripped through JSON so the wire format is exercised too, not just the
   * in-memory objects.
   */
  private RESTCatalog restCatalogReturning(ReadRestrictions restrictions) {
    RESTClient client =
        new RESTCatalogAdapter(backend) {
          @Override
          public <T extends RESTResponse> T execute(
              HTTPRequest request,
              Class<T> responseType,
              Consumer<ErrorResponse> errorHandler,
              Consumer<Map<String, String>> responseHeaders) {
            T response = super.execute(request, responseType, errorHandler, responseHeaders);
            if (!(response instanceof LoadTableResponse)) {
              return response;
            }

            LoadTableResponse loaded = (LoadTableResponse) response;
            LoadTableResponse withRestrictions =
                LoadTableResponse.builder()
                    .withTableMetadata(loaded.tableMetadata())
                    .addAllConfig(loaded.config())
                    .addAllCredentials(loaded.credentials())
                    .withReadRestrictions(restrictions)
                    .build();

            return castResponse(
                responseType,
                LoadTableResponseParser.fromJson(LoadTableResponseParser.toJson(withRestrictions)));
          }
        };

    RESTCatalog catalog = new RESTCatalog(config -> client);
    catalog.initialize(
        "rest", ImmutableMap.of(CatalogProperties.FILE_IO_IMPL, InMemoryFileIO.class.getName()));
    return catalog;
  }

  private static List<Record> readAll(Table table) {
    return Lists.newArrayList(IcebergGenerics.read(table).build());
  }
}
