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
package org.apache.iceberg.spark.extensions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.functions.IcebergFunction;
import org.apache.iceberg.functions.MaskAlphanum;
import org.apache.iceberg.functions.ReplaceWithNull;
import org.apache.iceberg.functions.ShowLast4;
import org.apache.iceberg.functions.UnknownFunction;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.ParserContext;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadTableResponseParser;
import org.apache.iceberg.rest.restrictions.ReadRestrictions;
import org.apache.iceberg.rest.restrictions.ReadRestrictionsParser;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

/**
 * End-to-end coverage for read restrictions in Spark: a REST catalog attaches {@link
 * ReadRestrictions} to loadTable, the response crosses the wire as JSON, and the Catalyst rule
 * enforces them for plain SQL queries.
 *
 * <p>{@code TestIcebergRestrictionExpressions} covers the masking expressions in isolation. This
 * test exists because every failure mode in the surrounding plumbing is fail-open: if restrictions
 * are dropped anywhere between the response and the scan, queries silently return unmasked rows
 * rather than failing. Each test below asserts on the rows a SQL user actually receives.
 *
 * <p>Data is written through a Hadoop catalog and then read back through a REST catalog over the
 * same warehouse, so the restricted reads go through the real REST loadTable path.
 */
public class TestSparkReadRestrictions extends ExtensionsTestBase {

  private static final String RESTRICTED_CATALOG = "restricted";

  // Read by the REST catalog that Spark instantiates reflectively, so both have to be static.
  private static final AtomicReference<ReadRestrictions> RESTRICTIONS =
      new AtomicReference<>(ReadRestrictions.empty());
  private static final AtomicReference<Catalog> BACKEND = new AtomicReference<>();

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        SparkCatalogConfig.HADOOP.properties()
      }
    };
  }

  private String restrictedTable;

  @BeforeEach
  public void createTableAndRestrictedCatalog() {
    sql(
        "CREATE TABLE %s (id BIGINT NOT NULL, email STRING, country STRING) USING iceberg",
        tableName);
    sql(
        "INSERT INTO %s VALUES (1, 'alice@example.com', 'US'), (2, 'bob@example.com', 'CA'), "
            + "(3, 'carol@example.com', 'US')",
        tableName);

    BACKEND.set(validationCatalog);
    RESTRICTIONS.set(ReadRestrictions.empty());

    // Caching is off so every query reloads the table and sees the restrictions the running test
    // installed, rather than a table cached by an earlier one.
    spark.conf().set("spark.sql.catalog." + RESTRICTED_CATALOG, SparkCatalog.class.getName());
    spark
        .conf()
        .set(
            "spark.sql.catalog." + RESTRICTED_CATALOG + ".catalog-impl",
            RestrictingRESTCatalog.class.getName());
    spark.conf().set("spark.sql.catalog." + RESTRICTED_CATALOG + ".cache-enabled", "false");

    this.restrictedTable = RESTRICTED_CATALOG + ".default." + tableIdent.name();
  }

  @AfterEach
  public void removeTable() {
    RESTRICTIONS.set(ReadRestrictions.empty());
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void masksAreEnforcedThroughTheRestCatalog() {
    restrict(null, new MaskAlphanum(fieldId("email")));

    assertEquals(
        "Email should be masked for every row",
        ImmutableList.of(
            row(1L, "xxxxx@xxxxxxx.xxx"), row(2L, "xxx@xxxxxxx.xxx"), row(3L, "xxxxx@xxxxxxx.xxx")),
        sql("SELECT id, email FROM %s ORDER BY id", restrictedTable));
  }

  @TestTemplate
  public void masksApplyToTheColumnUnderItsOriginalName() {
    // Spec: the reader must present the action's result under the original field id, so a query
    // that names the column explicitly must still see the masked value.
    restrict(null, new ShowLast4(fieldId("email")));

    // Everything but the last four code points is masked with mask-alphanum rules, which keep the
    // allow-listed punctuation as-is.
    assertEquals(
        "show-last-4 should keep the last four code points",
        ImmutableList.of(row("xxxxx@xxxxxxx.com")),
        sql("SELECT email FROM %s WHERE id = 1", restrictedTable));
  }

  @TestTemplate
  public void rowFilterIsEnforcedThroughTheRestCatalog() {
    restrict(boundFilter(Expressions.equal("country", "US")));

    assertEquals(
        "Only US rows should be returned",
        ImmutableList.of(row(1L), row(3L)),
        sql("SELECT id FROM %s ORDER BY id", restrictedTable));
  }

  @TestTemplate
  public void rowFilterIsEvaluatedOnOriginalValuesBeforeMasksApply() {
    // If the mask ran first, "alice@example.com" would already be masked and the filter would match
    // nothing.
    restrict(
        boundFilter(Expressions.equal("email", "alice@example.com")),
        new MaskAlphanum(fieldId("email")));

    assertEquals(
        "The surviving row should be masked, not filtered away",
        ImmutableList.of(row(1L, "xxxxx@xxxxxxx.xxx")),
        sql("SELECT id, email FROM %s ORDER BY id", restrictedTable));
  }

  @TestTemplate
  public void rowFilterIsNotLeakedIntoTheQueryPlan() {
    restrict(boundFilter(Expressions.equal("country", "US")));

    String plan = (String) sql("EXPLAIN EXTENDED SELECT id FROM %s", restrictedTable).get(0)[0];

    assertThat(plan).contains("iceberg_row_filter()");
  }

  @TestTemplate
  public void projectionForAColumnThatIsNotReadDoesNotFailTheQuery() {
    int droppedFieldId = fieldId("country");
    sql("ALTER TABLE %s DROP COLUMN country", tableName);

    // The field id is real but absent from the current schema, so per spec the projection does not
    // apply rather than failing the query.
    restrict(null, new MaskAlphanum(droppedFieldId));

    assertEquals(
        "The scan should succeed with no masking applied",
        ImmutableList.of(row(1L), row(2L), row(3L)),
        sql("SELECT id FROM %s ORDER BY id", restrictedTable));
  }

  @TestTemplate
  public void unrecognizedActionFailsClosedRatherThanReturningRawValues() {
    restrict(null, new UnknownFunction(fieldId("email"), "mask-from-the-future"));

    assertThatThrownBy(() -> sql("SELECT id, email FROM %s", restrictedTable))
        .hasStackTraceContaining("Cannot bind unknown function 'mask-from-the-future'");
  }

  @TestTemplate
  public void replaceWithNullOnARequiredFieldFails() {
    restrict(null, new ReplaceWithNull(fieldId("id")));

    assertThatThrownBy(() -> sql("SELECT id FROM %s", restrictedTable))
        .hasStackTraceContaining("Cannot apply replace-with-null to required field: id");
  }

  @TestTemplate
  public void writesToARestrictedTableAreRejected() {
    restrict(null, new MaskAlphanum(fieldId("email")));

    assertThatThrownBy(
            () -> sql("INSERT INTO %s VALUES (4, 'dan@example.com', 'US')", restrictedTable))
        .hasStackTraceContaining("Cannot write to table with read restrictions");
  }

  @TestTemplate
  public void rowFilterCrossesTheWireAsAFieldIdReference() {
    // Spec: column references in required-row-filter must be field ids, so that the filter survives
    // a column rename.
    ReadRestrictions restrictions =
        ReadRestrictions.of(boundFilter(Expressions.equal("country", "US")), ImmutableList.of());
    String json = ReadRestrictionsParser.toJson(restrictions, true);

    assertThat(json).contains("\"id\" : " + fieldId("country"));
    assertThat(json).doesNotContain("\"name\"");

    // Without a schema the id cannot be resolved, and an unenforceable filter must not be read as
    // no filter at all.
    assertThatThrownBy(() -> ReadRestrictionsParser.fromJson(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot parse reference by field ID");

    assertThat(ReadRestrictionsParser.fromJson(json, table().schema()).rowFilter().toString())
        .isEqualTo(Expressions.equal("country", "US").toString());
  }

  private void restrict(Expression rowFilter, IcebergFunction<?, ?>... projections) {
    RESTRICTIONS.set(ReadRestrictions.of(rowFilter, ImmutableList.copyOf(projections)));
  }

  private Expression boundFilter(Expression unbound) {
    return Binder.bind(table().schema().asStruct(), unbound, true);
  }

  private Table table() {
    return validationCatalog.loadTable(tableIdent);
  }

  private int fieldId(String column) {
    return table().schema().findField(column).fieldId();
  }

  /**
   * A REST catalog whose loadTable responses carry the restrictions the running test installed.
   * Spark instantiates this reflectively from {@code catalog-impl}, so it must be public with a
   * no-arg constructor.
   */
  public static class RestrictingRESTCatalog extends RESTCatalog {
    public RestrictingRESTCatalog() {
      super(config -> new RestrictionInjectingAdapter(BACKEND.get()));
    }
  }

  /**
   * Attaches {@link ReadRestrictions} to every loadTable response. The rewritten response is
   * round-tripped through JSON so the wire format is exercised too, not just the in-memory objects.
   */
  private static class RestrictionInjectingAdapter extends RESTCatalogAdapter {
    RestrictionInjectingAdapter(Catalog backend) {
      super(backend);
    }

    @Override
    protected <T extends RESTResponse> T execute(
        HTTPRequest request,
        Class<T> responseType,
        Consumer<ErrorResponse> errorHandler,
        Consumer<Map<String, String>> responseHeaders,
        ParserContext parserContext) {
      T response =
          super.execute(request, responseType, errorHandler, responseHeaders, parserContext);

      ReadRestrictions restrictions = RESTRICTIONS.get();
      if (restrictions.isEmpty() || !(response instanceof LoadTableResponse)) {
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
  }
}
