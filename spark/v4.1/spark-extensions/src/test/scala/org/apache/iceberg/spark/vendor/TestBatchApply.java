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
package org.apache.iceberg.spark.vendor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.ParserContext;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.apache.iceberg.rest.RESTCatalogProperties;
import org.apache.iceberg.rest.RESTCatalogServlet;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.RemoteSignerServlet;
import org.apache.iceberg.rest.requests.RemoteSignRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.ImmutableRemoteSignResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.RemoteSignResponse;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.extensions.ExtensionsTestBase;
import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions;
import org.apache.iceberg.spark.extensions.SparkPlanUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import scala.Option;

@ExtendWith(ParameterizedTestExtension.class)
public class TestBatchApply extends ExtensionsTestBase {

  private static final int ROWS = 12;
  private static final int INLINE_BYTES = 4096;
  private static final int SIGNING_BATCH_MAX = 5;
  private static final String CATALOG_NAME = "filecat";
  private static final String SIGN_ENDPOINT = "v1/namespaces/default/tables/table/sign";
  private static final Map<String, String> CATALOG_CONFIG =
      Maps.newHashMap(Map.of("type", "rest", "cache-enabled", "false"));

  private static Server catalog;
  private static SigningServlet servlet;

  private static final AdaptiveSparkPlanHelper PLANS = new AdaptiveSparkPlanHelper() {};

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {{CATALOG_NAME, SparkCatalog.class.getName(), CATALOG_CONFIG}};
  }

  @BeforeAll
  public static void startCatalogAndAddVendorExtension() throws Exception {
    servlet = new SigningServlet();
    Catalog backend = new HadoopCatalog(new Configuration(), "file:" + warehouse);
    catalog = new Server(0);
    ServletContextHandler context = new ServletContextHandler();
    context.addServlet(
        new ServletHolder(
            new FileCatalogServlet(
                servlet, new RESTCatalogServlet(new FileCatalogAdapter(backend)))),
        "/*");
    catalog.setHandler(context);
    catalog.start();
    CATALOG_CONFIG.put(CatalogProperties.URI, catalog.getURI().toString());

    SparkSession base = spark;
    spark.stop();
    spark =
        SparkSession.builder()
            .config(base.sparkContext().getConf())
            .config(
                "spark.sql.extensions",
                IcebergSparkSessionExtensions.class.getName()
                    + ","
                    + FileIcebergSparkSessionExtensions.class.getName())
            .enableHiveSupport()
            .getOrCreate();
  }

  @AfterAll
  public static void stopCatalog() throws Exception {
    catalog.stop();
  }

  @BeforeEach
  public void createTable() {
    servlet.resetSignCounters();
    sql(
        "CREATE TABLE %s ("
            + "id BIGINT, "
            + "file STRUCT<uri: STRING, `offset`: BIGINT, size: BIGINT, "
            + "content_type: STRING, checksum: STRING, inline: BINARY>, "
            + "partition_date DATE) USING iceberg PARTITIONED BY (partition_date)",
        tableName);
    for (int i = 0; i < ROWS; i += 1) {
      sql(
          "INSERT INTO %s VALUES (%d, named_struct("
              + "'uri', 's3://bucket/blob-%d', "
              + "'offset', 0L, "
              + "'size', %dL, "
              + "'content_type', 'application/octet-stream', "
              + "'checksum', 'sha256:%d', "
              + "'inline', CAST(repeat('x', %d) AS BINARY)), DATE '2026-09-%02d')",
          tableName, i, i, INLINE_BYTES, i, INLINE_BYTES, 18 + (i % 2));
    }
  }

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testEveryRowIsSigned() {
    List<Row> rows =
        spark.sql("SELECT * FROM sign_files(TABLE(" + tableName + "), 'file', 4)").collectAsList();

    assertThat(rows).hasSize(ROWS);
    assertThat(rows.get(0).getString(0)).startsWith("s3://bucket/blob-");
    assertThat(rows.get(0).getString(1)).startsWith(rows.get(0).getString(0) + "?sig=");
  }

  @TestTemplate
  public void testBatchesArePerPartitionNotPerRow() {
    Dataset<Row> rows = spark.sql("SELECT * FROM sign_files(TABLE(" + tableName + "), 'file', 4)");
    rows.collectAsList();

    assertThat(batches(rows.queryExecution().executedPlan())).isLessThan(ROWS);
  }

  @TestTemplate
  public void testOneSigningRoundTripPerBatch() {
    Dataset<Row> rows = spark.sql("SELECT * FROM sign_files(TABLE(" + tableName + "), 'file', 4)");
    rows.collectAsList();

    assertThat(servlet.signedLocations()).isEqualTo(ROWS);
    assertThat(servlet.signRoundTrips()).isEqualTo(batches(rows.queryExecution().executedPlan()));
    System.out.printf(
        "SQL sign_files over %d rows: %d signing round trips%n", ROWS, servlet.signRoundTrips());
  }

  @TestTemplate
  public void testBatchSizeDefaultsToTheSigningMaximum() {
    Dataset<Row> rows = spark.sql("SELECT * FROM sign_files(TABLE(" + tableName + "), 'file')");
    rows.collectAsList();

    assertThat(batchApply(rows.queryExecution().executedPlan()).batchSize())
        .isEqualTo(SIGNING_BATCH_MAX);
    assertThat(servlet.signRoundTrips()).isEqualTo(batches(rows.queryExecution().executedPlan()));
  }

  @TestTemplate
  public void testBatchSizeAboveTheSigningMaximumIsRejected() {
    assertThatThrownBy(
            () ->
                spark
                    .sql(
                        "SELECT * FROM sign_files(TABLE("
                            + tableName
                            + "), 'file', "
                            + (SIGNING_BATCH_MAX + 1)
                            + ")")
                    .collectAsList())
        .hasMessageContaining("is not in 1.." + SIGNING_BATCH_MAX);
  }

  @TestTemplate
  public void testTableArgumentMustHoldOneIcebergTable() {
    assertThatThrownBy(
            () ->
                spark
                    .sql(
                        "SELECT * FROM sign_files(TABLE("
                            + "SELECT named_struct('uri', 's3://bucket/blob') AS file"
                            + "), 'file', 4)")
                    .collectAsList())
        .hasMessageContaining("holds no Iceberg table");
    assertThatThrownBy(
            () ->
                spark
                    .sql(
                        "SELECT * FROM sign_files(TABLE("
                            + "SELECT a.file FROM "
                            + tableName
                            + " a JOIN "
                            + tableName
                            + " b ON a.id = b.id"
                            + "), 'file', 4)")
                    .collectAsList())
        .hasMessageContaining("holds 2 tables");
  }

  @TestTemplate
  public void testOperatorAppearsInExplain() {
    String plan =
        spark
            .sql("EXPLAIN EXTENDED SELECT * FROM sign_files(TABLE(" + tableName + "), 'file', 4)")
            .collectAsList()
            .get(0)
            .getString(0);

    assertThat(plan).contains("BatchApply sign_files");
  }

  @TestTemplate
  public void testFilterInsideTheTableArgumentReachesTheScan() {
    String plan =
        spark
            .sql(
                "EXPLAIN EXTENDED SELECT * FROM sign_files(TABLE("
                    + "SELECT file FROM "
                    + tableName
                    + " WHERE partition_date = DATE '2026-09-18'"
                    + "), 'file', 4)")
            .collectAsList()
            .get(0)
            .getString(0);

    assertThat(plan.substring(plan.indexOf("== Physical Plan ==")))
        .contains("filters=partition_date IS NOT NULL, partition_date =");
  }

  @TestTemplate
  public void testScanReadsOnlyTheFileColumn() {
    Dataset<Row> rows = spark.sql("SELECT * FROM sign_files(TABLE(" + tableName + "), 'file', 4)");

    List<SparkPlan> scans = SparkPlanUtil.collectBatchScans(rows.queryExecution().executedPlan());

    assertThat(scans).hasSize(1);
    assertThat(scans.get(0).schema().catalogString())
        .isEqualTo(
            "struct<file:struct<uri:string,offset:bigint,size:bigint,content_type:string,"
                + "checksum:string,inline:binary>>");
  }

  static class SigningServlet extends RemoteSignerServlet {
    SigningServlet() {
      super(SIGN_ENDPOINT);
    }

    @Override
    protected RemoteSignResponse signRequest(RemoteSignRequest request) {
      return ImmutableRemoteSignResponse.builder()
          .uri(URI.create(request.uri() + "?sig=" + Integer.toHexString(request.uri().hashCode())))
          .build();
    }
  }

  /**
   * The catalog side: advertises the signing endpoints and batch maximum, hands each table a
   * region.
   */
  static class FileCatalogAdapter extends RESTCatalogAdapter {
    FileCatalogAdapter(Catalog backend) {
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
      if (response instanceof ConfigResponse config) {
        return responseType.cast(
            ConfigResponse.builder()
                .withDefaults(config.defaults())
                .withOverrides(config.overrides())
                .withOverride(
                    RESTCatalogProperties.REMOTE_SIGNING_BATCH_MAX_SIZE,
                    String.valueOf(SIGNING_BATCH_MAX))
                .withEndpoints(config.endpoints())
                .withEndpoints(
                    List.of(Endpoint.V1_TABLE_REMOTE_SIGN, Endpoint.V1_TABLE_REMOTE_SIGN_BATCH))
                .build());
      } else if (response instanceof LoadTableResponse table) {
        table.config().put("client.region", "us-east-1");
      }

      return response;
    }
  }

  static class FileCatalogServlet extends HttpServlet {
    private final HttpServlet signer;
    private final HttpServlet catalogServlet;

    FileCatalogServlet(HttpServlet signer, HttpServlet catalogServlet) {
      this.signer = signer;
      this.catalogServlet = catalogServlet;
    }

    @Override
    protected void service(HttpServletRequest request, HttpServletResponse response)
        throws ServletException, IOException {
      String path = request.getRequestURI();
      if (path.endsWith("/sign") || path.endsWith("/sign/batch")) {
        signer.service(request, response);
      } else {
        catalogServlet.service(request, response);
      }
    }
  }

  private static long batches(SparkPlan plan) {
    return batchApply(plan).batches().value();
  }

  private static BatchApplyExec batchApply(SparkPlan plan) {
    Option<BatchApplyExec> found =
        PLANS.collectFirst(
            plan,
            new scala.PartialFunction<SparkPlan, BatchApplyExec>() {
              @Override
              public boolean isDefinedAt(SparkPlan node) {
                return node instanceof BatchApplyExec;
              }

              @Override
              public BatchApplyExec apply(SparkPlan node) {
                return (BatchApplyExec) node;
              }
            });
    assertThat(found.isDefined()).as("BatchApplyExec in the physical plan").isTrue();
    return found.get();
  }
}
