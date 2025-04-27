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
package org.apache.iceberg.aws.glue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.AwsIntegTestUtil;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.view.BaseView;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewCatalogTests;
import org.apache.iceberg.view.ViewHistoryEntry;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewProperties;
import org.apache.iceberg.view.ViewUtil;
import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariables;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.s3.S3Client;

@EnabledIfEnvironmentVariables({
  @EnabledIfEnvironmentVariable(named = AwsIntegTestUtil.AWS_ACCESS_KEY_ID, matches = ".*"),
  @EnabledIfEnvironmentVariable(named = AwsIntegTestUtil.AWS_SECRET_ACCESS_KEY, matches = ".*"),
  @EnabledIfEnvironmentVariable(named = AwsIntegTestUtil.AWS_SESSION_TOKEN, matches = ".*"),
  @EnabledIfEnvironmentVariable(named = AwsIntegTestUtil.AWS_REGION, matches = ".*"),
  @EnabledIfEnvironmentVariable(named = AwsIntegTestUtil.AWS_TEST_BUCKET, matches = ".*")
})
public class TestGlueCatalogView extends ViewCatalogTests<GlueCatalog> {

  protected static final String TEST_BUCKET_NAME = AwsIntegTestUtil.testBucketName();
  protected static final String CATALOG_NAME = "glue";
  protected static final String TEST_PATH_PREFIX = getRandomName();
  protected static final List<String> NAMESPACES = Lists.newArrayList();

  protected static final AwsClientFactory CLIENT_FACTORY = AwsClientFactories.defaultFactory();
  protected static final GlueClient GLUE = CLIENT_FACTORY.glue();
  protected static final S3Client S3 = CLIENT_FACTORY.s3();

  private static GlueCatalog glueCatalog;
  protected static final String TEST_BUCKET_PATH =
      "s3://" + TEST_BUCKET_NAME + "/" + TEST_PATH_PREFIX;

  @BeforeEach
  public void beforeEach() {
    AwsProperties properties = new AwsProperties();
    properties.setGlueCatalogSkipNameValidation(true);
    glueCatalog = new GlueCatalog();
    Map<String, String> catalogProps = Maps.newHashMap();
    catalogProps.put(CatalogProperties.VIEW_DEFAULT_PREFIX + "key1", "catalog-default-key1");
    catalogProps.put(CatalogProperties.VIEW_DEFAULT_PREFIX + "key2", "catalog-default-key2");
    catalogProps.put(CatalogProperties.VIEW_DEFAULT_PREFIX + "key3", "catalog-default-key3");
    catalogProps.put(CatalogProperties.VIEW_OVERRIDE_PREFIX + "key3", "catalog-override-key3");
    catalogProps.put(CatalogProperties.VIEW_OVERRIDE_PREFIX + "key4", "catalog-override-key4");
    glueCatalog.initialize(
        CATALOG_NAME,
        TEST_BUCKET_PATH,
        properties,
        new org.apache.iceberg.aws.s3.S3FileIOProperties(),
        GLUE,
        null,
        catalogProps);

    try {
      Namespace ns = Namespace.of("ns");
      if (!glueCatalog.namespaceExists(ns)) {
        glueCatalog.createNamespace(ns);
        NAMESPACES.add("ns");
      }

      Namespace ns1 = Namespace.of("ns1");
      if (!glueCatalog.namespaceExists(ns1)) {
        glueCatalog.createNamespace(ns1);
        NAMESPACES.add("ns1");
      }

      Namespace ns2 = Namespace.of("ns2");
      if (!glueCatalog.namespaceExists(ns2)) {
        glueCatalog.createNamespace(ns2);
        NAMESPACES.add("ns2");
      }
    } catch (Exception e) {
      // Failure to create namespace will be caught by test assertions
    }
    cleanupTestData();
  }

  private void cleanupTestData() {
    deleteCommonGlueTables();
    cleanupViewsInNamespaces("ns", "ns1", "ns2");
  }

  private void deleteCommonGlueTables() {
    deleteTableIgnoringNotFound("view");
    deleteTableIgnoringNotFound("renamedView");
  }

  private void cleanupViewsInNamespaces(String... namespaces) {
    for (String nsStr : namespaces) {
      Namespace ns = Namespace.of(nsStr);
      if (catalog().namespaceExists(ns)) {
        cleanupNamespace(ns);
      }
    }
  }

  private void cleanupNamespace(Namespace ns) {
    for (TableIdentifier view : catalog().listViews(ns)) {
      catalog().dropView(view);
    }
    for (TableIdentifier table : catalog().listTables(ns)) {
      catalog().dropTable(table);
    }
  }

  private void deleteTableIgnoringNotFound(String tableName) {
    try {
      GLUE.deleteTable(
          DeleteTableRequest.builder()
              .catalogId(awsProperties().glueCatalogId())
              .databaseName("ns")
              .name(tableName)
              .build());
    } catch (EntityNotFoundException e) {
      // Entity doesn't exist - this is one of the expected states
    }
  }

  @AfterAll
  public static void afterClass() {
    AwsIntegTestUtil.cleanGlueCatalog(GLUE, NAMESPACES);
    AwsIntegTestUtil.cleanS3GeneralPurposeBucket(S3, TEST_BUCKET_NAME, TEST_PATH_PREFIX);
  }

  @Override
  protected GlueCatalog catalog() {
    return glueCatalog;
  }

  @Override
  protected Catalog tableCatalog() {
    return glueCatalog;
  }

  protected static String getRandomName() {
    return UUID.randomUUID().toString().replace("-", "");
  }

  protected String createNamespace() {
    String namespace = getRandomName();
    NAMESPACES.add(namespace);
    glueCatalog.createNamespace(Namespace.of(namespace));
    return namespace;
  }

  protected AwsProperties awsProperties() {
    return new AwsProperties();
  }

  @Override
  @Test
  public void completeCreateView() {
    TableIdentifier identifier = TableIdentifier.of("ns", "view");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();

    String location = String.format("s3://%s/%s/ns/view", TEST_BUCKET_NAME, TEST_PATH_PREFIX);

    View view =
        catalog()
            .buildView(identifier)
            .withSchema(SCHEMA)
            .withDefaultNamespace(identifier.namespace())
            .withDefaultCatalog(catalog().name())
            .withQuery("spark", "select * from ns.tbl")
            .withQuery("trino", "select * from ns.tbl using X")
            .withProperty("prop1", "val1")
            .withProperty("prop2", "val2")
            .withLocation(location)
            .create();

    assertThat(view).isNotNull();
    assertThat(catalog().viewExists(identifier)).as("View should exist").isTrue();
    assertThat(((BaseView) view).operations().current().metadataFileLocation()).isNotNull();

    if (!overridesRequestedLocation()) {
      assertThat(view.location()).isEqualTo(location);
    } else {
      assertThat(view.location()).isNotNull();
    }

    // validate view settings
    assertThat(view.uuid())
        .isEqualTo(UUID.fromString(((BaseView) view).operations().current().uuid()));
    assertThat(view.name()).isEqualTo(ViewUtil.fullViewName(catalog().name(), identifier));
    assertThat(view.properties()).containsEntry("prop1", "val1").containsEntry("prop2", "val2");
    assertThat(view.history()).singleElement().extracting(ViewHistoryEntry::versionId).isEqualTo(1);
    assertThat(view.currentVersion().operation()).isEqualTo("create");
    assertThat(view.schema().schemaId()).isEqualTo(0);
    assertThat(view.schema().asStruct()).isEqualTo(SCHEMA.asStruct());
    assertThat(view.schemas()).hasSize(1).containsKey(0);
    assertThat(view.versions()).hasSize(1).containsExactly(view.currentVersion());

    assertThat(view.currentVersion())
        .isEqualTo(
            ImmutableViewVersion.builder()
                .timestampMillis(view.currentVersion().timestampMillis())
                .versionId(1)
                .schemaId(0)
                .summary(view.currentVersion().summary())
                .defaultNamespace(identifier.namespace())
                .defaultCatalog(catalog().name())
                .addRepresentations(
                    ImmutableSQLViewRepresentation.builder()
                        .sql("select * from ns.tbl")
                        .dialect("spark")
                        .build())
                .addRepresentations(
                    ImmutableSQLViewRepresentation.builder()
                        .sql("select * from ns.tbl using X")
                        .dialect("trino")
                        .build())
                .build());

    assertThat(catalog().dropView(identifier)).isTrue();
    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();
  }

  @Override
  @Test
  public void createAndReplaceViewWithLocation() {
    TableIdentifier identifier = TableIdentifier.of("ns", "view");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();

    String location = String.format("s3://%s/%s/ns/view", TEST_BUCKET_NAME, TEST_PATH_PREFIX);
    View view =
        catalog()
            .buildView(identifier)
            .withSchema(SCHEMA)
            .withDefaultNamespace(identifier.namespace())
            .withQuery("trino", "select * from ns.tbl")
            .withLocation(location)
            .create();

    assertThat(catalog().viewExists(identifier)).as("View should exist").isTrue();

    if (!overridesRequestedLocation()) {
      assertThat(view.location()).isEqualTo(location);
    } else {
      assertThat(view.location()).isNotNull();
    }

    String updatedLocation =
        String.format("s3://%s/%s/updated/ns/view", TEST_BUCKET_NAME, TEST_PATH_PREFIX);
    view =
        catalog()
            .buildView(identifier)
            .withSchema(SCHEMA)
            .withDefaultNamespace(identifier.namespace())
            .withQuery("trino", "select * from ns.tbl")
            .withLocation(updatedLocation)
            .replace();

    if (!overridesRequestedLocation()) {
      assertThat(view.location()).isEqualTo(updatedLocation);
    } else {
      assertThat(view.location()).isNotNull();
    }

    assertThat(catalog().dropView(identifier)).isTrue();
    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();
  }

  @Override
  @Test
  public void renameViewTargetAlreadyExistsAsView() {
    TableIdentifier viewOne = TableIdentifier.of("ns", "viewOne");
    TableIdentifier viewTwo = TableIdentifier.of("ns", "viewTwo");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(viewOne.namespace());
    }

    for (TableIdentifier identifier : ImmutableList.of(viewOne, viewTwo)) {
      assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();

      catalog()
          .buildView(identifier)
          .withSchema(SCHEMA)
          .withDefaultNamespace(viewOne.namespace())
          .withQuery("spark", "select * from ns.tbl")
          .create();

      assertThat(catalog().viewExists(identifier)).as("View should exist").isTrue();
    }

    assertThatThrownBy(() -> catalog().renameView(viewOne, viewTwo))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("Cannot rename ns.viewOne to ns.viewTwo. Glue Table already exists");
  }

  @Override
  @Test
  public void renameViewTargetAlreadyExistsAsTable() {
    Assumptions.assumeThat(tableCatalog())
        .as("Only valid for catalogs that support tables")
        .isNotNull();

    TableIdentifier viewIdentifier = TableIdentifier.of("ns", "view");
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should not exist").isFalse();

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();

    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    assertThat(catalog().viewExists(viewIdentifier)).as("View should not exist").isFalse();

    catalog()
        .buildView(viewIdentifier)
        .withSchema(SCHEMA)
        .withDefaultNamespace(viewIdentifier.namespace())
        .withQuery("spark", "select * from ns.tbl")
        .create();

    assertThat(catalog().viewExists(viewIdentifier)).as("View should exist").isTrue();

    assertThatThrownBy(() -> catalog().renameView(viewIdentifier, tableIdentifier))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("Cannot rename ns.view to ns.table. Glue Table already exists");
  }

  @Override
  @Test
  public void updateViewLocation() {
    TableIdentifier identifier = TableIdentifier.of("ns", "view");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();

    String location = String.format("s3://%s/%s/ns/view", TEST_BUCKET_NAME, TEST_PATH_PREFIX);
    View view =
        catalog()
            .buildView(identifier)
            .withSchema(SCHEMA)
            .withDefaultNamespace(identifier.namespace())
            .withQuery("trino", "select * from ns.tbl")
            .withLocation(location)
            .create();

    assertThat(catalog().viewExists(identifier)).as("View should exist").isTrue();
    if (!overridesRequestedLocation()) {
      assertThat(view.location()).isEqualTo(location);
    } else {
      assertThat(view.location()).isNotNull();
    }

    String updatedLocation =
        String.format("s3://%s/%s/updated/ns/view", TEST_BUCKET_NAME, TEST_PATH_PREFIX);
    view.updateLocation().setLocation(updatedLocation).commit();

    View updatedView = catalog().loadView(identifier);

    if (!overridesRequestedLocation()) {
      assertThat(updatedView.location()).isEqualTo(updatedLocation);
    } else {
      assertThat(view.location()).isNotNull();
    }

    // history and view versions should stay the same after updating view properties
    assertThat(updatedView.history()).hasSize(1).isEqualTo(view.history());
    assertThat(updatedView.versions()).hasSize(1).containsExactly(view.currentVersion());

    assertThat(catalog().dropView(identifier)).isTrue();
    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();
  }

  @Override
  @Test
  public void createViewWithCustomMetadataLocation() {
    TableIdentifier identifier = TableIdentifier.of("ns", "view");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();

    String location = String.format("s3://%s/%s/ns/view", TEST_BUCKET_NAME, TEST_PATH_PREFIX);
    String customLocation =
        String.format("s3://%s/%s/custom-location", TEST_BUCKET_NAME, TEST_PATH_PREFIX);

    View view =
        catalog()
            .buildView(identifier)
            .withSchema(SCHEMA)
            .withDefaultNamespace(identifier.namespace())
            .withDefaultCatalog(catalog().name())
            .withQuery("spark", "select * from ns.tbl")
            .withProperty(ViewProperties.WRITE_METADATA_LOCATION, customLocation)
            .withLocation(location)
            .create();

    assertThat(view).isNotNull();
    assertThat(catalog().viewExists(identifier)).as("View should exist").isTrue();
    assertThat(view.properties()).containsEntry("write.metadata.path", customLocation);
    assertThat(((BaseView) view).operations().current().metadataFileLocation())
        .isNotNull()
        .startsWith(customLocation);
  }

  @Override
  @Test
  public void renameTableTargetAlreadyExistsAsView() {
    Assumptions.assumeThat(tableCatalog())
        .as("Only valid for catalogs that support tables")
        .isNotNull();

    TableIdentifier viewIdentifier = TableIdentifier.of("ns", "view");
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should not exist").isFalse();

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();

    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    assertThat(catalog().viewExists(viewIdentifier)).as("View should not exist").isFalse();

    catalog()
        .buildView(viewIdentifier)
        .withSchema(SCHEMA)
        .withDefaultNamespace(viewIdentifier.namespace())
        .withQuery("spark", "select * from ns.tbl")
        .create();

    assertThat(catalog().viewExists(viewIdentifier)).as("View should exist").isTrue();

    assertThatThrownBy(() -> tableCatalog().renameTable(tableIdentifier, viewIdentifier))
        .isInstanceOf(software.amazon.awssdk.services.glue.model.AlreadyExistsException.class)
        .hasMessageContaining("Table already exists.");
  }

  @Override
  @Test
  public void renameViewUsingDifferentNamespace() {
    TableIdentifier from = TableIdentifier.of("ns", "view");
    TableIdentifier to = TableIdentifier.of("ns1", "renamedView");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(from.namespace());
      catalog().createNamespace(to.namespace());
    }

    assertThat(catalog().viewExists(from)).as("View should not exist").isFalse();

    View view =
        catalog()
            .buildView(from)
            .withSchema(SCHEMA)
            .withDefaultNamespace(from.namespace())
            .withQuery("spark", "select * from ns.tbl")
            .create();

    assertThat(catalog().viewExists(from)).as("View should exist").isTrue();

    ViewMetadata original = ((BaseView) view).operations().current();

    catalog().renameView(from, to);

    assertThat(catalog().viewExists(from)).as("View should not exist with old name").isFalse();
    assertThat(catalog().viewExists(to)).as("View should exist with new name").isTrue();

    // ensure view metadata didn't change after renaming
    View renamed = catalog().loadView(to);
    assertThat(((BaseView) renamed).operations().current())
        .usingRecursiveComparison()
        .ignoringFieldsOfTypes(Schema.class)
        .isEqualTo(original);

    assertThat(catalog().dropView(from)).isFalse();
    assertThat(catalog().dropView(to)).isTrue();
    assertThat(catalog().viewExists(to)).as("View should not exist").isFalse();
  }

  @Test
  public void testGlueTableTypeIsVirtualView() {
    String namespace = createNamespace();
    TableIdentifier identifier = TableIdentifier.of(namespace, "test_view");

    View view =
        catalog()
            .buildView(identifier)
            .withSchema(SCHEMA)
            .withDefaultNamespace(identifier.namespace())
            .withQuery("spark", "select * from test.table")
            .create();

    // Verify the Glue table has correct table type
    GetTableResponse response =
        GLUE.getTable(
            GetTableRequest.builder()
                .catalogId(awsProperties().glueCatalogId())
                .databaseName(namespace)
                .name(identifier.name())
                .build());

    Table glueTable = response.table();

    assertThat(glueTable.tableType()).isEqualTo("VIRTUAL_VIEW");
    assertThat(glueTable.parameters())
        .containsEntry(BaseMetastoreTableOperations.TABLE_TYPE_PROP, "iceberg-view");

    // Test SQL text is saved correctly
    SQLViewRepresentation sql = view.sqlFor("spark");
    assertThat(sql.sql()).isEqualTo("select * from test.table");
    assertThat(glueTable.viewOriginalText()).isEqualTo("select * from test.table");
    assertThat(glueTable.viewExpandedText()).isEqualTo("select * from test.table");
  }

  @Test
  public void testListViewsFiltersNonIcebergViews() {
    String namespace = createNamespace();
    String icebergViewName = "iceberg_view";
    String regularViewName = "regular_view";

    // Create a regular Glue view (non-Iceberg)
    Map<String, String> regularProperties = Maps.newHashMap();
    TableInput regularViewInput =
        TableInput.builder()
            .name(regularViewName)
            .tableType("VIRTUAL_VIEW")
            .parameters(regularProperties)
            .viewOriginalText("SELECT * FROM some_table")
            .build();

    GLUE.createTable(
        CreateTableRequest.builder()
            .catalogId(awsProperties().glueCatalogId())
            .databaseName(namespace)
            .tableInput(regularViewInput)
            .build());

    // Create an Iceberg view
    TableIdentifier icebergViewId = TableIdentifier.of(namespace, icebergViewName);
    catalog()
        .buildView(icebergViewId)
        .withSchema(SCHEMA)
        .withDefaultNamespace(icebergViewId.namespace())
        .withQuery("spark", "select * from test.table")
        .create();

    // listViews should only return the Iceberg view
    assertThat(catalog().listViews(Namespace.of(namespace)))
        .hasSize(1)
        .extracting(TableIdentifier::name)
        .containsExactly(icebergViewName);
  }

  @Test
  public void testDropViewVerifiesIcebergView() {
    String namespace = createNamespace();
    String regularViewName = "regular_view";

    // Create a regular Glue view (non-Iceberg)
    Map<String, String> regularProperties = Maps.newHashMap();
    TableInput regularViewInput =
        TableInput.builder()
            .name(regularViewName)
            .tableType("VIRTUAL_VIEW")
            .parameters(regularProperties)
            .viewOriginalText("SELECT * FROM some_table")
            .build();

    GLUE.createTable(
        CreateTableRequest.builder()
            .catalogId(awsProperties().glueCatalogId())
            .databaseName(namespace)
            .tableInput(regularViewInput)
            .build());

    TableIdentifier regularViewId = TableIdentifier.of(namespace, regularViewName);

    // Dropping a non-Iceberg view should return false
    assertThat(catalog().dropView(regularViewId)).isFalse();

    // Regular view should still exist in Glue
    GetTableResponse response =
        GLUE.getTable(
            GetTableRequest.builder()
                .catalogId(awsProperties().glueCatalogId())
                .databaseName(namespace)
                .name(regularViewName)
                .build());

    assertThat(response.table()).isNotNull();
  }

  @Test
  public void testRenameViewCheckTargetExists() {
    String namespace = createNamespace();
    String sourceViewName = "source_view";
    String targetViewName = "target_view";

    // Create source (Iceberg) view
    TableIdentifier sourceViewId = TableIdentifier.of(namespace, sourceViewName);
    catalog()
        .buildView(sourceViewId)
        .withSchema(SCHEMA)
        .withDefaultNamespace(sourceViewId.namespace())
        .withQuery("spark", "select * from test.table")
        .create();

    // Create target (non-Iceberg) view
    Map<String, String> targetProperties = Maps.newHashMap();
    TableInput targetViewInput =
        TableInput.builder()
            .name(targetViewName)
            .tableType("VIRTUAL_VIEW")
            .parameters(targetProperties)
            .viewOriginalText("SELECT * FROM some_table")
            .build();

    GLUE.createTable(
        CreateTableRequest.builder()
            .catalogId(awsProperties().glueCatalogId())
            .databaseName(namespace)
            .tableInput(targetViewInput)
            .build());

    TableIdentifier targetViewId = TableIdentifier.of(namespace, targetViewName);

    // Should throw AlreadyExistsException since target already exists
    assertThatThrownBy(() -> catalog().renameView(sourceViewId, targetViewId))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining(
            "Cannot rename "
                + sourceViewId
                + " to "
                + targetViewId
                + ". Glue Table already exists");
  }

  @Test
  public void testIcebergTableAndViewCannotHaveSameName() {
    String namespace = createNamespace();
    String name = "same_name";

    // Create a regular Iceberg table
    TableIdentifier tableId = TableIdentifier.of(namespace, name);
    tableCatalog().buildTable(tableId, SCHEMA).create();

    // Try to create a view with the same name
    assertThatThrownBy(
            () ->
                catalog()
                    .buildView(tableId)
                    .withSchema(SCHEMA)
                    .withDefaultNamespace(tableId.namespace())
                    .withQuery("spark", "select * from test.table")
                    .create())
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("Table with same name already exists");

    // Cleanup
    tableCatalog().dropTable(tableId);
  }

  @Test
  public void testTableCannotShareNameWithView() {
    String namespace = createNamespace();
    String name = "view_name";

    // Create a view
    TableIdentifier viewId = TableIdentifier.of(namespace, name);
    catalog()
        .buildView(viewId)
        .withSchema(SCHEMA)
        .withDefaultNamespace(viewId.namespace())
        .withQuery("spark", "select * from test.table")
        .create();

    // Try to create a table with the same name
    assertThatThrownBy(() -> tableCatalog().buildTable(viewId, SCHEMA).create())
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("View with same name already exists");
  }

  @Test
  public void testDropViewCleansGlueMetadata() {
    String namespace = createNamespace();
    String viewName = "drop_view_test";

    TableIdentifier viewId = TableIdentifier.of(namespace, viewName);
    catalog()
        .buildView(viewId)
        .withSchema(SCHEMA)
        .withDefaultNamespace(viewId.namespace())
        .withQuery("spark", "select * from test.table")
        .create();

    // Verify the view exists
    assertThat(catalog().viewExists(viewId)).isTrue();

    // Drop the view
    assertThat(catalog().dropView(viewId)).isTrue();

    // Verify the view no longer exists in Glue Catalog
    assertThatThrownBy(
            () ->
                GLUE.getTable(
                    GetTableRequest.builder()
                        .catalogId(awsProperties().glueCatalogId())
                        .databaseName(namespace)
                        .name(viewName)
                        .build()))
        .isInstanceOf(EntityNotFoundException.class)
        .hasMessageContaining("Entity Not Found");
  }

  @Test
  public void testDropNamespaceWithViews() {
    String namespace = createNamespace();
    String viewName = "view_for_drop_namespace";

    TableIdentifier viewId = TableIdentifier.of(namespace, viewName);
    catalog()
        .buildView(viewId)
        .withSchema(SCHEMA)
        .withDefaultNamespace(viewId.namespace())
        .withQuery("spark", "select * from test.table")
        .create();

    // Verify the view exists
    assertThat(catalog().viewExists(viewId)).isTrue();

    // Try to drop the namespace, should fail since it contains views
    assertThatThrownBy(() -> catalog().dropNamespace(Namespace.of(namespace)))
        .hasMessageContaining("Cannot drop namespace " + namespace + " because it still contains");

    // Drop the view first
    assertThat(catalog().dropView(viewId)).isTrue();

    // Now we should be able to drop the namespace
    assertThat(catalog().dropNamespace(Namespace.of(namespace))).isTrue();
  }

  @Test
  public void testMultiDialectViews() {
    String namespace = createNamespace();
    String viewName = "multi_dialect_view";

    TableIdentifier viewId = TableIdentifier.of(namespace, viewName);
    View view =
        catalog()
            .buildView(viewId)
            .withSchema(SCHEMA)
            .withDefaultNamespace(viewId.namespace())
            .withQuery("spark", "select * from test.table")
            .withQuery("trino", "SELECT * FROM test.table")
            .create();

    // Check that we can retrieve both dialects
    assertThat(view.sqlFor("spark").sql()).isEqualTo("select * from test.table");
    assertThat(view.sqlFor("trino").sql()).isEqualTo("SELECT * FROM test.table");

    // Check that we save the first dialect to Glue view text
    GetTableResponse response =
        GLUE.getTable(
            GetTableRequest.builder()
                .catalogId(awsProperties().glueCatalogId())
                .databaseName(namespace)
                .name(viewName)
                .build());

    String viewText = response.table().viewOriginalText();

    // Since order is not guaranteed in maps, we check for either dialect
    assertThat(viewText)
        .satisfiesAnyOf(
            text -> assertThat(text).isEqualTo("select * from test.table"),
            text -> assertThat(text).isEqualTo("SELECT * FROM test.table"));
  }

  @Test
  public void testViewMetadataLocationFormat() {
    String namespace = createNamespace();
    String viewName = "view_metadata_test";

    TableIdentifier viewId = TableIdentifier.of(namespace, viewName);
    catalog()
        .buildView(viewId)
        .withSchema(SCHEMA)
        .withDefaultNamespace(viewId.namespace())
        .withQuery("spark", "select * from test.table")
        .create();

    // Check Glue parameters contain metadata location
    GetTableResponse response =
        GLUE.getTable(
            GetTableRequest.builder()
                .catalogId(awsProperties().glueCatalogId())
                .databaseName(namespace)
                .name(viewName)
                .build());

    String metadataLocation =
        response.table().parameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);
    assertThat(metadataLocation).isNotNull();
    assertThat(metadataLocation).contains("metadata/");
    assertThat(metadataLocation).endsWith(".metadata.json");
  }
}
