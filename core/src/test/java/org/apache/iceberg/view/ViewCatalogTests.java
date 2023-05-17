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
package org.apache.iceberg.view;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public abstract class ViewCatalogTests<C extends ViewCatalog & SupportsNamespaces> {
  protected static final Schema SCHEMA =
      new Schema(
          1,
          required(3, "id", Types.IntegerType.get(), "unique ID"),
          required(4, "data", Types.StringType.get()));

  private static final Schema OTHER_SCHEMA =
      new Schema(2, required(1, "some_id", Types.IntegerType.get()));

  protected abstract C catalog();

  protected abstract Catalog tableCatalog();

  protected boolean requiresNamespaceCreate() {
    return false;
  }

  @Test
  public void basicCreateView() {
    TableIdentifier identifier = TableIdentifier.of("ns", "view");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();

    View view =
        catalog()
            .buildView(identifier)
            .withSchema(SCHEMA)
            .withDefaultNamespace(identifier.namespace())
            .withQuery("spark", "select * from ns.tbl")
            .create();

    assertThat(view).isNotNull();
    assertThat(catalog().viewExists(identifier)).as("View should exist").isTrue();

    // validate view settings
    assertThat(view.name()).isEqualTo(catalog().name() + "." + identifier);
    assertThat(view.properties()).isEmpty();
    assertThat(view.history())
        .hasSize(1)
        .first()
        .extracting(ViewHistoryEntry::versionId)
        .isEqualTo(1);
    assertThat(view.schemas()).hasSize(1).containsKey(SCHEMA.schemaId());
    assertThat(view.schema().asStruct()).isEqualTo(SCHEMA.asStruct());
    assertThat(view.versions()).hasSize(1).containsExactly(view.currentVersion());

    assertThat(view.currentVersion())
        .isEqualTo(
            ImmutableViewVersion.builder()
                .timestampMillis(view.currentVersion().timestampMillis())
                .versionId(1)
                .schemaId(SCHEMA.schemaId())
                .putSummary("operation", "create")
                .defaultNamespace(identifier.namespace())
                .addRepresentations(
                    ImmutableSQLViewRepresentation.builder()
                        .sql("select * from ns.tbl")
                        .dialect("spark")
                        .build())
                .build());

    assertThat(catalog().dropView(identifier)).isTrue();
    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();
  }

  @Test
  public void completeCreateView() {
    TableIdentifier identifier = TableIdentifier.of("ns", "view");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();

    View view =
        catalog()
            .buildView(identifier)
            .withSchema(SCHEMA)
            .withDefaultNamespace(identifier.namespace())
            .withQuery("spark", "select * from ns.tbl")
            .withQuery("trino", "select * from ns.tbl using X")
            .withProperty("prop1", "val1")
            .withProperty("prop2", "val2")
            .create();

    assertThat(view).isNotNull();
    assertThat(catalog().viewExists(identifier)).as("View should exist").isTrue();

    // validate view settings
    assertThat(view.name()).isEqualTo(catalog().name() + "." + identifier);
    assertThat(view.properties()).isEqualTo(ImmutableMap.of("prop1", "val1", "prop2", "val2"));
    assertThat(view.history())
        .hasSize(1)
        .first()
        .extracting(ViewHistoryEntry::versionId)
        .isEqualTo(1);
    assertThat(view.schema().schemaId()).isEqualTo(SCHEMA.schemaId());
    assertThat(view.schema().asStruct()).isEqualTo(SCHEMA.asStruct());
    assertThat(view.schemas()).hasSize(1).containsKey(SCHEMA.schemaId());
    assertThat(view.versions()).hasSize(1).containsExactly(view.currentVersion());

    assertThat(view.currentVersion())
        .isEqualTo(
            ImmutableViewVersion.builder()
                .timestampMillis(view.currentVersion().timestampMillis())
                .versionId(1)
                .schemaId(SCHEMA.schemaId())
                .putSummary("operation", "create")
                .defaultNamespace(identifier.namespace())
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

  @Test
  public void createViewThatAlreadyExists() {
    TableIdentifier viewIdentifier = TableIdentifier.of("ns", "view");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(viewIdentifier.namespace());
    }

    assertThat(catalog().viewExists(viewIdentifier)).isFalse();

    View view =
        catalog()
            .buildView(viewIdentifier)
            .withSchema(SCHEMA)
            .withDefaultNamespace(viewIdentifier.namespace())
            .withQuery("spark", "select * from ns.tbl")
            .create();

    assertThat(view).isNotNull();
    assertThat(catalog().viewExists(viewIdentifier)).isTrue();

    assertThatThrownBy(
            () ->
                catalog()
                    .buildView(viewIdentifier)
                    .withSchema(OTHER_SCHEMA)
                    .withQuery("spark", "select * from ns.tbl")
                    .withDefaultNamespace(viewIdentifier.namespace())
                    .create())
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageStartingWith("View already exists: ns.view");
  }

  @Test
  public void createViewThatAlreadyExistsAsTable() {
    Assumptions.assumeThat(tableCatalog())
        .as("Only valid for catalogs that support tables")
        .isNotNull();

    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");
    TableIdentifier viewIdentifier = TableIdentifier.of("ns", "view");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(viewIdentifier.namespace());
    }

    assertThat(catalog().viewExists(viewIdentifier)).isFalse();

    View view =
        catalog()
            .buildView(viewIdentifier)
            .withSchema(SCHEMA)
            .withDefaultNamespace(viewIdentifier.namespace())
            .withQuery("spark", "select * from ns.tbl")
            .create();

    assertThat(view).isNotNull();
    assertThat(catalog().viewExists(tableIdentifier)).isFalse();
    assertThat(catalog().viewExists(viewIdentifier)).isTrue();

    assertThatThrownBy(
            () ->
                catalog()
                    .buildView(viewIdentifier)
                    .withSchema(OTHER_SCHEMA)
                    .withQuery("spark", "select * from ns.tbl")
                    .withDefaultNamespace(viewIdentifier.namespace())
                    .create())
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageStartingWith("View already exists: ns.view");

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
    assertThat(tableCatalog().tableExists(tableIdentifier)).isTrue();
    assertThat(catalog().viewExists(tableIdentifier)).isFalse();

    assertThatThrownBy(
            () ->
                catalog()
                    .buildView(tableIdentifier)
                    .withSchema(OTHER_SCHEMA)
                    .withDefaultNamespace(tableIdentifier.namespace())
                    .withQuery("spark", "select * from ns.tbl")
                    .create())
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageStartingWith("Table with same name already exists: ns.table");
  }

  @Test
  public void createTableThatAlreadyExistsAsView() {
    Assumptions.assumeThat(tableCatalog())
        .as("Only valid for catalogs that support tables")
        .isNotNull();

    TableIdentifier viewOne = TableIdentifier.of("ns", "viewOne");
    TableIdentifier viewTwo = TableIdentifier.of("ns", "viewTwo");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(viewOne.namespace());
    }

    assertThat(catalog().viewExists(viewOne)).isFalse();
    assertThat(tableCatalog().tableExists(viewTwo)).isFalse();

    for (TableIdentifier identifier : Arrays.asList(viewTwo, viewOne)) {
      assertThat(
              catalog()
                  .buildView(identifier)
                  .withSchema(SCHEMA)
                  .withDefaultNamespace(identifier.namespace())
                  .withQuery("spark", "select * from ns.tbl")
                  .create())
          .isNotNull();

      assertThat(catalog().viewExists(identifier)).isTrue();
      assertThat(tableCatalog().tableExists(identifier)).isFalse();
    }

    assertThatThrownBy(() -> tableCatalog().buildTable(viewTwo, SCHEMA).create())
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageStartingWith("View with same name already exists: ns.viewTwo");
  }

  @Test
  public void renameView() {
    TableIdentifier from = TableIdentifier.of("ns", "view");
    TableIdentifier to = TableIdentifier.of("ns", "renamedView");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(from.namespace());
    }

    catalog()
        .buildView(from)
        .withSchema(SCHEMA)
        .withDefaultNamespace(from.namespace())
        .withQuery("spark", "select * from ns.tbl")
        .create();

    assertThat(catalog().viewExists(from)).as("View should exist").isTrue();
    assertThat(catalog().listViews(from.namespace())).containsExactly(from);

    catalog().renameView(from, to);

    assertThat(catalog().listViews(to.namespace())).containsExactly(to);
    assertThat(catalog().viewExists(from)).as("View should not exist").isFalse();
    assertThat(catalog().viewExists(to)).as("View should exist").isTrue();

    View view = catalog().loadView(to);
    assertThat(view).isNotNull();

    // validate view settings
    assertThat(view.name()).isEqualTo(catalog().name() + "." + to);
    assertThat(view.properties()).isEmpty();
    assertThat(view.history())
        .hasSize(1)
        .first()
        .extracting(ViewHistoryEntry::versionId)
        .isEqualTo(1);
    assertThat(view.schema().asStruct()).isEqualTo(SCHEMA.asStruct());
    assertThat(view.schemas()).hasSize(1).containsKey(SCHEMA.schemaId());
    assertThat(view.versions()).hasSize(1).containsExactly(view.currentVersion());

    assertThat(view.currentVersion())
        .isEqualTo(
            ImmutableViewVersion.builder()
                .timestampMillis(view.currentVersion().timestampMillis())
                .versionId(1)
                .schemaId(SCHEMA.schemaId())
                .putSummary("operation", "create")
                .defaultNamespace(to.namespace())
                .addRepresentations(
                    ImmutableSQLViewRepresentation.builder()
                        .sql("select * from ns.tbl")
                        .dialect("spark")
                        .build())
                .build());

    assertThat(catalog().dropView(to)).isTrue();
    assertThat(catalog().viewExists(to)).as("View should not exist").isFalse();
  }

  @Test
  public void renameViewUsingDifferentNamespace() {
    TableIdentifier from = TableIdentifier.of("ns", "view");
    TableIdentifier to = TableIdentifier.of("other_ns", "renamedView");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(from.namespace());
      catalog().createNamespace(to.namespace());
    }

    catalog()
        .buildView(from)
        .withSchema(SCHEMA)
        .withDefaultNamespace(from.namespace())
        .withQuery("spark", "select * from ns.tbl")
        .create();

    assertThat(catalog().listViews(from.namespace())).containsExactly(from);
    assertThat(catalog().viewExists(from)).as("View should exist").isTrue();

    catalog().renameView(from, to);

    assertThat(catalog().listViews(to.namespace())).containsExactly(to);
    assertThat(catalog().viewExists(from)).as("View should not exist").isFalse();
    assertThat(catalog().viewExists(to)).as("View should exist").isTrue();

    View view = catalog().loadView(to);
    assertThat(view).isNotNull();

    // validate view settings
    assertThat(view.name()).isEqualTo(catalog().name() + "." + to);
    assertThat(view.properties()).isEmpty();
    assertThat(view.history())
        .hasSize(1)
        .first()
        .extracting(ViewHistoryEntry::versionId)
        .isEqualTo(1);
    assertThat(view.schema().asStruct()).isEqualTo(SCHEMA.asStruct());
    assertThat(view.schemas()).hasSize(1).containsKey(SCHEMA.schemaId());
    assertThat(view.versions()).hasSize(1).containsExactly(view.currentVersion());

    assertThat(view.currentVersion())
        .isEqualTo(
            ImmutableViewVersion.builder()
                .timestampMillis(view.currentVersion().timestampMillis())
                .versionId(1)
                .schemaId(SCHEMA.schemaId())
                .putSummary("operation", "create")
                .defaultNamespace(from.namespace())
                .addRepresentations(
                    ImmutableSQLViewRepresentation.builder()
                        .sql("select * from ns.tbl")
                        .dialect("spark")
                        .build())
                .build());

    assertThat(catalog().dropView(from)).isFalse();
    assertThat(catalog().dropView(to)).isTrue();
    assertThat(catalog().viewExists(to)).as("View should not exist").isFalse();
  }

  @Test
  public void renameViewNamespaceMissing() {
    TableIdentifier from = TableIdentifier.of("ns", "view");
    TableIdentifier to = TableIdentifier.of("non_existing", "renamedView");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(from.namespace());
    }

    catalog()
        .buildView(from)
        .withSchema(SCHEMA)
        .withDefaultNamespace(from.namespace())
        .withQuery("spark", "select * from ns.tbl")
        .create();

    assertThat(catalog().viewExists(from)).as("View should exist").isTrue();

    assertThatThrownBy(() -> catalog().renameView(from, to))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContaining("Namespace does not exist: non_existing");
  }

  @Test
  public void renameViewSourceMissing() {
    TableIdentifier from = TableIdentifier.of("ns", "view");
    TableIdentifier to = TableIdentifier.of("ns", "renamedView");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(from.namespace());
    }

    assertThat(catalog().viewExists(from)).as("View should not exist").isFalse();

    assertThatThrownBy(() -> catalog().renameView(from, to))
        .isInstanceOf(NoSuchViewException.class)
        .hasMessageContaining("View does not exist");

    assertThat(catalog().viewExists(from)).as("View should not exist").isFalse();
    assertThat(catalog().viewExists(to)).as("View should not exist").isFalse();
  }

  @Test
  public void renameViewTargetAlreadyExists() {
    TableIdentifier from = TableIdentifier.of("ns", "view");
    TableIdentifier to = TableIdentifier.of("ns", "renamedView");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(from.namespace());
    }

    for (TableIdentifier viewIdentifier : ImmutableList.of(from, to)) {
      catalog()
          .buildView(viewIdentifier)
          .withSchema(SCHEMA)
          .withDefaultNamespace(from.namespace())
          .withQuery("spark", "select * from ns.tbl")
          .create();
    }

    assertThatThrownBy(() -> catalog().renameView(from, to))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("Cannot rename ns.view to ns.renamedView. View already exists");

    // rename view where a table with the same name already exists
    TableIdentifier identifier = TableIdentifier.of("ns", "tbl");
    tableCatalog().buildTable(identifier, SCHEMA).create();

    assertThatThrownBy(() -> catalog().renameView(from, identifier))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("Cannot rename ns.view to ns.tbl. Table already exists");
  }

  @Test
  public void listViews() {
    Namespace ns1 = Namespace.of("ns1");
    Namespace ns2 = Namespace.of("ns2");

    TableIdentifier tableIdentifier = TableIdentifier.of(ns1, "table");
    TableIdentifier view1 = TableIdentifier.of(ns1, "view1");
    TableIdentifier view2 = TableIdentifier.of(ns2, "view2");
    TableIdentifier view3 = TableIdentifier.of(ns2, "view3");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(ns1);
      catalog().createNamespace(ns2);
    }

    if (null != tableCatalog()) {
      tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
      assertThat(tableCatalog().listTables(ns1)).containsExactly(tableIdentifier);
      assertThat(tableCatalog().listTables(ns2)).isEmpty();
    }

    assertThat(catalog().listViews(ns1)).isEmpty();
    assertThat(catalog().listViews(ns2)).isEmpty();

    catalog()
        .buildView(view1)
        .withSchema(SCHEMA)
        .withDefaultNamespace(view1.namespace())
        .withQuery("spark", "select * from ns1.tbl")
        .create();

    assertThat(catalog().listViews(ns1)).containsExactly(view1);
    assertThat(catalog().listViews(ns2)).isEmpty();

    catalog()
        .buildView(view2)
        .withSchema(SCHEMA)
        .withDefaultNamespace(view2.namespace())
        .withQuery("spark", "select * from ns1.tbl")
        .create();

    assertThat(catalog().listViews(ns1)).containsExactly(view1);
    assertThat(catalog().listViews(ns2)).containsExactly(view2);

    catalog()
        .buildView(view3)
        .withSchema(SCHEMA)
        .withDefaultNamespace(view3.namespace())
        .withQuery("spark", "select * from ns.tbl")
        .create();

    assertThat(catalog().listViews(ns1)).containsExactly(view1);
    assertThat(catalog().listViews(ns2)).containsExactlyInAnyOrder(view2, view3);

    if (null != tableCatalog()) {
      assertThat(tableCatalog().listTables(ns1)).containsExactly(tableIdentifier);
      assertThat(tableCatalog().listTables(ns2)).isEmpty();
    }

    assertThat(catalog().dropView(view2)).isTrue();
    assertThat(catalog().listViews(ns1)).containsExactly(view1);
    assertThat(catalog().listViews(ns2)).containsExactly(view3);

    assertThat(catalog().dropView(view3)).isTrue();
    assertThat(catalog().listViews(ns1)).containsExactly(view1);
    assertThat(catalog().listViews(ns2)).isEmpty();

    assertThat(catalog().dropView(view1)).isTrue();
    assertThat(catalog().listViews(ns1)).isEmpty();
    assertThat(catalog().listViews(ns2)).isEmpty();
  }

  @ParameterizedTest(name = ".createOrReplace() = {arguments}")
  @ValueSource(booleans = {false, true})
  public void createOrReplaceView(boolean useCreateOrReplace) {
    TableIdentifier identifier = TableIdentifier.of("ns", "view");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    ViewBuilder viewBuilder =
        catalog()
            .buildView(identifier)
            .withSchema(SCHEMA)
            .withDefaultNamespace(identifier.namespace())
            .withQuery("spark", "select * from ns.tbl")
            .withProperty("prop1", "val1")
            .withProperty("prop2", "val2");
    View view = useCreateOrReplace ? viewBuilder.createOrReplace() : viewBuilder.create();

    assertThat(catalog().viewExists(identifier)).as("View should exist").isTrue();

    // validate view settings
    assertThat(view.name()).isEqualTo(catalog().name() + "." + identifier);
    assertThat(view.properties()).isEqualTo(ImmutableMap.of("prop1", "val1", "prop2", "val2"));
    assertThat(view.history())
        .hasSize(1)
        .first()
        .extracting(ViewHistoryEntry::versionId)
        .isEqualTo(1);
    assertThat(view.schema().schemaId()).isEqualTo(SCHEMA.schemaId());
    assertThat(view.schema().asStruct()).isEqualTo(SCHEMA.asStruct());
    assertThat(view.schemas()).hasSize(1).containsKey(SCHEMA.schemaId());
    assertThat(view.versions()).hasSize(1).containsExactly(view.currentVersion());

    ViewVersion viewVersion = view.currentVersion();
    assertThat(viewVersion).isNotNull();
    assertThat(viewVersion.versionId()).isEqualTo(1);
    assertThat(viewVersion.schemaId()).isEqualTo(SCHEMA.schemaId());
    assertThat(viewVersion.summary()).hasSize(1).containsEntry("operation", "create");
    assertThat(viewVersion.operation()).isEqualTo("create");
    assertThat(viewVersion.defaultNamespace()).isEqualTo(identifier.namespace());
    assertThat(viewVersion.representations())
        .hasSize(1)
        .containsExactly(
            ImmutableSQLViewRepresentation.builder()
                .sql("select * from ns.tbl")
                .dialect("spark")
                .build());

    viewBuilder =
        catalog()
            .buildView(identifier)
            .withSchema(OTHER_SCHEMA)
            .withDefaultNamespace(identifier.namespace())
            .withQuery("trino", "select count(*) from ns.tbl")
            .withProperty("replacedProp1", "val1")
            .withProperty("replacedProp2", "val2");
    View replacedView = useCreateOrReplace ? viewBuilder.createOrReplace() : viewBuilder.replace();

    // validate replaced view settings
    assertThat(replacedView.name()).isEqualTo(catalog().name() + "." + identifier);
    assertThat(replacedView.properties())
        .hasSize(4)
        .containsEntry("prop1", "val1")
        .containsEntry("prop2", "val2")
        .containsEntry("replacedProp1", "val1")
        .containsEntry("replacedProp2", "val2");
    assertThat(replacedView.history())
        .hasSize(2)
        .first()
        .extracting(ViewHistoryEntry::versionId)
        .isEqualTo(1);
    assertThat(replacedView.history())
        .element(1)
        .extracting(ViewHistoryEntry::versionId)
        .isEqualTo(2);

    assertThat(replacedView.schema().schemaId()).isEqualTo(OTHER_SCHEMA.schemaId());
    assertThat(replacedView.schema().asStruct()).isEqualTo(OTHER_SCHEMA.asStruct());
    assertThat(replacedView.schemas())
        .hasSize(2)
        .containsKey(SCHEMA.schemaId())
        .containsKey(OTHER_SCHEMA.schemaId());

    ViewVersion replacedViewVersion = replacedView.currentVersion();
    assertThat(replacedView.versions())
        .hasSize(2)
        .containsExactly(viewVersion, replacedViewVersion);
    assertThat(replacedViewVersion).isNotNull();
    assertThat(replacedViewVersion.versionId()).isEqualTo(2);
    assertThat(replacedViewVersion.schemaId()).isEqualTo(OTHER_SCHEMA.schemaId());
    assertThat(replacedViewVersion.operation()).isEqualTo("replace");
    assertThat(replacedViewVersion.summary()).hasSize(1).containsEntry("operation", "replace");
    assertThat(replacedViewVersion.representations())
        .hasSize(1)
        .containsExactly(
            ImmutableSQLViewRepresentation.builder()
                .sql("select count(*) from ns.tbl")
                .dialect("trino")
                .build());

    assertThat(catalog().dropView(identifier)).isTrue();
    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();
  }

  @Test
  public void updateViewProperties() {
    TableIdentifier identifier = TableIdentifier.of("ns", "view");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();

    View view =
        catalog()
            .buildView(identifier)
            .withSchema(SCHEMA)
            .withDefaultNamespace(identifier.namespace())
            .withQuery("spark", "select * from ns.tbl")
            .create();

    assertThat(view.properties()).isEmpty();
    ViewVersion viewVersion = view.currentVersion();
    assertThat(viewVersion.operation()).isEqualTo("create");
    assertThat(viewVersion.versionId()).isEqualTo(1);
    assertThat(view.history()).hasSize(1);
    assertThat(view.schemas()).hasSize(1).containsKey(SCHEMA.schemaId());
    assertThat(view.versions()).hasSize(1).containsExactly(viewVersion);

    assertThatThrownBy(
            () -> catalog().loadView(identifier).updateProperties().set(null, "new-val1").commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid key: null");

    assertThatThrownBy(
            () -> catalog().loadView(identifier).updateProperties().set("key1", null).commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid value: null");

    assertThatThrownBy(
            () -> catalog().loadView(identifier).updateProperties().remove(null).commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid key: null");

    assertThatThrownBy(
            () ->
                catalog()
                    .loadView(identifier)
                    .updateProperties()
                    .set("key1", "x")
                    .set("key3", "y")
                    .remove("key2")
                    .set("key2", "z")
                    .commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot remove and update the same key: key2");

    view.updateProperties().set("key1", "val1").set("key2", "val2").remove("non-existing").commit();

    View updatedView = catalog().loadView(identifier);
    assertThat(updatedView.properties())
        .hasSize(2)
        .containsEntry("key1", "val1")
        .containsEntry("key2", "val2");
    assertThat(updatedView.history()).hasSize(1).isEqualTo(view.history());
    assertThat(updatedView.schemas()).hasSize(1).containsKey(SCHEMA.schemaId());
    assertThat(updatedView.versions()).hasSize(1).containsExactly(viewVersion);

    // updating properties doesn't change the view version
    ViewVersion updatedViewVersion = updatedView.currentVersion();
    assertThat(updatedViewVersion).isNotNull();
    assertThat(updatedViewVersion.versionId()).isEqualTo(viewVersion.versionId());
    assertThat(updatedViewVersion.summary()).isEqualTo(viewVersion.summary());
    assertThat(updatedViewVersion.operation()).isEqualTo(viewVersion.operation());

    assertThatThrownBy(
            () ->
                catalog()
                    .loadView(identifier)
                    .updateProperties()
                    .set("key1", "new-val1")
                    .set("key3", "val3")
                    .remove("key2")
                    .set("key2", "new-val2")
                    .commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot remove and update the same key: key2");

    view.updateProperties().set("key1", "new-val1").set("key3", "val3").remove("key2").commit();

    View updatedView2 = catalog().loadView(identifier);
    assertThat(updatedView2.properties())
        .hasSize(2)
        .containsEntry("key1", "new-val1")
        .containsEntry("key3", "val3");
    assertThat(updatedView2.history()).hasSize(1).isEqualTo(view.history());
    assertThat(updatedView2.schemas()).hasSize(1).containsKey(SCHEMA.schemaId());
    assertThat(updatedView2.versions()).hasSize(1).containsExactly(viewVersion);

    ViewVersion updatedViewVersion2 = updatedView2.currentVersion();
    assertThat(updatedViewVersion2).isNotNull();
    assertThat(updatedViewVersion2.versionId()).isEqualTo(viewVersion.versionId());
    assertThat(updatedViewVersion2.summary()).isEqualTo(viewVersion.summary());
    assertThat(updatedViewVersion2.operation()).isEqualTo(viewVersion.operation());

    assertThat(catalog().dropView(identifier)).isTrue();
    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();
  }

  @Test
  public void replaceViewVersion() {
    TableIdentifier identifier = TableIdentifier.of("ns", "view");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();

    SQLViewRepresentation spark =
        ImmutableSQLViewRepresentation.builder()
            .dialect("spark")
            .sql("select * from ns.tbl")
            .build();

    SQLViewRepresentation trino =
        ImmutableSQLViewRepresentation.builder()
            .sql("select * from ns.tbl")
            .dialect("trino")
            .build();

    View view =
        catalog()
            .buildView(identifier)
            .withSchema(SCHEMA)
            .withDefaultNamespace(identifier.namespace())
            .withQuery(trino.dialect(), trino.sql())
            .withQuery(spark.dialect(), spark.sql())
            .create();

    ViewVersion viewVersion = view.currentVersion();
    assertThat(view.properties()).isEmpty();
    assertThat(view.history())
        .hasSize(1)
        .first()
        .extracting(ViewHistoryEntry::versionId)
        .isEqualTo(viewVersion.versionId());
    assertThat(view.history())
        .hasSize(1)
        .first()
        .extracting(ViewHistoryEntry::versionId)
        .isEqualTo(view.currentVersion().versionId());
    assertThat(view.schemas()).hasSize(1).containsKey(SCHEMA.schemaId());
    assertThat(viewVersion.operation()).isEqualTo("create");
    assertThat(viewVersion.versionId()).isEqualTo(1);
    assertThat(viewVersion.representations()).hasSize(2).containsExactly(trino, spark);
    assertThat(view.versions()).hasSize(1).containsExactly(viewVersion);

    view.replaceVersion()
        .withSchema(OTHER_SCHEMA)
        .withQuery(trino.dialect(), trino.sql())
        .withDefaultCatalog("default")
        .withDefaultNamespace(identifier.namespace())
        .commit();

    View updatedView = catalog().loadView(identifier);
    assertThat(updatedView.properties()).isEmpty();
    assertThat(updatedView.history())
        .hasSize(2)
        .element(0)
        .extracting(ViewHistoryEntry::versionId)
        .isEqualTo(viewVersion.versionId());
    assertThat(updatedView.history())
        .hasSize(2)
        .element(1)
        .extracting(ViewHistoryEntry::versionId)
        .isEqualTo(updatedView.currentVersion().versionId());
    assertThat(updatedView.schemas())
        .hasSize(2)
        .containsKey(SCHEMA.schemaId())
        .containsKey(OTHER_SCHEMA.schemaId());
    assertThat(updatedView.versions())
        .hasSize(2)
        .containsExactly(viewVersion, updatedView.currentVersion());

    ViewVersion updatedViewVersion = updatedView.currentVersion();
    assertThat(updatedViewVersion).isNotNull();
    assertThat(updatedViewVersion.versionId()).isEqualTo(viewVersion.versionId() + 1);
    assertThat(updatedViewVersion.summary()).hasSize(1).containsEntry("operation", "replace");
    assertThat(updatedViewVersion.operation()).isEqualTo("replace");
    assertThat(updatedViewVersion.representations()).hasSize(1).containsExactly(trino);
    assertThat(updatedViewVersion.schemaId()).isEqualTo(OTHER_SCHEMA.schemaId());
    assertThat(updatedViewVersion.defaultCatalog()).isEqualTo("default");
    assertThat(updatedViewVersion.defaultNamespace()).isEqualTo(identifier.namespace());

    SQLViewRepresentation updatedSpark =
        ImmutableSQLViewRepresentation.builder()
            .sql("select * from ns.updated_tbl")
            .dialect("spark")
            .build();

    view.replaceVersion()
        .withQuery(updatedSpark.dialect(), updatedSpark.sql())
        .withDefaultNamespace(identifier.namespace())
        .withSchema(OTHER_SCHEMA)
        .commit();

    View updatedView2 = catalog().loadView(identifier);
    assertThat(updatedView2.properties()).isEmpty();
    assertThat(updatedView2.history())
        .hasSize(3)
        .element(0)
        .extracting(ViewHistoryEntry::versionId)
        .isEqualTo(viewVersion.versionId());
    assertThat(updatedView2.history())
        .element(1)
        .extracting(ViewHistoryEntry::versionId)
        .isEqualTo(updatedViewVersion.versionId());
    assertThat(updatedView2.history())
        .element(2)
        .extracting(ViewHistoryEntry::versionId)
        .isEqualTo(updatedView2.currentVersion().versionId());
    assertThat(updatedView.schemas())
        .hasSize(2)
        .containsKey(SCHEMA.schemaId())
        .containsKey(OTHER_SCHEMA.schemaId());
    assertThat(updatedView2.versions())
        .hasSize(3)
        .containsExactly(viewVersion, updatedViewVersion, updatedView2.currentVersion());

    ViewVersion updatedViewVersion2 = updatedView2.currentVersion();
    assertThat(updatedViewVersion2).isNotNull();
    assertThat(updatedViewVersion2.versionId()).isEqualTo(updatedViewVersion.versionId() + 1);
    assertThat(updatedViewVersion2.summary()).hasSize(1).containsEntry("operation", "replace");
    assertThat(updatedViewVersion2.operation()).isEqualTo("replace");
    assertThat(updatedViewVersion2.representations()).hasSize(1).containsExactly(updatedSpark);

    assertThat(catalog().dropView(identifier)).isTrue();
    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();
  }

  @Test
  public void replaceViewVersionErrorCases() {
    TableIdentifier identifier = TableIdentifier.of("ns", "view");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();

    SQLViewRepresentation trino =
        ImmutableSQLViewRepresentation.builder()
            .sql("select * from ns.tbl")
            .dialect("trino")
            .build();

    View view =
        catalog()
            .buildView(identifier)
            .withSchema(SCHEMA)
            .withDefaultNamespace(identifier.namespace())
            .withQuery(trino.dialect(), trino.sql())
            .create();

    // empty commits are not allowed
    assertThatThrownBy(() -> view.replaceVersion().commit())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot replace view without specifying a query");

    // schema is required
    assertThatThrownBy(
            () ->
                view.replaceVersion()
                    .withQuery(trino.dialect(), trino.sql())
                    .withDefaultNamespace(identifier.namespace())
                    .commit())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot replace view without specifying schema");
  }
}
