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

import org.apache.iceberg.Schema;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateLocation;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public abstract class ViewCatalogTests<C extends ViewCatalog & SupportsNamespaces> {
  protected static final Schema SCHEMA =
      new Schema(
          5,
          required(3, "id", Types.IntegerType.get(), "unique ID"),
          required(4, "data", Types.StringType.get()));

  private static final Schema OTHER_SCHEMA =
      new Schema(7, required(1, "some_id", Types.IntegerType.get()));

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
    assertThat(view.name()).isEqualTo(ViewUtil.fullViewName(catalog().name(), identifier));
    assertThat(view.history())
        .hasSize(1)
        .first()
        .extracting(ViewHistoryEntry::versionId)
        .isEqualTo(1);
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
            .withDefaultCatalog(catalog().name())
            .withQuery("spark", "select * from ns.tbl")
            .withQuery("trino", "select * from ns.tbl using X")
            .withProperty("prop1", "val1")
            .withProperty("prop2", "val2")
            .withLocation("file://tmp/ns/view")
            .create();

    assertThat(view).isNotNull();
    assertThat(catalog().viewExists(identifier)).as("View should exist").isTrue();

    // validate view settings
    assertThat(view.name()).isEqualTo(ViewUtil.fullViewName(catalog().name(), identifier));
    assertThat(view.location()).isEqualTo("file://tmp/ns/view");
    assertThat(view.properties()).containsEntry("prop1", "val1").containsEntry("prop2", "val2");
    assertThat(view.history())
        .hasSize(1)
        .first()
        .extracting(ViewHistoryEntry::versionId)
        .isEqualTo(1);
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
                .putSummary("operation", "create")
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

  @Test
  public void createViewErrorCases() {
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

    // query is required
    assertThatThrownBy(() -> catalog().buildView(identifier).create())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot create view without specifying a query");

    // schema is required
    assertThatThrownBy(
            () -> catalog().buildView(identifier).withQuery(trino.dialect(), trino.sql()).create())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot create view without specifying schema");

    // default namespace is required
    assertThatThrownBy(
            () ->
                catalog()
                    .buildView(identifier)
                    .withQuery(trino.dialect(), trino.sql())
                    .withSchema(SCHEMA)
                    .create())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot create view without specifying a default namespace");

    // cannot define multiple SQLs for same dialect
    assertThatThrownBy(
            () ->
                catalog()
                    .buildView(identifier)
                    .withSchema(SCHEMA)
                    .withDefaultNamespace(identifier.namespace())
                    .withQuery(trino.dialect(), trino.sql())
                    .withQuery(trino.dialect(), trino.sql())
                    .create())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid view version: Cannot add multiple queries for dialect trino");
  }

  @Test
  public void createViewThatAlreadyExists() {
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

    assertThatThrownBy(
            () ->
                catalog()
                    .buildView(identifier)
                    .withSchema(OTHER_SCHEMA)
                    .withQuery("spark", "select * from ns.tbl")
                    .withDefaultNamespace(identifier.namespace())
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

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should not exist").isFalse();

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();

    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

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

    TableIdentifier viewIdentifier = TableIdentifier.of("ns", "view");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(viewIdentifier.namespace());
    }

    assertThat(catalog().viewExists(viewIdentifier)).as("View should not exist").isFalse();

    catalog()
        .buildView(viewIdentifier)
        .withSchema(SCHEMA)
        .withDefaultNamespace(viewIdentifier.namespace())
        .withQuery("spark", "select * from ns.tbl")
        .create();

    assertThat(catalog().viewExists(viewIdentifier)).as("View should exist").isTrue();

    assertThatThrownBy(() -> tableCatalog().buildTable(viewIdentifier, SCHEMA).create())
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageStartingWith("View with same name already exists: ns.view");
  }

  @Test
  public void createTableViaTransactionThatAlreadyExistsAsView() {
    Assumptions.assumeThat(tableCatalog())
        .as("Only valid for catalogs that support tables")
        .isNotNull();

    TableIdentifier viewIdentifier = TableIdentifier.of("ns", "view");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(viewIdentifier.namespace());
    }

    assertThat(catalog().viewExists(viewIdentifier)).as("View should not exist").isFalse();

    Transaction transaction = tableCatalog().buildTable(viewIdentifier, SCHEMA).createTransaction();

    catalog()
        .buildView(viewIdentifier)
        .withSchema(SCHEMA)
        .withDefaultNamespace(viewIdentifier.namespace())
        .withQuery("spark", "select * from ns.tbl")
        .create();

    assertThat(catalog().viewExists(viewIdentifier)).as("View should exist").isTrue();

    assertThatThrownBy(transaction::commitTransaction)
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageStartingWith("View with same name already exists: ns.view");
  }

  @Test
  public void replaceTableViaTransactionThatAlreadyExistsAsView() {
    Assumptions.assumeThat(tableCatalog())
        .as("Only valid for catalogs that support tables")
        .isNotNull();

    TableIdentifier viewIdentifier = TableIdentifier.of("ns", "view");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(viewIdentifier.namespace());
    }

    assertThat(catalog().viewExists(viewIdentifier)).as("View should not exist").isFalse();

    catalog()
        .buildView(viewIdentifier)
        .withSchema(SCHEMA)
        .withDefaultNamespace(viewIdentifier.namespace())
        .withQuery("spark", "select * from ns.tbl")
        .create();

    assertThat(catalog().viewExists(viewIdentifier)).as("View should exist").isTrue();

    // replace transaction requires table existence
    // TODO: replace should check whether the table exists as a view
    assertThatThrownBy(
            () ->
                tableCatalog()
                    .buildTable(viewIdentifier, SCHEMA)
                    .replaceTransaction()
                    .commitTransaction())
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageStartingWith("Table does not exist: ns.view");
  }

  @Test
  public void createOrReplaceTableViaTransactionThatAlreadyExistsAsView() {
    Assumptions.assumeThat(tableCatalog())
        .as("Only valid for catalogs that support tables")
        .isNotNull();

    TableIdentifier viewIdentifier = TableIdentifier.of("ns", "view");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(viewIdentifier.namespace());
    }

    assertThat(catalog().viewExists(viewIdentifier)).as("View should not exist").isFalse();

    catalog()
        .buildView(viewIdentifier)
        .withSchema(SCHEMA)
        .withDefaultNamespace(viewIdentifier.namespace())
        .withQuery("spark", "select * from ns.tbl")
        .create();

    assertThat(catalog().viewExists(viewIdentifier)).as("View should exist").isTrue();

    assertThatThrownBy(
            () ->
                tableCatalog()
                    .buildTable(viewIdentifier, SCHEMA)
                    .createOrReplaceTransaction()
                    .commitTransaction())
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageStartingWith("View with same name already exists: ns.view");
  }

  @Test
  public void replaceViewThatAlreadyExistsAsTable() {
    Assumptions.assumeThat(tableCatalog())
        .as("Only valid for catalogs that support tables")
        .isNotNull();

    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should not exist").isFalse();

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();

    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    // replace view requires the view to exist
    // TODO: replace should check whether the view exists as a table
    assertThatThrownBy(
            () ->
                catalog()
                    .buildView(tableIdentifier)
                    .withSchema(OTHER_SCHEMA)
                    .withDefaultNamespace(tableIdentifier.namespace())
                    .withQuery("spark", "select * from ns.tbl")
                    .replace())
        .isInstanceOf(NoSuchViewException.class)
        .hasMessageStartingWith("View does not exist: ns.table");
  }

  @Test
  public void createOrReplaceViewThatAlreadyExistsAsTable() {
    Assumptions.assumeThat(tableCatalog())
        .as("Only valid for catalogs that support tables")
        .isNotNull();

    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should not exist").isFalse();

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();

    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    assertThatThrownBy(
            () ->
                catalog()
                    .buildView(tableIdentifier)
                    .withSchema(OTHER_SCHEMA)
                    .withDefaultNamespace(tableIdentifier.namespace())
                    .withQuery("spark", "select * from ns.tbl")
                    .createOrReplace())
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageStartingWith("Table with same name already exists: ns.table");
  }

  @Test
  public void renameView() {
    TableIdentifier from = TableIdentifier.of("ns", "view");
    TableIdentifier to = TableIdentifier.of("ns", "renamedView");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(from.namespace());
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
  public void renameViewUsingDifferentNamespace() {
    TableIdentifier from = TableIdentifier.of("ns", "view");
    TableIdentifier to = TableIdentifier.of("other_ns", "renamedView");

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
  public void renameViewNamespaceMissing() {
    TableIdentifier from = TableIdentifier.of("ns", "view");
    TableIdentifier to = TableIdentifier.of("non_existing", "renamedView");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(from.namespace());
    }

    assertThat(catalog().viewExists(from)).as("View should not exist").isFalse();

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
    TableIdentifier from = TableIdentifier.of("ns", "non_existing");
    TableIdentifier to = TableIdentifier.of("ns", "renamedView");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(from.namespace());
    }

    assertThat(catalog().viewExists(from)).as("View should not exist").isFalse();

    assertThatThrownBy(() -> catalog().renameView(from, to))
        .isInstanceOf(NoSuchViewException.class)
        .hasMessageContaining("View does not exist");
  }

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
        .hasMessageContaining("Cannot rename ns.viewOne to ns.viewTwo. View already exists");
  }

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
        .hasMessageContaining("Cannot rename ns.view to ns.table. Table already exists");
  }

  @Test
  public void listViews() {
    Namespace ns1 = Namespace.of("ns1");
    Namespace ns2 = Namespace.of("ns2");

    TableIdentifier view1 = TableIdentifier.of(ns1, "view1");
    TableIdentifier view2 = TableIdentifier.of(ns2, "view2");
    TableIdentifier view3 = TableIdentifier.of(ns2, "view3");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(ns1);
      catalog().createNamespace(ns2);
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

  @Test
  public void listViewsAndTables() {
    Assumptions.assumeThat(tableCatalog())
        .as("Only valid for catalogs that support tables")
        .isNotNull();

    Namespace ns = Namespace.of("ns");

    TableIdentifier tableIdentifier = TableIdentifier.of(ns, "table");
    TableIdentifier viewIdentifier = TableIdentifier.of(ns, "view");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(ns);
    }

    assertThat(catalog().listViews(ns)).isEmpty();
    assertThat(tableCatalog().listTables(ns)).isEmpty();

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
    assertThat(catalog().listViews(ns)).isEmpty();
    assertThat(tableCatalog().listTables(ns)).containsExactly(tableIdentifier);

    catalog()
        .buildView(viewIdentifier)
        .withSchema(SCHEMA)
        .withDefaultNamespace(viewIdentifier.namespace())
        .withQuery("spark", "select * from ns1.tbl")
        .create();

    assertThat(catalog().listViews(ns)).containsExactly(viewIdentifier);
    assertThat(tableCatalog().listTables(ns)).containsExactly(tableIdentifier);

    assertThat(tableCatalog().dropTable(tableIdentifier)).isTrue();
    assertThat(catalog().listViews(ns)).containsExactly(viewIdentifier);
    assertThat(tableCatalog().listTables(ns)).isEmpty();

    assertThat(catalog().dropView(viewIdentifier)).isTrue();
    assertThat(catalog().listViews(ns)).isEmpty();
    assertThat(tableCatalog().listTables(ns)).isEmpty();
  }

  @ParameterizedTest(name = ".createOrReplace() = {arguments}")
  @ValueSource(booleans = {false, true})
  public void createOrReplaceView(boolean useCreateOrReplace) {
    TableIdentifier identifier = TableIdentifier.of("ns", "view");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();

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

    ViewVersion viewVersion = view.currentVersion();
    assertThat(viewVersion.representations())
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
    assertThat(replacedView.name()).isEqualTo(ViewUtil.fullViewName(catalog().name(), identifier));
    assertThat(replacedView.properties())
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

    assertThat(replacedView.schema().schemaId()).isEqualTo(1);
    assertThat(replacedView.schema().asStruct()).isEqualTo(OTHER_SCHEMA.asStruct());
    assertThat(replacedView.schemas()).hasSize(2).containsKey(0).containsKey(1);

    ViewVersion replacedViewVersion = replacedView.currentVersion();
    assertThat(replacedView.versions())
        .hasSize(2)
        .containsExactly(viewVersion, replacedViewVersion);
    assertThat(replacedViewVersion).isNotNull();
    assertThat(replacedViewVersion.versionId()).isEqualTo(2);
    assertThat(replacedViewVersion.schemaId()).isEqualTo(1);
    assertThat(replacedViewVersion.operation()).isEqualTo("replace");
    assertThat(replacedViewVersion.summary()).hasSize(1).containsEntry("operation", "replace");
    assertThat(replacedViewVersion.representations())
        .containsExactly(
            ImmutableSQLViewRepresentation.builder()
                .sql("select count(*) from ns.tbl")
                .dialect("trino")
                .build());

    assertThat(catalog().dropView(identifier)).isTrue();
    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();
  }

  @Test
  public void replaceViewErrorCases() {
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

    catalog()
        .buildView(identifier)
        .withSchema(SCHEMA)
        .withDefaultNamespace(identifier.namespace())
        .withQuery(trino.dialect(), trino.sql())
        .create();

    assertThat(catalog().viewExists(identifier)).as("View should exist").isTrue();

    // query is required
    assertThatThrownBy(() -> catalog().buildView(identifier).replace())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot replace view without specifying a query");

    // schema is required
    assertThatThrownBy(
            () -> catalog().buildView(identifier).withQuery(trino.dialect(), trino.sql()).replace())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot replace view without specifying schema");

    // default namespace is required
    assertThatThrownBy(
            () ->
                catalog()
                    .buildView(identifier)
                    .withQuery(trino.dialect(), trino.sql())
                    .withSchema(SCHEMA)
                    .replace())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot replace view without specifying a default namespace");

    // cannot replace non-existing view
    assertThatThrownBy(
            () ->
                catalog()
                    .buildView(TableIdentifier.of("ns", "non_existing"))
                    .withQuery(trino.dialect(), trino.sql())
                    .withSchema(SCHEMA)
                    .withDefaultNamespace(identifier.namespace())
                    .replace())
        .isInstanceOf(NoSuchViewException.class)
        .hasMessageStartingWith("View does not exist: ns.non_existing");

    // cannot define multiple SQLs for same dialect
    assertThatThrownBy(
            () ->
                catalog()
                    .buildView(identifier)
                    .withSchema(SCHEMA)
                    .withDefaultNamespace(identifier.namespace())
                    .withQuery(trino.dialect(), trino.sql())
                    .withQuery(trino.dialect(), trino.sql())
                    .replace())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid view version: Cannot add multiple queries for dialect trino");
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

    ViewVersion viewVersion = view.currentVersion();

    view.updateProperties().set("key1", "val1").set("key2", "val2").remove("non-existing").commit();

    View updatedView = catalog().loadView(identifier);
    assertThat(updatedView.properties())
        .containsEntry("key1", "val1")
        .containsEntry("key2", "val2");

    // history and view versions should stay the same after updating view properties
    assertThat(updatedView.history()).hasSize(1).isEqualTo(view.history());
    assertThat(updatedView.versions()).hasSize(1).containsExactly(viewVersion);

    assertThat(catalog().dropView(identifier)).isTrue();
    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();
  }

  @Test
  public void updateViewPropertiesErrorCases() {
    TableIdentifier identifier = TableIdentifier.of("ns", "view");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();

    catalog()
        .buildView(identifier)
        .withSchema(SCHEMA)
        .withDefaultNamespace(identifier.namespace())
        .withQuery("spark", "select * from ns.tbl")
        .create();

    assertThat(catalog().viewExists(identifier)).as("View should exist").isTrue();

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

    assertThat(catalog().viewExists(identifier)).as("View should exist").isTrue();

    ViewVersion viewVersion = view.currentVersion();
    assertThat(viewVersion.representations()).hasSize(2).containsExactly(trino, spark);

    // uses a different schema and view representation
    view.replaceVersion()
        .withSchema(OTHER_SCHEMA)
        .withQuery(trino.dialect(), trino.sql())
        .withDefaultCatalog("default")
        .withDefaultNamespace(identifier.namespace())
        .commit();

    // history and view versions should reflect the changes
    View updatedView = catalog().loadView(identifier);
    assertThat(updatedView.history())
        .hasSize(2)
        .element(0)
        .extracting(ViewHistoryEntry::versionId)
        .isEqualTo(viewVersion.versionId());
    assertThat(updatedView.history())
        .element(1)
        .extracting(ViewHistoryEntry::versionId)
        .isEqualTo(updatedView.currentVersion().versionId());
    assertThat(updatedView.schemas()).hasSize(2).containsKey(0).containsKey(1);
    assertThat(updatedView.versions())
        .hasSize(2)
        .containsExactly(viewVersion, updatedView.currentVersion());

    ViewVersion updatedViewVersion = updatedView.currentVersion();
    assertThat(updatedViewVersion).isNotNull();
    assertThat(updatedViewVersion.versionId()).isEqualTo(viewVersion.versionId() + 1);
    assertThat(updatedViewVersion.summary()).hasSize(1).containsEntry("operation", "replace");
    assertThat(updatedViewVersion.operation()).isEqualTo("replace");
    assertThat(updatedViewVersion.representations()).hasSize(1).containsExactly(trino);
    assertThat(updatedViewVersion.schemaId()).isEqualTo(1);
    assertThat(updatedViewVersion.defaultCatalog()).isEqualTo("default");
    assertThat(updatedViewVersion.defaultNamespace()).isEqualTo(identifier.namespace());

    assertThat(catalog().dropView(identifier)).isTrue();
    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();
  }

  @Test
  public void replaceViewVersionByUpdatingSQLForDialect() {
    TableIdentifier identifier = TableIdentifier.of("ns", "view");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();

    SQLViewRepresentation spark =
        ImmutableSQLViewRepresentation.builder()
            .sql("select * from ns.tbl")
            .dialect("spark")
            .build();

    View view =
        catalog()
            .buildView(identifier)
            .withSchema(SCHEMA)
            .withDefaultNamespace(identifier.namespace())
            .withQuery(spark.dialect(), spark.sql())
            .create();

    assertThat(catalog().viewExists(identifier)).as("View should exist").isTrue();

    ViewVersion viewVersion = view.currentVersion();
    assertThat(viewVersion.representations()).hasSize(1).containsExactly(spark);

    SQLViewRepresentation updatedSpark =
        ImmutableSQLViewRepresentation.builder()
            .sql("select * from ns.updated_tbl")
            .dialect("spark")
            .build();

    // only update the SQL for spark
    view.replaceVersion()
        .withSchema(SCHEMA)
        .withDefaultNamespace(identifier.namespace())
        .withQuery(updatedSpark.dialect(), updatedSpark.sql())
        .commit();

    // history and view versions should reflect the changes
    View updatedView = catalog().loadView(identifier);
    assertThat(updatedView.history())
        .hasSize(2)
        .element(0)
        .extracting(ViewHistoryEntry::versionId)
        .isEqualTo(viewVersion.versionId());
    assertThat(updatedView.history())
        .element(1)
        .extracting(ViewHistoryEntry::versionId)
        .isEqualTo(updatedView.currentVersion().versionId());
    assertThat(updatedView.versions())
        .hasSize(2)
        .containsExactly(viewVersion, updatedView.currentVersion());

    // updated view should have the new SQL
    assertThat(updatedView.currentVersion().representations())
        .hasSize(1)
        .containsExactly(updatedSpark);

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

    assertThat(catalog().viewExists(identifier)).as("View should exist").isTrue();

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

    // default namespace is required
    assertThatThrownBy(
            () ->
                view.replaceVersion()
                    .withQuery(trino.dialect(), trino.sql())
                    .withSchema(SCHEMA)
                    .commit())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot replace view without specifying a default namespace");

    // cannot define multiple SQLs for same dialect
    assertThatThrownBy(
            () ->
                view.replaceVersion()
                    .withQuery(trino.dialect(), trino.sql())
                    .withSchema(SCHEMA)
                    .withDefaultNamespace(identifier.namespace())
                    .withQuery(trino.dialect(), trino.sql())
                    .withQuery(trino.dialect(), trino.sql())
                    .commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid view version: Cannot add multiple queries for dialect trino");
  }

  @Test
  public void updateViewPropertiesConflict() {
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
            .withQuery("trino", "select * from ns.tbl")
            .create();

    assertThat(catalog().viewExists(identifier)).as("View should exist").isTrue();
    UpdateViewProperties updateViewProperties = view.updateProperties();

    // drop view and then try to use the updateProperties API
    catalog().dropView(identifier);
    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();

    assertThatThrownBy(() -> updateViewProperties.set("key1", "val1").commit())
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("Cannot commit");
  }

  @Test
  public void replaceViewVersionConflict() {
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
            .withQuery("trino", "select * from ns.tbl")
            .create();

    assertThat(catalog().viewExists(identifier)).as("View should exist").isTrue();
    ReplaceViewVersion replaceViewVersion = view.replaceVersion();

    // drop view and then try to use the replaceVersion API
    catalog().dropView(identifier);
    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();

    assertThatThrownBy(
            () ->
                replaceViewVersion
                    .withQuery("trino", "select * from ns.tbl")
                    .withSchema(SCHEMA)
                    .withDefaultNamespace(identifier.namespace())
                    .commit())
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("Cannot commit");
  }

  @Test
  public void createViewConflict() {
    TableIdentifier identifier = TableIdentifier.of("ns", "view");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();
    ViewBuilder viewBuilder = catalog().buildView(identifier);

    catalog()
        .buildView(identifier)
        .withSchema(SCHEMA)
        .withDefaultNamespace(identifier.namespace())
        .withQuery("trino", "select * from ns.tbl")
        .create();

    assertThat(catalog().viewExists(identifier)).as("View should exist").isTrue();

    // the view was already created concurrently
    assertThatThrownBy(
            () ->
                viewBuilder
                    .withQuery("trino", "select * from ns.tbl")
                    .withSchema(SCHEMA)
                    .withDefaultNamespace(identifier.namespace())
                    .create())
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("View already exists: ns.view");
  }

  @Test
  public void replaceViewConflict() {
    TableIdentifier identifier = TableIdentifier.of("ns", "view");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();

    catalog()
        .buildView(identifier)
        .withSchema(SCHEMA)
        .withDefaultNamespace(identifier.namespace())
        .withQuery("trino", "select * from ns.tbl")
        .create();

    assertThat(catalog().viewExists(identifier)).as("View should exist").isTrue();
    ViewBuilder viewBuilder = catalog().buildView(identifier);

    catalog().dropView(identifier);
    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();

    // the view was already dropped concurrently
    assertThatThrownBy(
            () ->
                viewBuilder
                    .withQuery("trino", "select * from ns.tbl")
                    .withSchema(SCHEMA)
                    .withDefaultNamespace(identifier.namespace())
                    .replace())
        .isInstanceOf(NoSuchViewException.class)
        .hasMessageStartingWith("View does not exist: ns.view");
  }

  @Test
  public void createAndReplaceViewWithLocation() {
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
            .withQuery("trino", "select * from ns.tbl")
            .withLocation("file://tmp/ns/view")
            .create();

    assertThat(catalog().viewExists(identifier)).as("View should exist").isTrue();
    assertThat(view.location()).isEqualTo("file://tmp/ns/view");

    view =
        catalog()
            .buildView(identifier)
            .withSchema(SCHEMA)
            .withDefaultNamespace(identifier.namespace())
            .withQuery("trino", "select * from ns.tbl")
            .withLocation("file://updated_tmp/ns/view")
            .replace();

    assertThat(view.location()).isEqualTo("file://updated_tmp/ns/view");

    assertThat(catalog().dropView(identifier)).isTrue();
    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();
  }

  @Test
  public void updateViewLocation() {
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
            .withQuery("trino", "select * from ns.tbl")
            .withLocation("file://tmp/ns/view")
            .create();

    assertThat(catalog().viewExists(identifier)).as("View should exist").isTrue();
    assertThat(view.location()).isEqualTo("file://tmp/ns/view");

    view.updateLocation().setLocation("file://updated_tmp/ns/view").commit();

    View updatedView = catalog().loadView(identifier);

    assertThat(updatedView.location()).isEqualTo("file://updated_tmp/ns/view");

    // history and view versions should stay the same after updating view properties
    assertThat(updatedView.history()).hasSize(1).isEqualTo(view.history());
    assertThat(updatedView.versions()).hasSize(1).containsExactly(view.currentVersion());

    assertThat(catalog().dropView(identifier)).isTrue();
    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();
  }

  @Test
  public void updateViewLocationConflict() {
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
            .withQuery("trino", "select * from ns.tbl")
            .create();

    assertThat(catalog().viewExists(identifier)).as("View should exist").isTrue();

    // new location must be non-null
    assertThatThrownBy(() -> view.updateLocation().setLocation(null).commit())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Invalid view location: null");

    UpdateLocation updateViewLocation = view.updateLocation();

    catalog().dropView(identifier);
    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();

    // the view was already dropped concurrently
    assertThatThrownBy(() -> updateViewLocation.setLocation("new-location").commit())
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("Cannot commit");
  }
}
