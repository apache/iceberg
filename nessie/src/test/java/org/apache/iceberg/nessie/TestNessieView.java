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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.View;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.ext.NessieClientFactory;
import org.projectnessie.client.ext.NessieClientUri;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.ImmutableTableReference;
import org.projectnessie.model.LogResponse.LogEntry;

public class TestNessieView extends BaseTestIceberg {

  private static final String BRANCH = "iceberg-view-test";

  private static final String DB_NAME = "db";
  private static final String VIEW_NAME = "view";
  private static final TableIdentifier VIEW_IDENTIFIER = TableIdentifier.of(DB_NAME, VIEW_NAME);
  private static final ContentKey KEY = ContentKey.of(DB_NAME, VIEW_NAME);
  private static final Schema SCHEMA =
      new Schema(Types.StructType.of(required(1, "id", Types.LongType.get())).fields());
  private static final Schema ALTERED =
      new Schema(
          Types.StructType.of(
                  required(1, "id", Types.LongType.get()),
                  optional(2, "data", Types.LongType.get()))
              .fields());

  private String viewLocation;

  public TestNessieView() {
    super(BRANCH);
  }

  @Override
  @BeforeEach
  public void beforeEach(NessieClientFactory clientFactory, @NessieClientUri URI nessieUri)
      throws IOException {
    super.beforeEach(clientFactory, nessieUri);
    this.viewLocation =
        createView(catalog, VIEW_IDENTIFIER, SCHEMA).location().replaceFirst("file:", "");
  }

  @Override
  @AfterEach
  public void afterEach() throws Exception {
    // drop the view data
    if (viewLocation != null) {
      try (Stream<Path> walk = Files.walk(Paths.get(viewLocation))) {
        walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
      }
      catalog.dropView(VIEW_IDENTIFIER);
    }

    super.afterEach();
  }

  private IcebergView getView(ContentKey key) throws NessieNotFoundException {
    return getView(BRANCH, key);
  }

  private IcebergView getView(String ref, ContentKey key) throws NessieNotFoundException {
    return api.getContent().key(key).refName(ref).get().get(key).unwrap(IcebergView.class).get();
  }

  /** Verify that Nessie always returns the globally-current global-content w/ only DMLs. */
  @Test
  public void verifyStateMovesForDML() throws Exception {
    //  1. initialize view
    View icebergView = catalog.loadView(VIEW_IDENTIFIER);
    icebergView
        .replaceVersion()
        .withQuery("spark", "some query")
        .withSchema(SCHEMA)
        .withDefaultNamespace(VIEW_IDENTIFIER.namespace())
        .commit();

    //  2. create 2nd branch
    String testCaseBranch = "verify-global-moving";
    api.createReference()
        .sourceRefName(BRANCH)
        .reference(Branch.of(testCaseBranch, catalog.currentHash()))
        .create();
    IcebergView contentInitialMain = getView(BRANCH, KEY);
    IcebergView contentInitialBranch = getView(testCaseBranch, KEY);
    View viewInitialMain = catalog.loadView(VIEW_IDENTIFIER);

    // verify view-metadata-location + version-id
    Assertions.assertThat(contentInitialMain)
        .as("global-contents + snapshot-id equal on both branches in Nessie")
        .isEqualTo(contentInitialBranch);
    Assertions.assertThat(viewInitialMain.currentVersion()).isNotNull();

    //  3. modify view in "main" branch

    icebergView
        .replaceVersion()
        .withQuery("trino", "some other query")
        .withSchema(SCHEMA)
        .withDefaultNamespace(VIEW_IDENTIFIER.namespace())
        .commit();

    IcebergView contentsAfter1Main = getView(KEY);
    IcebergView contentsAfter1Branch = getView(testCaseBranch, KEY);
    View viewAfter1Main = catalog.loadView(VIEW_IDENTIFIER);

    //  --> assert getValue() against both branches returns the updated metadata-location
    // verify view-metadata-location
    Assertions.assertThat(contentInitialMain.getMetadataLocation())
        .describedAs("metadata-location must change on %s", BRANCH)
        .isNotEqualTo(contentsAfter1Main.getMetadataLocation());
    Assertions.assertThat(contentInitialBranch.getMetadataLocation())
        .describedAs("metadata-location must not change on %s", testCaseBranch)
        .isEqualTo(contentsAfter1Branch.getMetadataLocation());
    Assertions.assertThat(contentsAfter1Main)
        .extracting(IcebergView::getSchemaId)
        .describedAs("schema ID must be same across branches")
        .isEqualTo(contentsAfter1Branch.getSchemaId());
    // verify updates
    Assertions.assertThat(
            ((SQLViewRepresentation) viewAfter1Main.currentVersion().representations().get(0))
                .dialect())
        .isEqualTo("trino");

    //  4. modify view in "main" branch again

    icebergView
        .replaceVersion()
        .withQuery("flink", "some query")
        .withSchema(SCHEMA)
        .withDefaultNamespace(VIEW_IDENTIFIER.namespace())
        .commit();

    IcebergView contentsAfter2Main = getView(KEY);
    IcebergView contentsAfter2Branch = getView(testCaseBranch, KEY);
    View viewAfter2Main = catalog.loadView(VIEW_IDENTIFIER);

    //  --> assert getValue() against both branches returns the updated metadata-location
    // verify view-metadata-location
    Assertions.assertThat(contentsAfter2Main.getMetadataLocation())
        .describedAs("metadata-location must change on %s", BRANCH)
        .isNotEqualTo(contentsAfter1Main.getMetadataLocation());
    Assertions.assertThat(contentsAfter2Branch.getMetadataLocation())
        .describedAs("on-reference-state must not change on %s", testCaseBranch)
        .isEqualTo(contentsAfter1Branch.getMetadataLocation());
    Assertions.assertThat(
            ((SQLViewRepresentation) viewAfter2Main.currentVersion().representations().get(0))
                .dialect())
        .isEqualTo("flink");
  }

  @Test
  public void testUpdate() throws IOException {
    String viewName = VIEW_IDENTIFIER.name();
    View icebergView = catalog.loadView(VIEW_IDENTIFIER);
    // add a column
    icebergView
        .replaceVersion()
        .withQuery("spark", "some query")
        .withSchema(ALTERED)
        .withDefaultNamespace(VIEW_IDENTIFIER.namespace())
        .commit();

    getView(KEY); // sanity, check view exists
    // check parameters are in expected state
    String expected = temp.toUri() + DB_NAME + "/" + viewName;
    Assertions.assertThat(getViewBasePath(viewName)).isEqualTo(expected);

    Assertions.assertThat(metadataVersionFiles(viewLocation)).isNotNull().hasSize(2);

    verifyCommitMetadata();
  }

  @Test
  public void testRenameWithTableReference() throws NessieNotFoundException {
    String renamedViewName = "rename_view_name";
    TableIdentifier renameViewIdentifier =
        TableIdentifier.of(VIEW_IDENTIFIER.namespace(), renamedViewName);

    ImmutableTableReference fromTableReference =
        ImmutableTableReference.builder()
            .reference(catalog.currentRefName())
            .name(VIEW_IDENTIFIER.name())
            .build();
    ImmutableTableReference toTableReference =
        ImmutableTableReference.builder()
            .reference(catalog.currentRefName())
            .name(renameViewIdentifier.name())
            .build();
    TableIdentifier fromIdentifier =
        TableIdentifier.of(VIEW_IDENTIFIER.namespace(), fromTableReference.toString());
    TableIdentifier toIdentifier =
        TableIdentifier.of(VIEW_IDENTIFIER.namespace(), toTableReference.toString());

    catalog.renameView(fromIdentifier, toIdentifier);
    Assertions.assertThat(catalog.viewExists(fromIdentifier)).isFalse();
    Assertions.assertThat(catalog.viewExists(toIdentifier)).isTrue();

    Assertions.assertThat(catalog.dropView(toIdentifier)).isTrue();

    verifyCommitMetadata();
  }

  @Test
  public void testRenameWithTableReferenceInvalidCase() {
    String renamedViewName = "rename_view_name";
    TableIdentifier renameViewIdentifier =
        TableIdentifier.of(VIEW_IDENTIFIER.namespace(), renamedViewName);

    ImmutableTableReference fromTableReference =
        ImmutableTableReference.builder()
            .reference("Something")
            .name(VIEW_IDENTIFIER.name())
            .build();
    ImmutableTableReference toTableReference =
        ImmutableTableReference.builder()
            .reference(catalog.currentRefName())
            .name(renameViewIdentifier.name())
            .build();
    TableIdentifier fromIdentifier =
        TableIdentifier.of(VIEW_IDENTIFIER.namespace(), fromTableReference.toString());
    TableIdentifier toIdentifier =
        TableIdentifier.of(VIEW_IDENTIFIER.namespace(), toTableReference.toString());

    Assertions.assertThatThrownBy(() -> catalog.renameView(fromIdentifier, toIdentifier))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot rename view 'view' on reference 'Something' to 'rename_view_name' on reference 'iceberg-view-test': source and target references must be the same.");

    fromTableReference =
        ImmutableTableReference.builder()
            .reference(catalog.currentRefName())
            .name(VIEW_IDENTIFIER.name())
            .build();
    toTableReference =
        ImmutableTableReference.builder()
            .reference("Something")
            .name(renameViewIdentifier.name())
            .build();
    TableIdentifier fromIdentifierNew =
        TableIdentifier.of(VIEW_IDENTIFIER.namespace(), fromTableReference.toString());
    TableIdentifier toIdentifierNew =
        TableIdentifier.of(VIEW_IDENTIFIER.namespace(), toTableReference.toString());

    Assertions.assertThatThrownBy(() -> catalog.renameView(fromIdentifierNew, toIdentifierNew))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot rename view 'view' on reference 'iceberg-view-test' to 'rename_view_name' on reference 'Something': source and target references must be the same.");
  }

  private void verifyCommitMetadata() throws NessieNotFoundException {
    // check that the author is properly set
    List<LogEntry> log = api.getCommitLog().refName(BRANCH).get().getLogEntries();
    Assertions.assertThat(log)
        .isNotNull()
        .isNotEmpty()
        .filteredOn(e -> !e.getCommitMeta().getMessage().startsWith("create namespace "))
        .allSatisfy(
            logEntry -> {
              CommitMeta commit = logEntry.getCommitMeta();
              Assertions.assertThat(commit.getAuthor()).isNotNull().isNotEmpty();
              Assertions.assertThat(commit.getAuthor()).isEqualTo(System.getProperty("user.name"));
              Assertions.assertThat(commit.getProperties().get(NessieUtil.APPLICATION_TYPE))
                  .isEqualTo("iceberg");
              Assertions.assertThat(commit.getMessage()).startsWith("Iceberg");
            });
  }

  @Test
  public void testDrop() throws NessieNotFoundException {
    Assertions.assertThat(catalog.viewExists(VIEW_IDENTIFIER)).isTrue();
    Assertions.assertThat(catalog.dropView(VIEW_IDENTIFIER)).isTrue();
    Assertions.assertThat(catalog.viewExists(VIEW_IDENTIFIER)).isFalse();
    Assertions.assertThat(catalog.dropView(VIEW_IDENTIFIER)).isFalse();
    verifyCommitMetadata();
  }

  @Test
  public void testListviews() {
    TableIdentifier newIdentifier = TableIdentifier.of(DB_NAME, "newView");
    createView(catalog, newIdentifier, SCHEMA);

    List<TableIdentifier> tableIdents = catalog.listViews(VIEW_IDENTIFIER.namespace());
    List<TableIdentifier> expectedIdents =
        tableIdents.stream()
            .filter(t -> t.equals(newIdentifier) || t.equals(VIEW_IDENTIFIER))
            .collect(Collectors.toList());
    Assertions.assertThat(expectedIdents).hasSize(2);
    Assertions.assertThat(catalog.viewExists(VIEW_IDENTIFIER)).isTrue();
    Assertions.assertThat(catalog.viewExists(newIdentifier)).isTrue();
  }

  private String getViewBasePath(String viewName) {
    return temp.toUri() + DB_NAME + "/" + viewName;
  }
}
