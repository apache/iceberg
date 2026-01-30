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
package org.apache.iceberg.rest;

import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseMetadataTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IncrementalAppendScan;
import org.apache.iceberg.MetadataUpdate.UpgradeFormatVersion;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Scan;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateRequirement;
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
import org.apache.iceberg.exceptions.RetryableValidationException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.rest.RESTCatalogProperties.SnapshotMode;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.FetchScanTasksRequest;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RegisterViewRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.FetchPlanningResultResponse;
import org.apache.iceberg.rest.responses.FetchScanTasksResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ImmutableLoadViewResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.view.BaseView;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewBuilder;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewOperations;
import org.apache.iceberg.view.ViewRepresentation;

public class CatalogHandlers {
  private static final Schema EMPTY_SCHEMA = new Schema();
  private static final String INITIAL_PAGE_TOKEN = "";
  private static final InMemoryPlanningState IN_MEMORY_PLANNING_STATE =
      InMemoryPlanningState.getInstance();
  private static final ExecutorService ASYNC_PLANNING_POOL = Executors.newSingleThreadExecutor();

  // Advanced idempotency store with TTL and in-flight coalescing.
  //
  // Note: This is a simple in-memory implementation meant for tests and lightweight usage.
  // Production servers should provide a durable store.
  private static final ConcurrentMap<String, IdempotencyEntry> IDEMPOTENCY_STORE =
      Maps.newConcurrentMap();
  private static volatile long idempotencyLifetimeMillis = TimeUnit.MINUTES.toMillis(30);

  private CatalogHandlers() {}

  @SuppressWarnings("unchecked")
  static <T extends RESTResponse> T withIdempotency(HTTPRequest httpRequest, Supplier<T> action) {
    return withIdempotencyInternal(httpRequest, action);
  }

  static void withIdempotency(HTTPRequest httpRequest, Runnable action) {
    withIdempotencyInternal(
        httpRequest,
        () -> {
          action.run();
          return Boolean.TRUE;
        });
  }

  @SuppressWarnings("unchecked")
  private static <T> T withIdempotencyInternal(HTTPRequest httpRequest, Supplier<T> action) {
    Optional<HTTPHeaders.HTTPHeader> keyHeader =
        httpRequest.headers().firstEntry(RESTUtil.IDEMPOTENCY_KEY_HEADER);
    if (keyHeader.isEmpty()) {
      return action.get();
    }

    String key = keyHeader.get().value();

    // The "first" request for this Idempotency-Key is the one that wins
    // IDEMPOTENCY_STORE.compute(...)
    // and creates (or replaces) the IN_PROGRESS entry. Only that request executes the action and
    // finalizes the entry; concurrent requests for the same key wait on the latch and then replay
    // the finalized result/error.
    AtomicBoolean isFirst = new AtomicBoolean(false);
    IdempotencyEntry entry =
        IDEMPOTENCY_STORE.compute(
            key,
            (k, current) -> {
              if (current == null || current.isExpired()) {
                isFirst.set(true);
                return IdempotencyEntry.inProgress();
              }
              return current;
            });

    // Fast-path: already finalized (another request completed earlier)
    if (entry.status == IdempotencyEntry.Status.FINALIZED) {
      if (entry.error != null) {
        throw entry.error;
      }
      return (T) entry.responseBody;
    }

    if (!isFirst.get()) {
      // In-flight coalescing: wait for the first request to finalize
      entry.awaitFinalization();
      if (entry.error != null) {
        throw entry.error;
      }
      return (T) entry.responseBody;
    }

    // First request: execute the action and finalize the entry
    try {
      T res = action.get();
      entry.finalizeSuccess(res);
      return res;
    } catch (RuntimeException e) {
      entry.finalizeError(e);
      throw e;
    }
  }

  @VisibleForTesting
  static void setIdempotencyLifetimeFromIso(String isoDuration) {
    if (isoDuration == null) {
      return;
    }
    try {
      idempotencyLifetimeMillis = Duration.parse(isoDuration).toMillis();
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid idempotency lifetime: " + isoDuration, e);
    }
  }

  private static final class IdempotencyEntry {
    enum Status {
      IN_PROGRESS,
      FINALIZED
    }

    private final CountDownLatch latch;
    private final long firstSeenMillis;
    private volatile Status status;
    private volatile Object responseBody;
    private volatile RuntimeException error;

    private IdempotencyEntry(Status status) {
      this.status = status;
      this.latch = new CountDownLatch(1);
      this.firstSeenMillis = System.currentTimeMillis();
    }

    static IdempotencyEntry inProgress() {
      return new IdempotencyEntry(Status.IN_PROGRESS);
    }

    void finalizeSuccess(Object body) {
      this.responseBody = body;
      this.status = Status.FINALIZED;
      this.latch.countDown();
    }

    void finalizeError(RuntimeException cause) {
      this.error = cause;
      this.status = Status.FINALIZED;
      this.latch.countDown();
    }

    void awaitFinalization() {
      try {
        this.latch.await();
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(
            "Interrupted while waiting for idempotent request to complete", ie);
      }
    }

    boolean isExpired() {
      if (this.status != Status.FINALIZED) {
        return false;
      }

      Instant expiry =
          Instant.ofEpochMilli(this.firstSeenMillis).plusMillis(idempotencyLifetimeMillis);
      return Instant.now().isAfter(expiry);
    }
  }

  /**
   * Exception used to avoid retrying commits when assertions fail.
   *
   * <p>When a REST assertion fails, it will throw CommitFailedException to send back to the client.
   * But the assertion checks happen in the block that is retried if {@link
   * TableOperations#commit(TableMetadata, TableMetadata)} throws CommitFailedException. This is
   * used to avoid retries for assertion failures, which are unwrapped and rethrown outside of the
   * commit loop.
   */
  private static class ValidationFailureException extends RuntimeException {
    private final CommitFailedException wrapped;

    private ValidationFailureException(CommitFailedException cause) {
      super(cause);
      this.wrapped = cause;
    }

    public CommitFailedException wrapped() {
      return wrapped;
    }
  }

  private static <T> Pair<List<T>, String> paginate(List<T> list, String pageToken, int pageSize) {
    boolean isFirstPage = pageToken == null || pageToken.equals(INITIAL_PAGE_TOKEN);
    int pageStart = isFirstPage ? 0 : Integer.parseInt(pageToken);
    if (pageStart >= list.size()) {
      return Pair.of(Collections.emptyList(), null);
    }

    int end = Math.min(pageStart + pageSize, list.size());
    List<T> subList = list.subList(pageStart, end);
    String nextPageToken = end >= list.size() ? null : String.valueOf(end);

    return Pair.of(subList, nextPageToken);
  }

  public static ListNamespacesResponse listNamespaces(
      SupportsNamespaces catalog, Namespace parent) {
    List<Namespace> results;
    if (parent.isEmpty()) {
      results = catalog.listNamespaces();
    } else {
      results = catalog.listNamespaces(parent);
    }

    return ListNamespacesResponse.builder().addAll(results).build();
  }

  public static ListNamespacesResponse listNamespaces(
      SupportsNamespaces catalog, Namespace parent, String pageToken, String pageSize) {
    List<Namespace> results;

    if (parent.isEmpty()) {
      results = catalog.listNamespaces();
    } else {
      results = catalog.listNamespaces(parent);
    }

    Pair<List<Namespace>, String> page = paginate(results, pageToken, Integer.parseInt(pageSize));

    return ListNamespacesResponse.builder()
        .addAll(page.first())
        .nextPageToken(page.second())
        .build();
  }

  public static CreateNamespaceResponse createNamespace(
      SupportsNamespaces catalog, CreateNamespaceRequest request) {
    Namespace namespace = request.namespace();
    catalog.createNamespace(namespace, request.properties());
    return CreateNamespaceResponse.builder()
        .withNamespace(namespace)
        .setProperties(catalog.loadNamespaceMetadata(namespace))
        .build();
  }

  public static void namespaceExists(SupportsNamespaces catalog, Namespace namespace) {
    if (!catalog.namespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
  }

  public static GetNamespaceResponse loadNamespace(
      SupportsNamespaces catalog, Namespace namespace) {
    Map<String, String> properties = catalog.loadNamespaceMetadata(namespace);
    return GetNamespaceResponse.builder()
        .withNamespace(namespace)
        .setProperties(properties)
        .build();
  }

  public static void dropNamespace(SupportsNamespaces catalog, Namespace namespace) {
    boolean dropped = catalog.dropNamespace(namespace);
    if (!dropped) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
  }

  public static UpdateNamespacePropertiesResponse updateNamespaceProperties(
      SupportsNamespaces catalog, Namespace namespace, UpdateNamespacePropertiesRequest request) {
    request.validate();

    Set<String> removals = Sets.newHashSet(request.removals());
    Map<String, String> updates = request.updates();

    Map<String, String> startProperties = catalog.loadNamespaceMetadata(namespace);
    Set<String> missing = Sets.difference(removals, startProperties.keySet());

    if (!updates.isEmpty()) {
      catalog.setProperties(namespace, updates);
    }

    if (!removals.isEmpty()) {
      // remove the original set just in case there was an update just after loading properties
      catalog.removeProperties(namespace, removals);
    }

    return UpdateNamespacePropertiesResponse.builder()
        .addMissing(missing)
        .addUpdated(updates.keySet())
        .addRemoved(Sets.difference(removals, missing))
        .build();
  }

  public static ListTablesResponse listTables(Catalog catalog, Namespace namespace) {
    List<TableIdentifier> idents = catalog.listTables(namespace);
    return ListTablesResponse.builder().addAll(idents).build();
  }

  public static ListTablesResponse listTables(
      Catalog catalog, Namespace namespace, String pageToken, String pageSize) {
    List<TableIdentifier> results = catalog.listTables(namespace);

    Pair<List<TableIdentifier>, String> page =
        paginate(results, pageToken, Integer.parseInt(pageSize));

    return ListTablesResponse.builder().addAll(page.first()).nextPageToken(page.second()).build();
  }

  public static LoadTableResponse stageTableCreate(
      Catalog catalog, Namespace namespace, CreateTableRequest request) {
    request.validate();

    TableIdentifier ident = TableIdentifier.of(namespace, request.name());
    if (catalog.tableExists(ident)) {
      throw new AlreadyExistsException("Table already exists: %s", ident);
    }

    Map<String, String> properties = Maps.newHashMap();
    properties.put("created-at", OffsetDateTime.now(ZoneOffset.UTC).toString());
    properties.putAll(request.properties());

    String location;
    if (request.location() != null) {
      location = request.location();
    } else {
      location =
          catalog
              .buildTable(ident, request.schema())
              .withPartitionSpec(request.spec())
              .withSortOrder(request.writeOrder())
              .withProperties(properties)
              .createTransaction()
              .table()
              .location();
    }

    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            request.schema(),
            request.spec() != null ? request.spec() : PartitionSpec.unpartitioned(),
            request.writeOrder() != null ? request.writeOrder() : SortOrder.unsorted(),
            location,
            properties);

    return LoadTableResponse.builder().withTableMetadata(metadata).build();
  }

  public static LoadTableResponse createTable(
      Catalog catalog, Namespace namespace, CreateTableRequest request) {
    request.validate();

    TableIdentifier ident = TableIdentifier.of(namespace, request.name());
    Table table =
        catalog
            .buildTable(ident, request.schema())
            .withLocation(request.location())
            .withPartitionSpec(request.spec())
            .withSortOrder(request.writeOrder())
            .withProperties(request.properties())
            .create();

    if (table instanceof BaseTable) {
      return LoadTableResponse.builder()
          .withTableMetadata(((BaseTable) table).operations().current())
          .build();
    }

    throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
  }

  public static LoadTableResponse registerTable(
      Catalog catalog, Namespace namespace, RegisterTableRequest request) {
    request.validate();

    TableIdentifier identifier = TableIdentifier.of(namespace, request.name());
    Table table = catalog.registerTable(identifier, request.metadataLocation());
    if (table instanceof BaseTable) {
      return LoadTableResponse.builder()
          .withTableMetadata(((BaseTable) table).operations().current())
          .build();
    }

    throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
  }

  public static void dropTable(Catalog catalog, TableIdentifier ident) {
    boolean dropped = catalog.dropTable(ident, false);
    if (!dropped) {
      throw new NoSuchTableException("Table does not exist: %s", ident);
    }
  }

  public static void purgeTable(Catalog catalog, TableIdentifier ident) {
    boolean dropped = catalog.dropTable(ident, true);
    if (!dropped) {
      throw new NoSuchTableException("Table does not exist: %s", ident);
    }
  }

  public static void tableExists(Catalog catalog, TableIdentifier ident) {
    boolean exists = catalog.tableExists(ident);
    if (!exists) {
      throw new NoSuchTableException("Table does not exist: %s", ident);
    }
  }

  /**
   * @deprecated since 1.11.0, will be removed in 1.12.0. Use {@link #loadTable(Catalog,
   *     TableIdentifier, SnapshotMode)} instead.
   */
  @Deprecated
  public static LoadTableResponse loadTable(Catalog catalog, TableIdentifier ident) {
    return loadTable(catalog, ident, SnapshotMode.ALL);
  }

  public static LoadTableResponse loadTable(
      Catalog catalog, TableIdentifier ident, SnapshotMode mode) {
    Table table = catalog.loadTable(ident);

    if (table instanceof BaseTable) {
      TableMetadata loadedMetadata = ((BaseTable) table).operations().current();

      TableMetadata metadata;
      switch (mode) {
        case ALL:
          metadata = loadedMetadata;
          break;
        case REFS:
          metadata =
              TableMetadata.buildFrom(loadedMetadata)
                  .withMetadataLocation(loadedMetadata.metadataFileLocation())
                  .suppressHistoricalSnapshots()
                  .build();
          break;
        default:
          throw new IllegalArgumentException(String.format("Invalid snapshot mode: %s", mode));
      }

      return LoadTableResponse.builder().withTableMetadata(metadata).build();
    } else if (table instanceof BaseMetadataTable) {
      // metadata tables are loaded on the client side, return NoSuchTableException for now
      throw new NoSuchTableException("Table does not exist: %s", ident.toString());
    }

    throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
  }

  public static LoadTableResponse updateTable(
      Catalog catalog, TableIdentifier ident, UpdateTableRequest request) {
    TableMetadata finalMetadata;
    if (isCreate(request)) {
      // this is a hacky way to get TableOperations for an uncommitted table
      Transaction transaction =
          catalog.buildTable(ident, EMPTY_SCHEMA).createOrReplaceTransaction();
      if (transaction instanceof BaseTransaction) {
        BaseTransaction baseTransaction = (BaseTransaction) transaction;
        finalMetadata = create(baseTransaction.underlyingOps(), request);
      } else {
        throw new IllegalStateException(
            "Cannot wrap catalog that does not produce BaseTransaction");
      }

    } else {
      Table table = catalog.loadTable(ident);
      if (table instanceof BaseTable) {
        TableOperations ops = ((BaseTable) table).operations();
        finalMetadata = commit(ops, request);
      } else {
        throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
      }
    }

    return LoadTableResponse.builder().withTableMetadata(finalMetadata).build();
  }

  public static void renameTable(Catalog catalog, RenameTableRequest request) {
    catalog.renameTable(request.source(), request.destination());
  }

  private static boolean isCreate(UpdateTableRequest request) {
    boolean isCreate =
        request.requirements().stream()
            .anyMatch(UpdateRequirement.AssertTableDoesNotExist.class::isInstance);

    if (isCreate) {
      List<UpdateRequirement> invalidRequirements =
          request.requirements().stream()
              .filter(req -> !(req instanceof UpdateRequirement.AssertTableDoesNotExist))
              .collect(Collectors.toList());
      Preconditions.checkArgument(
          invalidRequirements.isEmpty(), "Invalid create requirements: %s", invalidRequirements);
    }

    return isCreate;
  }

  private static TableMetadata create(TableOperations ops, UpdateTableRequest request) {
    // the only valid requirement is that the table will be created
    request.requirements().forEach(requirement -> requirement.validate(ops.current()));
    Optional<Integer> formatVersion =
        request.updates().stream()
            .filter(update -> update instanceof UpgradeFormatVersion)
            .map(update -> ((UpgradeFormatVersion) update).formatVersion())
            .findFirst();

    TableMetadata.Builder builder =
        formatVersion.map(TableMetadata::buildFromEmpty).orElseGet(TableMetadata::buildFromEmpty);
    request.updates().forEach(update -> update.applyTo(builder));
    // create transactions do not retry. if the table exists, retrying is not a solution
    ops.commit(null, builder.build());

    return ops.current();
  }

  static TableMetadata commit(TableOperations ops, UpdateTableRequest request) {
    AtomicBoolean isRetry = new AtomicBoolean(false);
    try {
      Tasks.foreach(ops)
          .retry(COMMIT_NUM_RETRIES_DEFAULT)
          .exponentialBackoff(
              COMMIT_MIN_RETRY_WAIT_MS_DEFAULT,
              COMMIT_MAX_RETRY_WAIT_MS_DEFAULT,
              COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT,
              2.0 /* exponential */)
          .onlyRetryOn(CommitFailedException.class)
          .run(
              taskOps -> {
                TableMetadata base = isRetry.get() ? taskOps.refresh() : taskOps.current();
                isRetry.set(true);

                // validate requirements
                try {
                  request.requirements().forEach(requirement -> requirement.validate(base));
                } catch (CommitFailedException e) {
                  // wrap and rethrow outside of tasks to avoid unnecessary retry
                  throw new ValidationFailureException(e);
                }

                // apply changes
                TableMetadata.Builder metadataBuilder = TableMetadata.buildFrom(base);
                try {
                  request.updates().forEach(update -> update.applyTo(metadataBuilder));
                } catch (RetryableValidationException e) {
                  // Sequence number conflicts from concurrent commits are retryable by the client,
                  // but server-side retry won't help since the sequence number is in the request.
                  // Wrap in ValidationFailureException to skip server retry, return to client as
                  // CommitFailedException so the client can retry with refreshed metadata.
                  throw new ValidationFailureException(
                      new CommitFailedException(e, "Commit conflict: %s", e.getMessage()));
                }

                TableMetadata updated = metadataBuilder.build();
                if (updated.changes().isEmpty()) {
                  // do not commit if the metadata has not changed
                  return;
                }

                // commit
                taskOps.commit(base, updated);
              });

    } catch (ValidationFailureException e) {
      throw e.wrapped();
    }

    return ops.current();
  }

  private static BaseView asBaseView(View view) {
    Preconditions.checkState(
        view instanceof BaseView, "Cannot wrap catalog that does not produce BaseView");
    return (BaseView) view;
  }

  public static ListTablesResponse listViews(ViewCatalog catalog, Namespace namespace) {
    return ListTablesResponse.builder().addAll(catalog.listViews(namespace)).build();
  }

  public static ListTablesResponse listViews(
      ViewCatalog catalog, Namespace namespace, String pageToken, String pageSize) {
    List<TableIdentifier> results = catalog.listViews(namespace);

    Pair<List<TableIdentifier>, String> page =
        paginate(results, pageToken, Integer.parseInt(pageSize));

    return ListTablesResponse.builder().addAll(page.first()).nextPageToken(page.second()).build();
  }

  public static LoadViewResponse createView(
      ViewCatalog catalog, Namespace namespace, CreateViewRequest request) {
    request.validate();

    ViewBuilder viewBuilder =
        catalog
            .buildView(TableIdentifier.of(namespace, request.name()))
            .withSchema(request.schema())
            .withProperties(request.properties())
            .withDefaultNamespace(request.viewVersion().defaultNamespace())
            .withDefaultCatalog(request.viewVersion().defaultCatalog())
            .withLocation(request.location());

    Set<String> unsupportedRepresentations =
        request.viewVersion().representations().stream()
            .filter(r -> !(r instanceof SQLViewRepresentation))
            .map(ViewRepresentation::type)
            .collect(Collectors.toSet());

    if (!unsupportedRepresentations.isEmpty()) {
      throw new IllegalStateException(
          String.format("Found unsupported view representations: %s", unsupportedRepresentations));
    }

    request.viewVersion().representations().stream()
        .filter(SQLViewRepresentation.class::isInstance)
        .map(SQLViewRepresentation.class::cast)
        .forEach(r -> viewBuilder.withQuery(r.dialect(), r.sql()));

    View view = viewBuilder.create();

    return viewResponse(view);
  }

  private static LoadViewResponse viewResponse(View view) {
    ViewMetadata metadata = asBaseView(view).operations().current();
    return ImmutableLoadViewResponse.builder()
        .metadata(metadata)
        .metadataLocation(metadata.metadataFileLocation())
        .build();
  }

  public static void viewExists(ViewCatalog catalog, TableIdentifier viewIdentifier) {
    if (!catalog.viewExists(viewIdentifier)) {
      throw new NoSuchViewException("View does not exist: %s", viewIdentifier);
    }
  }

  public static LoadViewResponse loadView(ViewCatalog catalog, TableIdentifier viewIdentifier) {
    View view = catalog.loadView(viewIdentifier);
    return viewResponse(view);
  }

  public static LoadViewResponse updateView(
      ViewCatalog catalog, TableIdentifier ident, UpdateTableRequest request) {
    View view = catalog.loadView(ident);
    ViewMetadata metadata = commit(asBaseView(view).operations(), request);

    return ImmutableLoadViewResponse.builder()
        .metadata(metadata)
        .metadataLocation(metadata.metadataFileLocation())
        .build();
  }

  public static void renameView(ViewCatalog catalog, RenameTableRequest request) {
    catalog.renameView(request.source(), request.destination());
  }

  public static void dropView(ViewCatalog catalog, TableIdentifier viewIdentifier) {
    boolean dropped = catalog.dropView(viewIdentifier);
    if (!dropped) {
      throw new NoSuchViewException("View does not exist: %s", viewIdentifier);
    }
  }

  public static LoadViewResponse registerView(
      ViewCatalog catalog, Namespace namespace, RegisterViewRequest request) {
    request.validate();

    TableIdentifier identifier = TableIdentifier.of(namespace, request.name());
    View view = catalog.registerView(identifier, request.metadataLocation());
    return ImmutableLoadViewResponse.builder()
        .metadata(asBaseView(view).operations().current())
        .metadataLocation(request.metadataLocation())
        .build();
  }

  static ViewMetadata commit(ViewOperations ops, UpdateTableRequest request) {
    AtomicBoolean isRetry = new AtomicBoolean(false);
    try {
      Tasks.foreach(ops)
          .retry(COMMIT_NUM_RETRIES_DEFAULT)
          .exponentialBackoff(
              COMMIT_MIN_RETRY_WAIT_MS_DEFAULT,
              COMMIT_MAX_RETRY_WAIT_MS_DEFAULT,
              COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT,
              2.0 /* exponential */)
          .onlyRetryOn(CommitFailedException.class)
          .run(
              taskOps -> {
                ViewMetadata base = isRetry.get() ? taskOps.refresh() : taskOps.current();
                isRetry.set(true);

                // validate requirements
                try {
                  request.requirements().forEach(requirement -> requirement.validate(base));
                } catch (CommitFailedException e) {
                  // wrap and rethrow outside of tasks to avoid unnecessary retry
                  throw new ValidationFailureException(e);
                }

                // apply changes
                ViewMetadata.Builder metadataBuilder = ViewMetadata.buildFrom(base);
                request.updates().forEach(update -> update.applyTo(metadataBuilder));

                ViewMetadata updated = metadataBuilder.build();

                if (updated.changes().isEmpty()) {
                  // do not commit if the metadata has not changed
                  return;
                }

                // commit
                taskOps.commit(base, updated);
              });

    } catch (ValidationFailureException e) {
      throw e.wrapped();
    }

    return ops.current();
  }

  public static PlanTableScanResponse planTableScan(
      Catalog catalog,
      TableIdentifier ident,
      PlanTableScanRequest request,
      Predicate<Scan<?, FileScanTask, ?>> shouldPlanAsync,
      ToIntFunction<Scan<?, FileScanTask, ?>> tasksPerPlanTask) {
    Table table = catalog.loadTable(ident);
    // Configure the appropriate scan type
    Scan<?, FileScanTask, ?> configuredScan;

    if (request.startSnapshotId() != null && request.endSnapshotId() != null) {
      // Incremental append scan for reading changes between snapshots
      IncrementalAppendScan incrementalScan =
          table
              .newIncrementalAppendScan()
              .fromSnapshotInclusive(request.startSnapshotId())
              .toSnapshot(request.endSnapshotId());

      configuredScan = configureScan(incrementalScan, request);
    } else {
      // Regular table scan at a specific snapshot
      TableScan tableScan = table.newScan();

      if (request.snapshotId() != null) {
        tableScan = tableScan.useSnapshot(request.snapshotId());
      }

      // Apply filters and projections using common method
      configuredScan = configureScan(tableScan, request);
    }

    if (shouldPlanAsync.test(configuredScan)) {
      String asyncPlanId = "async-" + UUID.randomUUID();
      asyncPlanFiles(
          configuredScan,
          asyncPlanId,
          table.uuid().toString(),
          tasksPerPlanTask.applyAsInt(configuredScan));
      return PlanTableScanResponse.builder()
          .withPlanId(asyncPlanId)
          .withPlanStatus(PlanStatus.SUBMITTED)
          .withSpecsById(table.specs())
          .build();
    }

    String planId = "sync-" + UUID.randomUUID();
    Pair<List<FileScanTask>, String> initial =
        planFilesFor(
            configuredScan,
            planId,
            table.uuid().toString(),
            tasksPerPlanTask.applyAsInt(configuredScan));
    List<String> nextPlanTasks =
        initial.second() == null
            ? Collections.emptyList()
            : IN_MEMORY_PLANNING_STATE.nextPlanTask(initial.second());
    PlanTableScanResponse.Builder builder =
        PlanTableScanResponse.builder()
            .withPlanStatus(PlanStatus.COMPLETED)
            .withPlanId(planId)
            .withFileScanTasks(initial.first())
            .withSpecsById(table.specs());

    if (!nextPlanTasks.isEmpty()) {
      builder.withPlanTasks(nextPlanTasks);
    }

    return builder.build();
  }

  /**
   * Fetches the planning result for an async plan.
   *
   * @param catalog the catalog to use for loading the table
   * @param ident the table identifier
   * @param planId the plan identifier
   * @return the fetch planning result response
   */
  public static FetchPlanningResultResponse fetchPlanningResult(
      Catalog catalog, TableIdentifier ident, String planId) {
    Table table = catalog.loadTable(ident);
    PlanStatus status = IN_MEMORY_PLANNING_STATE.asyncPlanStatus(planId);
    if (status != PlanStatus.COMPLETED) {
      return FetchPlanningResultResponse.builder().withPlanStatus(status).build();
    }

    Pair<List<FileScanTask>, String> initial = IN_MEMORY_PLANNING_STATE.initialScanTasksFor(planId);
    return FetchPlanningResultResponse.builder()
        .withPlanStatus(PlanStatus.COMPLETED)
        .withFileScanTasks(initial.first())
        .withPlanTasks(IN_MEMORY_PLANNING_STATE.nextPlanTask(initial.second()))
        .withSpecsById(table.specs())
        .build();
  }

  /**
   * Fetches scan tasks for a specific plan task.
   *
   * @param catalog the catalog to use for loading the table
   * @param ident the table identifier
   * @param request the fetch scan tasks request
   * @return the fetch scan tasks response
   */
  public static FetchScanTasksResponse fetchScanTasks(
      Catalog catalog, TableIdentifier ident, FetchScanTasksRequest request) {
    Table table = catalog.loadTable(ident);
    String planTask = request.planTask();
    List<FileScanTask> fileScanTasks = IN_MEMORY_PLANNING_STATE.fileScanTasksForPlanTask(planTask);

    return FetchScanTasksResponse.builder()
        .withFileScanTasks(fileScanTasks)
        .withPlanTasks(IN_MEMORY_PLANNING_STATE.nextPlanTask(planTask))
        .withSpecsById(table.specs())
        .build();
  }

  /**
   * Cancels a plan table scan by removing all associated state.
   *
   * @param planId the plan identifier to cancel
   */
  public static void cancelPlanTableScan(String planId) {
    IN_MEMORY_PLANNING_STATE.cancelPlan(planId);
  }

  static void clearPlanningState() {
    InMemoryPlanningState.getInstance().clear();
  }

  /**
   * Applies filters, projections, and other scan configurations from the request to the scan.
   *
   * @param scan the scan to configure
   * @param request the plan table scan request containing filters and projections
   * @param <T> the specific scan type (TableScan, IncrementalAppendScan, etc.)
   * @return the configured scan with filters and projections applied
   */
  private static <T extends Scan<T, FileScanTask, ?>> T configureScan(
      T scan, PlanTableScanRequest request) {
    T configuredScan = scan;

    if (request.select() != null) {
      configuredScan = configuredScan.select(request.select());
    }
    if (request.filter() != null) {
      configuredScan = configuredScan.filter(request.filter());
    }
    if (request.statsFields() != null) {
      configuredScan = configuredScan.includeColumnStats(request.statsFields());
    }
    configuredScan = configuredScan.caseSensitive(request.caseSensitive());

    return configuredScan;
  }

  /**
   * Plans file scan tasks for a table scan, grouping them into plan tasks for pagination.
   *
   * @param scan the table scan to plan files for
   * @param planId the unique identifier for this plan
   * @param tableId the uuid of the table being scanned
   * @param tasksPerPlanTask number of file scan tasks to group per plan task
   * @return the initial file scan tasks and the first plan task key
   */
  private static Pair<List<FileScanTask>, String> planFilesFor(
      Scan<?, FileScanTask, ?> scan, String planId, String tableId, int tasksPerPlanTask) {
    try (CloseableIterable<FileScanTask> planTasks = scan.planFiles()) {
      String planTaskPrefix = planId + "-" + tableId + "-";

      // Handle empty table scans
      if (!planTasks.iterator().hasNext()) {
        String planTaskKey = planTaskPrefix + "0";
        // Add empty scan to planning state so async calls know the scan completed
        IN_MEMORY_PLANNING_STATE.addPlanTask(planTaskKey, Collections.emptyList());
        return Pair.of(Collections.emptyList(), planTaskKey);
      }

      Iterable<List<FileScanTask>> taskGroupings = Iterables.partition(planTasks, tasksPerPlanTask);
      int planTaskSequence = 0;
      String previousPlanTask = null;
      String firstPlanTaskKey = null;
      List<FileScanTask> initialFileScanTasks = null;
      for (List<FileScanTask> taskGrouping : taskGroupings) {
        String planTaskKey = planTaskPrefix + planTaskSequence++;
        IN_MEMORY_PLANNING_STATE.addPlanTask(planTaskKey, taskGrouping);
        if (previousPlanTask != null) {
          IN_MEMORY_PLANNING_STATE.addNextPlanTask(previousPlanTask, planTaskKey);
        } else {
          firstPlanTaskKey = planTaskKey;
          initialFileScanTasks = taskGrouping;
        }

        previousPlanTask = planTaskKey;
      }
      return Pair.of(initialFileScanTasks, firstPlanTaskKey);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private static void asyncPlanFiles(
      Scan<?, FileScanTask, ?> scan, String asyncPlanId, String tableId, int tasksPerPlanTask) {
    IN_MEMORY_PLANNING_STATE.addAsyncPlan(asyncPlanId);
    CompletableFuture.runAsync(
            () -> {
              planFilesFor(scan, asyncPlanId, tableId, tasksPerPlanTask);
            },
            ASYNC_PLANNING_POOL)
        .whenComplete(
            (result, exception) -> {
              if (exception != null) {
                IN_MEMORY_PLANNING_STATE.markAsyncPlanFailed(asyncPlanId);
              } else {
                IN_MEMORY_PLANNING_STATE.markAsyncPlanAsComplete(asyncPlanId);
              }
            });
  }
}
