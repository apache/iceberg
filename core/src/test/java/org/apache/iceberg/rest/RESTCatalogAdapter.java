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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.http.HttpHeaders;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.Transactions;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchIcebergTableException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchPlanIdException;
import org.apache.iceberg.exceptions.NoSuchPlanTaskException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.exceptions.UnprocessableEntityException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.HTTPRequest.HTTPMethod;
import org.apache.iceberg.rest.RESTCatalogProperties.SnapshotMode;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.FetchScanTasksRequest;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.FetchPlanningResultResponse;
import org.apache.iceberg.rest.responses.FetchScanTasksResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PropertyUtil;

/** Adaptor class to translate REST requests into {@link Catalog} API calls. */
public class RESTCatalogAdapter extends BaseHTTPClient {
  private static final Map<Class<? extends Exception>, Integer> EXCEPTION_ERROR_CODES =
      ImmutableMap.<Class<? extends Exception>, Integer>builder()
          .put(IllegalArgumentException.class, 400)
          .put(ValidationException.class, 400)
          .put(NamespaceNotEmptyException.class, 409)
          .put(NotAuthorizedException.class, 401)
          .put(ForbiddenException.class, 403)
          .put(NoSuchNamespaceException.class, 404)
          .put(NoSuchTableException.class, 404)
          .put(NoSuchViewException.class, 404)
          .put(NoSuchIcebergTableException.class, 404)
          .put(UnsupportedOperationException.class, 406)
          .put(AlreadyExistsException.class, 409)
          .put(CommitFailedException.class, 409)
          .put(UnprocessableEntityException.class, 422)
          .put(CommitStateUnknownException.class, 500)
          .put(NoSuchPlanIdException.class, 404)
          .put(NoSuchPlanTaskException.class, 404)
          .buildOrThrow();

  private final Catalog catalog;
  private final SupportsNamespaces asNamespaceCatalog;
  private final ViewCatalog asViewCatalog;

  private AuthSession authSession = AuthSession.EMPTY;
  private final PlanningBehavior planningBehavior = planningBehavior();
  private final Map<String, List<FileScanTask>> planTaskToFileScanTasks;

  // Simple implementation where every plan task just has a single next plan task
  private final Map<String, String> planTaskToNext;
  private static final ExecutorService asyncPlanningPool = Executors.newSingleThreadExecutor();

  public RESTCatalogAdapter(Catalog catalog) {
    this.catalog = catalog;
    this.asNamespaceCatalog =
        catalog instanceof SupportsNamespaces ? (SupportsNamespaces) catalog : null;
    this.asViewCatalog = catalog instanceof ViewCatalog ? (ViewCatalog) catalog : null;
    this.planTaskToFileScanTasks = Maps.newConcurrentMap();
    this.planTaskToNext = Maps.newConcurrentMap();
  }

  private static OAuthTokenResponse handleOAuthRequest(Object body) {
    Map<String, String> request = (Map<String, String>) castRequest(Map.class, body);
    String grantType = request.get("grant_type");
    switch (grantType) {
      case "client_credentials":
        return OAuthTokenResponse.builder()
            .withToken("client-credentials-token:sub=" + request.get("client_id"))
            .withTokenType("Bearer")
            .build();

      case "urn:ietf:params:oauth:grant-type:token-exchange":
        String actor = request.get("actor_token");
        String token =
            String.format(
                "token-exchange-token:sub=%s%s",
                request.get("subject_token"), actor != null ? ",act=" + actor : "");
        return OAuthTokenResponse.builder()
            .withToken(token)
            .withIssuedTokenType("urn:ietf:params:oauth:token-type:access_token")
            .withTokenType("Bearer")
            .build();

      default:
        throw new UnsupportedOperationException("Unsupported grant_type: " + grantType);
    }
  }

  @Override
  public RESTClient withAuthSession(AuthSession session) {
    this.authSession = session;
    return this;
  }

  @SuppressWarnings({"MethodLength", "checkstyle:CyclomaticComplexity"})
  public <T extends RESTResponse> T handleRequest(
      Route route,
      Map<String, String> vars,
      HTTPRequest httpRequest,
      Class<T> responseType,
      Consumer<Map<String, String>> responseHeaders) {
    Object body = httpRequest.body();
    switch (route) {
      case TOKENS:
        return castResponse(responseType, handleOAuthRequest(body));

      case CONFIG:
        return castResponse(
            responseType,
            ConfigResponse.builder()
                .withEndpoints(
                    Arrays.stream(Route.values())
                        .map(r -> Endpoint.create(r.method().name(), r.resourcePath()))
                        .collect(Collectors.toList()))
                .build());

      case LIST_NAMESPACES:
        if (asNamespaceCatalog != null) {
          Namespace ns;
          if (vars.containsKey("parent")) {
            ns = RESTUtil.namespaceFromQueryParam(vars.get("parent"));
          } else {
            ns = Namespace.empty();
          }

          String pageToken = PropertyUtil.propertyAsString(vars, "pageToken", null);
          String pageSize = PropertyUtil.propertyAsString(vars, "pageSize", null);

          if (pageSize != null) {
            return castResponse(
                responseType,
                CatalogHandlers.listNamespaces(asNamespaceCatalog, ns, pageToken, pageSize));
          } else {
            return castResponse(
                responseType, CatalogHandlers.listNamespaces(asNamespaceCatalog, ns));
          }
        }
        break;

      case CREATE_NAMESPACE:
        if (asNamespaceCatalog != null) {
          CreateNamespaceRequest request = castRequest(CreateNamespaceRequest.class, body);
          return castResponse(
              responseType, CatalogHandlers.createNamespace(asNamespaceCatalog, request));
        }
        break;

      case NAMESPACE_EXISTS:
        if (asNamespaceCatalog != null) {
          CatalogHandlers.namespaceExists(asNamespaceCatalog, namespaceFromPathVars(vars));
          return null;
        }
        break;

      case LOAD_NAMESPACE:
        if (asNamespaceCatalog != null) {
          Namespace namespace = namespaceFromPathVars(vars);
          return castResponse(
              responseType, CatalogHandlers.loadNamespace(asNamespaceCatalog, namespace));
        }
        break;

      case DROP_NAMESPACE:
        if (asNamespaceCatalog != null) {
          CatalogHandlers.dropNamespace(asNamespaceCatalog, namespaceFromPathVars(vars));
          return null;
        }
        break;

      case UPDATE_NAMESPACE:
        if (asNamespaceCatalog != null) {
          Namespace namespace = namespaceFromPathVars(vars);
          UpdateNamespacePropertiesRequest request =
              castRequest(UpdateNamespacePropertiesRequest.class, body);
          return castResponse(
              responseType,
              CatalogHandlers.updateNamespaceProperties(asNamespaceCatalog, namespace, request));
        }
        break;

      case LIST_TABLES:
        {
          Namespace namespace = namespaceFromPathVars(vars);
          String pageToken = PropertyUtil.propertyAsString(vars, "pageToken", null);
          String pageSize = PropertyUtil.propertyAsString(vars, "pageSize", null);
          if (pageSize != null) {
            return castResponse(
                responseType, CatalogHandlers.listTables(catalog, namespace, pageToken, pageSize));
          } else {
            return castResponse(responseType, CatalogHandlers.listTables(catalog, namespace));
          }
        }

      case CREATE_TABLE:
        {
          Namespace namespace = namespaceFromPathVars(vars);
          CreateTableRequest request = castRequest(CreateTableRequest.class, body);
          request.validate();

          if (request.stageCreate()) {
            return castResponse(
                responseType, CatalogHandlers.stageTableCreate(catalog, namespace, request));
          } else {
            LoadTableResponse response = CatalogHandlers.createTable(catalog, namespace, request);
            responseHeaders.accept(
                ImmutableMap.of(HttpHeaders.ETAG, ETagProvider.of(response.metadataLocation())));
            return castResponse(responseType, response);
          }
        }

      case DROP_TABLE:
        {
          if (PropertyUtil.propertyAsBoolean(vars, "purgeRequested", false)) {
            CatalogHandlers.purgeTable(catalog, tableIdentFromPathVars(vars));
          } else {
            CatalogHandlers.dropTable(catalog, tableIdentFromPathVars(vars));
          }
          return null;
        }

      case TABLE_EXISTS:
        {
          TableIdentifier ident = tableIdentFromPathVars(vars);
          CatalogHandlers.tableExists(catalog, ident);
          return null;
        }

      case LOAD_TABLE:
        {
          LoadTableResponse response =
              CatalogHandlers.loadTable(
                  catalog,
                  tableIdentFromPathVars(vars),
                  snapshotModeFromQueryParams(httpRequest.queryParameters()));

          Optional<HTTPHeaders.HTTPHeader> ifNoneMatchHeader =
              httpRequest.headers().firstEntry(HttpHeaders.IF_NONE_MATCH);

          String eTag = ETagProvider.of(response.metadataLocation());

          if (ifNoneMatchHeader.isPresent() && eTag.equals(ifNoneMatchHeader.get().value())) {
            return null;
          }

          responseHeaders.accept(ImmutableMap.of(HttpHeaders.ETAG, eTag));

          return castResponse(responseType, response);
        }

      case PLAN_TABLE_SCAN:
        {
          TableIdentifier ident = tableIdentFromPathVars(vars);
          PlanTableScanRequest request = castRequest(PlanTableScanRequest.class, body);
          Table table = catalog.loadTable(ident);
          TableScan tableScan = table.newScan();

          if (request.snapshotId() != null) {
            tableScan = tableScan.useSnapshot(request.snapshotId());
          }
          if (request.select() != null) {
            tableScan = tableScan.select(request.select());
          }
          if (request.filter() != null) {
            tableScan = tableScan.filter(request.filter());
          }
          if (request.statsFields() != null) {
            tableScan = tableScan.includeColumnStats(request.statsFields());
          }

          tableScan = tableScan.caseSensitive(request.caseSensitive());

          if (planningBehavior.shouldPlanTableScanAsync(tableScan)) {
            String asyncPlanId = UUID.randomUUID().toString();
            asyncPlanFiles(tableScan, asyncPlanId);
            return castResponse(
                responseType,
                PlanTableScanResponse.builder()
                    .withPlanId(asyncPlanId)
                    .withPlanStatus(PlanStatus.SUBMITTED)
                    .withSpecsById(table.specs())
                    .build());
          }

          String planId = UUID.randomUUID().toString();
          planFilesFor(tableScan, planId);
          Pair<List<FileScanTask>, String> tasksAndPlan = initialScanTasksForPlan(planId);
          return castResponse(
              responseType,
              PlanTableScanResponse.builder()
                  .withPlanStatus(PlanStatus.COMPLETED)
                  .withPlanTasks(nextPlanTasks(tasksAndPlan.second()))
                  .withFileScanTasks(tasksAndPlan.first())
                  .withDeleteFiles(
                      tasksAndPlan.first().stream()
                          .flatMap(t -> t.deletes().stream())
                          .distinct()
                          .collect(Collectors.toList()))
                  .withSpecsById(table.specs())
                  .build());
        }

      case FETCH_PLANNING_RESULT:
        {
          TableIdentifier ident = tableIdentFromPathVars(vars);
          Table table = catalog.loadTable(ident);
          String planId = planIDFromPathVars(vars);
          Pair<List<FileScanTask>, String> tasksAndPlan = initialScanTasksForPlan(planId);
          return castResponse(
              responseType,
              FetchPlanningResultResponse.builder()
                  .withPlanStatus(PlanStatus.COMPLETED)
                  .withDeleteFiles(
                      tasksAndPlan.first().stream()
                          .flatMap(t -> t.deletes().stream())
                          .distinct()
                          .collect(Collectors.toList()))
                  .withFileScanTasks(tasksAndPlan.first())
                  .withPlanTasks(nextPlanTasks(tasksAndPlan.second()))
                  .withSpecsById(table.specs())
                  .build());
        }

      case FETCH_SCAN_TASKS:
        {
          TableIdentifier ident = tableIdentFromPathVars(vars);
          Table table = catalog.loadTable(ident);
          FetchScanTasksRequest request = castRequest(FetchScanTasksRequest.class, body);
          String planTask = request.planTask();
          List<FileScanTask> fileScanTasks = planTaskToFileScanTasks.get(planTask);
          if (fileScanTasks == null) {
            throw new NoSuchPlanTaskException("Could not find tasks for plan task %s", planTask);
          }

          // Simple implementation, only have at most 1 "next" plan task to simulate pagination
          return castResponse(
              responseType,
              FetchScanTasksResponse.builder()
                  .withFileScanTasks(fileScanTasks)
                  .withPlanTasks(nextPlanTasks(planTask))
                  .withSpecsById(table.specs())
                  .withDeleteFiles(
                      fileScanTasks.stream()
                          .flatMap(t -> t.deletes().stream())
                          .distinct()
                          .collect(Collectors.toList()))
                  .build());
        }

      case CANCEL_PLAN_TABLE_SCAN:
        {
          String planId = planIDFromPathVars(vars);
          // Remove all the state related to the provided plan ID
          planTaskToNext.entrySet().removeIf(entry -> entry.getKey().contains(planId));
          planTaskToFileScanTasks.entrySet().removeIf(entry -> entry.getKey().contains(planId));
        }

      case REGISTER_TABLE:
        {
          LoadTableResponse response =
              CatalogHandlers.registerTable(
                  catalog,
                  namespaceFromPathVars(vars),
                  castRequest(RegisterTableRequest.class, body));

          responseHeaders.accept(
              ImmutableMap.of(HttpHeaders.ETAG, ETagProvider.of(response.metadataLocation())));

          return castResponse(responseType, response);
        }

      case UPDATE_TABLE:
        {
          LoadTableResponse response =
              CatalogHandlers.updateTable(
                  catalog,
                  tableIdentFromPathVars(vars),
                  castRequest(UpdateTableRequest.class, body));

          responseHeaders.accept(
              ImmutableMap.of(HttpHeaders.ETAG, ETagProvider.of(response.metadataLocation())));

          return castResponse(responseType, response);
        }

      case RENAME_TABLE:
        {
          RenameTableRequest request = castRequest(RenameTableRequest.class, body);
          CatalogHandlers.renameTable(catalog, request);
          return null;
        }

      case REPORT_METRICS:
        {
          // nothing to do here other than checking that we're getting the correct request
          castRequest(ReportMetricsRequest.class, body);
          return null;
        }

      case COMMIT_TRANSACTION:
        {
          CommitTransactionRequest request = castRequest(CommitTransactionRequest.class, body);
          commitTransaction(catalog, request);
          return null;
        }

      case LIST_VIEWS:
        {
          if (null != asViewCatalog) {
            Namespace namespace = namespaceFromPathVars(vars);
            String pageToken = PropertyUtil.propertyAsString(vars, "pageToken", null);
            String pageSize = PropertyUtil.propertyAsString(vars, "pageSize", null);
            if (pageSize != null) {
              return castResponse(
                  responseType,
                  CatalogHandlers.listViews(asViewCatalog, namespace, pageToken, pageSize));
            } else {
              return castResponse(
                  responseType, CatalogHandlers.listViews(asViewCatalog, namespace));
            }
          }
          break;
        }

      case CREATE_VIEW:
        {
          if (null != asViewCatalog) {
            Namespace namespace = namespaceFromPathVars(vars);
            CreateViewRequest request = castRequest(CreateViewRequest.class, body);
            return castResponse(
                responseType, CatalogHandlers.createView(asViewCatalog, namespace, request));
          }
          break;
        }

      case VIEW_EXISTS:
        {
          if (null != asViewCatalog) {
            CatalogHandlers.viewExists(asViewCatalog, viewIdentFromPathVars(vars));
            return null;
          }
          break;
        }

      case LOAD_VIEW:
        {
          if (null != asViewCatalog) {
            TableIdentifier ident = viewIdentFromPathVars(vars);
            return castResponse(responseType, CatalogHandlers.loadView(asViewCatalog, ident));
          }
          break;
        }

      case UPDATE_VIEW:
        {
          if (null != asViewCatalog) {
            TableIdentifier ident = viewIdentFromPathVars(vars);
            UpdateTableRequest request = castRequest(UpdateTableRequest.class, body);
            return castResponse(
                responseType, CatalogHandlers.updateView(asViewCatalog, ident, request));
          }
          break;
        }

      case RENAME_VIEW:
        {
          if (null != asViewCatalog) {
            RenameTableRequest request = castRequest(RenameTableRequest.class, body);
            CatalogHandlers.renameView(asViewCatalog, request);
            return null;
          }
          break;
        }

      case DROP_VIEW:
        {
          if (null != asViewCatalog) {
            CatalogHandlers.dropView(asViewCatalog, viewIdentFromPathVars(vars));
            return null;
          }
          break;
        }

      default:
        if (responseType == OAuthTokenResponse.class) {
          return castResponse(responseType, handleOAuthRequest(body));
        }
    }

    return null;
  }

  /**
   * Do all the planning upfront but batch the file scan tasks across plan tasks. Plan Tasks have a
   * key like <plan ID - table UUID - plan task sequence> The current implementation simply uses
   * plan tasks as a pagination mechanism to control response sizes.
   *
   * @param tableScan
   * @param planId
   */
  private void planFilesFor(TableScan tableScan, String planId) {
    Iterable<List<FileScanTask>> taskGroupings =
        Iterables.partition(
            tableScan.planFiles(), planningBehavior.numberFileScanTasksPerPlanTask());
    int planTaskSequence = 0;
    String prevPlanTask = null;
    for (List<FileScanTask> taskGrouping : taskGroupings) {
      String planTaskKey =
          String.format("%s-%s-%s", planId, tableScan.table().uuid(), planTaskSequence++);
      planTaskToFileScanTasks.put(planTaskKey, taskGrouping);
      if (prevPlanTask != null) {
        planTaskToNext.put(prevPlanTask, planTaskKey);
      }

      prevPlanTask = planTaskKey;
    }
  }

  private void asyncPlanFiles(TableScan scan, String asyncPlanId) {
    asyncPlanningPool.submit(
        () -> {
          planFilesFor(scan, asyncPlanId);
        });
  }

  // The initial set of file scan tasks is going to have a sentinel plan task which ends in
  // 0. Directly return this set of file scan tasks as the initial set, along with
  // any next plan task if applicable
  private Pair<List<FileScanTask>, String> initialScanTasksForPlan(String planId) {
    Set<Map.Entry<String, List<FileScanTask>>> initialPlanTaskAndFileScanTasks =
        planTaskToFileScanTasks.entrySet().stream()
            .filter(
                planTask -> planTask.getKey().contains(planId) && planTask.getKey().endsWith("0"))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            .entrySet();
    if (initialPlanTaskAndFileScanTasks.isEmpty()) {
      throw new NoSuchPlanIdException("Could not find plan ID %s", planId);
    }

    Map.Entry<String, List<FileScanTask>> initialTasks =
        Iterables.getOnlyElement(initialPlanTaskAndFileScanTasks);
    return Pair.of(initialTasks.getValue(), initialTasks.getKey());
  }

  // Use plan tasks as a way to simulate pagination, at most one plan task per response
  private List<String> nextPlanTasks(String planTaskKey) {
    String nextPlanTask = planTaskToNext.get(planTaskKey);
    if (nextPlanTask != null) {
      return ImmutableList.of(nextPlanTask);
    }

    return ImmutableList.of();
  }

  /**
   * This is a very simplistic approach that only validates the requirements for each table and does
   * not do any other conflict detection. Therefore, it does not guarantee true transactional
   * atomicity, which is left to the implementation details of a REST server.
   */
  private static void commitTransaction(Catalog catalog, CommitTransactionRequest request) {
    List<Transaction> transactions = Lists.newArrayList();

    for (UpdateTableRequest tableChange : request.tableChanges()) {
      Table table = catalog.loadTable(tableChange.identifier());
      if (table instanceof BaseTable) {
        Transaction transaction =
            Transactions.newTransaction(
                tableChange.identifier().toString(), ((BaseTable) table).operations());
        transactions.add(transaction);

        BaseTransaction.TransactionTable txTable =
            (BaseTransaction.TransactionTable) transaction.table();

        // this performs validations and makes temporary commits that are in-memory
        CatalogHandlers.commit(txTable.operations(), tableChange);
      } else {
        throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
      }
    }

    // only commit if validations passed previously
    transactions.forEach(Transaction::commitTransaction);
  }

  @Override
  protected HTTPRequest buildRequest(
      HTTPMethod method,
      String path,
      Map<String, String> queryParams,
      Map<String, String> headers,
      Object body) {
    URI baseUri = URI.create("https://localhost:8080");
    ObjectMapper mapper = RESTObjectMapper.mapper();
    ImmutableHTTPRequest.Builder builder =
        ImmutableHTTPRequest.builder()
            .baseUri(baseUri)
            .mapper(mapper)
            .method(method)
            .path(path)
            .body(body);

    if (queryParams != null) {
      builder.queryParameters(queryParams);
    }

    if (headers != null) {
      builder.headers(HTTPHeaders.of(headers));
    }

    return authSession.authenticate(builder.build());
  }

  @Override
  protected <T extends RESTResponse> T execute(
      HTTPRequest request,
      Class<T> responseType,
      Consumer<ErrorResponse> errorHandler,
      Consumer<Map<String, String>> responseHeaders) {
    return execute(
        request, responseType, errorHandler, responseHeaders, ParserContext.builder().build());
  }

  @Override
  protected <T extends RESTResponse> T execute(
      HTTPRequest request,
      Class<T> responseType,
      Consumer<ErrorResponse> errorHandler,
      Consumer<Map<String, String>> responseHeaders,
      ParserContext parserContext) {
    ErrorResponse.Builder errorBuilder = ErrorResponse.builder();
    Pair<Route, Map<String, String>> routeAndVars = Route.from(request.method(), request.path());
    if (routeAndVars != null) {
      try {
        ImmutableMap.Builder<String, String> vars = ImmutableMap.builder();
        vars.putAll(request.queryParameters());
        vars.putAll(routeAndVars.second());

        return handleRequest(
            routeAndVars.first(), vars.build(), request, responseType, responseHeaders);
      } catch (RuntimeException e) {
        configureResponseFromException(e, errorBuilder);
      }

    } else {
      errorBuilder
          .responseCode(400)
          .withType("BadRequestException")
          .withMessage(
              String.format("No route for request: %s %s", request.method(), request.path()));
    }

    ErrorResponse error = errorBuilder.build();
    errorHandler.accept(error);

    // if the error handler doesn't throw an exception, throw a generic one
    throw new RESTException("Unhandled error: %s", error);
  }

  /**
   * Supplied interface to allow RESTCatalogAdapter implementations to have a really simple way to
   * change how many file scan tasks get grouped in a plan task or under what conditions a table
   * scan should be performed async. Primarily used in testing to allow overriding more
   * deterministic ways of planning behavior.
   */
  public interface PlanningBehavior {
    default int numberFileScanTasksPerPlanTask() {
      return 100;
    }

    default boolean shouldPlanTableScanAsync(TableScan tableScan) {
      return false;
    }
  }

  protected PlanningBehavior planningBehavior() {
    return new PlanningBehavior() {};
  }

  @Override
  public void close() throws IOException {
    // The calling test is responsible for closing the underlying catalog backing this REST catalog
    // so that the underlying backend catalog is not closed and reopened during the REST catalog's
    // initialize method when fetching the server configuration.
  }

  private static class BadResponseType extends RuntimeException {
    private BadResponseType(Class<?> responseType, Object response) {
      super(
          String.format("Invalid response object, not a %s: %s", responseType.getName(), response));
    }
  }

  private static class BadRequestType extends RuntimeException {
    private BadRequestType(Class<?> requestType, Object request) {
      super(String.format("Invalid request object, not a %s: %s", requestType.getName(), request));
    }
  }

  public static <T> T castRequest(Class<T> requestType, Object request) {
    if (requestType.isInstance(request)) {
      return requestType.cast(request);
    }

    throw new BadRequestType(requestType, request);
  }

  public static <T extends RESTResponse> T castResponse(Class<T> responseType, Object response) {
    if (responseType.isInstance(response)) {
      return responseType.cast(response);
    }

    throw new BadResponseType(responseType, response);
  }

  public static void configureResponseFromException(
      Exception exc, ErrorResponse.Builder errorBuilder) {
    errorBuilder
        .responseCode(EXCEPTION_ERROR_CODES.getOrDefault(exc.getClass(), 500))
        .withType(exc.getClass().getSimpleName())
        .withMessage(exc.getMessage())
        .withStackTrace(exc);
  }

  private static Namespace namespaceFromPathVars(Map<String, String> pathVars) {
    return RESTUtil.decodeNamespace(pathVars.get("namespace"));
  }

  private static TableIdentifier tableIdentFromPathVars(Map<String, String> pathVars) {
    return TableIdentifier.of(
        namespaceFromPathVars(pathVars), RESTUtil.decodeString(pathVars.get("table")));
  }

  private static TableIdentifier viewIdentFromPathVars(Map<String, String> pathVars) {
    return TableIdentifier.of(
        namespaceFromPathVars(pathVars), RESTUtil.decodeString(pathVars.get("view")));
  }

  private static String planIDFromPathVars(Map<String, String> pathVars) {
    return RESTUtil.decodeString(pathVars.get("plan-id"));
  }

  private static SnapshotMode snapshotModeFromQueryParams(Map<String, String> queryParams) {
    return SnapshotMode.valueOf(
        queryParams
            .getOrDefault("snapshots", RESTCatalogProperties.SNAPSHOT_LOADING_MODE_DEFAULT)
            .toUpperCase(Locale.US));
  }
}
