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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.Table;
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
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.exceptions.UnprocessableEntityException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PropertyUtil;

/** Adaptor class to translate REST requests into {@link Catalog} API calls. */
public class RESTCatalogAdapter implements RESTClient {
  private static final Splitter SLASH = Splitter.on('/');

  private static final Map<Class<? extends Exception>, Integer> EXCEPTION_ERROR_CODES =
      ImmutableMap.<Class<? extends Exception>, Integer>builder()
          .put(IllegalArgumentException.class, 400)
          .put(ValidationException.class, 400)
          .put(NamespaceNotEmptyException.class, 400) // TODO: should this be more specific?
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
          .buildOrThrow();

  private final Catalog catalog;
  private final SupportsNamespaces asNamespaceCatalog;
  private final ViewCatalog asViewCatalog;

  public RESTCatalogAdapter(Catalog catalog) {
    this.catalog = catalog;
    this.asNamespaceCatalog =
        catalog instanceof SupportsNamespaces ? (SupportsNamespaces) catalog : null;
    this.asViewCatalog = catalog instanceof ViewCatalog ? (ViewCatalog) catalog : null;
  }

  enum HTTPMethod {
    GET,
    HEAD,
    POST,
    DELETE
  }

  enum Route {
    TOKENS(HTTPMethod.POST, "v1/oauth/tokens", null, OAuthTokenResponse.class),
    SEPARATE_AUTH_TOKENS_URI(
        HTTPMethod.POST, "https://auth-server.com/token", null, OAuthTokenResponse.class),
    CONFIG(HTTPMethod.GET, "v1/config", null, ConfigResponse.class),
    LIST_NAMESPACES(
        HTTPMethod.GET, ResourcePaths.V1_NAMESPACES, null, ListNamespacesResponse.class),
    CREATE_NAMESPACE(
        HTTPMethod.POST,
        ResourcePaths.V1_NAMESPACES,
        CreateNamespaceRequest.class,
        CreateNamespaceResponse.class),
    LOAD_NAMESPACE(HTTPMethod.GET, ResourcePaths.V1_NAMESPACE, null, GetNamespaceResponse.class),
    DROP_NAMESPACE(HTTPMethod.DELETE, ResourcePaths.V1_NAMESPACE),
    UPDATE_NAMESPACE(
        HTTPMethod.POST,
        ResourcePaths.V1_NAMESPACE_PROPERTIES,
        UpdateNamespacePropertiesRequest.class,
        UpdateNamespacePropertiesResponse.class),
    LIST_TABLES(HTTPMethod.GET, ResourcePaths.V1_TABLES, null, ListTablesResponse.class),
    CREATE_TABLE(
        HTTPMethod.POST,
        ResourcePaths.V1_TABLES,
        CreateTableRequest.class,
        LoadTableResponse.class),
    LOAD_TABLE(HTTPMethod.GET, ResourcePaths.V1_TABLE, null, LoadTableResponse.class),
    REGISTER_TABLE(
        HTTPMethod.POST,
        ResourcePaths.V1_TABLE_REGISTER,
        RegisterTableRequest.class,
        LoadTableResponse.class),
    UPDATE_TABLE(
        HTTPMethod.POST, ResourcePaths.V1_TABLE, UpdateTableRequest.class, LoadTableResponse.class),
    DROP_TABLE(HTTPMethod.DELETE, ResourcePaths.V1_TABLE),
    RENAME_TABLE(HTTPMethod.POST, ResourcePaths.V1_TABLE_RENAME, RenameTableRequest.class, null),
    REPORT_METRICS(
        HTTPMethod.POST, ResourcePaths.V1_TABLE_METRICS, ReportMetricsRequest.class, null),
    COMMIT_TRANSACTION(
        HTTPMethod.POST,
        ResourcePaths.V1_TRANSACTIONS_COMMIT,
        CommitTransactionRequest.class,
        null),
    LIST_VIEWS(HTTPMethod.GET, ResourcePaths.V1_VIEWS, null, ListTablesResponse.class),
    LOAD_VIEW(HTTPMethod.GET, ResourcePaths.V1_VIEW, null, LoadViewResponse.class),
    CREATE_VIEW(
        HTTPMethod.POST, ResourcePaths.V1_VIEWS, CreateViewRequest.class, LoadViewResponse.class),
    UPDATE_VIEW(
        HTTPMethod.POST, ResourcePaths.V1_VIEW, UpdateTableRequest.class, LoadViewResponse.class),
    RENAME_VIEW(HTTPMethod.POST, ResourcePaths.V1_VIEW_RENAME, RenameTableRequest.class, null),
    DROP_VIEW(HTTPMethod.DELETE, ResourcePaths.V1_VIEW);

    private final HTTPMethod method;
    private final int requiredLength;
    private final Map<Integer, String> requirements;
    private final Map<Integer, String> variables;
    private final Class<? extends RESTRequest> requestClass;
    private final Class<? extends RESTResponse> responseClass;
    private final String resourcePath;

    Route(HTTPMethod method, String pattern) {
      this(method, pattern, null, null);
    }

    Route(
        HTTPMethod method,
        String pattern,
        Class<? extends RESTRequest> requestClass,
        Class<? extends RESTResponse> responseClass) {
      this.method = method;
      this.resourcePath = pattern;

      // parse the pattern into requirements and variables
      List<String> parts =
          SLASH.splitToList(pattern.replaceFirst("/v1/", "v1/").replace("/{prefix}", ""));
      ImmutableMap.Builder<Integer, String> requirementsBuilder = ImmutableMap.builder();
      ImmutableMap.Builder<Integer, String> variablesBuilder = ImmutableMap.builder();
      for (int pos = 0; pos < parts.size(); pos += 1) {
        String part = parts.get(pos);
        if (part.startsWith("{") && part.endsWith("}")) {
          variablesBuilder.put(pos, part.substring(1, part.length() - 1));
        } else {
          requirementsBuilder.put(pos, part);
        }
      }

      this.requestClass = requestClass;
      this.responseClass = responseClass;

      this.requiredLength = parts.size();
      this.requirements = requirementsBuilder.build();
      this.variables = variablesBuilder.build();
    }

    private boolean matches(HTTPMethod requestMethod, List<String> requestPath) {
      return method == requestMethod
          && requiredLength == requestPath.size()
          && requirements.entrySet().stream()
              .allMatch(
                  requirement ->
                      requirement
                          .getValue()
                          .equalsIgnoreCase(requestPath.get(requirement.getKey())));
    }

    private Map<String, String> variables(List<String> requestPath) {
      ImmutableMap.Builder<String, String> vars = ImmutableMap.builder();
      variables.forEach((key, value) -> vars.put(value, requestPath.get(key)));
      return vars.build();
    }

    public static Pair<Route, Map<String, String>> from(HTTPMethod method, String path) {
      List<String> parts = SLASH.splitToList(path);
      for (Route candidate : Route.values()) {
        if (candidate.matches(method, parts)) {
          return Pair.of(candidate, candidate.variables(parts));
        }
      }

      return null;
    }

    public Class<? extends RESTRequest> requestClass() {
      return requestClass;
    }

    public Class<? extends RESTResponse> responseClass() {
      return responseClass;
    }
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

  @SuppressWarnings({"MethodLength", "checkstyle:CyclomaticComplexity"})
  public <T extends RESTResponse> T handleRequest(
      Route route, Map<String, String> vars, Object body, Class<T> responseType) {
    switch (route) {
      case TOKENS:
        return castResponse(responseType, handleOAuthRequest(body));

      case CONFIG:
        return castResponse(
            responseType,
            ConfigResponse.builder()
                .withEndpoints(
                    Arrays.stream(Route.values())
                        .map(r -> Endpoint.create(r.method.name(), r.resourcePath))
                        .collect(Collectors.toList()))
                .build());

      case LIST_NAMESPACES:
        if (asNamespaceCatalog != null) {
          Namespace ns;
          if (vars.containsKey("parent")) {
            ns = RESTUtil.decodeNamespace(vars.get("parent"));
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
            return castResponse(
                responseType, CatalogHandlers.createTable(catalog, namespace, request));
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

      case LOAD_TABLE:
        {
          TableIdentifier ident = tableIdentFromPathVars(vars);
          return castResponse(responseType, CatalogHandlers.loadTable(catalog, ident));
        }

      case REGISTER_TABLE:
        {
          Namespace namespace = namespaceFromPathVars(vars);
          RegisterTableRequest request = castRequest(RegisterTableRequest.class, body);
          return castResponse(
              responseType, CatalogHandlers.registerTable(catalog, namespace, request));
        }

      case UPDATE_TABLE:
        {
          TableIdentifier ident = tableIdentFromPathVars(vars);
          UpdateTableRequest request = castRequest(UpdateTableRequest.class, body);
          return castResponse(responseType, CatalogHandlers.updateTable(catalog, ident, request));
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

  public <T extends RESTResponse> T execute(
      HTTPMethod method,
      String path,
      Map<String, String> queryParams,
      Object body,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    ErrorResponse.Builder errorBuilder = ErrorResponse.builder();
    Pair<Route, Map<String, String>> routeAndVars = Route.from(method, path);
    if (routeAndVars != null) {
      try {
        ImmutableMap.Builder<String, String> vars = ImmutableMap.builder();
        if (queryParams != null) {
          vars.putAll(queryParams);
        }
        vars.putAll(routeAndVars.second());

        return handleRequest(routeAndVars.first(), vars.build(), body, responseType);

      } catch (RuntimeException e) {
        configureResponseFromException(e, errorBuilder);
      }

    } else {
      errorBuilder
          .responseCode(400)
          .withType("BadRequestException")
          .withMessage(String.format("No route for request: %s %s", method, path));
    }

    ErrorResponse error = errorBuilder.build();
    errorHandler.accept(error);

    // if the error handler doesn't throw an exception, throw a generic one
    throw new RESTException("Unhandled error: %s", error);
  }

  @Override
  public <T extends RESTResponse> T delete(
      String path,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return execute(HTTPMethod.DELETE, path, null, null, responseType, headers, errorHandler);
  }

  @Override
  public <T extends RESTResponse> T delete(
      String path,
      Map<String, String> queryParams,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return execute(HTTPMethod.DELETE, path, queryParams, null, responseType, headers, errorHandler);
  }

  @Override
  public <T extends RESTResponse> T post(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return execute(HTTPMethod.POST, path, null, body, responseType, headers, errorHandler);
  }

  @Override
  public <T extends RESTResponse> T get(
      String path,
      Map<String, String> queryParams,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return execute(HTTPMethod.GET, path, queryParams, null, responseType, headers, errorHandler);
  }

  @Override
  public void head(String path, Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
    execute(HTTPMethod.HEAD, path, null, null, null, headers, errorHandler);
  }

  @Override
  public <T extends RESTResponse> T postForm(
      String path,
      Map<String, String> formData,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return execute(HTTPMethod.POST, path, null, formData, responseType, headers, errorHandler);
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
}
