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
package org.apache.iceberg.gcp.bigquery;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.HttpStatusCodes;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.util.Data;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetList;
import com.google.api.services.bigquery.model.DatasetList.Datasets;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.ExternalCatalogDatasetOptions;
import com.google.api.services.bigquery.model.ExternalCatalogTableOptions;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableList;
import com.google.api.services.bigquery.model.TableList.Tables;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.BaseServiceException;
import com.google.cloud.ExceptionHandler;
import com.google.cloud.bigquery.BigQueryErrorMessages;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.BigQueryRetryConfig;
import com.google.cloud.bigquery.BigQueryRetryHelper;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchIcebergTableException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.exceptions.ServiceUnavailableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;

/** A client of Google Bigquery Metastore functions over the BigQuery service. */
public final class BigQueryMetastoreClientImpl implements BigQueryMetastoreClient {

  private final Bigquery client;
  private final BigQueryOptions bigqueryOptions;

  public static final ExceptionHandler.Interceptor EXCEPTION_HANDLER_INTERCEPTOR =
      new ExceptionHandler.Interceptor() {

        @Override
        public RetryResult afterEval(Exception exception, RetryResult retryResult) {
          return ExceptionHandler.Interceptor.RetryResult.CONTINUE_EVALUATION;
        }

        @Override
        public RetryResult beforeEval(Exception exception) {
          if (exception instanceof BaseServiceException) {
            boolean retriable = ((BaseServiceException) exception).isRetryable();
            return retriable
                ? ExceptionHandler.Interceptor.RetryResult.RETRY
                : ExceptionHandler.Interceptor.RetryResult.CONTINUE_EVALUATION;
          }

          return ExceptionHandler.Interceptor.RetryResult.CONTINUE_EVALUATION;
        }
      };

  // Retry config with error messages and regex for rate limit exceeded errors.
  private static final BigQueryRetryConfig DEFAULT_RETRY_CONFIG =
      BigQueryRetryConfig.newBuilder()
          .retryOnMessage(BigQueryErrorMessages.RATE_LIMIT_EXCEEDED_MSG)
          .retryOnMessage(BigQueryErrorMessages.JOB_RATE_LIMIT_EXCEEDED_MSG)
          .retryOnRegEx(BigQueryErrorMessages.RetryRegExPatterns.RATE_LIMIT_EXCEEDED_REGEX)
          .build();

  public static final ExceptionHandler BIGQUERY_EXCEPTION_HANDLER =
      ExceptionHandler.newBuilder()
          .abortOn(RuntimeException.class)
          // Retry on connection failures due to transient network issues.
          .retryOn(java.net.ConnectException.class)
          // Retry to recover from temporary DNS resolution failures.
          .retryOn(java.net.UnknownHostException.class)
          .addInterceptors(EXCEPTION_HANDLER_INTERCEPTOR)
          .build();

  /** Constructs a client of the Google BigQuery service. */
  public BigQueryMetastoreClientImpl(BigQueryOptions options)
      throws IOException, GeneralSecurityException {
    // Initialize client that will be used to send requests. This client only needs to be created
    // once, and can be reused for multiple requests
    HttpCredentialsAdapter httpCredentialsAdapter =
        new HttpCredentialsAdapter(
            GoogleCredentials.getApplicationDefault().createScoped(BigqueryScopes.all()));
    this.client =
        new Bigquery.Builder(
                GoogleNetHttpTransport.newTrustedTransport(),
                GsonFactory.getDefaultInstance(),
                httpRequest -> {
                  httpCredentialsAdapter.initialize(httpRequest);
                  // Instead of throwing exceptions, analyze the HttpResponse object and inspect its
                  // status code. This will allow BigQuery API errors to be converted into Iceberg
                  // exceptions.
                  httpRequest.setThrowExceptionOnExecuteError(false);
                })
            .setApplicationName("BigQuery Metastore Iceberg Catalog Plugin")
            .build();
    this.bigqueryOptions = options;
  }

  @Override
  public Dataset create(Dataset dataset) {
    Dataset response = null;
    try {
      response =
          BigQueryRetryHelper.runWithRetries(
              () -> internalCreate(dataset),
              bigqueryOptions.getRetrySettings(),
              BIGQUERY_EXCEPTION_HANDLER,
              bigqueryOptions.getClock(),
              DEFAULT_RETRY_CONFIG,
              bigqueryOptions.isOpenTelemetryTracingEnabled(),
              bigqueryOptions.getOpenTelemetryTracer());
    } catch (BigQueryRetryHelper.BigQueryRetryHelperException e) {
      handleBigQueryRetryException(e);
    }
    return response;
  }

  private Dataset internalCreate(Dataset dataset) {
    try {
      HttpResponse response =
          client
              .datasets()
              .insert(dataset.getDatasetReference().getProjectId(), dataset)
              .executeUnparsed();
      return convertExceptionIfUnsuccessful(response).parseAs(Dataset.class);
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    } catch (AlreadyExistsException e) {
      throw new AlreadyExistsException("Namespace already exists: %s", dataset.getId());
    }
  }

  @Override
  public Dataset load(DatasetReference datasetReference) {
    try {
      HttpResponse response =
          client
              .datasets()
              .get(datasetReference.getProjectId(), datasetReference.getDatasetId())
              .executeUnparsed();
      if (response.getStatusCode() == HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
        throw new NoSuchNamespaceException(
            "Namespace does not exist: %s", datasetReference.getDatasetId());
      }

      return convertExceptionIfUnsuccessful(response).parseAs(Dataset.class);
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  @Override
  public void delete(DatasetReference datasetReference) {
    try {
      HttpResponse response =
          client
              .datasets()
              .delete(datasetReference.getProjectId(), datasetReference.getDatasetId())
              .executeUnparsed();
      if (response.getStatusCode() == HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
        throw new NoSuchNamespaceException(
            "Namespace does not exist: %s", datasetReference.getDatasetId());
      }

      convertExceptionIfUnsuccessful(response);
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    } catch (NamespaceNotEmptyException e) {
      throw new NamespaceNotEmptyException(
          "%s is not empty: %s", datasetReference.getDatasetId(), e.getMessage());
    }
  }

  @Override
  public boolean setParameters(DatasetReference datasetReference, Map<String, String> parameters) {
    Dataset dataset = load(datasetReference);
    ExternalCatalogDatasetOptions existingOptions = dataset.getExternalCatalogDatasetOptions();

    Map<String, String> existingParameters =
        (existingOptions == null || existingOptions.getParameters() == null)
            ? Maps.newHashMap() // Use HashMap to allow modification below
            : Maps.newHashMap(existingOptions.getParameters()); // Copy to compare later

    // Calculate what the new parameters would be.
    Map<String, String> newParameters = Maps.newHashMap(existingParameters);
    newParameters.putAll(parameters);

    if (Objects.equals(existingParameters, newParameters)) {
      // No change in parameters detected
      return false;
    }

    ExternalCatalogDatasetOptions optionsToUpdate =
        existingOptions == null ? new ExternalCatalogDatasetOptions() : existingOptions;

    dataset.setExternalCatalogDatasetOptions(optionsToUpdate.setParameters(newParameters));

    try {
      BigQueryRetryHelper.runWithRetries(
          () -> internalUpdate(dataset),
          bigqueryOptions.getRetrySettings(),
          BIGQUERY_EXCEPTION_HANDLER,
          bigqueryOptions.getClock(),
          DEFAULT_RETRY_CONFIG,
          bigqueryOptions.isOpenTelemetryTracingEnabled(),
          bigqueryOptions.getOpenTelemetryTracer());
    } catch (BigQueryRetryHelper.BigQueryRetryHelperException e) {
      handleBigQueryRetryException(e);
    }

    return true;
  }

  @Override
  public boolean removeParameters(DatasetReference datasetReference, Set<String> parameters) {
    Dataset dataset = load(datasetReference); // Throws NoSuchNamespaceException if not found.

    ExternalCatalogDatasetOptions existingOptions = dataset.getExternalCatalogDatasetOptions();

    if (existingOptions == null
        || existingOptions.getParameters() == null
        || existingOptions.getParameters().isEmpty()) {
      return false;
    }

    Map<String, String> existingParameters = Maps.newHashMap(existingOptions.getParameters());

    // Calculate the new parameters map.
    Map<String, String> newParameters = Maps.newHashMap(existingParameters);
    parameters.forEach(newParameters::remove);

    if (Objects.equals(existingParameters, newParameters)) {
      // No change in parameters detected (e.g., keys to remove didn't exist)
      return false;
    }

    dataset.setExternalCatalogDatasetOptions(existingOptions.setParameters(newParameters));

    try {
      BigQueryRetryHelper.runWithRetries(
          () -> internalUpdate(dataset),
          bigqueryOptions.getRetrySettings(),
          BIGQUERY_EXCEPTION_HANDLER,
          bigqueryOptions.getClock(),
          DEFAULT_RETRY_CONFIG,
          bigqueryOptions.isOpenTelemetryTracingEnabled(),
          bigqueryOptions.getOpenTelemetryTracer());
    } catch (BigQueryRetryHelper.BigQueryRetryHelperException e) {
      handleBigQueryRetryException(e);
    }
    return true;
  }

  @Override
  public List<Datasets> list(String projectId) {
    try {
      String nextPageToken = null;
      List<Datasets> datasets = Lists.newArrayList();
      do {
        HttpResponse pageResponse =
            client.datasets().list(projectId).setPageToken(nextPageToken).executeUnparsed();
        DatasetList result =
            convertExceptionIfUnsuccessful(pageResponse).parseAs(DatasetList.class);
        nextPageToken = result.getNextPageToken();
        if (result.getDatasets() != null) {
          datasets.addAll(result.getDatasets());
        }

      } while (nextPageToken != null && !nextPageToken.isEmpty());
      return datasets;
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  @Override
  public Table create(Table table) {
    // Ensure it is an Iceberg table supported by the BigQuery metastore catalog.
    validateTable(table);
    // TODO: Ensure table creation is idempotent when handling retries.
    Table response = null;
    try {
      response =
          BigQueryRetryHelper.runWithRetries(
              () -> internalCreate(table),
              bigqueryOptions.getRetrySettings(),
              BIGQUERY_EXCEPTION_HANDLER,
              bigqueryOptions.getClock(),
              DEFAULT_RETRY_CONFIG,
              bigqueryOptions.isOpenTelemetryTracingEnabled(),
              bigqueryOptions.getOpenTelemetryTracer());
    } catch (BigQueryRetryHelper.BigQueryRetryHelperException e) {
      handleBigQueryRetryException(e);
    }

    return response;
  }

  private Table internalCreate(Table table) {
    try {
      HttpResponse response =
          client
              .tables()
              .insert(
                  Preconditions.checkNotNull(table.getTableReference()).getProjectId(),
                  Preconditions.checkNotNull(table.getTableReference()).getDatasetId(),
                  table)
              .executeUnparsed();
      return convertExceptionIfUnsuccessful(response).parseAs(Table.class);
    } catch (IOException e) {
      throw new RuntimeIOException("%s", e);
    } catch (AlreadyExistsException e) {
      throw new AlreadyExistsException(e, "Table already exists: %s", table);
    }
  }

  @Override
  public Table load(TableReference tableReference) {
    try {
      HttpResponse response =
          client
              .tables()
              .get(
                  tableReference.getProjectId(),
                  tableReference.getDatasetId(),
                  tableReference.getTableId())
              .executeUnparsed();
      if (response.getStatusCode() == HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
        throw new NoSuchTableException("%s", response.getStatusMessage());
      }

      return validateTable(convertExceptionIfUnsuccessful(response).parseAs(Table.class));
    } catch (IOException e) {
      throw new RuntimeIOException("%s", e);
    }
  }

  @Override
  public Table update(TableReference tableReference, Table table) {
    // Ensure it is an Iceberg table supported by the BQ metastore catalog.
    validateTable(table);

    ExternalCatalogTableOptions newExternalCatalogTableOptions =
        new ExternalCatalogTableOptions()
            .setStorageDescriptor(table.getExternalCatalogTableOptions().getStorageDescriptor())
            .setConnectionId(table.getExternalCatalogTableOptions().getConnectionId())
            .setParameters(table.getExternalCatalogTableOptions().getParameters());
    Table updatedTable =
        new Table()
            .setExternalCatalogTableOptions(newExternalCatalogTableOptions)
            // Must set the schema as null for using schema auto-detect.
            .setSchema(Data.nullOf(TableSchema.class));

    Table response = null;
    try {
      response =
          BigQueryRetryHelper.runWithRetries(
              () -> internalUpdate(tableReference, updatedTable, table.getEtag()),
              bigqueryOptions.getRetrySettings(),
              BIGQUERY_EXCEPTION_HANDLER,
              bigqueryOptions.getClock(),
              DEFAULT_RETRY_CONFIG,
              bigqueryOptions.isOpenTelemetryTracingEnabled(),
              bigqueryOptions.getOpenTelemetryTracer());
    } catch (BigQueryRetryHelper.BigQueryRetryHelperException e) {
      handleBigQueryRetryException(e);
    }

    return response;
  }

  private Table internalUpdate(TableReference tableReference, Table table, String etag) {
    try {
      HttpResponse response =
          client
              .tables()
              .patch(
                  tableReference.getProjectId(),
                  tableReference.getDatasetId(),
                  tableReference.getTableId(),
                  table)
              .setRequestHeaders(new HttpHeaders().setIfMatch(etag))
              .executeUnparsed();

      if (response.getStatusCode() == HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
        String responseString = response.parseAsString();
        if (responseString.toLowerCase(Locale.ENGLISH).contains("not found: connection")) {
          throw new BadRequestException("%s", responseString);
        }

        throw new NoSuchTableException("%s", response.getStatusMessage());
      }

      return convertExceptionIfUnsuccessful(response).parseAs(Table.class);
    } catch (IOException e) {
      throw new RuntimeIOException("%s", e);
    }
  }

  @Override
  public void delete(TableReference tableReference) {
    try {
      load(tableReference); // Fetching it to validate it is a BigQuery Metastore table first

      HttpResponse response =
          client
              .tables()
              .delete(
                  tableReference.getProjectId(),
                  tableReference.getDatasetId(),
                  tableReference.getTableId())
              .executeUnparsed();

      if (response.getStatusCode() == HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
        throw new NoSuchTableException("%s", response.getStatusMessage());
      }

      convertExceptionIfUnsuccessful(response);
    } catch (IOException e) {
      throw new RuntimeIOException("%s", e);
    }
  }

  @Override
  public List<Tables> list(DatasetReference datasetReference, boolean listAllTables) {
    try {
      String nextPageToken = null;
      Stream<Tables> tablesStream = Stream.empty();
      do {
        HttpResponse pageResponse =
            client
                .tables()
                .list(datasetReference.getProjectId(), datasetReference.getDatasetId())
                .setPageToken(nextPageToken)
                .executeUnparsed();
        if (pageResponse.getStatusCode() == HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
          throw new NoSuchNamespaceException("%s", pageResponse.getStatusMessage());
        }
        TableList result = convertExceptionIfUnsuccessful(pageResponse).parseAs(TableList.class);
        nextPageToken = result.getNextPageToken();
        List<Tables> tablesPage = result.getTables();
        Stream<Tables> tablesPageStream =
            tablesPage == null ? Stream.empty() : result.getTables().stream();
        tablesStream = Stream.concat(tablesStream, tablesPageStream);
      } while (nextPageToken != null && !nextPageToken.isEmpty());

      // The server should return more metadata here (e.g. BigQuery Non Iceberg tables) to
      // distinguish Iceberg
      // tables for us to filter out those results since invoking `getTable` on them would
      // correctly raise a `NoSuchIcebergTableException` for being inoperable by this plugin.

      List<Tables> allTables = tablesStream.collect(Collectors.toList());

      if (!listAllTables) {
        List<Tables> validTables = Collections.synchronizedList(Lists.newArrayList());
        Tasks.foreach(allTables)
            .executeWith(ThreadPools.getWorkerPool())
            .noRetry()
            .suppressFailureWhenFinished()
            .run(
                table -> {
                  try {
                    load(table.getTableReference());
                    validTables.add(table);
                  } catch (NoSuchTableException e) {
                    // Silently ignore tables that are not valid Iceberg tables
                    // This is expected behavior as we're filtering out non-Iceberg tables
                  }
                });

        return validTables;
      } else {
        return allTables;
      }
    } catch (IOException e) {
      throw new RuntimeIOException("%s", e);
    }
  }

  private Dataset internalUpdate(Dataset dataset) {
    Preconditions.checkArgument(
        dataset.getDatasetReference() != null, "Dataset Reference can not be null!");
    Preconditions.checkArgument(
        dataset.getDatasetReference().getDatasetId() != null, "Dataset Id can not be null!");

    try {
      HttpResponse response =
          client
              .datasets()
              .update(
                  dataset.getDatasetReference().getProjectId(),
                  dataset.getDatasetReference().getDatasetId(),
                  dataset)
              .setRequestHeaders(new HttpHeaders().setIfMatch(dataset.getEtag()))
              .executeUnparsed();
      if (response.getStatusCode() == HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
        throw new NoSuchNamespaceException("%s", response.getStatusMessage());
      }

      return convertExceptionIfUnsuccessful(response).parseAs(Dataset.class);
    } catch (IOException e) {
      throw new RuntimeIOException("%s", e);
    }
  }

  //  private Dataset internalUpdate(Dataset dataset) {
  //    Preconditions.checkArgument(
  //        dataset.getDatasetReference() != null, "Dataset Reference can not be null!");
  //    Preconditions.checkArgument(
  //        dataset.getDatasetReference().getDatasetId() != null, "Dataset Id can not be null!");
  //
  //    try {
  //      HttpResponse response =
  //          client
  //              .datasets()
  //              .update(
  //                  dataset.getDatasetReference().getProjectId(),
  //                  dataset.getDatasetReference().getDatasetId(),
  //                  dataset)
  //              .setRequestHeaders(new HttpHeaders().setIfMatch(dataset.getEtag()))
  //              .executeUnparsed();
  //      if (response.getStatusCode() == HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
  //        throw new NoSuchNamespaceException("%s", response.getStatusMessage());
  //      }
  //
  //      return convertExceptionIfUnsuccessful(response).parseAs(Dataset.class);
  //    } catch (IOException e) {
  //      throw new RuntimeIOException("%s", e);
  //    }
  //  }

  /**
   * Checks if the given table represents a BigQuery Metastore Iceberg table. A table is considered
   * an Iceberg table if it has ExternalCatalogTableOptions, a non-empty parameters map containing
   * both METADATA_LOCATION_PROP and TABLE_TYPE_PROP with the value ICEBERG_TABLE_TYPE_VALUE.
   *
   * @param table The table to check.
   * @return true if the table is a BigQuery Metastore Iceberg table, false otherwise.
   */
  private static boolean isValidIcebergTable(Table table) {
    if (table.getExternalCatalogTableOptions() == null
        || table.getExternalCatalogTableOptions().isEmpty()
        || table.getExternalCatalogTableOptions().getParameters() == null) {
      return false;
    }

    java.util.Map<String, String> parameters =
        table.getExternalCatalogTableOptions().getParameters();

    if (!parameters.containsKey(BaseMetastoreTableOperations.METADATA_LOCATION_PROP)
        || !parameters.containsKey(BaseMetastoreTableOperations.TABLE_TYPE_PROP)) {
      return false;
    }

    String tableType = parameters.get(BaseMetastoreTableOperations.TABLE_TYPE_PROP);
    return BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(tableType);
  }

  private Table validateTable(Table table) {
    if (!isValidIcebergTable(table)) {
      throw new NoSuchIcebergTableException("This table is not a valid Iceberg table: %s", table);
    }

    return table;
  }

  /**
   * Converts BigQuery generic API errors to Iceberg exceptions, *without* handling the
   * resource-specific exceptions like NoSuchTableException, NoSuchNamespaceException, etc.
   */
  private static HttpResponse convertExceptionIfUnsuccessful(HttpResponse response)
      throws IOException {
    if (response.isSuccessStatusCode()) {
      return response;
    }

    GoogleJsonResponseException exception =
        GoogleJsonResponseException.from(GsonFactory.getDefaultInstance(), response);
    String errorMessage =
        exception.getStatusMessage()
            + (exception.getContent() != null ? "\n" + exception.getContent() : "");

    switch (response.getStatusCode()) {
      case HttpStatusCodes.STATUS_CODE_UNAUTHORIZED:
        throw new NotAuthorizedException(
            "Not authorized to call the BigQuery API or access this resource: %s", errorMessage);
      case HttpStatusCodes.STATUS_CODE_BAD_REQUEST:
        GoogleJsonError errorDetails = exception.getDetails();
        if (errorDetails != null) {
          List<GoogleJsonError.ErrorInfo> errors = errorDetails.getErrors();
          if (errors != null) {
            for (GoogleJsonError.ErrorInfo errorInfo : errors) {
              if (errorInfo.getReason().equals("resourceInUse")) {
                throw new NamespaceNotEmptyException("%s", errorInfo.getMessage());
              }
            }
          }
        }
        throw new BadRequestException("%s", errorMessage);
      case HttpStatusCodes.STATUS_CODE_FORBIDDEN:
        throw new ForbiddenException("%s", errorMessage);
      case HttpStatusCodes.STATUS_CODE_PRECONDITION_FAILED:
        throw new ValidationException("%s", errorMessage);
      case HttpStatusCodes.STATUS_CODE_NOT_FOUND:
        throw new IllegalArgumentException(errorMessage);
      case HttpStatusCodes.STATUS_CODE_SERVER_ERROR:
        throw new ServiceFailureException("%s", errorMessage);
      case HttpStatusCodes.STATUS_CODE_SERVICE_UNAVAILABLE:
        throw new ServiceUnavailableException("%s", errorMessage);
      case HttpStatusCodes.STATUS_CODE_CONFLICT:
        throw new AlreadyExistsException("%s", errorMessage);
      default:
        throw new HttpResponseException(response);
    }
  }

  /**
   * Translates BigQueryRetryHelperException to the RuntimeException that caused the error. This
   * method will always throw an exception.
   */
  private static void handleBigQueryRetryException(
      BigQueryRetryHelper.BigQueryRetryHelperException retryException) {
    Throwable cause = retryException.getCause();
    String message = retryException.getMessage();
    if (cause instanceof RuntimeException) {
      throw (RuntimeException) cause;
    } else {
      throw new RuntimeException(message, cause);
    }
  }
}
