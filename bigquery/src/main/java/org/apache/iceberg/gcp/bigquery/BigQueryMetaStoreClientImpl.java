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
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.ForbiddenException;
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

/** A client of Google Bigquery Metastore functions over the BigQuery service. */
public final class BigQueryMetaStoreClientImpl implements BigQueryMetaStoreClient {

  public static final ExceptionHandler.Interceptor EXCEPTION_HANDLER_INTERCEPTOR =
      new ExceptionHandler.Interceptor() {

        private static final long serialVersionUID = -8429573486870467828L;

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

  private final Bigquery client;
  private final BigQueryOptions bigqueryOptions;

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
          .retryOn(java.net.ConnectException.class) // retry on Connection Exception
          .retryOn(java.net.UnknownHostException.class) // retry on UnknownHostException
          .addInterceptors(EXCEPTION_HANDLER_INTERCEPTOR)
          .build();

  /** Constructs a client of the Google BigQuery service. */
  public BigQueryMetaStoreClientImpl(BigQueryOptions options)
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
  public Dataset createDataset(Dataset dataset) {
    // TODO (b/399885863): Ensure Dataset creation is idempotent when handling retries.
    Dataset response = null;
    try {
      response =
          BigQueryRetryHelper.runWithRetries(
              new Callable<Dataset>() {
                @Override
                public Dataset call() {
                  return create(dataset);
                }
              },
              bigqueryOptions.getRetrySettings(),
              BIGQUERY_EXCEPTION_HANDLER,
              bigqueryOptions.getClock(),
              DEFAULT_RETRY_CONFIG);
    } catch (BigQueryRetryHelper.BigQueryRetryHelperException e) {
      handleBigQueryRetryException(e);
    }
    return response;
  }

  @SuppressWarnings("FormatStringAnnotation")
  private Dataset create(Dataset dataset) {
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
      throw new AlreadyExistsException("Namespace already exists: " + dataset.getId());
    }
  }

  @Override
  @SuppressWarnings("FormatStringAnnotation")
  public Dataset getDataset(DatasetReference datasetReference) {
    try {
      HttpResponse response =
          client
              .datasets()
              .get(datasetReference.getProjectId(), datasetReference.getDatasetId())
              .executeUnparsed();
      if (response.getStatusCode() == HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
        throw new NoSuchNamespaceException(
            "Namespace does not exist: " + datasetReference.getDatasetId());
      }
      return convertExceptionIfUnsuccessful(response).parseAs(Dataset.class);
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  @Override
  @SuppressWarnings("FormatStringAnnotation")
  public void deleteDataset(DatasetReference datasetReference) {
    try {
      HttpResponse response =
          client
              .datasets()
              .delete(datasetReference.getProjectId(), datasetReference.getDatasetId())
              .executeUnparsed();
      if (response.getStatusCode() == HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
        throw new NoSuchNamespaceException(
            "Namespace does not exist: " + datasetReference.getDatasetId());
      }
      convertExceptionIfUnsuccessful(response);
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  @Override
  public Dataset setDatasetParameters(
      DatasetReference datasetReference, Map<String, String> parameters) {
    Dataset dataset = getDataset(datasetReference);
    ExternalCatalogDatasetOptions externalCatalogDatasetOptions =
        dataset.getExternalCatalogDatasetOptions() == null
            ? new ExternalCatalogDatasetOptions()
            : dataset.getExternalCatalogDatasetOptions();
    Map<String, String> finalParameters =
        externalCatalogDatasetOptions.getParameters() == null
            ? Maps.newHashMap()
            : externalCatalogDatasetOptions.getParameters();
    finalParameters.putAll(parameters);

    dataset.setExternalCatalogDatasetOptions(
        externalCatalogDatasetOptions.setParameters(finalParameters));

    return updateDataset(dataset);
  }

  @Override
  public Dataset removeDatasetParameters(
      DatasetReference datasetReference, Set<String> parameters) {
    Dataset dataset = getDataset(datasetReference);
    ExternalCatalogDatasetOptions externalCatalogDatasetOptions =
        dataset.getExternalCatalogDatasetOptions() == null
            ? new ExternalCatalogDatasetOptions()
            : dataset.getExternalCatalogDatasetOptions();
    Map<String, String> finalParameters =
        externalCatalogDatasetOptions.getParameters() == null
            ? Maps.newHashMap()
            : externalCatalogDatasetOptions.getParameters();
    parameters.forEach(finalParameters::remove);

    dataset.setExternalCatalogDatasetOptions(
        externalCatalogDatasetOptions.setParameters(finalParameters));

    return updateDataset(dataset);
  }

  @Override
  public List<Datasets> listDatasets(String projectId) {
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
  public Table createTable(Table table) {
    // Ensure it is an Iceberg table supported by the BigQuery metastore catalog.
    validateTable(table);
    // TODO (b/399885863): Ensure table creation is idempotent when handling retries.
    Table response = null;
    try {
      response =
          BigQueryRetryHelper.runWithRetries(
              new Callable<Table>() {
                @Override
                public Table call() {
                  return create(table);
                }
              },
              bigqueryOptions.getRetrySettings(),
              BIGQUERY_EXCEPTION_HANDLER,
              bigqueryOptions.getClock(),
              DEFAULT_RETRY_CONFIG);
    } catch (BigQueryRetryHelper.BigQueryRetryHelperException e) {
      handleBigQueryRetryException(e);
    }
    return response;
  }

  @SuppressWarnings("FormatStringAnnotation")
  private Table create(Table table) {
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
      throw new RuntimeIOException(e);
    } catch (AlreadyExistsException e) {
      throw new AlreadyExistsException("Table already exists", e);
    }
  }

  @Override
  @SuppressWarnings("FormatStringAnnotation")
  public Table getTable(TableReference tableReference) {
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
        throw new NoSuchTableException(response.getStatusMessage());
      }
      return validateTable(convertExceptionIfUnsuccessful(response).parseAs(Table.class));
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  @Override
  @SuppressWarnings("FormatStringAnnotation")
  public Table patchTable(TableReference tableReference, Table table) {
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
              new Callable<Table>() {
                @Override
                public Table call() {
                  return patch(tableReference, updatedTable, table.getEtag());
                }
              },
              bigqueryOptions.getRetrySettings(),
              BIGQUERY_EXCEPTION_HANDLER,
              bigqueryOptions.getClock(),
              DEFAULT_RETRY_CONFIG);
    } catch (BigQueryRetryHelper.BigQueryRetryHelperException e) {
      handleBigQueryRetryException(e);
    }
    return response;
  }

  @SuppressWarnings("FormatStringAnnotation")
  private Table patch(TableReference tableReference, Table table, String etag) {
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
        if (responseString.toLowerCase(Locale.ROOT).contains("not found: connection")) {
          throw new BadRequestException(responseString);
        }
        throw new NoSuchTableException(response.getStatusMessage());
      }
      return convertExceptionIfUnsuccessful(response).parseAs(Table.class);
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  @Override
  @SuppressWarnings("FormatStringAnnotation")
  public Table renameTable(TableReference tableToRename, String newTableId) {
    Table table = getTable(tableToRename); // Verify table first
    Table patch =
        new Table()
            .setTableReference(
                new TableReference()
                    .setProjectId(table.getTableReference().getProjectId())
                    .setDatasetId(table.getTableReference().getDatasetId())
                    .setTableId(newTableId));

    try {
      HttpResponse response =
          client
              .tables()
              .patch(
                  tableToRename.getProjectId(),
                  tableToRename.getDatasetId(),
                  tableToRename.getTableId(),
                  patch)
              .setRequestHeaders(new HttpHeaders().setIfMatch(table.getEtag()))
              .executeUnparsed();

      if (response.getStatusCode() == HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
        throw new NoSuchTableException(response.getStatusMessage());
      }
      return convertExceptionIfUnsuccessful(response).parseAs(Table.class);
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  @Override
  @SuppressWarnings("FormatStringAnnotation")
  public void deleteTable(TableReference tableReference) {
    try {
      getTable(tableReference); // Fetching it to validate it is a BigQuery Metastore table first

      HttpResponse response =
          client
              .tables()
              .delete(
                  tableReference.getProjectId(),
                  tableReference.getDatasetId(),
                  tableReference.getTableId())
              .executeUnparsed();

      if (response.getStatusCode() == HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
        throw new NoSuchTableException(response.getStatusMessage());
      }
      convertExceptionIfUnsuccessful(response);
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  @Override
  @SuppressWarnings("FormatStringAnnotation")
  public List<Tables> listTables(
      DatasetReference datasetReference, boolean filterUnsupportedTables) {
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
          throw new NoSuchNamespaceException(pageResponse.getStatusMessage());
        }
        TableList result = convertExceptionIfUnsuccessful(pageResponse).parseAs(TableList.class);
        nextPageToken = result.getNextPageToken();
        List<Tables> tablesPage = result.getTables();
        Stream<Tables> tablesPageStream =
            tablesPage == null ? Stream.empty() : result.getTables().stream();
        tablesStream = Stream.concat(tablesStream, tablesPageStream);
      } while (nextPageToken != null && !nextPageToken.isEmpty());

      // TODO(b/345839927): The server should return more metadata here to distinguish Iceberg
      // BQMS tables for us to filter out those results since invoking `getTable` on them would
      // correctly raise a `NoSuchIcebergTableException` for being inoperable by this plugin.
      if (filterUnsupportedTables) {
        tablesStream =
            tablesStream
                .parallel()
                .filter(
                    table -> {
                      try {
                        getTable(table.getTableReference());
                      } catch (NoSuchTableException e) {
                        return false;
                      }
                      return true;
                    });
      }

      return tablesStream.collect(Collectors.toList());
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  @SuppressWarnings("FormatStringAnnotation")
  private Dataset updateDataset(Dataset dataset) {
    try {
      HttpResponse response =
          client
              .datasets()
              .update(
                  Preconditions.checkNotNull(dataset.getDatasetReference()).getProjectId(),
                  Preconditions.checkNotNull(dataset.getDatasetReference().getDatasetId()),
                  dataset)
              .setRequestHeaders(new HttpHeaders().setIfMatch(dataset.getEtag()))
              .executeUnparsed();
      if (response.getStatusCode() == HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
        throw new NoSuchNamespaceException(response.getStatusMessage());
      }
      return convertExceptionIfUnsuccessful(response).parseAs(Dataset.class);
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  /**
   * Returns true when it is a BigQuery Metastore Iceberg table, defined by having the
   * ExternalCatalogTableOptions object and a parameter of METADATA_LOCATION_PROP as part of its
   * parameters map.
   *
   * @param table to check
   */
  private boolean isValidIcebergTable(Table table) {
    return table.getExternalCatalogTableOptions() != null
        && !table.getExternalCatalogTableOptions().isEmpty()
        && table.getExternalCatalogTableOptions().getParameters() != null
        && table
            .getExternalCatalogTableOptions()
            .getParameters()
            .containsKey(BaseMetastoreTableOperations.METADATA_LOCATION_PROP)
        && BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(
            table
                .getExternalCatalogTableOptions()
                .getParameters()
                .get(BaseMetastoreTableOperations.TABLE_TYPE_PROP));
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
  @SuppressWarnings("FormatStringAnnotation")
  private HttpResponse convertExceptionIfUnsuccessful(HttpResponse response) throws IOException {
    if (response.isSuccessStatusCode()) {
      return response;
    }

    String errorMessage =
        response.getStatusMessage()
            + (response.getContent() != null
                ? "\n" + new String(response.getContent().readAllBytes(), StandardCharsets.UTF_8)
                : "");

    switch (response.getStatusCode()) {
      case HttpStatusCodes.STATUS_CODE_UNAUTHORIZED:
        throw new NotAuthorizedException(
            errorMessage, "Not authorized to call the BigQuery API or access this resource");
      case HttpStatusCodes.STATUS_CODE_BAD_REQUEST:
        throw new BadRequestException(errorMessage);
      case HttpStatusCodes.STATUS_CODE_FORBIDDEN:
        throw new ForbiddenException(errorMessage);
      case HttpStatusCodes.STATUS_CODE_PRECONDITION_FAILED:
        throw new ValidationException(errorMessage);
      case HttpStatusCodes.STATUS_CODE_NOT_FOUND:
        throw new IllegalArgumentException(errorMessage);
      case HttpStatusCodes.STATUS_CODE_SERVER_ERROR:
        throw new ServiceFailureException(errorMessage);
      case HttpStatusCodes.STATUS_CODE_SERVICE_UNAVAILABLE:
        throw new ServiceUnavailableException(errorMessage);
      case HttpStatusCodes.STATUS_CODE_CONFLICT:
        throw new AlreadyExistsException(errorMessage);
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
