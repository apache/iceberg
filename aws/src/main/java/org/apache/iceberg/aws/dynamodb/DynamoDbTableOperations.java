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
package org.apache.iceberg.aws.dynamodb;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.util.RetryDetector;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

class DynamoDbTableOperations extends BaseMetastoreTableOperations {

  private static final Logger LOG = LoggerFactory.getLogger(DynamoDbTableOperations.class);

  private final DynamoDbClient dynamo;
  private final AwsProperties awsProperties;
  private final TableIdentifier tableIdentifier;
  private final String fullTableName;
  private final FileIO fileIO;

  DynamoDbTableOperations(
      DynamoDbClient dynamo,
      AwsProperties awsProperties,
      String catalogName,
      FileIO fileIO,
      TableIdentifier tableIdentifier) {
    this.dynamo = dynamo;
    this.awsProperties = awsProperties;
    this.fullTableName = String.format("%s.%s", catalogName, tableIdentifier);
    this.tableIdentifier = tableIdentifier;
    this.fileIO = fileIO;
  }

  @Override
  protected String tableName() {
    return fullTableName;
  }

  @Override
  public FileIO io() {
    return fileIO;
  }

  @Override
  protected void doRefresh() {
    String metadataLocation = null;
    GetItemResponse table =
        dynamo.getItem(
            GetItemRequest.builder()
                .tableName(awsProperties.dynamoDbTableName())
                .consistentRead(true)
                .key(DynamoDbCatalog.tablePrimaryKey(tableIdentifier))
                .build());
    if (table.hasItem()) {
      metadataLocation = getMetadataLocation(table);
    } else {
      if (currentMetadataLocation() != null) {
        throw new NoSuchTableException(
            "Cannot find table %s after refresh, "
                + "maybe another process deleted it or revoked your access permission",
            tableName());
      }
    }

    refreshFromMetadataLocation(metadataLocation);
  }

  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    boolean newTable = base == null;
    String newMetadataLocation = writeNewMetadataIfRequired(newTable, metadata);
    CommitStatus commitStatus = CommitStatus.FAILURE;
    RetryDetector retryDetector = new RetryDetector();
    Map<String, AttributeValue> tableKey = DynamoDbCatalog.tablePrimaryKey(tableIdentifier);
    try {
      GetItemResponse table =
          dynamo.getItem(
              GetItemRequest.builder()
                  .tableName(awsProperties.dynamoDbTableName())
                  .consistentRead(true)
                  .key(tableKey)
                  .build());
      checkMetadataLocation(table, base);
      Map<String, String> properties = prepareProperties(table, newMetadataLocation);
      persistTable(tableKey, table, properties, retryDetector);
      commitStatus = CommitStatus.SUCCESS;
    } catch (CommitFailedException e) {
      // any explicit commit failures are passed up and out to the retry handler
      throw e;
    } catch (RuntimeException persistFailure) {
      boolean conditionCheckFailed = persistFailure instanceof ConditionalCheckFailedException;

      // If we got an exception we weren't expecting, or we got a ConditionalCheckFailedException
      // but retries were performed, attempt to reconcile the actual commit status.
      if (!conditionCheckFailed || retryDetector.retried()) {
        LOG.warn(
            "Received unexpected failure when committing to {}, validating if commit ended up succeeding.",
            fullTableName,
            persistFailure);
        commitStatus = checkCommitStatus(newMetadataLocation, metadata);
      }

      if (commitStatus != CommitStatus.SUCCESS && conditionCheckFailed) {
        throw new CommitFailedException(
            persistFailure, "Cannot commit %s: concurrent update detected", tableName());
      }

      switch (commitStatus) {
        case SUCCESS:
          break;
        case FAILURE:
          throw new CommitFailedException(
              persistFailure, "Cannot commit %s due to unexpected exception", tableName());
        case UNKNOWN:
          throw new CommitStateUnknownException(persistFailure);
      }
    } finally {
      try {
        if (commitStatus == CommitStatus.FAILURE) {
          // if anything went wrong, clean up the uncommitted metadata file
          io().deleteFile(newMetadataLocation);
        }
      } catch (RuntimeException e) {
        LOG.error("Failed to cleanup metadata file at {}", newMetadataLocation, e);
      }
    }
  }

  private void checkMetadataLocation(GetItemResponse table, TableMetadata base) {
    String dynamoMetadataLocation = table.hasItem() ? getMetadataLocation(table) : null;
    String baseMetadataLocation = base != null ? base.metadataFileLocation() : null;
    if (!Objects.equals(baseMetadataLocation, dynamoMetadataLocation)) {
      throw new CommitFailedException(
          "Cannot commit %s because base metadata location '%s' is not same as the current DynamoDb location '%s'",
          tableName(), baseMetadataLocation, dynamoMetadataLocation);
    }
  }

  private String getMetadataLocation(GetItemResponse table) {
    return table.item().get(DynamoDbCatalog.toPropertyCol(METADATA_LOCATION_PROP)).s();
  }

  private Map<String, String> prepareProperties(
      GetItemResponse response, String newMetadataLocation) {
    Map<String, String> properties =
        response.hasItem() ? getProperties(response) : Maps.newHashMap();
    properties.put(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(Locale.ENGLISH));
    properties.put(METADATA_LOCATION_PROP, newMetadataLocation);
    if (currentMetadataLocation() != null && !currentMetadataLocation().isEmpty()) {
      properties.put(PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation());
    }

    return properties;
  }

  private Map<String, String> getProperties(GetItemResponse table) {
    return table.item().entrySet().stream()
        .filter(e -> DynamoDbCatalog.isProperty(e.getKey()))
        .collect(
            Collectors.toMap(
                e -> DynamoDbCatalog.toPropertyKey(e.getKey()), e -> e.getValue().s()));
  }

  void persistTable(
      Map<String, AttributeValue> tableKey,
      GetItemResponse table,
      Map<String, String> parameters,
      RetryDetector retryDetector) {
    if (table.hasItem()) {
      LOG.debug("Committing existing DynamoDb catalog table: {}", tableName());
      List<String> updateParts = Lists.newArrayList();
      Map<String, String> attributeNames = Maps.newHashMap();
      Map<String, AttributeValue> attributeValues = Maps.newHashMap();
      int idx = 0;
      for (Map.Entry<String, String> property : parameters.entrySet()) {
        String attributeValue = ":v" + idx;
        String attributeKey = "#k" + idx;
        idx++;
        updateParts.add(attributeKey + " = " + attributeValue);
        attributeNames.put(attributeKey, DynamoDbCatalog.toPropertyCol(property.getKey()));
        attributeValues.put(
            attributeValue, AttributeValue.builder().s(property.getValue()).build());
      }
      DynamoDbCatalog.updateCatalogEntryMetadata(updateParts, attributeValues);
      String updateExpression = "SET " + DynamoDbCatalog.COMMA.join(updateParts);
      attributeValues.put(":v", table.item().get(DynamoDbCatalog.COL_VERSION));
      dynamo.updateItem(
          UpdateItemRequest.builder()
              .overrideConfiguration(c -> c.addMetricPublisher(retryDetector))
              .tableName(awsProperties.dynamoDbTableName())
              .key(tableKey)
              .conditionExpression(DynamoDbCatalog.COL_VERSION + " = :v")
              .updateExpression(updateExpression)
              .expressionAttributeValues(attributeValues)
              .expressionAttributeNames(attributeNames)
              .build());
    } else {
      LOG.debug("Committing new DynamoDb catalog table: {}", tableName());
      Map<String, AttributeValue> values = Maps.newHashMap(tableKey);
      parameters.forEach(
          (k, v) ->
              values.put(DynamoDbCatalog.toPropertyCol(k), AttributeValue.builder().s(v).build()));
      DynamoDbCatalog.setNewCatalogEntryMetadata(values);

      dynamo.putItem(
          PutItemRequest.builder()
              .overrideConfiguration(c -> c.addMetricPublisher(retryDetector))
              .tableName(awsProperties.dynamoDbTableName())
              .item(values)
              .conditionExpression("attribute_not_exists(" + DynamoDbCatalog.COL_VERSION + ")")
              .build());
    }
  }
}
