/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.aws.glue.util;

import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.PartitionError;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.iceberg.aws.glue.converters.CatalogToHiveConverter;
import org.apache.iceberg.aws.glue.converters.GlueInputConverter;
import org.apache.iceberg.aws.glue.metastore.AWSGlueMetastore;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("Slf4jConstantLogMessage")
public final class BatchCreatePartitionsHelper {

  private static final Logger LOG = LoggerFactory.getLogger(BatchCreatePartitionsHelper.class);

  private final AWSGlueMetastore glueClient;
  private final String databaseName;
  private final String tableName;
  private final List<Partition> partitions;
  private final boolean ifNotExists;
  private Map<PartitionKey, Partition> partitionMap;
  private List<Partition> partitionsFailed;
  private TException firstTException;
  private String catalogId;

  public BatchCreatePartitionsHelper(
      AWSGlueMetastore glueClient,
      String databaseName,
      String tableName,
      String catalogId,
      List<Partition> partitions,
      boolean ifNotExists) {
    this.glueClient = glueClient;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.catalogId = catalogId;
    this.partitions = partitions;
    this.ifNotExists = ifNotExists;
  }

  public BatchCreatePartitionsHelper createPartitions() {
    partitionMap = PartitionUtils.buildPartitionMap(partitions);
    partitionsFailed = Lists.newArrayList();

    try {
      List<PartitionError> result =
              glueClient.createPartitions(databaseName, tableName,
                      GlueInputConverter.convertToPartitionInputs(partitionMap.values()));
      processResult(result);
    } catch (Exception e) {
      LOG.error("Exception thrown while creating partitions in DataCatalog: ", e);
      firstTException = CatalogToHiveConverter.wrapInHiveException(e);
      if (PartitionUtils.isInvalidUserInputException(e)) {
        setAllFailed();
      } else {
        checkIfPartitionsCreated();
      }
    }
    return this;
  }

  private void setAllFailed() {
    partitionsFailed = partitions;
    partitionMap.clear();
  }

  private void processResult(List<PartitionError> partitionErrors) {
    if (partitionErrors == null || partitionErrors.isEmpty()) {
      return;
    }

    LOG.error("BatchCreatePartitions failed to create {} out of {} partitions.",
        partitionErrors.size(), partitionMap.size());

    for (PartitionError partitionError : partitionErrors) {
      Partition partitionFailed = partitionMap.remove(new PartitionKey(partitionError.getPartitionValues()));

      TException exception = CatalogToHiveConverter.errorDetailToHiveException(partitionError.getErrorDetail());
      if (ifNotExists && exception instanceof AlreadyExistsException) {
        // AlreadyExistsException is allowed, so we shouldn't add the partition to partitionsFailed list
        continue;
      }
      LOG.error("encountered partition error", exception);
      if (firstTException == null) {
        firstTException = exception;
      }
      partitionsFailed.add(partitionFailed);
    }
  }

  private void checkIfPartitionsCreated() {
    for (Partition partition : partitions) {
      if (!partitionExists(partition)) {
        partitionsFailed.add(partition);
        partitionMap.remove(new PartitionKey(partition));
      }
    }
  }

  private boolean partitionExists(Partition partition) {
    try {
      Partition partitionReturned = glueClient.getPartition(databaseName, tableName, partition.getValues());
      return partitionReturned != null; // probably always true here
    } catch (EntityNotFoundException e) {
      // here we assume namespace and table exist. It is assured by calling "isInvalidUserInputException" method above
      return false;
    } catch (Exception e) {
      String message = String.format("Get partition request %s failed.",
          StringUtils.join(partition.getValues(), "/"));
      LOG.error(message, e);
      // partition status unknown, we assume that the partition was not created
      return false;
    }
  }

  public TException getFirstTException() {
    return firstTException;
  }

  public Collection<Partition> getPartitionsCreated() {
    return partitionMap.values();
  }

  public List<Partition> getPartitionsFailed() {
    return partitionsFailed;
  }

}
