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

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.BatchDeletePartitionRequest;
import com.amazonaws.services.glue.model.BatchDeletePartitionResult;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.ErrorDetail;
import com.amazonaws.services.glue.model.GetPartitionRequest;
import com.amazonaws.services.glue.model.GetPartitionResult;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.PartitionError;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.aws.glue.converters.CatalogToHiveConverter;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BatchDeletePartitionsHelper {
  private static final Logger LOG = LoggerFactory.getLogger(BatchDeletePartitionsHelper.class);

  private final AWSGlue client;
  private final String namespaceName;
  private final String tableName;
  private final String catalogId;
  private final List<Partition> partitions;
  private Map<PartitionKey, Partition> partitionMap;
  private TException firstTException;

  public BatchDeletePartitionsHelper(AWSGlue client, String namespaceName, String tableName,
                                     String catalogId, List<Partition> partitions) {
    this.client = client;
    this.namespaceName = namespaceName;
    this.tableName = tableName;
    this.catalogId = catalogId;
    this.partitions = partitions;
  }

  public BatchDeletePartitionsHelper deletePartitions() {
    partitionMap = PartitionUtils.buildPartitionMap(partitions);

    BatchDeletePartitionRequest request = new BatchDeletePartitionRequest().withDatabaseName(namespaceName)
        .withTableName(tableName).withCatalogId(catalogId)
        .withPartitionsToDelete(PartitionUtils.getPartitionValuesList(partitionMap));

    try {
      BatchDeletePartitionResult result = client.batchDeletePartition(request);
      processResult(result);
    } catch (Exception e) {
      LOG.error("Exception thrown while deleting partitions in DataCatalog: ", e);
      firstTException = CatalogToHiveConverter.wrapInHiveException(e);
      if (PartitionUtils.isInvalidUserInputException(e)) {
        setAllFailed();
      } else {
        checkIfPartitionsDeleted();
      }
    }
    return this;
  }

  private void setAllFailed() {
    partitionMap.clear();
  }

  private void processResult(final BatchDeletePartitionResult batchDeletePartitionsResult) {
    List<PartitionError> partitionErrors = batchDeletePartitionsResult.getErrors();
    if (partitionErrors == null || partitionErrors.isEmpty()) {
      return;
    }

    LOG.error("BatchDeletePartitions failed to delete {} out of {} partitions.",
        partitionErrors.size(), partitionMap.size());

    for (PartitionError partitionError : partitionErrors) {
      partitionMap.remove(new PartitionKey(partitionError.getPartitionValues()));
      ErrorDetail errorDetail = partitionError.getErrorDetail();
      LOG.error("partition error {}", partitionError);
      if (firstTException == null) {
        firstTException = CatalogToHiveConverter.errorDetailToHiveException(errorDetail);
      }
    }
  }

  private void checkIfPartitionsDeleted() {
    for (Partition partition : partitions) {
      if (!partitionDeleted(partition)) {
        partitionMap.remove(new PartitionKey(partition));
      }
    }
  }

  private boolean partitionDeleted(Partition partition) {
    GetPartitionRequest request = new GetPartitionRequest()
        .withDatabaseName(partition.getDatabaseName())
        .withTableName(partition.getTableName())
        .withPartitionValues(partition.getValues())
        .withCatalogId(catalogId);

    try {
      GetPartitionResult result = client.getPartition(request);
      Partition partitionReturned = result.getPartition();
      return partitionReturned == null; // probably always false
    } catch (EntityNotFoundException e) {
      // here we assume namespace and table exist. It is assured by calling "isInvalidUserInputException" method above
      return true;
    } catch (Exception e) {
      LOG.error("Get partition request {} failed", request, e);
      // Partition status unknown, we assume that the partition was not deleted
      return false;
    }
  }

  public TException getFirstTException() {
    return firstTException;
  }

  public Collection<Partition> getPartitionsDeleted() {
    return partitionMap.values();
  }

}
