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
package org.apache.iceberg.aws.glue;

import org.apache.iceberg.aws.dynamodb.DynamoDbLockManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * @deprecated this class is kept only for backwards compatibility. For GlueCatalog, Glue has
 *     supported optimistic locking and lock manager is no longer needed. For HadoopCatalog and
 *     HadoopTables, please use {@link org.apache.iceberg.aws.dynamodb.DynamoDbLockManager} instead.
 */
@Deprecated
class DynamoLockManager extends DynamoDbLockManager {

  private static final Logger LOG = LoggerFactory.getLogger(DynamoLockManager.class);

  DynamoLockManager() {
    logDeprecationWarning();
  }

  DynamoLockManager(DynamoDbClient dynamo, String lockTableName) {
    super(dynamo, lockTableName);
    logDeprecationWarning();
  }

  private void logDeprecationWarning() {
    LOG.warn(
        "{} is deprecated. For GlueCatalog, Glue has supported optimistic locking and "
            + "lock manager is no longer needed. For HadoopCatalog and HadoopTables, please use {} instead.",
        DynamoLockManager.class.getName(),
        DynamoDbLockManager.class.getName());
  }
}
