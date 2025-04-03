/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.aws.dynamodb;

import java.io.Serializable;

public class DynamoDbProperties implements Serializable {
  /** Configure an alternative endpoint of the DynamoDB service to access. */
  public static final String DYNAMODB_ENDPOINT = "dynamodb.endpoint";

  /** DynamoDB table name for {@link DynamoDbCatalog} */
  public static final String DYNAMODB_TABLE_NAME = "dynamodb.table-name";

  public static final String DYNAMODB_TABLE_NAME_DEFAULT = "iceberg";
}
