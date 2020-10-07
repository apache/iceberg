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

package org.apache.iceberg.aws.glue.lock;

/**
 * Indicating the lock tables for Glue is in an inconsistent state and might need manual fix.
 */
public class InconsistentLockStateException extends IllegalStateException {

  public InconsistentLockStateException(DynamoLockComponent component, String lockId, int retry) {
    super(String.format("Fail to release lock %s component %s after %d retries, " +
            "you might have inconsistencies in lock tables that have to be manually fixed.",
        lockId, component, retry));
  }
}
