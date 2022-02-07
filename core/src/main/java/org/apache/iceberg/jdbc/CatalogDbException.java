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

package org.apache.iceberg.jdbc;

import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientException;

public class CatalogDbException extends Exception {
  public enum Code {
    NOT_EXISTS,
    ALREADY_EXISTS,
    TIME_OUT,
    TRANSIENT,
    DATABASE_ERROR,
    INTERRUPTED,
    UNKNOWN,
    ;
  }

  private final Code errorCode;

  public CatalogDbException(String msg, Code errorCode) {
    super(msg);
    this.errorCode = errorCode;
  }

  public CatalogDbException(String msg, Exception cause, Code errorCode) {
    super(msg, cause);
    this.errorCode = errorCode;
  }

  public static CatalogDbException fromSqlException(String queryName, SQLException sqlException) {
    // a subclass of SQLException indicating key constraint violation
    if (sqlException instanceof SQLIntegrityConstraintViolationException) {
      return new CatalogDbException(
          queryName + " violated key constraint.", sqlException, Code.ALREADY_EXISTS);
    } else if (sqlException instanceof SQLTimeoutException) {
      return new CatalogDbException(queryName + " time out with db.", sqlException, Code.TIME_OUT);
    } else if (sqlException instanceof SQLTransientException) {
      return new CatalogDbException(
          queryName + " failed with transient db exception.", sqlException, Code.TRANSIENT);
    }
    return new CatalogDbException(
        queryName + " failed with error from database.", sqlException, Code.DATABASE_ERROR);
  }

  public static CatalogDbException wrapInterruption(
      String queryName, InterruptedException interruption) {
    return new CatalogDbException(
        "Interrupted during " + queryName, interruption, Code.INTERRUPTED);
  }

  public Code getErrorCode() {
    return this.errorCode;
  }
}
