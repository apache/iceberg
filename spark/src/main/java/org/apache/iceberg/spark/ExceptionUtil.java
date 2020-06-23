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

package org.apache.iceberg.spark;

import java.io.IOException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.spark.sql.AnalysisException;

public class ExceptionUtil {

  private ExceptionUtil() {
  }

  /**
   * Converts Checked Exceptions to Unchecked Exceptions.
   *
   * @param exception - a checked exception object which is to be converted to its unchecked equivalent.
   * @return - unchecked exception.
   */
  public static RuntimeException toUncheckedException(Throwable exception) {

    if (exception instanceof RuntimeException) {
      return (RuntimeException) exception;

    } else if (exception instanceof org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException) {
      return new NoSuchNamespaceException(exception, exception.getMessage());

    } else if (exception instanceof org.apache.spark.sql.catalyst.analysis.NoSuchTableException) {
      return new NoSuchTableException(exception, exception.getMessage());

    } else if (exception instanceof AnalysisException) {
      return new ValidationException(exception, exception.getMessage());

    } else if (exception instanceof IOException) {
      return new RuntimeIOException((IOException) exception);

    } else {
      return new RuntimeException(exception);
    }
  }
}
