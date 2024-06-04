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
package org.apache.iceberg;

import com.google.errorprone.annotations.FormatMethod;
import java.util.function.Predicate;
import org.apache.iceberg.exceptions.ValidationException;

public class Validation {
  private final Predicate<Table> predicate;
  private final String message;
  private final Object[] args;

  /**
   * @param predicate The predicate the table needs to satisfy.
   * @param message The message that will be included in the {@link ValidationException} that will
   *     be thrown by {@link Validation#validate} if the predicate is not satisfied.
   * @param args The arguments referenced by the format specifiers in the message, if any.
   */
  @FormatMethod
  public Validation(Predicate<Table> predicate, String message, Object... args) {
    this.predicate = predicate;
    this.message = message;
    this.args = args;
  }

  /**
   * Ensures that the given table is valid according to the predicate.
   *
   * @param table The table to test.
   * @throws ValidationException If the predicate is not satisfied by the given table.
   */
  @SuppressWarnings("FormatStringAnnotation")
  public void validate(Table table) {
    ValidationException.check(predicate.test(table), message, args);
  }
}
