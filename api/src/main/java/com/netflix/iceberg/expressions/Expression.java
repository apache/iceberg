/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.expressions;

import java.io.Serializable;

/**
 * Represents a boolean expression tree.
 */
public interface Expression extends Serializable {
  enum Operation {
    TRUE,
    FALSE,
    IS_NULL,
    NOT_NULL,
    LT,
    LT_EQ,
    GT,
    GT_EQ,
    EQ,
    NOT_EQ,
    IN,
    NOT_IN,
    NOT,
    AND,
    OR
  }

  /**
   * @return the operation for an expression node.
   */
  Operation op();

  /**
   * @return the negation of this expression, equivalent to not(this).
   */
  Expression negate();
}
