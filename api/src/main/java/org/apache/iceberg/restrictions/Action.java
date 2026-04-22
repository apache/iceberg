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
package org.apache.iceberg.restrictions;

import java.io.Serializable;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * A column projection action in {@link ReadRestrictions}.
 *
 * <p>Each implementation corresponds to one of the predefined action discriminator values in the
 * REST catalog {@code Action} schema.
 */
public interface Action extends Serializable {

  String MASK_ALPHANUM = "mask-alphanum";
  String MASK_TO_DEFAULT = "mask-to-default";
  String REPLACE_WITH_NULL = "replace-with-null";
  String SHOW_FIRST_4 = "show-first-4";
  String SHOW_LAST_4 = "show-last-4";
  String TRUNCATE_TO_YEAR = "truncate-to-year";
  String TRUNCATE_TO_MONTH = "truncate-to-month";
  String SHA_256_GLOBAL = "sha-256-global";
  String SHA_256_QUERY_LOCAL = "sha-256-query-local";
  String APPLY_EXPRESSION = "apply-expression";

  /** The action discriminator string as sent on the wire. */
  String actionType();

  /** The field id of the column this action applies to. */
  int fieldId();

  abstract class Base implements Action {
    private final int fieldId;

    Base(int fieldId) {
      this.fieldId = fieldId;
    }

    @Override
    public int fieldId() {
      return fieldId;
    }
  }

  class MaskAlphanum extends Base {
    public MaskAlphanum(int fieldId) {
      super(fieldId);
    }

    @Override
    public String actionType() {
      return MASK_ALPHANUM;
    }
  }

  class MaskToDefault extends Base {
    public MaskToDefault(int fieldId) {
      super(fieldId);
    }

    @Override
    public String actionType() {
      return MASK_TO_DEFAULT;
    }
  }

  class ReplaceWithNull extends Base {
    public ReplaceWithNull(int fieldId) {
      super(fieldId);
    }

    @Override
    public String actionType() {
      return REPLACE_WITH_NULL;
    }
  }

  class ShowFirst4 extends Base {
    public ShowFirst4(int fieldId) {
      super(fieldId);
    }

    @Override
    public String actionType() {
      return SHOW_FIRST_4;
    }
  }

  class ShowLast4 extends Base {
    public ShowLast4(int fieldId) {
      super(fieldId);
    }

    @Override
    public String actionType() {
      return SHOW_LAST_4;
    }
  }

  class TruncateToYear extends Base {
    public TruncateToYear(int fieldId) {
      super(fieldId);
    }

    @Override
    public String actionType() {
      return TRUNCATE_TO_YEAR;
    }
  }

  class TruncateToMonth extends Base {
    public TruncateToMonth(int fieldId) {
      super(fieldId);
    }

    @Override
    public String actionType() {
      return TRUNCATE_TO_MONTH;
    }
  }

  class Sha256Global extends Base {
    public Sha256Global(int fieldId) {
      super(fieldId);
    }

    @Override
    public String actionType() {
      return SHA_256_GLOBAL;
    }
  }

  class Sha256QueryLocal extends Base {
    public Sha256QueryLocal(int fieldId) {
      super(fieldId);
    }

    @Override
    public String actionType() {
      return SHA_256_QUERY_LOCAL;
    }
  }

  class ApplyExpression extends Base {
    private final Expression expression;

    public ApplyExpression(int fieldId, Expression expression) {
      super(fieldId);
      Preconditions.checkArgument(expression != null, "Invalid expression: null");
      this.expression = expression;
    }

    public Expression expression() {
      return expression;
    }

    @Override
    public String actionType() {
      return APPLY_EXPRESSION;
    }
  }

  /**
   * Preserves an action with a discriminator string this client doesn't recognize so that newer
   * server-side action types don't break parsing. Callers that intend to enforce the action
   * (engine-side rules) must fail closed when they encounter this — silent skipping would leak
   * unmasked data.
   */
  class Unknown extends Base {
    private final String actionType;

    public Unknown(int fieldId, String actionType) {
      super(fieldId);
      Preconditions.checkArgument(actionType != null, "Invalid action type: null");
      this.actionType = actionType;
    }

    @Override
    public String actionType() {
      return actionType;
    }
  }
}
