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

package org.apache.iceberg;

import java.util.Deque;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Bound;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;

public class InformationSchemaNamespacesTable extends InformationSchemaTable {
  private static final int NAME_COLUMN_ID = 1;
  private static final int PARENT_COLUMN_ID = 2;

  private static final Schema NAMESPACES_SCHEMA =
      new Schema(
          Types.NestedField.required(
              NAME_COLUMN_ID,
              "namespace_name",
              Types.StringType.get(),
              "Namespace identifier as a dotted string"),
          Types.NestedField.optional(
              PARENT_COLUMN_ID,
              "parent_namespace_name",
              Types.StringType.get(),
              "Parent identifier as a dotted string"));

  public InformationSchemaNamespacesTable(Catalog catalog) {
    super(catalog, Type.NAMESPACES);
  }

  @Override
  public BatchScan newBatchScan() {
    return new NamespacesTableScan(new TableScanContext());
  }

  @Override
  public Schema schema() {
    return NAMESPACES_SCHEMA;
  }

  private List<Namespace> listAllNamespaces(Expression filter, boolean caseSensitive) {
    if (!(catalog() instanceof SupportsNamespaces)) {
      return ImmutableList.of();
    }

    PartialNamespaceEvaluator inclusiveEvaluator =
        new PartialNamespaceEvaluator(filter, caseSensitive);
    Evaluator evaluator = new Evaluator(NAMESPACES_SCHEMA.asStruct(), filter, caseSensitive);

    SupportsNamespaces catalog = (SupportsNamespaces) catalog();
    Deque<Namespace> namespaces = Lists.newLinkedList(catalog.listNamespaces());
    ImmutableList.Builder<Namespace> allNamespaces = ImmutableList.builder();
    while (!namespaces.isEmpty()) {
      Namespace ns = namespaces.removeFirst();

      String nsAsString = ns.toString();
      String parentAsString = ns.parent().isEmpty() ? null : ns.parent().toString();

      if (evaluator.eval(StaticDataTask.Row.of(nsAsString, parentAsString))) {
        allNamespaces.add(ns);
      }

      // top-level namespaces have an empty parent, but no filter on parent_namespace_name will
      // match empty parents. for example, if ns = ['a'] and the filter is parent_namespace_name
      // like 'a%', the namespace will be ignored even though its children can match.
      // to work around this problem, the namespace itself is passed as the parent. this works
      // because the inclusive evaluator matches any namespace that may contain matching children.
      if (inclusiveEvaluator.eval(
          nsAsString, parentAsString != null ? parentAsString : nsAsString)) {
        // the namespace matches or may contain matches
        List<Namespace> nestedNamespaces = catalog.listNamespaces(ns);

        // add nested namespaces to be processed first, in reverse order to maintain the listing
        // order
        for (int i = nestedNamespaces.size() - 1; i >= 0; i -= 1) {
          namespaces.addFirst(nestedNamespaces.get(i));
        }
      }
    }

    return allNamespaces.build();
  }

  private DataTask task(TableScanContext context) {
    // extract filters on the parent column, which can be used to limit catalog queries
    Expression filters =
        ExpressionUtil.extractByIdInclusive(
            context.rowFilter(),
            NAMESPACES_SCHEMA,
            context.caseSensitive(),
            PARENT_COLUMN_ID,
            NAME_COLUMN_ID);

    // list namespaces recursively while applying filters
    List<Namespace> namespaces = listAllNamespaces(filters, context.caseSensitive());

    return StaticDataTask.of(
        new EmptyInputFile("information_schema:namespaces"),
        NAMESPACES_SCHEMA,
        NAMESPACES_SCHEMA,
        namespaces,
        ns ->
            StaticDataTask.Row.of(
                ns.toString(), ns.parent().isEmpty() ? null : ns.parent().toString()));
  }

  private class NamespacesTableScan extends BaseInformationSchemaTableScan {

    protected NamespacesTableScan(TableScanContext context) {
      super(InformationSchemaNamespacesTable.this, context);
    }

    @Override
    protected NamespacesTableScan newRefinedScan(TableScanContext newContext) {
      return new NamespacesTableScan(newContext);
    }

    @Override
    public Schema schema() {
      // this table does not project columns
      return NAMESPACES_SCHEMA;
    }

    @Override
    public CloseableIterable<ScanTask> planFiles() {
      return CloseableIterable.withNoopClose(task(context()));
    }
  }

  static class PartialNamespaceEvaluator extends ExpressionVisitors.BoundVisitor<Boolean> {
    private final Expression bound;
    private String namespaceAsString = null;
    private String parentAsString = null;

    PartialNamespaceEvaluator(Expression expr, boolean caseSensitive) {
      this.bound = Binder.bind(NAMESPACES_SCHEMA.asStruct(), expr, caseSensitive);
    }

    boolean eval(String namespaceAsString, String parentAsString) {
      this.namespaceAsString = namespaceAsString;
      this.parentAsString = parentAsString;
      return ExpressionVisitors.visitEvaluator(bound, this);
    }

    @Override
    public Boolean alwaysTrue() {
      return true;
    }

    @Override
    public Boolean alwaysFalse() {
      return false;
    }

    @Override
    public Boolean not(Boolean result) {
      return !result;
    }

    @Override
    public Boolean and(Boolean leftResult, Boolean rightResult) {
      return leftResult && rightResult;
    }

    @Override
    public Boolean or(Boolean leftResult, Boolean rightResult) {
      return leftResult || rightResult;
    }

    @Override
    public <T> Boolean isNull(Bound<T> expr) {
      return false;
    }

    @Override
    public <T> Boolean notNull(Bound<T> expr) {
      return true;
    }

    @Override
    public <T> Boolean isNaN(Bound<T> expr) {
      return false;
    }

    @Override
    public <T> Boolean notNaN(Bound<T> expr) {
      return true;
    }

    @Override
    public <T> Boolean lt(Bound<T> expr, Literal<T> lit) {
      // namespace_name < a.b
      // should match: a.a, a
      if (expr.ref().fieldId() == PARENT_COLUMN_ID) {
        String litAsString = lit.value().toString();
        if (isMatchOrParent(litAsString, parentAsString)) {
          return !litAsString.equals(parentAsString); // cannot be equal
        } else {
          return Comparators.charSequences().compare(parentAsString, litAsString) < 0;
        }
      } else if (expr.ref().fieldId() == NAME_COLUMN_ID) {
        String litAsString = lit.value().toString();
        if (isMatchOrParent(litAsString, namespaceAsString)) {
          return !litAsString.equals(namespaceAsString); // cannot be equal
        } else {
          return Comparators.charSequences().compare(namespaceAsString, litAsString) < 0;
        }
      }

      return false;
    }

    @Override
    public <T> Boolean ltEq(Bound<T> expr, Literal<T> lit) {
      // namespace_name < a.b
      // should match: a.a, a.b, a
      if (expr.ref().fieldId() == PARENT_COLUMN_ID) {
        String litAsString = lit.value().toString();
        if (isMatchOrParent(litAsString, parentAsString)) {
          return true;
        } else {
          return Comparators.charSequences().compare(parentAsString, litAsString) < 0;
        }
      } else if (expr.ref().fieldId() == NAME_COLUMN_ID) {
        String litAsString = lit.value().toString();
        if (isMatchOrParent(litAsString, namespaceAsString)) {
          return true;
        } else {
          return Comparators.charSequences().compare(namespaceAsString, litAsString) < 0;
        }
      }

      return false;
    }

    @Override
    public <T> Boolean gt(Bound<T> expr, Literal<T> lit) {
      // namespace_name > a.b
      // should match: a.c, a
      if (expr.ref().fieldId() == PARENT_COLUMN_ID) {
        String litAsString = lit.value().toString();
        if (isMatchOrParent(litAsString, parentAsString)) {
          return !litAsString.equals(parentAsString); // cannot be equal
        } else {
          return Comparators.charSequences().compare(parentAsString, litAsString) > 0;
        }
      } else if (expr.ref().fieldId() == NAME_COLUMN_ID) {
        String litAsString = lit.value().toString();
        if (isMatchOrParent(litAsString, namespaceAsString)) {
          return !litAsString.equals(namespaceAsString); // cannot be equal
        } else {
          return Comparators.charSequences().compare(namespaceAsString, litAsString) > 0;
        }
      }

      return false;
    }

    @Override
    public <T> Boolean gtEq(Bound<T> expr, Literal<T> lit) {
      // namespace_name > a.b
      // should match: a.c, a.b, a
      if (expr.ref().fieldId() == PARENT_COLUMN_ID) {
        String litAsString = lit.value().toString();
        if (isMatchOrParent(litAsString, parentAsString)) {
          return true;
        } else {
          return Comparators.charSequences().compare(parentAsString, litAsString) > 0;
        }
      } else if (expr.ref().fieldId() == NAME_COLUMN_ID) {
        String litAsString = lit.value().toString();
        if (isMatchOrParent(litAsString, namespaceAsString)) {
          return true;
        } else {
          return Comparators.charSequences().compare(namespaceAsString, litAsString) > 0;
        }
      }

      return false;
    }

    @Override
    public <T> Boolean eq(Bound<T> expr, Literal<T> lit) {
      // this matches any namespace that may be a parent of the matching namespace
      if (expr.ref().fieldId() == PARENT_COLUMN_ID) {
        return isMatchOrParent(lit.value().toString(), parentAsString);
      } else if (expr.ref().fieldId() == NAME_COLUMN_ID) {
        return isMatchOrParent(lit.value().toString(), namespaceAsString);
      }

      return false;
    }

    @Override
    public <T> Boolean notEq(Bound<T> expr, Literal<T> lit) {
      // namespace_name != a.b
      // should match a, b, a.b.c
      if (expr.ref().fieldId() == PARENT_COLUMN_ID) {
        return !lit.value().toString().equalsIgnoreCase(parentAsString);
      } else if (expr.ref().fieldId() == NAME_COLUMN_ID) {
        return !lit.value().toString().equalsIgnoreCase(namespaceAsString);
      }

      return false;
    }

    @Override
    public <T> Boolean in(Bound<T> expr, Set<T> literalSet) {
      // this matches any namespace that may be a parent of any namespace in the set
      if (expr.ref().fieldId() == PARENT_COLUMN_ID) {
        for (T value : literalSet) {
          if (isMatchOrParent(value.toString(), parentAsString)) {
            return true;
          }
        }
      } else if (expr.ref().fieldId() == NAME_COLUMN_ID) {
        for (T value : literalSet) {
          if (isMatchOrParent(value.toString(), namespaceAsString)) {
            return true;
          }
        }
      }

      return false;
    }

    @Override
    public <T> Boolean notIn(Bound<T> expr, Set<T> literalSet) {
      if (expr.ref().fieldId() == PARENT_COLUMN_ID) {
        for (T value : literalSet) {
          if (value.toString().equalsIgnoreCase(parentAsString)) {
            return false;
          }
        }
        return true;
      } else if (expr.ref().fieldId() == NAME_COLUMN_ID) {
        for (T value : literalSet) {
          if (value.toString().equalsIgnoreCase(namespaceAsString)) {
            return false;
          }
        }
        return true;
      }

      return false;
    }

    @Override
    public <T> Boolean startsWith(Bound<T> expr, Literal<T> lit) {
      // this matches any namespace that may be a parent of the matching namespace
      if (expr.ref().fieldId() == PARENT_COLUMN_ID) {
        return isPrefix(lit.value().toString(), parentAsString);
      } else if (expr.ref().fieldId() == NAME_COLUMN_ID) {
        return isPrefix(lit.value().toString(), namespaceAsString);
      }

      return false;
    }

    @Override
    public <T> Boolean notStartsWith(Bound<T> expr, Literal<T> lit) {
      if (expr.ref().fieldId() == PARENT_COLUMN_ID) {
        String litAsString = lit.value().toString();
        if (litAsString.length() <= parentAsString.length()) {
          return !parentAsString.startsWith(litAsString);
        } else {
          return true;
        }
      } else if (expr.ref().fieldId() == NAME_COLUMN_ID) {
        String litAsString = lit.value().toString();
        if (litAsString.length() <= namespaceAsString.length()) {
          return !namespaceAsString.startsWith(litAsString);
        } else {
          return true;
        }
      }

      return false;
    }

    private boolean isMatchOrParent(String toMatch, String toTest) {
      // toMatch: a.b
      // should match: a, a.b
      // should not match: b, a.bc, a.b.c

      int sign = Integer.signum(toTest.length() - toMatch.length());
      switch (sign) {
        case 1:
          // namespace to test is longer, so it cannot match or be a parent
          return false;
        case 0:
          return toTest.equalsIgnoreCase(toMatch);
        default:
          // namespace is shorter and may be a parent if the one to match starts with it, followed
          // by '.'
          return toMatch.startsWith(toTest) && toMatch.charAt(toTest.length()) == '.';
      }
    }

    private boolean isPrefix(String toMatch, String toTest) {
      // toMatch: a.b
      // should match: a, a.b, a.bc
      // should not match: b, a.b.c

      int sign = Integer.signum(toTest.length() - toMatch.length());
      switch (sign) {
        case 1:
          return toTest.startsWith(toMatch);
        case 0:
          return toTest.equalsIgnoreCase(toMatch);
        default:
          // namespace is shorter and may be a parent if the one to match starts with it, followed
          // by '.'
          return toMatch.startsWith(toTest) && toMatch.charAt(toTest.length()) == '.';
      }
    }
  }
}
