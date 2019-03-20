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

package org.apache.iceberg.pig;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Tables;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.pig.IcebergPigInputFormat.IcebergRecordReader;
import org.apache.iceberg.types.Types;
import org.apache.pig.Expression;
import org.apache.pig.Expression.BetweenExpression;
import org.apache.pig.Expression.BinaryExpression;
import org.apache.pig.Expression.Column;
import org.apache.pig.Expression.Const;
import org.apache.pig.Expression.InExpression;
import org.apache.pig.Expression.OpType;
import org.apache.pig.Expression.UnaryExpression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.LoadPredicatePushdown;
import org.apache.pig.LoadPushDown;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.apache.iceberg.pig.IcebergPigInputFormat.ICEBERG_FILTER_EXPRESSION;
import static org.apache.iceberg.pig.IcebergPigInputFormat.ICEBERG_PROJECTED_FIELDS;
import static org.apache.iceberg.pig.IcebergPigInputFormat.ICEBERG_SCHEMA;
import static org.apache.pig.Expression.OpType.OP_AND;
import static org.apache.pig.Expression.OpType.OP_BETWEEN;
import static org.apache.pig.Expression.OpType.OP_EQ;
import static org.apache.pig.Expression.OpType.OP_GE;
import static org.apache.pig.Expression.OpType.OP_GT;
import static org.apache.pig.Expression.OpType.OP_IN;
import static org.apache.pig.Expression.OpType.OP_LE;
import static org.apache.pig.Expression.OpType.OP_LT;
import static org.apache.pig.Expression.OpType.OP_NE;
import static org.apache.pig.Expression.OpType.OP_NOT;
import static org.apache.pig.Expression.OpType.OP_NULL;
import static org.apache.pig.Expression.OpType.OP_OR;

public class IcebergStorage extends LoadFunc implements LoadMetadata, LoadPredicatePushdown, LoadPushDown {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergStorage.class);

  public static final String PIG_ICEBERG_TABLES_IMPL = "pig.iceberg.tables.impl";
  private static Tables iceberg;
  private static Map<String, Table> tables = Maps.newConcurrentMap();
  private static Map<String, String> locations = Maps.newConcurrentMap();

  private String signature;

  private IcebergRecordReader reader;

  @Override
  public void setLocation(String location, Job job) {
    LOG.info(format("[%s]: setLocation() -> %s ", signature, location));

    locations.put(signature, location);

    Configuration conf = job.getConfiguration();

    copyUDFContextToConfiguration(conf, ICEBERG_SCHEMA);
    copyUDFContextToConfiguration(conf, ICEBERG_PROJECTED_FIELDS);
    copyUDFContextToConfiguration(conf, ICEBERG_FILTER_EXPRESSION);
  }

  @Override
  public InputFormat getInputFormat() {
    LOG.info(format("[%s]: getInputFormat()", signature));
    String location = locations.get(signature);

    return new IcebergPigInputFormat(tables.get(location));
  }

  @Override
  public Tuple getNext() throws IOException {
    if (!reader.nextKeyValue()) {
      return null;
    }

    return (Tuple) reader.getCurrentValue();
  }

  @Override
  public void prepareToRead(RecordReader reader, PigSplit split) {
    LOG.info(format("[%s]: prepareToRead() -> %s", signature, split));

    this.reader = (IcebergRecordReader) reader;
  }

  @Override
  public ResourceSchema getSchema(String location, Job job) throws IOException {
    LOG.info(format("[%s]: getSchema() -> %s", signature, location));

    Schema schema = load(location, job).schema();
    storeInUDFContext(ICEBERG_SCHEMA, schema);

    return SchemaUtil.convert(schema);
  }


  @Override
  public ResourceStatistics getStatistics(String location, Job job) {
    LOG.info(format("[%s]: getStatistics() -> : %s", signature, location));

    return null;
  }

  @Override
  public String[] getPartitionKeys(String location, Job job) {
    LOG.info(format("[%s]: getPartitionKeys()", signature));
    return new String[0];
  }

  @Override
  public void setPartitionFilter(Expression partitionFilter) {
    LOG.info(format("[%s]: setPartitionFilter() ->  %s", signature, partitionFilter));
  }

  @Override
  public List<String> getPredicateFields(String location, Job job) throws IOException {
    LOG.info(format("[%s]: getPredicateFields() -> %s", signature, location));
    Schema schema = load(location, job).schema();

    List<String> result = Lists.newArrayList();

    for (Types.NestedField nf : schema.columns()) {
      switch (nf.type().typeId()) {
        case MAP:
        case LIST:
        case STRUCT:
          continue;
        default:
          result.add(nf.name());
      }
    }

    return result;
  }

  @Override
  public List<Expression.OpType> getSupportedExpressionTypes() {
    LOG.info(format("[%s]: getSupportedExpressionTypes()", signature));
    return asList(OP_AND, OP_OR, OP_EQ, OP_NE, OP_NOT, OP_GE, OP_GT, OP_LE, OP_LT, OP_BETWEEN, OP_IN, OP_NULL);
  }

  @Override
  public void setPushdownPredicate(Expression predicate) throws IOException {
    LOG.info(format("[%s]: setPushdownPredicate()", signature));
    LOG.info(format("[%s]: Pig predicate expression: %s", signature, predicate));

    org.apache.iceberg.expressions.Expression icebergExpression = convert(predicate);

    LOG.info(format("[%s]: Iceberg predicate expression: %s", signature, icebergExpression));

    storeInUDFContext(ICEBERG_FILTER_EXPRESSION, icebergExpression);
  }

  private org.apache.iceberg.expressions.Expression convert(Expression e) throws IOException {
    OpType op = e.getOpType();

    if (e instanceof BinaryExpression) {
      Expression lhs = ((BinaryExpression) e).getLhs();
      Expression rhs = ((BinaryExpression) e).getRhs();

      switch (op) {
        case OP_AND:
          return and(convert(lhs), convert(rhs));
        case OP_OR:
          return or(convert(lhs), convert(rhs));
        case OP_BETWEEN:
          BetweenExpression between = (BetweenExpression) rhs;
          return and(
              convert(OP_GE, (Column) lhs, (Const) between.getLower()),
              convert(OP_LE, (Column) lhs, (Const) between.getUpper())
          );
        case OP_IN:
          return ((InExpression) rhs).getValues().stream()
              .map((value) -> convert(OP_EQ, (Column) lhs, (Const) value))
              .reduce(Expressions.alwaysFalse(), (m, v) -> (or(m, v)));
        default:
          if (lhs instanceof Column && rhs instanceof Const) {
            return convert(op, (Column) lhs, (Const) rhs);
          } else if (lhs instanceof Const && rhs instanceof Column) {
            throw new FrontendException("Invalid expression ordering " + e);
          }
      }

    } else if (e instanceof UnaryExpression) {
      Expression unary = ((UnaryExpression) e).getExpression();

      switch (op) {
        case OP_NOT:  return not(convert(unary));
        case OP_NULL: return isNull(((Column)unary).getName());
        default:
          throw new FrontendException("Unsupported unary operator" + op);
      }
    }

    throw new FrontendException("Failed to pushdown expression " + e);
  }

  private org.apache.iceberg.expressions.Expression convert(OpType op, Column col, Const constant) {
    String name = col.getName();
    Object value = constant.getValue();

    switch (op) {
      case OP_GE: return greaterThanOrEqual(name, value);
      case OP_GT: return greaterThan(name, value);
      case OP_LE: return lessThanOrEqual(name, value);
      case OP_LT: return lessThan(name, value);
      case OP_EQ: return equal(name, value);
      case OP_NE: return notEqual(name, value);
    }

    throw new RuntimeException(format("[%s]: Failed to pushdown expression: %s %s %s", signature, col, op, constant));
  }

  @Override
  public List<OperatorSet> getFeatures() {
    return Collections.singletonList(OperatorSet.PROJECTION);
  }

  @Override
  public RequiredFieldResponse pushProjection(RequiredFieldList requiredFieldList) {
    LOG.info(format("[%s]: pushProjection() -> %s", signature, requiredFieldList));

    try {
      List<String> projection = requiredFieldList.getFields().stream().map(RequiredField::getAlias).collect(Collectors.toList());

      storeInUDFContext(ICEBERG_PROJECTED_FIELDS, (Serializable) projection);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return new RequiredFieldResponse(true);
  }

  @Override
  public void setUDFContextSignature(String signature) {
    this.signature = signature;
  }

  private void storeInUDFContext(String key, Serializable value) throws IOException {
    Properties properties = UDFContext.getUDFContext().getUDFProperties(this.getClass(), new String[]{signature});

    properties.setProperty(key, ObjectSerializer.serialize(value));
  }

  private void copyUDFContextToConfiguration(Configuration conf, String key) {
    String value = UDFContext.getUDFContext().getUDFProperties(this.getClass(), new String[]{signature}).getProperty(key);

    if (value != null) {
      conf.set(key, value);
    }
  }

  @Override
  public String relativeToAbsolutePath(String location, Path curDir) throws IOException {
    return location;
  }

  @SuppressWarnings("unchecked")
  public <T extends Serializable> T getFromUDFContext(String key, Class<T> clazz) throws IOException {
    Properties properties = UDFContext.getUDFContext().getUDFProperties(this.getClass(), new String[]{signature});

    return (T) ObjectSerializer.deserialize(properties.getProperty(key));
  }

  private Table load(String location, Job job) throws IOException {
    if(iceberg == null) {
      Class<?> tablesImpl = job.getConfiguration().getClass(PIG_ICEBERG_TABLES_IMPL, HadoopTables.class);
      Log.info("Initializing iceberg tables implementation: " + tablesImpl);
      iceberg = (Tables) ReflectionUtils.newInstance(tablesImpl, job.getConfiguration());
    }

    Table result = tables.get(location);

    if (result == null) {
      try {
        LOG.info(format("[%s]: Loading table for location: %s", signature, location));
        result = iceberg.load(location);
        tables.put(location, result);
      } catch (Exception e) {
        throw new FrontendException("Failed to instantiate tables implementation", e);
      }
    }

    return result;
  }

}


