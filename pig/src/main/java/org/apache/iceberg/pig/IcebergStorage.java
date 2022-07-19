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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.NaNUtil;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergStorage extends LoadFunc
    implements LoadMetadata, LoadPredicatePushdown, LoadPushDown {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergStorage.class);

  public static final String PIG_ICEBERG_TABLES_IMPL = "pig.iceberg.tables.impl";
  private static Tables iceberg;
  private static Map<String, Table> tables = Maps.newConcurrentMap();
  private static Map<String, String> locations = Maps.newConcurrentMap();

  private String signature;

  private IcebergRecordReader reader;

  @Override
  public void setLocation(String location, Job job) {
    LOG.info("[{}]: setLocation() -> {}", signature, location);

    locations.put(signature, location);

    Configuration conf = job.getConfiguration();

    copyUDFContextToScopedConfiguration(conf, IcebergPigInputFormat.ICEBERG_SCHEMA);
    copyUDFContextToScopedConfiguration(conf, IcebergPigInputFormat.ICEBERG_PROJECTED_FIELDS);
    copyUDFContextToScopedConfiguration(conf, IcebergPigInputFormat.ICEBERG_FILTER_EXPRESSION);
  }

  @Override
  public InputFormat getInputFormat() {
    LOG.info("[{}]: getInputFormat()", signature);
    String location = locations.get(signature);

    return new IcebergPigInputFormat(tables.get(location), signature);
  }

  @Override
  public Tuple getNext() throws IOException {
    if (!reader.nextKeyValue()) {
      return null;
    }

    return (Tuple) reader.getCurrentValue();
  }

  @Override
  public void prepareToRead(RecordReader newReader, PigSplit split) {
    LOG.info("[{}]: prepareToRead() -> {}", signature, split);

    this.reader = (IcebergRecordReader) newReader;
  }

  @Override
  public ResourceSchema getSchema(String location, Job job) throws IOException {
    LOG.info("[{}]: getSchema() -> {}", signature, location);

    Schema schema = load(location, job).schema();
    storeInUDFContext(IcebergPigInputFormat.ICEBERG_SCHEMA, schema);

    return SchemaUtil.convert(schema);
  }

  @Override
  public ResourceStatistics getStatistics(String location, Job job) {
    LOG.info("[{}]: getStatistics() -> : {}", signature, location);

    return null;
  }

  @Override
  public String[] getPartitionKeys(String location, Job job) {
    LOG.info("[{}]: getPartitionKeys()", signature);
    return new String[0];
  }

  @Override
  public void setPartitionFilter(Expression partitionFilter) {
    LOG.info("[{}]: setPartitionFilter() -> {}", signature, partitionFilter);
  }

  @Override
  public List<String> getPredicateFields(String location, Job job) throws IOException {
    LOG.info("[{}]: getPredicateFields() -> {}", signature, location);
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
  public ImmutableList<OpType> getSupportedExpressionTypes() {
    LOG.info("[{}]: getSupportedExpressionTypes()", signature);
    return ImmutableList.of(
        OpType.OP_AND,
        OpType.OP_OR,
        OpType.OP_EQ,
        OpType.OP_NE,
        OpType.OP_NOT,
        OpType.OP_GE,
        OpType.OP_GT,
        OpType.OP_LE,
        OpType.OP_LT,
        OpType.OP_BETWEEN,
        OpType.OP_IN,
        OpType.OP_NULL);
  }

  @Override
  public void setPushdownPredicate(Expression predicate) throws IOException {
    LOG.info("[{}]: setPushdownPredicate()", signature);
    LOG.info("[{}]: Pig predicate expression: {}", signature, predicate);

    org.apache.iceberg.expressions.Expression icebergExpression = convert(predicate);

    LOG.info("[{}]: Iceberg predicate expression: {}", signature, icebergExpression);

    storeInUDFContext(IcebergPigInputFormat.ICEBERG_FILTER_EXPRESSION, icebergExpression);
  }

  private org.apache.iceberg.expressions.Expression convert(Expression expression)
      throws IOException {
    OpType op = expression.getOpType();

    if (expression instanceof BinaryExpression) {
      Expression lhs = ((BinaryExpression) expression).getLhs();
      Expression rhs = ((BinaryExpression) expression).getRhs();

      switch (op) {
        case OP_AND:
          return Expressions.and(convert(lhs), convert(rhs));
        case OP_OR:
          return Expressions.or(convert(lhs), convert(rhs));
        case OP_BETWEEN:
          BetweenExpression between = (BetweenExpression) rhs;
          return Expressions.and(
              convert(OpType.OP_GE, (Column) lhs, (Const) between.getLower()),
              convert(OpType.OP_LE, (Column) lhs, (Const) between.getUpper()));
        case OP_IN:
          return ((InExpression) rhs)
              .getValues().stream()
                  .map(value -> convert(OpType.OP_EQ, (Column) lhs, (Const) value))
                  .reduce(Expressions.alwaysFalse(), Expressions::or);
        default:
          if (lhs instanceof Column && rhs instanceof Const) {
            return convert(op, (Column) lhs, (Const) rhs);
          } else if (lhs instanceof Const && rhs instanceof Column) {
            throw new FrontendException("Invalid expression ordering " + expression);
          }
      }

    } else if (expression instanceof UnaryExpression) {
      Expression unary = ((UnaryExpression) expression).getExpression();

      switch (op) {
        case OP_NOT:
          return Expressions.not(convert(unary));
        case OP_NULL:
          return Expressions.isNull(((Column) unary).getName());
        default:
          throw new FrontendException("Unsupported unary operator" + op);
      }
    }

    throw new FrontendException("Failed to pushdown expression " + expression);
  }

  private org.apache.iceberg.expressions.Expression convert(OpType op, Column col, Const constant) {
    String name = col.getName();
    Object value = constant.getValue();

    switch (op) {
      case OP_GE:
        return Expressions.greaterThanOrEqual(name, value);
      case OP_GT:
        return Expressions.greaterThan(name, value);
      case OP_LE:
        return Expressions.lessThanOrEqual(name, value);
      case OP_LT:
        return Expressions.lessThan(name, value);
      case OP_EQ:
        return NaNUtil.isNaN(value) ? Expressions.isNaN(name) : Expressions.equal(name, value);
      case OP_NE:
        return NaNUtil.isNaN(value) ? Expressions.notNaN(name) : Expressions.notEqual(name, value);
    }

    throw new RuntimeException(
        String.format(
            "[%s]: Failed to pushdown expression: %s %s %s", signature, col, op, constant));
  }

  @Override
  public List<OperatorSet> getFeatures() {
    return Collections.singletonList(OperatorSet.PROJECTION);
  }

  @Override
  public RequiredFieldResponse pushProjection(RequiredFieldList requiredFieldList) {
    LOG.info("[{}]: pushProjection() -> {}", signature, requiredFieldList);

    try {
      List<String> projection =
          requiredFieldList.getFields().stream()
              .map(RequiredField::getAlias)
              .collect(Collectors.toList());

      storeInUDFContext(IcebergPigInputFormat.ICEBERG_PROJECTED_FIELDS, (Serializable) projection);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return new RequiredFieldResponse(true);
  }

  @Override
  public void setUDFContextSignature(String newSignature) {
    this.signature = newSignature;
  }

  private void storeInUDFContext(String key, Serializable value) throws IOException {
    Properties properties =
        UDFContext.getUDFContext().getUDFProperties(this.getClass(), new String[] {signature});

    properties.setProperty(key, ObjectSerializer.serialize(value));
  }

  private void copyUDFContextToScopedConfiguration(Configuration conf, String key) {
    String value =
        UDFContext.getUDFContext()
            .getUDFProperties(this.getClass(), new String[] {signature})
            .getProperty(key);

    if (value != null) {
      conf.set(key + '.' + signature, value);
    }
  }

  @Override
  public String relativeToAbsolutePath(String location, Path curDir) throws IOException {
    return location;
  }

  private Table load(String location, Job job) throws IOException {
    if (iceberg == null) {
      Class<?> tablesImpl =
          job.getConfiguration().getClass(PIG_ICEBERG_TABLES_IMPL, HadoopTables.class);
      LOG.info("Initializing iceberg tables implementation: {}", tablesImpl);
      iceberg = (Tables) ReflectionUtils.newInstance(tablesImpl, job.getConfiguration());
    }

    Table result = tables.get(location);

    if (result == null) {
      try {
        LOG.info("[{}]: Loading table for location: {}", signature, location);
        result = iceberg.load(location);
        tables.put(location, result);
      } catch (Exception e) {
        throw new FrontendException("Failed to instantiate tables implementation", e);
      }
    }

    return result;
  }
}
