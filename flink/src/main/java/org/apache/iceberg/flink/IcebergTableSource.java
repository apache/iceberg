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

package org.apache.iceberg.flink;

import java.time.Duration;
import java.util.*;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.*;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.TimeUtils;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.source.FlinkInputFormat;
import org.apache.iceberg.flink.source.FlinkSource;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Flink Iceberg table source.
 */
public class IcebergTableSource
        implements ScanTableSource, SupportsProjectionPushDown, SupportsFilterPushDown, SupportsLimitPushDown, LookupTableSource {

  private long limit;
  private List<Expression> filters;

  private final TableLoader loader;
  private final TableSchema schema;
  private final Map<String, String> properties;
  private final boolean isLimitPushDown;
  private final ReadableConfig readableConfig;

  private int[][] projectPhysicalFields;
  private String[] projectedFieldNames;
  private DataType[] projectedFieldTypes;
  private int[][] schemaIndexes;

  private IcebergTableSource(IcebergTableSource toCopy) {
    this.loader = toCopy.loader;
    this.schema = toCopy.schema;
    this.properties = toCopy.properties;
    this.projectPhysicalFields = toCopy.projectPhysicalFields;
    this.isLimitPushDown = toCopy.isLimitPushDown;
    this.limit = toCopy.limit;
    this.filters = toCopy.filters;
    this.readableConfig = toCopy.readableConfig;
    this.projectedFieldNames = toCopy.projectedFieldNames;
    this.projectedFieldTypes = toCopy.projectedFieldTypes;
    this.schemaIndexes = toCopy.schemaIndexes;
  }

  public IcebergTableSource(TableLoader loader, TableSchema schema, Map<String, String> properties,
                            ReadableConfig readableConfig) {
    this(loader, schema, properties, null, false, -1, ImmutableList.of(), readableConfig);
  }

  private IcebergTableSource(TableLoader loader, TableSchema schema, Map<String, String> properties,
                             int[][] projectPhysicalFields, boolean isLimitPushDown,
                             long limit, List<Expression> filters, ReadableConfig readableConfig) {
    this.loader = loader;
    this.schema = schema;
    this.properties = properties;
    this.projectPhysicalFields = projectPhysicalFields;
    this.isLimitPushDown = isLimitPushDown;
    this.limit = limit;
    this.filters = filters;
    this.readableConfig = readableConfig;
    this.projectedFieldTypes = null;
    this.projectedFieldNames = null;
    this.schemaIndexes = null;
  }

  @Override
  public void applyProjection(int[][] projectFields) {
    this.projectPhysicalFields = projectFields;
  }

  private DataStream<RowData> createDataStream(StreamExecutionEnvironment execEnv) {
    return FlinkSource.forRowData()
            .env(execEnv)
            .tableLoader(loader)
            .properties(properties)
            .project(getProjectedSchema())
            .projectedFieldNames(projectedFieldNames)
            .projectedFieldTypes(projectedFieldTypes)
            .schemaIndexes(schemaIndexes)
            .limit(limit)
            .filters(filters)
            .flinkConf(readableConfig)
            .build();
  }

  private TableSchema getProjectedSchema() {
    if (projectPhysicalFields == null) {
      return schema;
    }
    DataType producedDataType = schema.toRowDataType();
    TableSchema tableSchema = projectSchema(producedDataType, projectPhysicalFields);
    parseFieldNameType();
    parseSchemaIndexes(tableSchema);
    return tableSchema;
  }

  private void parseSchemaIndexes(TableSchema tableSchema) {
    schemaIndexes = new int[this.projectedFieldNames.length][];
    for (int i = 0; i < this.projectedFieldNames.length; i++) {
      schemaIndexes[i] = schemaIndexes(this.projectedFieldNames[i], tableSchema);
    }
  }

  private int[] schemaIndexes(String projectedFieldName, TableSchema tableSchema) {
    String[] nestedFieldDim = projectedFieldName.split("-");
    String[] tableSchemaFieldNames = tableSchema.getFieldNames();
    int channel = 0;
    for (; channel < tableSchemaFieldNames.length; channel++) {
      if (nestedFieldDim[0].equals(tableSchemaFieldNames[channel])) {
        break;
      }
    }

    int[] schemaIndexes = new int[nestedFieldDim.length];
    schemaIndexes[0] = channel;
    DataType dataType = tableSchema.toRowDataType().getChildren().get(channel);
    for (int i = 1; i < nestedFieldDim.length; i++) {
      int rank = schemaIndexesFromSpecColumn(dataType, nestedFieldDim[i]);
      Preconditions.checkArgument(rank >= 0);
      schemaIndexes[i] = rank;
      if (i == nestedFieldDim.length - 1) {
        break;
      }
      dataType = dataType.getChildren().get(rank);
      if (dataType.getLogicalType() instanceof RowType) {
        List<String> logicalFieldNames = ((RowType) dataType.getLogicalType()).getFieldNames();
        for (int j = 0; j < logicalFieldNames.size(); j++) {
          if (logicalFieldNames.get(j).equals(nestedFieldDim[i])) {
            dataType = dataType.getChildren().get(j);
          }
        }
      }
    }

    return schemaIndexes;
  }

  private int schemaIndexesFromSpecColumn(DataType dataType, String fieldName) {
    List<String> subFieldName = ((RowType) dataType.getLogicalType()).getFieldNames();
    for (int i = 0; i < subFieldName.size(); i++) {
      if (fieldName.equals(subFieldName.get(i))) {
        return i;
      }
    }
    return -1;
  }

  private void parseFieldNameType() {
    List<DataType> fieldDataTypes = this.schema.toRowDataType().getChildren();

    this.projectedFieldTypes = new DataType[this.projectPhysicalFields.length];
    this.projectedFieldNames = new String[this.projectPhysicalFields.length];

    String[] fieldNames = this.schema.getFieldNames();

    for (int index = 0; index < projectPhysicalFields.length; index++) {
      int[] projectPhysicalField = projectPhysicalFields[index];
      DataType dataType = fieldDataTypes.get(projectPhysicalFields[index][0]);

      if (projectPhysicalField.length == 1) {
        projectedFieldTypes[index] = dataType;
        projectedFieldNames[index] = fieldNames[projectPhysicalFields[index][0]];
        continue;
      }
      StringJoiner sj = new StringJoiner("-");
      sj.add(fieldNames[projectPhysicalFields[index][0]]);
      for (int i = 1; i < projectPhysicalField.length;i++) {
        sj.add(((RowType)dataType.getLogicalType()).getFieldNames().get(projectPhysicalField[i]));
        dataType = dataType.getChildren().get(projectPhysicalField[i]);
      }
      projectedFieldTypes[index] = dataType;
      projectedFieldNames[index] = sj.toString();
    }
  }

  private TableSchema projectSchema(DataType dataType, int[][] indexPaths) {
    Set<String> nameDomain = new HashSet<>();
    HashMap<String, LinkedList<Integer>> fieldIndexMap = new HashMap<>(indexPaths.length);
    Set<Integer> fieldLen = new HashSet<>();
    for (int[] path : indexPaths) {
      fieldLen.add(path[0]);
    }
    String[] fieldNames = new String[fieldLen.size()];
    DataType[] types = new DataType[indexPaths.length];
    int count = 0;
    for (int i = 0; i < indexPaths.length; i++) {
      int[] indexPath = indexPaths[i];
      String fieldName = ((RowType) dataType.getLogicalType()).getFieldNames().get(indexPath[0]);

      LinkedList<Integer> indexes = new LinkedList<>();
      if (fieldIndexMap.containsKey(fieldName)) {
        indexes = fieldIndexMap.get(fieldName);
      }
      indexes.add(i);
      fieldIndexMap.put(fieldName, indexes);

      if (!nameDomain.contains(fieldName)) {
        nameDomain.add(fieldName);
        fieldNames[count++] = fieldName;
      }
      types[i] = parseType(dataType, indexPaths, i, 1, null);
    }

    return TableSchema.builder().fields(fieldNames, combineField(fieldNames, types, fieldIndexMap)).build();
  }

  private DataType[] combineField(String[] fieldNames, DataType[] types, HashMap<String, LinkedList<Integer>> fieldIndexMap) {
    HashSet<String> consumedFieldNameSet = new HashSet<>();
    DataType[] dataTypes = new DataType[fieldIndexMap.size()];

    int index = 0;
    int i = 0;
    while (index < fieldNames.length) {
      String fieldName = fieldNames[index];
      if (consumedFieldNameSet.contains(fieldName)) {
        continue;
      }
      LinkedList<Integer> indexes = fieldIndexMap.get(fieldName);
      if (indexes.size() == 1) {
        dataTypes[index++] = types[i];
        i++;
      } else {
        dataTypes[index++] = combineFieldByFieldInfo(types, indexes, 1, null);
        i = i + indexes.size();
      }
      consumedFieldNameSet.add(fieldName);
    }

    return dataTypes;
  }

  private DataType combineFieldByFieldInfo(DataType[] types, LinkedList<Integer> indexes, int indexDim, DataType combinedType) {
    DataType[] nestedDataTypes = new DataType[indexes.size()];
    RowType.RowField[] rowFields =  new RowType.RowField[indexes.size()];
    int count = 0;
    for (Integer index : indexes) {
      nestedDataTypes[count++] = types[index];
    }

    if (nestedDataTypes.length == 1) {
      return nestedDataTypes[0];
    }

    if (isSameField(nestedDataTypes, indexDim)) {
      combinedType = combineFieldByFieldInfo(nestedDataTypes, indexes, indexDim + 1, combinedType);
    } else {
      HashMap<String, ArrayList<DataType>> duplicateDataTypeMap = new HashMap<>();
      ArrayList<String> fieldNames = new ArrayList<>();

      for (DataType nestedDataType : nestedDataTypes) {
        String fieldName = ((RowType) nestedDataType.getLogicalType()).getFieldNames().get(0);
        if (!fieldNames.contains(fieldName)) {
          fieldNames.add(fieldName);
        }
        ArrayList<DataType> dataTypes;
        if (!duplicateDataTypeMap.containsKey(fieldName)) {
          dataTypes = new ArrayList<>();
        } else {
          dataTypes = duplicateDataTypeMap.get(fieldName);
        }
        dataTypes.add(nestedDataType);
        duplicateDataTypeMap.put(fieldName, dataTypes);
      }
      if (duplicateDataTypeMap.size() == nestedDataTypes.length) {
        for (int i = 0; i < indexes.size(); i++) {
          rowFields[i] = new RowType.RowField(getFieldName(nestedDataTypes[i], indexDim), getFieldType(nestedDataTypes[i], indexDim).getLogicalType());
        }
        return new FieldsDataType(
                new RowType(Arrays.asList(rowFields)),
                Row.class,
                Arrays.asList(nestedDataTypes));
      } else {
        DataType[] generatedDataType = new DataType[duplicateDataTypeMap.size()];
        int index = 0;
        for (String fieldName : fieldNames) {
          ArrayList<DataType> dataTypes = duplicateDataTypeMap.get(fieldName);
          if (dataTypes.size() == 1) {
            combinedType = dataTypes.get(0);
            generatedDataType[index++] = dataTypes.get(0);
            continue;
          }
          DataType[] tmpDataTypes = new DataType[dataTypes.size()];
          LinkedList<Integer> tmpIndexesList = new LinkedList<>();
          for (int i = 0; i < dataTypes.size(); i++) {
            tmpIndexesList.add(i);
            tmpDataTypes[i] = dataTypes.get(i).getChildren().get(0);
          }

          combinedType = combineFieldByFieldInfo(tmpDataTypes, tmpIndexesList, 1, combinedType);
          RowType.RowField rowField = new RowType.RowField(fieldName, combinedType.getLogicalType());
          generatedDataType[index++] = new FieldsDataType(new RowType(Collections.singletonList(rowField)), Row.class, Collections.singletonList(combinedType));
//          index++;
        }

        LinkedList<Integer> indexesList = new LinkedList<>();
        for (int i = 0; i < generatedDataType.length; i++) {
          indexesList.add(i);
        }
        combinedType = combineFieldByFieldInfo(generatedDataType, indexesList, 1, combinedType);
      }
    }
    return combinedType;
  }

  private boolean isSameField(DataType[] nestedDataTypes, int indexDim) {
    HashSet<String> duplicateName = new HashSet<>();

    for (DataType dataType : nestedDataTypes) {
      for (int i = 0; i < indexDim - 1; i++) {
        dataType = dataType.getChildren().get(0);
      }
      duplicateName.add(((RowType)dataType.getLogicalType()).getFieldNames().get(0));
    }

    return duplicateName.size() == 1;
  }

  private String getFieldName(DataType dataType, int indexDim) {
    for (int i = 0; i < indexDim - 1; i++) {
      dataType = dataType.getChildren().get(0);
    }
    List<String> fieldNames = ((RowType) dataType.getLogicalType()).getFieldNames();
    Preconditions.checkArgument(fieldNames != null && fieldNames.size() == 1);
    return fieldNames.get(0);
  }

  private DataType getFieldType(DataType dataType, int indexDim) {
    for (int i = 0; i < indexDim - 1; i++) {
      dataType = dataType.getChildren().get(0);
    }
    List<DataType> childrenTypes = dataType.getChildren();
    Preconditions.checkArgument(childrenTypes != null && childrenTypes.size() == 1);
    return childrenTypes.get(0);
  }

  private DataType parseType(DataType dataType, int[][] indexPaths, int index, int indexDim, DataType generatedType) {
    if (indexPaths[index].length == 1) {
      return dataType.getChildren().get(indexPaths[index][0]);
    }

    if (indexDim == indexPaths[index].length) {
      DataType subNestedFieldType = dataType.getChildren().get(indexPaths[index][indexDim - 1]);
      String subNestedFieldName = ((RowType) dataType.getLogicalType()).getFieldNames().get(indexPaths[index][indexDim - 1]);

      RowType.RowField rowField = new RowType.RowField(subNestedFieldName, subNestedFieldType.getLogicalType());
      generatedType =  new FieldsDataType(
              new RowType(Collections.singletonList(rowField)),
              Row.class,
              Collections.singletonList(subNestedFieldType));
      return generatedType;
    }

    generatedType = parseType(dataType.getChildren().get(indexPaths[index][indexDim - 1]), indexPaths, index, indexDim + 1, generatedType);

    if (indexPaths[index].length == 2 || indexDim == 1) {
      return generatedType;
    }

    String fieldName = ((RowType) dataType.getLogicalType()).getFieldNames().get(indexPaths[index][indexDim - 1]);
    RowType.RowField rowField = new RowType.RowField(fieldName, generatedType.getLogicalType());
    return new FieldsDataType(
            new RowType(Collections.singletonList(rowField)),
            Row.class,
            Collections.singletonList(generatedType));
  }

  @Override
  public void applyLimit(long newLimit) {
    this.limit = newLimit;
  }

  @Override
  public Result applyFilters(List<ResolvedExpression> flinkFilters) {
    List<ResolvedExpression> acceptedFilters = Lists.newArrayList();
    List<Expression> expressions = Lists.newArrayList();

    for (ResolvedExpression resolvedExpression : flinkFilters) {
      Optional<Expression> icebergExpression = FlinkFilters.convert(resolvedExpression);
      if (icebergExpression.isPresent()) {
        expressions.add(icebergExpression.get());
        acceptedFilters.add(resolvedExpression);
      }
    }

    this.filters = expressions;
    return Result.of(acceptedFilters, flinkFilters);
  }

  @Override
  public boolean supportsNestedProjection() {
    return true;
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
    return new DataStreamScanProvider() {
      @Override
      public DataStream<RowData> produceDataStream(StreamExecutionEnvironment execEnv) {
        return createDataStream(execEnv);
      }

      @Override
      public boolean isBounded() {
        return FlinkSource.isBounded(properties);
      }
    };
  }

  @Override
  public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
    FlinkInputFormat inputFormat = FlinkSource.forRowData()
            .tableLoader(loader)
            .properties(properties)
            .project(getProjectedSchema())
            .limit(limit)
            .filters(filters)
            .flinkConf(readableConfig)
            .buildFormat();

    Duration reloadInterval =
            properties.get(
                    FlinkTableProperties.LOOKUP_JOIN_CACHE_TTL) != null
                    ? TimeUtils.parseDuration(properties.get(FlinkTableProperties.LOOKUP_JOIN_CACHE_TTL))
                    : FlinkTableProperties.LOOKUP_JOIN_CACHE_TTL_DEFAULT;

    int[][] keys = context.getKeys();
    int[] keyIndices = new int[keys.length];
    int i = 0;
    for (int[] key : keys) {
      if (key.length > 1) {
        throw new UnsupportedOperationException(
                "Hive lookup can not support nested key now.");
      }
      keyIndices[i] = key[0];
      i++;
    }

    return TableFunctionProvider.of(
            new IcebergLookupFunction(reloadInterval, inputFormat, inputFormat.getRowtype(), keyIndices, loader));
  }

  @Override
  public DynamicTableSource copy() {
    return new IcebergTableSource(this);
  }

  @Override
  public String asSummaryString() {
    return "Iceberg table source";
  }
}