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
package org.apache.iceberg.spark.procedures;

import java.lang.reflect.Array;
import java.util.Map;
import java.util.function.BiFunction;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.Spark3Util.CatalogAndIdentifier;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/** A class that abstracts common logic for working with input to a procedure. */
class ProcedureInput {

  private static final DataType STRING_ARRAY = DataTypes.createArrayType(DataTypes.StringType);
  private static final DataType STRING_MAP =
      DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType);

  private final SparkSession spark;
  private final TableCatalog catalog;
  private final Map<String, Integer> paramOrdinals;
  private final InternalRow args;

  ProcedureInput(
      SparkSession spark, TableCatalog catalog, ProcedureParameter[] params, InternalRow args) {
    this.spark = spark;
    this.catalog = catalog;
    this.paramOrdinals = computeParamOrdinals(params);
    this.args = args;
  }

  public boolean isProvided(ProcedureParameter param) {
    int ordinal = ordinal(param);
    return !args.isNullAt(ordinal);
  }

  public boolean bool(ProcedureParameter param, boolean defaultValue) {
    validateParamType(param, DataTypes.BooleanType);
    int ordinal = ordinal(param);
    return args.isNullAt(ordinal) ? defaultValue : args.getBoolean(ordinal);
  }

  public String string(ProcedureParameter param) {
    String value = string(param, null);
    Preconditions.checkArgument(value != null, "Parameter '%s' is not set", param.name());
    return value;
  }

  public String string(ProcedureParameter param, String defaultValue) {
    validateParamType(param, DataTypes.StringType);
    int ordinal = ordinal(param);
    return args.isNullAt(ordinal) ? defaultValue : args.getString(ordinal);
  }

  public String[] stringArray(ProcedureParameter param) {
    String[] value = stringArray(param, null);
    Preconditions.checkArgument(value != null, "Parameter '%s' is not set", param.name());
    return value;
  }

  public String[] stringArray(ProcedureParameter param, String[] defaultValue) {
    validateParamType(param, STRING_ARRAY);
    return array(
        param,
        (array, ordinal) -> array.getUTF8String(ordinal).toString(),
        String.class,
        defaultValue);
  }

  @SuppressWarnings("unchecked")
  private <T> T[] array(
      ProcedureParameter param,
      BiFunction<ArrayData, Integer, T> convertElement,
      Class<T> elementClass,
      T[] defaultValue) {

    int ordinal = ordinal(param);

    if (args.isNullAt(ordinal)) {
      return defaultValue;
    }

    ArrayData arrayData = args.getArray(ordinal);

    T[] convertedArray = (T[]) Array.newInstance(elementClass, arrayData.numElements());

    for (int index = 0; index < arrayData.numElements(); index++) {
      convertedArray[index] = convertElement.apply(arrayData, index);
    }

    return convertedArray;
  }

  public Map<String, String> stringMap(ProcedureParameter param, Map<String, String> defaultValue) {
    validateParamType(param, STRING_MAP);
    return map(
        param,
        (keys, ordinal) -> keys.getUTF8String(ordinal).toString(),
        (values, ordinal) -> values.getUTF8String(ordinal).toString(),
        defaultValue);
  }

  private <K, V> Map<K, V> map(
      ProcedureParameter param,
      BiFunction<ArrayData, Integer, K> convertKey,
      BiFunction<ArrayData, Integer, V> convertValue,
      Map<K, V> defaultValue) {

    int ordinal = ordinal(param);

    if (args.isNullAt(ordinal)) {
      return defaultValue;
    }

    MapData mapData = args.getMap(ordinal);

    Map<K, V> convertedMap = Maps.newHashMap();

    for (int index = 0; index < mapData.numElements(); index++) {
      K convertedKey = convertKey.apply(mapData.keyArray(), index);
      V convertedValue = convertValue.apply(mapData.valueArray(), index);
      convertedMap.put(convertedKey, convertedValue);
    }

    return convertedMap;
  }

  public Identifier ident(ProcedureParameter param) {
    String identAsString = string(param);
    CatalogAndIdentifier catalogAndIdent = toCatalogAndIdent(identAsString, param.name(), catalog);

    Preconditions.checkArgument(
        catalogAndIdent.catalog().equals(catalog),
        "Cannot run procedure in catalog '%s': '%s' is a table in catalog '%s'",
        catalog.name(),
        identAsString,
        catalogAndIdent.catalog().name());

    return catalogAndIdent.identifier();
  }

  public Identifier ident(ProcedureParameter param, CatalogPlugin defaultCatalog) {
    String identAsString = string(param);
    return toCatalogAndIdent(identAsString, param.name(), defaultCatalog).identifier();
  }

  private CatalogAndIdentifier toCatalogAndIdent(
      String identAsString, String paramName, CatalogPlugin defaultCatalog) {

    Preconditions.checkArgument(
        StringUtils.isNotBlank(identAsString),
        "Cannot handle an empty identifier for parameter '%s'",
        paramName);

    String desc = String.format("identifier for parameter '%s'", paramName);
    return Spark3Util.catalogAndIdentifier(desc, spark, identAsString, defaultCatalog);
  }

  private int ordinal(ProcedureParameter param) {
    return paramOrdinals.get(param.name());
  }

  private Map<String, Integer> computeParamOrdinals(ProcedureParameter[] params) {
    Map<String, Integer> ordinals = Maps.newHashMap();

    for (int index = 0; index < params.length; index++) {
      String paramName = params[index].name();

      Preconditions.checkArgument(
          !ordinals.containsKey(paramName),
          "Detected multiple parameters named as '%s'",
          paramName);

      ordinals.put(paramName, index);
    }

    return ordinals;
  }

  private void validateParamType(ProcedureParameter param, DataType expectedDataType) {
    Preconditions.checkArgument(
        expectedDataType.sameType(param.dataType()),
        "Parameter '%s' must be of type %s",
        param.name(),
        expectedDataType.catalogString());
  }
}
