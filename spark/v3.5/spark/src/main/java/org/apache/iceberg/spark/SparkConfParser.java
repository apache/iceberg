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

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;

class SparkConfParser {

  private final Map<String, String> properties;
  private final RuntimeConfig sessionConf;
  private final Map<String, String> options;

  SparkConfParser(SparkSession spark, Table table, Map<String, String> options) {
    this.properties = table.properties();
    this.sessionConf = spark.conf();
    this.options = options;
  }

  public BooleanConfParser booleanConf() {
    return new BooleanConfParser();
  }

  public IntConfParser intConf() {
    return new IntConfParser();
  }

  public LongConfParser longConf() {
    return new LongConfParser();
  }

  public StringConfParser stringConf() {
    return new StringConfParser();
  }

  class BooleanConfParser extends ConfParser<BooleanConfParser, Boolean> {
    private Boolean defaultValue;
    private boolean negate = false;

    @Override
    protected BooleanConfParser self() {
      return this;
    }

    public BooleanConfParser defaultValue(boolean value) {
      this.defaultValue = value;
      return self();
    }

    public BooleanConfParser defaultValue(String value) {
      this.defaultValue = Boolean.parseBoolean(value);
      return self();
    }

    public BooleanConfParser negate() {
      this.negate = true;
      return self();
    }

    public boolean parse() {
      Preconditions.checkArgument(defaultValue != null, "Default value cannot be null");
      boolean value = parse(Boolean::parseBoolean, defaultValue);
      return negate ? !value : value;
    }
  }

  class IntConfParser extends ConfParser<IntConfParser, Integer> {
    private Integer defaultValue;

    @Override
    protected IntConfParser self() {
      return this;
    }

    public IntConfParser defaultValue(int value) {
      this.defaultValue = value;
      return self();
    }

    public int parse() {
      Preconditions.checkArgument(defaultValue != null, "Default value cannot be null");
      return parse(Integer::parseInt, defaultValue);
    }

    public Integer parseOptional() {
      return parse(Integer::parseInt, defaultValue);
    }
  }

  class LongConfParser extends ConfParser<LongConfParser, Long> {
    private Long defaultValue;

    @Override
    protected LongConfParser self() {
      return this;
    }

    public LongConfParser defaultValue(long value) {
      this.defaultValue = value;
      return self();
    }

    public long parse() {
      Preconditions.checkArgument(defaultValue != null, "Default value cannot be null");
      return parse(Long::parseLong, defaultValue);
    }

    public Long parseOptional() {
      return parse(Long::parseLong, defaultValue);
    }
  }

  class StringConfParser extends ConfParser<StringConfParser, String> {
    private String defaultValue;

    @Override
    protected StringConfParser self() {
      return this;
    }

    public StringConfParser defaultValue(String value) {
      this.defaultValue = value;
      return self();
    }

    public String parse() {
      Preconditions.checkArgument(defaultValue != null, "Default value cannot be null");
      return parse(Function.identity(), defaultValue);
    }

    public String parseOptional() {
      return parse(Function.identity(), defaultValue);
    }
  }

  abstract class ConfParser<ThisT, T> {
    private final List<String> optionNames = Lists.newArrayList();
    private String sessionConfName;
    private String tablePropertyName;

    protected abstract ThisT self();

    public ThisT option(String name) {
      this.optionNames.add(name);
      return self();
    }

    public ThisT sessionConf(String name) {
      this.sessionConfName = name;
      return self();
    }

    public ThisT tableProperty(String name) {
      this.tablePropertyName = name;
      return self();
    }

    protected T parse(Function<String, T> conversion, T defaultValue) {
      if (!optionNames.isEmpty()) {
        for (String optionName : optionNames) {
          // use lower case comparison as DataSourceOptions.asMap() in Spark 2 returns a lower case
          // map
          String optionValue = options.get(optionName.toLowerCase(Locale.ROOT));
          if (optionValue != null) {
            return conversion.apply(optionValue);
          }
        }
      }

      if (sessionConfName != null) {
        String sessionConfValue = sessionConf.get(sessionConfName, null);
        if (sessionConfValue != null) {
          return conversion.apply(sessionConfValue);
        }
      }

      if (tablePropertyName != null) {
        String propertyValue = properties.get(tablePropertyName);
        if (propertyValue != null) {
          return conversion.apply(propertyValue);
        }
      }

      return defaultValue;
    }
  }
}
