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
package org.apache.iceberg.connect.transforms;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

/**
 * A Kafka Connect transformation that converts fields of logical type {@code Time} to their integer
 * representation (milliseconds since midnight) to make it compatible with spark iceberg readers.
 * {@code Time} fields.
 *
 * @param <R> the type of {@link ConnectRecord} the transform operates on
 */
public abstract class TimeTypeConverter<R extends ConnectRecord<R>> implements Transformation<R> {

  public static final String OVERVIEW_DOC =
      "Convert timestamps between different formats such as Unix epoch, strings, and Connect Date/Timestamp types."
          + "Supports scanning the entire schema."
          + "<p/>Use the concrete transformation type designed for the record key (<code>"
          + TimeTypeConverter.Key.class.getName()
          + "</code>) "
          + "or value (<code>"
          + TimeTypeConverter.Value.class.getName()
          + "</code>).";
  public static final String INPUT_TYPE_CONFIG = "input.type";
  public static final String TARGET_TYPE_CONFIG = "target.type";
  public static final String FORMAT_CONFIG = "format";
  public static final String UNIX_PRECISION_CONFIG = "unix.precision";
  public static final String REPLACE_NULL_WITH_DEFAULT_CONFIG = "replace.null.with.default";

  // Default value
  private static final String FORMAT_DEFAULT = "";
  private static final String UNIX_PRECISION_DEFAULT = "milliseconds";
  private static final boolean REPLACE_NULL_WITH_DEFAULT_DEFAULT = true;

  private static final String PURPOSE = "converting time type to {STRING, UNIX, DATE, TIMESTAMP}";

  // Supported timestamp types
  private static final String TYPE_STRING = "string";
  private static final String TYPE_UNIX = "unix";
  private static final String TYPE_DATE = "Date";
  private static final String TYPE_TIME = "Time";
  private static final String TYPE_TIMESTAMP = "Timestamp";

  // Unix precision values
  private static final String UNIX_PRECISION_MILLIS = "milliseconds";
  private static final String UNIX_PRECISION_MICROS = "microseconds";
  private static final String UNIX_PRECISION_NANOS = "nanoseconds";
  private static final String UNIX_PRECISION_SECONDS = "seconds";

  private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

  // Schema definitions
  public static final Schema OPTIONAL_DATE_SCHEMA =
      org.apache.kafka.connect.data.Date.builder().optional().schema();
  public static final Schema OPTIONAL_TIMESTAMP_SCHEMA = Timestamp.builder().optional().schema();
  public static final Schema OPTIONAL_TIME_SCHEMA = Time.builder().optional().schema();

  public static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              INPUT_TYPE_CONFIG,
              ConfigDef.Type.STRING,
              ConfigDef.NO_DEFAULT_VALUE,
              ConfigDef.ValidString.in(
                  TYPE_STRING, TYPE_UNIX, TYPE_DATE, TYPE_TIME, TYPE_TIMESTAMP, ""),
              ConfigDef.Importance.HIGH,
              "The input time type representation to look for: string, unix, Date, Time, or Timestamp")
          .define(
              TARGET_TYPE_CONFIG,
              ConfigDef.Type.STRING,
              ConfigDef.NO_DEFAULT_VALUE,
              ConfigDef.ValidString.in(TYPE_STRING, TYPE_UNIX, TYPE_DATE, TYPE_TIMESTAMP),
              ConfigDef.Importance.HIGH,
              "The desired timestamp representation: string, unix, Date, Time, or Timestamp")
          .define(
              FORMAT_CONFIG,
              ConfigDef.Type.STRING,
              FORMAT_DEFAULT,
              ConfigDef.Importance.MEDIUM,
              "A SimpleDateFormat-compatible format for the timestamp. Used to generate the output when type=string "
                  + "or used to parse the input if the input is a string.")
          .define(
              UNIX_PRECISION_CONFIG,
              ConfigDef.Type.STRING,
              UNIX_PRECISION_DEFAULT,
              ConfigDef.ValidString.in(
                  UNIX_PRECISION_NANOS, UNIX_PRECISION_MICROS,
                  UNIX_PRECISION_MILLIS, UNIX_PRECISION_SECONDS),
              ConfigDef.Importance.LOW,
              "The desired Unix precision for the timestamp: seconds, milliseconds, microseconds, or nanoseconds")
          .define(
              REPLACE_NULL_WITH_DEFAULT_CONFIG,
              ConfigDef.Type.BOOLEAN,
              REPLACE_NULL_WITH_DEFAULT_DEFAULT,
              ConfigDef.Importance.MEDIUM,
              "Whether to replace fields that have a default value and that are null with the default value");

  /** Interface for converting between different timestamp representations */
  private interface TimestampTranslator {
    /**
     * Convert from the type-specific format to the universal java.util.Date format
     *
     * @param config the transformation configuration
     * @param orig the original timestamp value
     * @return the timestamp as a Date object
     */
    Date toRaw(Config config, Object orig);

    /**
     * Get the schema for this timestamp type
     *
     * @param isOptional whether the schema should be optional
     * @return the Schema for this timestamp type
     */
    Schema typeSchema(boolean isOptional);

    /**
     * Convert from the universal java.util.Date format to the type-specific format
     *
     * @param config the transformation configuration
     * @param orig the Date object to convert
     * @return the timestamp in the target type's format
     */
    Object toType(Config config, Date orig);
  }

  // Registry of timestamp translators
  private static final Map<String, TimestampTranslator> TRANSLATORS = Maps.newHashMap();

  static {
    TRANSLATORS.put(
        TYPE_STRING,
        new TimestampTranslator() {
          @Override
          public Date toRaw(Config config, Object orig) {
            if (!(orig instanceof String)) {
              throw new DataException(
                  "Expected string timestamp to be a String, but found " + orig.getClass());
            }
            try {
              return config.format.parse((String) orig);
            } catch (ParseException e) {
              throw new DataException(
                  "Could not parse timestamp: value ("
                      + orig
                      + ") does not match pattern ("
                      + config.format.toPattern()
                      + ")",
                  e);
            }
          }

          @Override
          public Schema typeSchema(boolean isOptional) {
            return isOptional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
          }

          @Override
          public String toType(Config config, Date orig) {
            synchronized (config.format) {
              return config.format.format(orig);
            }
          }
        });

    TRANSLATORS.put(
        TYPE_UNIX,
        new TimestampTranslator() {
          @Override
          public Date toRaw(Config config, Object orig) {
            if (!(orig instanceof Long)) {
              throw new DataException(
                  "Expected Unix timestamp to be a Long, but found " + orig.getClass());
            }
            long unixTime = (Long) orig;

            switch (config.unixPrecision) {
              case UNIX_PRECISION_SECONDS:
                return Timestamp.toLogical(Timestamp.SCHEMA, TimeUnit.SECONDS.toMillis(unixTime));
              case UNIX_PRECISION_MICROS:
                return Timestamp.toLogical(
                    Timestamp.SCHEMA, TimeUnit.MICROSECONDS.toMillis(unixTime));
              case UNIX_PRECISION_NANOS:
                return Timestamp.toLogical(
                    Timestamp.SCHEMA, TimeUnit.NANOSECONDS.toMillis(unixTime));
              default:
                return Timestamp.toLogical(Timestamp.SCHEMA, unixTime);
            }
          }

          @Override
          public Schema typeSchema(boolean isOptional) {
            return isOptional ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA;
          }

          @Override
          public Long toType(Config config, Date orig) {
            long unixTimeMillis = Timestamp.fromLogical(Timestamp.SCHEMA, orig);

            switch (config.unixPrecision) {
              case UNIX_PRECISION_SECONDS:
                return TimeUnit.MILLISECONDS.toSeconds(unixTimeMillis);
              case UNIX_PRECISION_MICROS:
                return TimeUnit.MILLISECONDS.toMicros(unixTimeMillis);
              case UNIX_PRECISION_NANOS:
                return TimeUnit.MILLISECONDS.toNanos(unixTimeMillis);
              default:
                return unixTimeMillis;
            }
          }
        });

    TRANSLATORS.put(
        TYPE_DATE,
        new TimestampTranslator() {
          @Override
          public Date toRaw(Config config, Object orig) {
            if (!(orig instanceof Date)) {
              throw new DataException(
                  "Expected Date to be a java.util.Date, but found " + orig.getClass());
            }
            // Already represented as a java.util.Date and Connect Dates are a subset of valid
            // java.util.Date values
            return (Date) orig;
          }

          @Override
          public Schema typeSchema(boolean isOptional) {
            return isOptional ? OPTIONAL_DATE_SCHEMA : org.apache.kafka.connect.data.Date.SCHEMA;
          }

          @Override
          public Date toType(Config config, Date orig) {
            Calendar result = Calendar.getInstance(UTC);
            result.setTime(orig);
            result.set(Calendar.HOUR_OF_DAY, 0);
            result.set(Calendar.MINUTE, 0);
            result.set(Calendar.SECOND, 0);
            result.set(Calendar.MILLISECOND, 0);
            return result.getTime();
          }
        });

    TRANSLATORS.put(
        TYPE_TIME,
        new TimestampTranslator() {
          @Override
          public Date toRaw(Config config, Object orig) {
            if (!(orig instanceof Date)) {
              throw new DataException(
                  "Expected Time to be a java.util.Date, but found " + orig.getClass());
            }
            // Already represented as a java.util.Date and Connect Times are a subset of valid
            // java.util.Date values
            return (Date) orig;
          }

          @Override
          public Schema typeSchema(boolean isOptional) {
            return isOptional ? OPTIONAL_TIME_SCHEMA : Time.SCHEMA;
          }

          @Override
          public Date toType(Config config, Date orig) {
            Calendar origCalendar = Calendar.getInstance(UTC);
            origCalendar.setTime(orig);
            Calendar result = Calendar.getInstance(UTC);
            result.setTimeInMillis(0L);
            result.set(Calendar.HOUR_OF_DAY, origCalendar.get(Calendar.HOUR_OF_DAY));
            result.set(Calendar.MINUTE, origCalendar.get(Calendar.MINUTE));
            result.set(Calendar.SECOND, origCalendar.get(Calendar.SECOND));
            result.set(Calendar.MILLISECOND, origCalendar.get(Calendar.MILLISECOND));
            return result.getTime();
          }
        });

    TRANSLATORS.put(
        TYPE_TIMESTAMP,
        new TimestampTranslator() {
          @Override
          public Date toRaw(Config config, Object orig) {
            if (!(orig instanceof Date)) {
              throw new DataException(
                  "Expected Timestamp to be a java.util.Date, but found " + orig.getClass());
            }
            return (Date) orig;
          }

          @Override
          public Schema typeSchema(boolean isOptional) {
            return isOptional ? OPTIONAL_TIMESTAMP_SCHEMA : Timestamp.SCHEMA;
          }

          @Override
          public Date toType(Config config, Date orig) {
            return orig;
          }
        });
  }

  /** Configuration holder class */
  private static class Config {
    private final String type;
    private final SimpleDateFormat format;
    private final String unixPrecision;

    Config(String type, SimpleDateFormat format, String unixPrecision) {
      this.type = type;
      this.format = format;
      this.unixPrecision = unixPrecision;
    }
  }

  private Config config;
  private Cache<Schema, Schema> schemaUpdateCache;
  private boolean replaceNullWithDefault;
  private String inputType;

  @Override
  public void configure(Map<String, ?> configs) {
    final SimpleConfig simpleConfig = new SimpleConfig(CONFIG_DEF, configs);
    inputType = simpleConfig.getString(INPUT_TYPE_CONFIG);
    final String type = simpleConfig.getString(TARGET_TYPE_CONFIG);
    String formatPattern = simpleConfig.getString(FORMAT_CONFIG);
    final String unixPrecision = simpleConfig.getString(UNIX_PRECISION_CONFIG);

    schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    replaceNullWithDefault = simpleConfig.getBoolean(REPLACE_NULL_WITH_DEFAULT_CONFIG);
    validateFormat(type, formatPattern);

    SimpleDateFormat format = createDateFormat(formatPattern);

    config = new Config(type, format, unixPrecision);
  }

  private void validateFormat(String type, String formatPattern) {
    if (TYPE_STRING.equals(type) && Utils.isBlank(formatPattern)) {
      throw new ConfigException(
          "TimeTypeConverter requires format option to be specified when using string timestamps");
    }
  }

  private SimpleDateFormat createDateFormat(String pattern) {
    if (Utils.isBlank(pattern)) {
      return null;
    }
    try {
      SimpleDateFormat format = new SimpleDateFormat(pattern, Locale.getDefault());
      format.setTimeZone(UTC);
      return format;
    } catch (IllegalArgumentException e) {
      throw new ConfigException(
          "TimeTypeConverter requires a SimpleDateFormat-compatible pattern for string timestamps: "
              + pattern,
          e);
    }
  }

  @Override
  public R apply(R record) {
    if (operatingSchema(record) == null) {
      return record;
    } else {
      return applyWithSchema(record);
    }
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
    schemaUpdateCache = null;
  }

  private R applyWithSchema(R record) {
    final Struct value = Requirements.requireStructOrNull(operatingValue(record), PURPOSE);
    final Schema schema = operatingSchema(record);
    Schema updatedSchema = updateSchemaRecursive(schema);
    Object updatedValue = convertValueRecursive(value, schema, updatedSchema);
    return newRecord(record, updatedSchema, updatedValue);
  }

  /** Recursively update the schema for bootstrap mode */
  private Schema updateSchemaRecursive(Schema schema) {
    Schema cachedSchema = schemaUpdateCache.get(schema);
    if (cachedSchema != null) {
      return cachedSchema;
    }

    SchemaBuilder builder;
    switch (schema.type()) {
      case STRUCT:
        builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (Field field : schema.fields()) {
          builder.field(field.name(), updateSchemaRecursive(field.schema()));
        }
        break;
      case ARRAY:
        Schema elementSchema = updateSchemaRecursive(schema.valueSchema());
        builder = SchemaBuilder.array(elementSchema);
        break;
      case MAP:
        Schema keySchema = updateSchemaRecursive(schema.keySchema());
        Schema valueSchema = updateSchemaRecursive(schema.valueSchema());
        builder = SchemaBuilder.map(keySchema, valueSchema);
        break;
      default:
        if (org.apache.kafka.connect.data.Date.LOGICAL_NAME.equalsIgnoreCase(schema.name())
            || Time.LOGICAL_NAME.equalsIgnoreCase(schema.name())
            || Timestamp.LOGICAL_NAME.equalsIgnoreCase(schema.name())
            || schema.type() == Schema.Type.STRING
            || schema.type() == Schema.Type.INT64) {
          String schemaType = timestampTypeFromSchema(schema);
          if (schemaType.equals(inputType)) {
            builder =
                SchemaUtil.copySchemaBasics(
                    TRANSLATORS.get(config.type).typeSchema(schema.isOptional()));
          } else {
            builder = SchemaUtil.copySchemaBasics(schema);
          }
        } else {
          builder = SchemaUtil.copySchemaBasics(schema);
        }
    }

    if (schema.isOptional()) {
      builder.optional();
    }
    if (schema.defaultValue() != null) {
      builder.defaultValue(convertValueRecursive(schema.defaultValue(), schema, builder));
    }
    Schema updatedSchema = builder.build();
    schemaUpdateCache.put(schema, updatedSchema);
    return updatedSchema;
  }

  /** Recursively convert values in bootstrap mode */
  @SuppressWarnings("unchecked")
  private Object convertValueRecursive(Object value, Schema schema, Schema updatedSchema) {
    if (value == null) {
      return null;
    }

    switch (schema.type()) {
      case STRUCT:
        Struct struct = (Struct) value;
        Struct updatedStruct = new Struct(updatedSchema);
        for (Field field : schema.fields()) {
          Object fieldValue = getFieldValue(struct, field);
          updatedStruct.put(
              field.name(),
              convertValueRecursive(
                  fieldValue, field.schema(), updatedSchema.field(field.name()).schema()));
        }
        return updatedStruct;
      case ARRAY:
        List<Object> array = (List<Object>) value;
        List<Object> updatedArray = Lists.newArrayList();
        for (Object element : array) {
          updatedArray.add(
              convertValueRecursive(element, schema.valueSchema(), updatedSchema.valueSchema()));
        }
        return updatedArray;
      case MAP:
        Map<Object, Object> map = (Map<Object, Object>) value;
        Map<Object, Object> updatedMap = Maps.newHashMap();
        for (Map.Entry<Object, Object> entry : map.entrySet()) {
          Object key =
              convertValueRecursive(entry.getKey(), schema.keySchema(), updatedSchema.keySchema());
          Object valueItem =
              convertValueRecursive(
                  entry.getValue(), schema.valueSchema(), updatedSchema.valueSchema());
          updatedMap.put(key, valueItem);
        }
        return updatedMap;
      default:
        if (org.apache.kafka.connect.data.Date.LOGICAL_NAME.equalsIgnoreCase(schema.name())
            || Time.LOGICAL_NAME.equalsIgnoreCase(schema.name())
            || Timestamp.LOGICAL_NAME.equalsIgnoreCase(schema.name())
            || schema.type() == Schema.Type.STRING
            || schema.type() == Schema.Type.INT64) {
          String schemaType = timestampTypeFromSchema(schema);
          if (schemaType.equals(inputType)) {
            Object convertedTimeStamp = convertTimestamp(value, schemaType);
            return convertedTimeStamp;
          } else {
            return value;
          }
        } else {
          return value;
        }
    }
  }

  private Object getFieldValue(Struct value, Field field) {
    if (replaceNullWithDefault) {
      return value.get(field);
    }
    return value.getWithoutDefault(field.name());
  }

  /** Determine the timestamp type from a schema */
  private String timestampTypeFromSchema(Schema schema) {
    if (schema == null) {
      throw new ConnectException("Schema must not be null");
    }
    if (Timestamp.LOGICAL_NAME.equals(schema.name())) {
      return TYPE_TIMESTAMP;
    } else if (org.apache.kafka.connect.data.Date.LOGICAL_NAME.equals(schema.name())) {
      return TYPE_DATE;
    } else if (Time.LOGICAL_NAME.equals(schema.name())) {
      return TYPE_TIME;
    } else if (schema.type() == Schema.Type.STRING) {
      return TYPE_STRING;
    } else if (schema.type() == Schema.Type.INT64) {
      return TYPE_UNIX;
    }
    throw new ConnectException(
        "Schema " + schema + " does not correspond to a known timestamp type format");
  }

  /** Infer the timestamp type from a Java object */
  private String inferTimestampType(Object timestamp) {
    if (timestamp instanceof Date) {
      return TYPE_TIMESTAMP;
    } else if (timestamp instanceof Long) {
      return TYPE_UNIX;
    } else if (timestamp instanceof String) {
      return TYPE_STRING;
    }
    throw new DataException(
        "TimeTypeConverter does not support "
            + (timestamp == null ? "null" : timestamp.getClass())
            + " objects as timestamps");
  }

  /** Convert a timestamp between formats */
  private Object convertTimestamp(Object timestamp, String timestampFormat) {
    if (timestamp == null) {
      return null;
    }
    String sourceTimeStampFormat = timestampFormat;
    if (sourceTimeStampFormat == null) {
      sourceTimeStampFormat = inferTimestampType(timestamp);
    }

    TimestampTranslator sourceTranslator = TRANSLATORS.get(sourceTimeStampFormat);
    if (sourceTranslator == null) {
      throw new ConnectException("Unsupported timestamp type: " + sourceTimeStampFormat);
    }
    Date rawTimestamp = sourceTranslator.toRaw(config, timestamp);

    TimestampTranslator targetTranslator = TRANSLATORS.get(config.type);
    if (targetTranslator == null) {
      throw new ConnectException("Unsupported timestamp type: " + config.type);
    }
    return targetTranslator.toType(config, rawTimestamp);
  }

  public static class Key<R extends ConnectRecord<R>> extends TimeTypeConverter<R> {
    @Override
    protected Schema operatingSchema(R record) {
      return record.keySchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.key();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(
          record.topic(),
          record.kafkaPartition(),
          updatedSchema,
          updatedValue,
          record.valueSchema(),
          record.value(),
          record.timestamp());
    }
  }

  public static class Value<R extends ConnectRecord<R>> extends TimeTypeConverter<R> {
    @Override
    protected Schema operatingSchema(R record) {
      return record.valueSchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.value();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(
          record.topic(),
          record.kafkaPartition(),
          record.keySchema(),
          record.key(),
          updatedSchema,
          updatedValue,
          record.timestamp());
    }
  }

  protected abstract Schema operatingSchema(R record);

  protected abstract Object operatingValue(R record);

  protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);
}
