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

package org.apache.iceberg.nessie;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalQuery;
import java.util.Optional;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.common.DynMethods;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableReference {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableReference.class);
  private static final ZoneId UTC = ZoneId.of("UTC");

  private static DynMethods.StaticMethod SPARK_SESSION_METHOD;
  private static DynMethods.UnboundMethod SPARK_CONTEXT_METHOD;
  private static DynMethods.UnboundMethod SPARK_CONF_METHOD;
  private static DynMethods.UnboundMethod SPARK_CONF_GET_METHOD;
  private static Boolean SPARK_AVAILABLE = null;
  private final TableIdentifier tableIdentifier;
  private final Instant timestamp;
  private final String reference;

  /**
   * Container class to specify a TableIdentifier on a specific Reference or at an Instant in time.
   */
  public TableReference(TableIdentifier tableIdentifier, Instant timestamp, String reference) {
    this.tableIdentifier = tableIdentifier;
    this.timestamp = timestamp;
    this.reference = reference;
  }

  public TableIdentifier tableIdentifier() {
    return tableIdentifier;
  }

  public Instant timestamp() {
    return timestamp;
  }

  public String reference() {
    return reference;
  }

  /**
   * Convert dataset read/write options to a table and ref/hash.
   */
  public static TableReference parse(TableIdentifier path) {
    TableReference pti = parse(path.name());
    return new TableReference(TableIdentifier.of(path.namespace(), pti.tableIdentifier().name()),
        pti.timestamp(),
        pti.reference());
  }

  /**
   * Convert dataset read/write options to a table and ref/hash.
   */
  public static TableReference parse(String path) {
    // I am assuming tables can't have @ or # symbols
    if (path.split("@").length > 2) {
      throw new IllegalArgumentException(String.format("Can only reference one branch in %s", path));
    }
    if (path.split("#").length > 2) {
      throw new IllegalArgumentException(String.format("Can only reference one timestamp in %s", path));
    }

    if (path.contains("@") && path.contains("#")) {
      String[] tableRef = path.split("@");
      if (tableRef[0].contains("#")) {
        throw new IllegalArgumentException("Invalid table name:" +
            " # is not allowed before @. Correct format is table@ref#timestamp");
      }
      TableIdentifier identifier = TableIdentifier.parse(tableRef[0]);
      String[] timestampRef = tableRef[1].split("#");
      return new TableReference(identifier, parseTimestamp(timestampRef[1]), timestampRef[0]);
    }

    if (path.contains("@")) {
      String[] tableRef = path.split("@");
      TableIdentifier identifier = TableIdentifier.parse(tableRef[0]);
      return new TableReference(identifier, null, tableRef[1]);
    }

    if (path.contains("#")) {
      String[] tableTimestamp = path.split("#");
      TableIdentifier identifier = TableIdentifier.parse(tableTimestamp[0]);
      return new TableReference(identifier, parseTimestamp(tableTimestamp[1]), null);
    }

    TableIdentifier identifier = TableIdentifier.parse(path);

    return new TableReference(identifier, null, null);
  }

  private enum FormatOptions {
    DATE_TIME(DateTimeFormatter.ISO_DATE_TIME, ZonedDateTime::from),
    LOCAL_DATE_TIME(DateTimeFormatter.ISO_LOCAL_DATE_TIME, t -> LocalDateTime.from(t).atZone(sparkTimezoneOrUTC())),
    LOCAL_DATE(DateTimeFormatter.ISO_LOCAL_DATE, t -> LocalDate.from(t).atStartOfDay(sparkTimezoneOrUTC()));

    private final DateTimeFormatter formatter;
    private final TemporalQuery<ZonedDateTime> converter;

    FormatOptions(DateTimeFormatter formatter, TemporalQuery<ZonedDateTime> converter) {
      this.formatter = formatter;
      this.converter = converter;
    }

    public ZonedDateTime convert(String timestampStr) {
      try {
        return formatter.parse(timestampStr, converter);
      } catch (DateTimeParseException e) {
        return null;
      }
    }
  }

  private static Instant parseTimestamp(String timestamp) {
    ZonedDateTime parsed;
    for (FormatOptions options : FormatOptions.values()) {
      parsed = options.convert(modifyTimestamp(timestamp));
      if (parsed != null) {
        return endOfPeriod(parsed, timestamp).toInstant();
      }
    }
    throw new IllegalArgumentException(
        String.format("Cannot parse timestamp: %s is not a legal format. (Use an ISO 8601 compliant string)", timestamp)
    );
  }

  private static String modifyTimestamp(String timestamp) {
    if (timestamp.length() == 7) {
      // only month. add a day
      return String.format("%s-01", timestamp);
    } else if (timestamp.length() == 4) {
      // only year. add a month and day
      return String.format("%s-01-01", timestamp);
    } else {
      return timestamp;
    }
  }

  private static ZonedDateTime endOfPeriod(ZonedDateTime instant, String timestamp) {
    if (timestamp.length() == 10) {
      // only date. Move to end of day
      return endOfPeriod(instant, ChronoUnit.DAYS);
    } else if (timestamp.length() == 7) {
      // only month. Move to end of month
      return endOfPeriod(instant, ChronoUnit.MONTHS);
    } else if (timestamp.length() == 4) {
      // only month. Move to end of month
      return endOfPeriod(instant, ChronoUnit.YEARS);
    } else {
      return instant;
    }
  }

  private static ZonedDateTime endOfPeriod(ZonedDateTime instant, ChronoUnit unit) {
    return instant.plus(1, unit).minus(1, ChronoUnit.MICROS);
  }

  private static ZoneId sparkTimezoneOrUTC() {
    return sparkTimezone().map(ZoneId::of).orElse(UTC);
  }

  private static Optional<String> sparkTimezone() {
    if (SPARK_AVAILABLE != null) {
      try {
        SPARK_SESSION_METHOD = DynMethods.builder("active")
            .impl("org.apache.spark.sql.SparkSession").buildStatic();
        SPARK_CONTEXT_METHOD = DynMethods.builder("sparkContext")
            .impl("org.apache.spark.sql.SparkSession").build();
        SPARK_CONF_METHOD = DynMethods.builder("conf")
            .impl("org.apache.spark.SparkContext").build();
        SPARK_CONF_GET_METHOD = DynMethods.builder("get")
            .impl("org.apache.spark.SparkConf").build();
        SPARK_AVAILABLE = true;
      } catch (RuntimeException e) {
        SPARK_AVAILABLE = false; // spark not on classpath
      }
    }
    if (SPARK_AVAILABLE != null && SPARK_AVAILABLE) {
      try {
        Object sparkContext = SPARK_CONTEXT_METHOD.bind(SPARK_SESSION_METHOD.invoke()).invoke();
        Object sparkConf = SPARK_CONF_METHOD.bind(sparkContext).invoke();
        return Optional.ofNullable(SPARK_CONF_GET_METHOD.bind(sparkConf).invoke("spark.sql.session.timeZone"));
      } catch (RuntimeException e) {
        // we may fail to get Spark timezone, we don't want to crash over that so just log and continue.
        LOGGER.warn("Cannot find Spark timezone. Using UTC instead.", e);
      }
    }
    return Optional.empty();
  }
}
