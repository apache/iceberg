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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.TextElement;
import org.apache.iceberg.SnapshotRef;

/** Flink sink write options */
public class FlinkWriteOptions {

  private FlinkWriteOptions() {}

  // File format for write operations(default: Table write.format.default )
  public static final ConfigOption<String> WRITE_FORMAT =
      ConfigOptions.key("write-format").stringType().noDefaultValue();

  // Overrides this table's write.target-file-size-bytes
  public static final ConfigOption<Long> TARGET_FILE_SIZE_BYTES =
      ConfigOptions.key("target-file-size-bytes").longType().noDefaultValue();

  // Overrides this table's write.<FILE_FORMAT>.compression-codec
  public static final ConfigOption<String> COMPRESSION_CODEC =
      ConfigOptions.key("compression-codec").stringType().noDefaultValue();

  // Overrides this table's write.<FILE_FORMAT>.compression-level
  public static final ConfigOption<String> COMPRESSION_LEVEL =
      ConfigOptions.key("compression-level").stringType().noDefaultValue();

  // Overrides this table's write.<FILE_FORMAT>.compression-strategy
  public static final ConfigOption<String> COMPRESSION_STRATEGY =
      ConfigOptions.key("compression-strategy").stringType().noDefaultValue();

  // Overrides this table's write.upsert.enabled
  public static final ConfigOption<Boolean> WRITE_UPSERT_ENABLED =
      ConfigOptions.key("upsert-enabled").booleanType().noDefaultValue();

  public static final ConfigOption<Boolean> OVERWRITE_MODE =
      ConfigOptions.key("overwrite-enabled").booleanType().defaultValue(false);

  // Overrides the table's write.distribution-mode
  public static final ConfigOption<String> DISTRIBUTION_MODE =
      ConfigOptions.key("distribution-mode").stringType().noDefaultValue();

  // Branch to write to
  public static final ConfigOption<String> BRANCH =
      ConfigOptions.key("branch").stringType().defaultValue(SnapshotRef.MAIN_BRANCH);

  public static final ConfigOption<Integer> WRITE_PARALLELISM =
      ConfigOptions.key("write-parallelism").intType().noDefaultValue();

  public static final ConfigOption<Boolean> SINK_PARTITION_COMMIT_ENABLED =
      ConfigOptions.key("sink.partition-commit.enabled").booleanType().defaultValue(false);

  public static final ConfigOption<Duration> SINK_PARTITION_COMMIT_DELAY =
      ConfigOptions.key("sink.partition-commit.delay")
          .durationType()
          .defaultValue(Duration.ofMillis(0))
          .withDescription(
              Description.builder()
                  .text(
                      "The partition will not commit until the delay time. "
                          + "The value should be '1 d' for day partitions and '1 h' for hour partitions.")
                  .build());

  public static final ConfigOption<String> SINK_PARTITION_COMMIT_WATERMARK_TIME_ZONE =
      ConfigOptions.key("sink.partition-commit.watermark-time-zone")
          .stringType()
          .defaultValue("UTC")
          .withDescription(
              "The time zone to parse the long watermark value to TIMESTAMP value,"
                  + " the parsed watermark timestamp is used to compare with partition time"
                  + " to decide the partition should commit or not."
                  + " The default value is 'UTC', which means the watermark is defined on TIMESTAMP column or not defined."
                  + " If the watermark is defined on TIMESTAMP_LTZ column, the time zone of watermark is user configured time zone,"
                  + " the value should be the user configured local time zone. The option value is either a full name"
                  + " such as 'America/Los_Angeles', or a custom timezone id such as 'GMT-08:00'.");

  public static final ConfigOption<String> SINK_PARTITION_COMMIT_POLICY_KIND =
      ConfigOptions.key("sink.partition-commit.policy.kind")
          .stringType()
          .noDefaultValue()
          .withDescription(
              "Policy to commit a partition is to notify the downstream"
                  + " application that the partition has finished writing, the partition"
                  + " is ready to be read."
                  + " default: add partition to meta data."
                  + " success-file: add '_success' file to directory."
                  + " Both can be configured at the same time: 'default,success-file'."
                  + " custom: use policy class to create a commit policy."
                  + " Support to configure multiple policies: 'metastore,success-file'.");

  public static final ConfigOption<String> SINK_PARTITION_COMMIT_POLICY_CLASS =
      ConfigOptions.key("sink.partition-commit.policy.class")
          .stringType()
          .noDefaultValue()
          .withDescription(
              "The partition commit policy class for implement"
                  + " PartitionCommitPolicy interface. Only work in custom commit policy");

  public static final ConfigOption<String> SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME =
      ConfigOptions.key("sink.partition-commit.success-file.name")
          .stringType()
          .defaultValue("_SUCCESS")
          .withDescription(
              "The file name for success-file partition commit policy,"
                  + " default is '_SUCCESS'.");

  public static final ConfigOption<String> PARTITION_TIME_EXTRACTOR_TIMESTAMP_FORMATTER =
      ConfigOptions.key("partition.time-extractor.timestamp-formatter")
          .stringType()
          .noDefaultValue()
          .withDescription(
              Description.builder()
                  .text(
                      "The formatter to format timestamp from string, it can be used with 'partition.time-extractor.timestamp-pattern', "
                          + "creates a formatter using the specified value. "
                          + "Supports multiple partition fields like '$year-$month-$day $hour:00:00'.")
                  .list(
                      TextElement.text(
                          "The timestamp-formatter is compatible with "
                              + "Java's DateTimeFormatter."))
                  .build());

  public static final ConfigOption<String> PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN =
      ConfigOptions.key("partition.time-extractor.timestamp-pattern")
          .stringType()
          .noDefaultValue()
          .withDescription(
              Description.builder()
                  .text(
                      "You can specify a pattern to get a timestamp from partitions. "
                          + "the formatter pattern is defined by 'partition.time-extractor.timestamp-formatter'.")
                  .list(
                      TextElement.text(
                          "By default, a format of 'yyyy-MM-dd hh:mm:ss' is read from the first field."),
                      TextElement.text(
                          "If the timestamp in the partition is a single field called 'dt', you can use '$dt'."),
                      TextElement.text(
                          "If it is spread across multiple fields for year, month, day, and hour, you can use '$year-$month-$day $hour:00:00'."),
                      TextElement.text(
                          "If the timestamp is in fields dt and hour, you can use '$dt "
                              + "$hour:00:00'."),
                      TextElement.text(
                          "By basicDate, a format of 'yyyyMMdd' is read from the first field."),
                      TextElement.text(
                          "If the timestamp in the partition is a single field called 'dt', you can use '$dt'."))
                  .build());
}
