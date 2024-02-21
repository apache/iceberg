/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.flink.sink;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkWriteConf;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iceberg.TableProperties.AVRO_COMPRESSION;
import static org.apache.iceberg.TableProperties.AVRO_COMPRESSION_LEVEL;
import static org.apache.iceberg.TableProperties.ORC_COMPRESSION;
import static org.apache.iceberg.TableProperties.ORC_COMPRESSION_STRATEGY;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION_LEVEL;

public class TablePropertyUtil {

    private static final Logger LOG = LoggerFactory.getLogger(TablePropertyUtil.class);

    /**
     * Based on the {@link FileFormat} overwrites the table level compression properties for the table
     * write.
     *
     * @param table The table to get the table level settings
     * @param format The FileFormat to use
     * @param conf The write configuration
     * @return The properties to use for writing
     */
    public static Map<String, String> writeProperties(
            Table table, FileFormat format, FlinkWriteConf conf) {
        Map<String, String> writeProperties = Maps.newHashMap(table.properties());

        switch (format) {
            case PARQUET:
                writeProperties.put(PARQUET_COMPRESSION, conf.parquetCompressionCodec());
                String parquetCompressionLevel = conf.parquetCompressionLevel();
                if (parquetCompressionLevel != null) {
                    writeProperties.put(PARQUET_COMPRESSION_LEVEL, parquetCompressionLevel);
                }

                break;
            case AVRO:
                writeProperties.put(AVRO_COMPRESSION, conf.avroCompressionCodec());
                String avroCompressionLevel = conf.avroCompressionLevel();
                if (avroCompressionLevel != null) {
                    writeProperties.put(AVRO_COMPRESSION_LEVEL, conf.avroCompressionLevel());
                }

                break;
            case ORC:
                writeProperties.put(ORC_COMPRESSION, conf.orcCompressionCodec());
                writeProperties.put(ORC_COMPRESSION_STRATEGY, conf.orcCompressionStrategy());
                break;
            default:
                throw new IllegalArgumentException(String.format("Unknown file format %s", format));
        }

        return writeProperties;
    }

    public static List<Integer> checkAndGetEqualityFieldIds(Table table, List<String> equalityFieldColumns) {
        List<Integer> equalityFieldIds = Lists.newArrayList(table.schema().identifierFieldIds());
        if (equalityFieldColumns != null && !equalityFieldColumns.isEmpty()) {
            Set<Integer> equalityFieldSet = Sets.newHashSetWithExpectedSize(equalityFieldColumns.size());
            for (String column : equalityFieldColumns) {
                org.apache.iceberg.types.Types.NestedField field = table.schema().findField(column);
                Preconditions.checkNotNull(
                        field,
                        "Missing required equality field column '%s' in table schema %s",
                        column,
                        table.schema());
                equalityFieldSet.add(field.fieldId());
            }

            if (!equalityFieldSet.equals(table.schema().identifierFieldIds())) {
                LOG.warn(
                        "The configured equality field column IDs {} are not matched with the schema identifier field IDs"
                                + " {}, use job specified equality field columns as the equality fields by default.",
                        equalityFieldSet,
                        table.schema().identifierFieldIds());
            }
            equalityFieldIds = Lists.newArrayList(equalityFieldSet);
        }
        return equalityFieldIds;
    }
}
