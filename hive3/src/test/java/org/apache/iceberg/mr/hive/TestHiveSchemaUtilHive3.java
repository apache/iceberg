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

package org.apache.iceberg.mr.hive;

import java.util.List;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.iceberg.Schema;
import org.apache.iceberg.hive.TestHiveSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;

import org.apache.crunch.types.orc.OrcUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.ql.io.orc.Writer;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestHiveSchemaUtilHive3 extends TestHiveSchemaUtil {

  @Override
  protected List<FieldSchema> getSupportedFieldSchemas() {
    List<FieldSchema> fields = Lists.newArrayList(super.getSupportedFieldSchemas());
    // timestamp local tz only present in Hive3
    fields.add(new FieldSchema("c_timestamptz", serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME, null));
    return fields;
  }

  @Override
  protected Schema getSchemaWithSupportedTypes() {
    Schema schema = super.getSchemaWithSupportedTypes();
    List<Types.NestedField> columns = Lists.newArrayList(schema.columns());
    // timestamp local tz only present in Hive3
    columns.add(optional(columns.size(), "c_timestamptz", Types.TimestampType.withZone()));
    return new Schema(columns);
  }

  public void createOrcFile(String input) throws IOException {
    String typeStr = "struct";
    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeStr);
    ObjectInspector inspector = OrcStruct.createObjectInspector(typeInfo);

    String[] inputTokens = input.split("\\t");

    OrcStruct orcLine = OrcUtils.createOrcStruct(
        typeInfo,
        new Text(inputTokens[0]),
        new ShortWritable(Short.valueOf(inputTokens[1])),
        new IntWritable(Integer.valueOf(inputTokens[2])),
        new LongWritable(Long.valueOf(inputTokens[3])),
        new DoubleWritable(Double.valueOf(inputTokens[4])),
        new FloatWritable(Float.valueOf(inputTokens[5])));
    Configuration conf = new Configuration();
    Path tempPath = new Path("/tmp/test.orc");

    Writer writer = OrcFile.createWriter(tempPath, OrcFile.writerOptions(conf).inspector(inspector).stripeSize(100000).bufferSize(10000));
    writer.addRow(orcLine);
    writer.close();
  }
}
