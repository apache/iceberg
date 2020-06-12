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

package org.apache.iceberg.mr.mapred;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotsTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class IcebergSerDe extends AbstractSerDe {

  private Schema schema;
  private ObjectInspector inspector;
  private List<Object> row;

  @Override
  public void initialize(@Nullable Configuration configuration, Properties serDeProperties) throws SerDeException {
    Table table = null;
    try {
      table = TableResolver.resolveTableFromConfiguration(configuration, serDeProperties);
    } catch (IOException e) {
      throw new UncheckedIOException("Unable to resolve table from configuration: ", e);
    }
    this.schema = table.schema();
    if (table instanceof SnapshotsTable) {
      try {
        this.inspector = new IcebergObjectInspectorGenerator().createObjectInspector(schema);
      } catch (Exception e) {
        throw new SerDeException(e);
      }
    } else {
      List<Types.NestedField> columns = new ArrayList<>(schema.columns());
      columns.add(Types.NestedField.optional(Integer.MAX_VALUE,
          SystemTableUtil.snapshotIdVirtualColumnName(serDeProperties), Types.LongType.get()));
      Schema withVirtualColumn = new Schema(columns);
      try {
        this.inspector = new IcebergObjectInspectorGenerator().createObjectInspector(withVirtualColumn);
      } catch (Exception e) {
        throw new SerDeException(e);
      }
    }
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return null;
  }

  @Override
  public Writable serialize(Object o, ObjectInspector objectInspector) {
    throw new UnsupportedOperationException("Serialization is not supported.");
  }

  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }

  @Override
  public Object deserialize(Writable writable) {
    IcebergWritable icebergWritable = (IcebergWritable) writable;
    List<Types.NestedField> fields = icebergWritable.schema().columns();

    if (row == null || row.size() != fields.size()) {
      row = new ArrayList<Object>(fields.size());
    } else {
      row.clear();
    }
    for (int i = 0; i < fields.size(); i++) {
      Object obj = ((IcebergWritable) writable).record().get(i);
      Type fieldType = fields.get(i).type();
      if (fieldType.equals(Types.DateType.get())) {
        row.add(Date.valueOf((LocalDate) obj));
      } else if (fieldType.equals(Types.TimestampType.withoutZone())) {
        row.add(Timestamp.valueOf((LocalDateTime) obj));
      } else if (fieldType.equals(Types.TimestampType.withZone())) {
        LocalDateTime timestamp = ((OffsetDateTime) obj).toLocalDateTime();
        row.add(Timestamp.valueOf(timestamp));
      } else if (fieldType.equals(Types.TimeType.get())) {
        row.add(((LocalTime) obj).toString());
      } else {
        row.add(obj);
      }
    }
    return Collections.unmodifiableList(row);
  }

  @Override
  public ObjectInspector getObjectInspector() {
    return inspector;
  }
}
