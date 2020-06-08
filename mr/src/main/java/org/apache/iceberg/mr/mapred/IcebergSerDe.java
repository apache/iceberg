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
import org.apache.iceberg.types.Types;

import static org.apache.iceberg.mr.mapred.SystemTableUtil.getVirtualColumnName;
import static org.apache.iceberg.mr.mapred.TableResolverUtil.resolveTableFromConfiguration;

public class IcebergSerDe extends AbstractSerDe {

  private Schema schema;
  private ObjectInspector inspector;

  @Override
  public void initialize(@Nullable Configuration configuration, Properties serDeProperties) throws SerDeException {
    Table table = null;
    try {
      table = resolveTableFromConfiguration(configuration, serDeProperties);
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
      columns.add(Types.NestedField.optional(Integer.MAX_VALUE, getVirtualColumnName(serDeProperties), Types.LongType.get()));
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
    return null;
  }

  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }

  @Override
  public Object deserialize(Writable writable) {
    IcebergWritable icebergWritable = (IcebergWritable) writable;
    Schema schema = icebergWritable.getSchema();
    List<Types.NestedField> fields = schema.columns();
    List<Object> row = new ArrayList<>();

    for (Types.NestedField field : fields) {
      Object obj = ((IcebergWritable) writable).getRecord().getField(field.name());
      row.add(obj);
    }
    return Collections.unmodifiableList(row);
  }

  @Override
  public ObjectInspector getObjectInspector() {
    return inspector;
  }
}
