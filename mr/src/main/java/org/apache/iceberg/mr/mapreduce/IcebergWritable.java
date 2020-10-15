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

package org.apache.iceberg.mr.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.Writable;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class IcebergWritable implements Writable {
  private Record record;

  public IcebergWritable(Record record) {
    this.record = record;
  }

  public Record record() {
    Preconditions.checkNotNull(record, "Should not return null record");
    return record;
  }

  public Object getValueObject(int colIndex) {
    if (record != null) {
      return record.get(colIndex);
    } else {
      return null;
    }
  }

  public Object getValueObject(String colName) {
    if (record != null) {
      return record.getField(colName);
    } else {
      return null;
    }
  }

  public boolean isSet(int colIndex) {
    if (record != null) {
      return record.get(colIndex) != null;
    } else {
      return false;
    }
  }

  public boolean isSet(String colName) {
    if (record != null) {
      return record.getField(colName) != null;
    } else {
      return false;
    }
  }

  @Override
  public void readFields(DataInput in) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(DataOutput out) {
    throw new UnsupportedOperationException();
  }
}
