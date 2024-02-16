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
package org.apache.iceberg.spark.source;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.spark.sql.catalyst.bcvar.ArrayWrapper;
import org.apache.spark.sql.catalyst.bcvar.BroadcastedJoinKeysWrapper;
import org.apache.spark.sql.types.DataType;

public class DummyBroadcastedJoinKeysWrapper implements BroadcastedJoinKeysWrapper {

  private int totalJoinKeys;
  private int index;
  private DataType keyDataType;
  private ArrayWrapper wrapper;

  private long id;

  public DummyBroadcastedJoinKeysWrapper(
      DataType keyDataType,
      Object array,
      long id,
      int index,
      int totalJoinKeys,
      boolean is1D) {
    this.keyDataType = keyDataType;
    this.wrapper = ArrayWrapper.wrapArray(array, is1D, index);
    this.id = id;
    this.index = index;
    this.totalJoinKeys = totalJoinKeys;
  }

  public DummyBroadcastedJoinKeysWrapper() {}

  public DummyBroadcastedJoinKeysWrapper(DataType keyDataType, Object array, long id) {
    this(keyDataType, array, id, 0, 1, true);
  }

  @Override
  public DataType getSingleKeyDataType() {
    return this.keyDataType;
  }

  @Override
  public ArrayWrapper getKeysArray() {
    return this.wrapper;
  }

  @Override
  public long getBroadcastVarId() {
    return this.id;
  }

  @Override
  public int getKeyIndex() {
    return this.index;
  }


  @Override
  public int getTotalJoinKeys() {
    return this.totalJoinKeys;
  }

  @Override
  public void invalidateSelf() {}

  @Override
  public Set<Object> getKeysAsSet() {
    ArrayWrapper wrapper = getKeysArray();
    Set<Object> data = new HashSet<>();
    int len = wrapper.getLength();
    for (int i = 0; i < len; ++i) {
      data.add(wrapper.get(i));
    }
    return data;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeInt(this.totalJoinKeys);
    out.writeInt(this.index);
    out.writeObject(this.keyDataType);
    out.writeBoolean(this.wrapper.isOneDimensional());
    out.writeObject(this.wrapper.getBaseArray());
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    this.totalJoinKeys = in.readInt();
    this.index = in.readInt();
    this.keyDataType = (DataType) in.readObject();
    boolean isOneD = in.readBoolean();
    this.wrapper = ArrayWrapper.wrapArray(in.readObject(), isOneD, this.index);
  }
}
