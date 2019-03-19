/*
 * Copyright 2018 Hortonworks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.spark.source;

import com.google.common.base.Objects;

public class SimpleRecord {
  private Integer id;
  private String data;

  public SimpleRecord() {
  }

  SimpleRecord(Integer id, String data) {
    this.id = id;
    this.data = data;
  }

  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public String getData() {
    return data;
  }

  public void setData(String data) {
    this.data = data;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o){
      return true;
    }
    if (o == null || getClass() != o.getClass()){
      return false;
    }

    SimpleRecord record = (SimpleRecord) o;
    return Objects.equal(id, record.id) && Objects.equal(data, record.data);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id, data);
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("{\"id\"=");
    buffer.append(id);
    buffer.append(",\"data\"=\"");
    buffer.append(data);
    buffer.append("\"}");
    return buffer.toString();
  }
}
