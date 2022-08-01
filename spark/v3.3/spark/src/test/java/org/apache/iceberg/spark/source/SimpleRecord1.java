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

import org.apache.iceberg.relocated.com.google.common.base.Objects;

// CHECKSTYLE:OFF
public class SimpleRecord1 {
  private Integer ID;
  private String DaTa;

  public SimpleRecord1() {}

  public SimpleRecord1(Integer id, String data) {
    this.ID = id;
    this.DaTa = data;
  }

  public Integer getID() {
    return ID;
  }

  public void setID(Integer ID) {
    this.ID = ID;
  }

  public String getDaTa() {
    return DaTa;
  }

  public void setDaTa(String daTa) {
    this.DaTa = daTa;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SimpleRecord1 record = (SimpleRecord1) o;
    return Objects.equal(ID, record.ID) && Objects.equal(DaTa, record.DaTa);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(ID, DaTa);
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("{\"id\"=");
    buffer.append(ID);
    buffer.append(",\"data\"=\"");
    buffer.append(DaTa);
    buffer.append("\"}");
    return buffer.toString();
  }
}
