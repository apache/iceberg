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
package org.apache.iceberg.spark.vendor;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsPreSigning;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.responses.RemoteSignResponse;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SignFiles implements FileBatchProcessor {

  public static final String NAME = "sign_files";

  public static final StructType SCHEMA =
      new StructType()
          .add("uri", DataTypes.StringType)
          .add("signed_uri", DataTypes.StringType)
          .add(
              "headers",
              DataTypes.createMapType(
                  DataTypes.StringType, DataTypes.createArrayType(DataTypes.StringType)));

  private FileIO io;

  @Override
  public void initialize(FileIO fileIO) {
    Preconditions.checkArgument(
        fileIO instanceof SupportsPreSigning, "%s cannot pre-sign", fileIO.getClass().getName());
    this.io = fileIO;
  }

  @Override
  public List<Object[]> apply(List<Object[]> batch) {
    List<String> uris =
        batch.stream()
            .map(values -> uri((Row) values[0]))
            .filter(uri -> uri != null)
            .distinct()
            .collect(Collectors.toList());
    Map<String, RemoteSignResponse> signed =
        uris.isEmpty() ? ImmutableMap.of() : ((SupportsPreSigning) io).preSign(uris);

    List<Object[]> results = Lists.newArrayListWithExpectedSize(batch.size());
    for (Object[] values : batch) {
      String uri = uri((Row) values[0]);
      if (uri == null) {
        results.add(new Object[] {null, null, null});
      } else {
        RemoteSignResponse response = signed.get(uri);
        results.add(new Object[] {uri, response.uri().toString(), response.headers()});
      }
    }

    return results;
  }

  @Override
  public void close() {
    io.close();
  }

  private static String uri(Row file) {
    return file == null ? null : file.<String>getAs("uri");
  }
}
