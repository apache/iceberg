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

package org.apache.iceberg.orc;

import java.util.List;
import java.util.Optional;
import org.apache.orc.TypeDescription;

public class EstimateOrcAveWidthVisitor extends OrcSchemaVisitor<Integer> {

  @Override
  public Integer record(TypeDescription record, List<String> names, List<Integer> fields) {
    return fields.stream().reduce(Integer::sum).orElse(0);
  }

  @Override
  public Integer list(TypeDescription array, Integer elementResult) {
    return elementResult;
  }

  @Override
  public Integer map(TypeDescription map, Integer keyWidth, Integer valueWidth) {
    return keyWidth + valueWidth;
  }

  @Override
  public Integer primitive(TypeDescription primitive) {
    Optional<Integer> icebergIdOpt = ORCSchemaUtil.icebergID(primitive);

    if (!icebergIdOpt.isPresent()) {
      return 0;
    }

    switch (primitive.getCategory()) {
      case BOOLEAN:
      case BYTE:
      case CHAR:
        return 1;
      case SHORT:
        return 2;
      case INT:
      case FLOAT:
        return 4;
      case LONG:
      case DOUBLE:
        return 8;
      case DATE:
      case TIMESTAMP:
      case TIMESTAMP_INSTANT:
        return 12;
      case STRING:
      case VARCHAR:
      case BINARY:
        return 128;
      case DECIMAL:
        return primitive.getPrecision() * 4 + 1;
      default:
        throw new IllegalArgumentException("Can't handle " + primitive);
    }
  }
}
