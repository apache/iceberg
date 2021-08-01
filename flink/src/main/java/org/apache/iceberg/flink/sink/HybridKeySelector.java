/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.flink.sink;

import java.util.List;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;

public class HybridKeySelector implements KeySelector<RowData, String> {

  private final PartitionKeySelector partitionKeySelector;
  private final EqualityFieldKeySelector equalityFieldKeySelector;

  HybridKeySelector(PartitionSpec spec, List<Integer> equalityFieldIds,  Schema schema, RowType flinkSchema) {
    partitionKeySelector = new PartitionKeySelector(spec, schema, flinkSchema);
    equalityFieldKeySelector = new EqualityFieldKeySelector(equalityFieldIds, schema, flinkSchema);
  }

  @Override
  public String getKey(RowData row) throws Exception {
    String partitionKey = partitionKeySelector.getKey(row);
    String equalityFieldKey = equalityFieldKeySelector.getKey(row);
    return partitionKey + "-" + equalityFieldKey;
  }
}
