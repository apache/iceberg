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

package org.apache.iceberg.io;

import java.util.Map;
import java.util.function.Supplier;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Supplier interface to allow for customizing table locations based on
 * contextual information from the table.
 */
public interface TableLocationSupplier extends Supplier<String> {
  default TableLocationSupplier uuid(String uuid) { return this; }
  default TableLocationSupplier identifier(TableIdentifier identifier) { return this; }
  default TableLocationSupplier schema(Schema schema) { return this; }
  default TableLocationSupplier partitionSpec(PartitionSpec partitionSpec) { return this; }
  default TableLocationSupplier location(String currentLocation) { return this; }
  default TableLocationSupplier sortOrder(SortOrder sortOrder) { return this; }
  default TableLocationSupplier properties(Map<String, String> properties) { return this; }
}
