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

package org.apache.iceberg.spark.extensions;

import java.util.Map;
import java.util.function.Supplier;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.procedures.ProcedureBuilder;
import org.apache.iceberg.spark.procedures.ProcedureProvider;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.iceberg.catalog.Procedure;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.unsafe.types.UTF8String;

public class SparkProceduresForTest implements ProcedureProvider {

  public static final String FOR_TEST = "for_test";
  public static final String NOOP_PROCEDURE = "noop_procedure";

  private static final Namespace TEST_NAMESPACE = Namespace.of(FOR_TEST);

  @Override
  public String getName() {
    return FOR_TEST;
  }

  @Override
  public String getDescription() {
    return "Some Iceberg procedure for tests";
  }

  @Override
  public Namespace getNamespace() {
    return TEST_NAMESPACE;
  }

  @Override
  public Map<String, Supplier<ProcedureBuilder>> getProcedureBuilders(String catalogName,
      CaseInsensitiveStringMap options, boolean forSessionCatalog) {
    return BUILDERS;
  }

  private static final Map<String, Supplier<ProcedureBuilder>> BUILDERS = initProcedureBuilders();

  private static Map<String, Supplier<ProcedureBuilder>> initProcedureBuilders() {
    ImmutableMap.Builder<String, Supplier<ProcedureBuilder>> mapBuilder = ImmutableMap.builder();
    mapBuilder.put(NOOP_PROCEDURE, NoopProcedure::builder);
    return mapBuilder.build();
  }

  static class NoopProcedure implements Procedure {

    @Override
    public ProcedureParameter[] parameters() {
      return new ProcedureParameter[]{
          ProcedureParameter.required("parameter", DataTypes.StringType)
      };
    }

    @Override
    public StructType outputType() {
      return new StructType(new StructField[]{
          new StructField("input_parameter", DataTypes.StringType, false, Metadata.empty())
      });
    }

    @Override
    public InternalRow[] call(InternalRow args) {
      return new InternalRow[] {
          new GenericInternalRow(new Object[]{UTF8String.fromString(args.getString(0))})
      };
    }

    public static ProcedureBuilder builder() {
      return new ProcedureBuilder.Builder<NoopProcedure>() {
        @Override
        protected NoopProcedure doBuild() {
          return new NoopProcedure();
        }
      };
    }
  }
}
