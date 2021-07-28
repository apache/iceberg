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

package org.apache.iceberg.flink.actions;

import com.twitter.chill.java.ClosureSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.ActionsProvider;
import org.apache.iceberg.actions.ExpireSnapshots;

public class FlinkActions implements ActionsProvider {

  private static final Configuration CONFIG = new Configuration()
      // disable classloader check as Avro may cache class/object in the serializers.
      .set(CoreOptions.CHECK_LEAKED_CLASSLOADER, false);

  private final StreamExecutionEnvironment env;

  private FlinkActions(StreamExecutionEnvironment env) {
    // register ClosureSerializer to prevent NPE when Kryo serialize lambda expr(e.g. HadoopFileIO.hadoopConf)
    env.getConfig().registerTypeWithKryoSerializer(ClosureSerializer.Closure.class, ClosureSerializer.class);
    this.env = env;
  }

  public static FlinkActions get(StreamExecutionEnvironment env) {
    return new FlinkActions(env);
  }

  public static FlinkActions get() {
    return get(StreamExecutionEnvironment.getExecutionEnvironment(CONFIG));
  }

  @Override
  public ExpireSnapshots expireSnapshots(Table table) {
    return new ExpireSnapshotsAction(env, table);
  }
}
