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

package org.apache.iceberg;

import com.google.common.collect.Maps;
import java.io.IOException;
import org.apache.iceberg.hadoop.HadoopMetricsContext;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.metrics.MetricsContext;
import org.junit.Test;

public class TestHadoopMetricsContextSerialization {

  @Test(expected = Test.None.class)
  public void testHadoopMetricsContextKryoSerialization() throws IOException {
    MetricsContext metricsContext = new HadoopMetricsContext("s3");

    metricsContext.initialize(Maps.newHashMap());

    MetricsContext deserializedMetricContext = KryoHelpers.roundTripSerialize(metricsContext);
    // statistics are properly re-initialized post de-serialization
    deserializedMetricContext
        .counter(FileIOMetricsContext.WRITE_BYTES, Long.class, MetricsContext.Unit.BYTES)
        .increment();
  }

  @Test(expected = Test.None.class)
  public void testHadoopMetricsContextJavaSerialization() throws IOException, ClassNotFoundException {
    MetricsContext metricsContext = new HadoopMetricsContext("s3");

    metricsContext.initialize(Maps.newHashMap());

    MetricsContext deserializedMetricContext = TestHelpers.roundTripSerialize(metricsContext);
    // statistics are properly re-initialized post de-serialization
    deserializedMetricContext
        .counter(FileIOMetricsContext.WRITE_BYTES, Long.class, MetricsContext.Unit.BYTES)
        .increment();
  }
}
