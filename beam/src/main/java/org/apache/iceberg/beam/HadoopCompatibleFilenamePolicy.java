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

package org.apache.iceberg.beam;

import java.io.Serializable;
import java.text.DecimalFormat;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Defines a custom {@link FileIO.Write.FileNaming} which will use the prefix and suffix supplied to create a name based
 * on the window, pane, number of shards, shard index, and compression. Removes window when in the {@link GlobalWindow}
 * and pane info when it is the only firing of the pane.
 *
 * Currently this is needed if you want to read the files using a Hadoop File System based API:
 * https://github.com/apache/iceberg/pull/1972#issuecomment-753598331
 * This is because semicolons aren't supported in the filename.
 */
public class HadoopCompatibleFilenamePolicy implements FileIO.Write.FileNaming, Serializable {
    private final String suffix;

    public HadoopCompatibleFilenamePolicy(String suffix) {
        this.suffix = suffix;
    }

    @Override
    public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex, Compression compression) {
        // Replace the HH:mm:ss with HH-mm-ss, so we don't use any semicolon's
        final String format = "yyyy-MM-dd'T'HH-mm-ss";
        final DateTimeFormatter formatter = DateTimeFormat.forPattern(format);

        final StringBuilder res = new StringBuilder();
        if (window instanceof IntervalWindow) {
            if (res.length() > 0) {
                res.append("-");
            }
            final IntervalWindow iw = (IntervalWindow) window;
            res.append(iw.start().toString(formatter)).append("-").append(iw.end().toString(formatter));
        }
        boolean isOnlyFiring = pane.isFirst() && pane.isLast();
        if (!isOnlyFiring) {
            if (res.length() > 0) {
                res.append("-");
            }
            res.append(pane.getIndex());
        }
        if (res.length() > 0) {
            res.append("-");
        }
        final String numShardsStr = String.valueOf(numShards);
        // A trillion shards per window per pane ought to be enough for everybody.
        final DecimalFormat df =
                new DecimalFormat("000000000000".substring(0, Math.max(5, numShardsStr.length())));
        res.append(df.format(shardIndex)).append("-of-").append(df.format(numShards));
        res.append(this.suffix);
        res.append(compression.getSuggestedSuffix());
        return res.toString();
    }
}
