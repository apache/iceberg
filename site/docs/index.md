---
title: "Intro to Apache Iceberg"
---
<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->
<link href="https://cdn.jsdelivr.net/gh/apache/iceberg-docs@main/iceberg-theme/static/css/termynal.css" rel="stylesheet">
<style>
section#intro {
    display: block;
    max-width: 100%;
    margin: 0;
    padding: 0;
}

.intro-header {
    background: url(assets/images/intro-bg.webp) no-repeat center center;
    background-size: cover;
}
</style>


<section id="intro">
   <div class="intro-header">
   </div>
</section>

## Apache Iceberg

### The open table format for analytic datasets.

* [Community](community.md)
* [GitHub](https://github.com/apache/iceberg)
* [Slack](https://github.com/apache/iceberg)

## What is Iceberg?

Iceberg is a high-performance format for huge analytic tables. Iceberg brings the reliability and simplicity of SQL tables to big data, while making it possible for engines like Spark, Trino, Flink, Presto, Hive and Impala to safely work with the same tables, at the same time.

[Learn More]('https://iceberg.apache.org/getting-started')

## Expressive SQL

Iceberg supports flexible SQL commands to merge new data, update existing rows, and perform targeted deletes. Iceberg can eagerly rewrite data files for read performance, or it can use delete deltas for faster updates.
[Learn More]('https://iceberg.apache.org/docs/latest/spark-writes/')

<div class="termynal-container">
<div id="termynal-expressive-sql" data-termynal="" data-ty-startdelay="2000" data-ty-typedelay="20" data-ty-linedelay="500" style="width: 445px; min-height: 333.344px;"><span data-ty="input" data-ty-prompt="sql&gt;">MERGE INTO prod.nyc.taxis pt</span><span data-ty="input" data-ty-prompt="">USING (SELECT * FROM staging.nyc.taxis) st</span><span data-ty="input" data-ty-prompt="">ON pt.id = st.id</span><span data-ty="input" data-ty-prompt="">WHEN NOT MATCHED THEN INSERT *;</span><span data-ty="">Done!</span><span data-ty="input" data-ty-prompt="sql&gt;"></span>
</div>
</div>

## Full Schema Evolution

Schema evolution just works. Adding a column won't bring back "zombie"🧟 data. Columns can be renamed and reordered. Best of all, schema changes never require rewriting your table.

[Learn More]('https://iceberg.apache.org/docs/latest/evolution/')

<div class="termynal-container">
<div id="termynal" data-termynal="" data-ty-startdelay="4000" data-ty-typedelay="20" data-ty-linedelay="500" style="width: 445px; min-height: 474.516px;"><span data-ty="input" data-ty-prompt="sql&gt;">ALTER TABLE taxis</span><span data-ty="input" data-ty-prompt="">ALTER COLUMN trip_distance</span><span data-ty="input" data-ty-prompt="" data-ty-delay="2500">TYPE double;</span><span data-ty="">Done!</span><span data-ty="input" data-ty-prompt="sql&gt;">ALTER TABLE taxis</span><span data-ty="input" data-ty-prompt="">ALTER COLUMN trip_distance</span><span data-ty="input" data-ty-prompt="">AFTER fare;</span><span data-ty="">Done!</span><span data-ty="input" data-ty-prompt="sql&gt;">ALTER TABLE taxis</span><span data-ty="input" data-ty-prompt="">RENAME COLUMN trip_distance</span><span data-ty="input" data-ty-prompt="">TO distance;</span><span data-ty="">Done!</span>
</div>
</div>

## Hidden Partitioning

Iceberg handles the tedious and error-prone task of producing partition values for rows in a table and skips unnecessary partitions and files automatically. No extra filters are needed for fast queries, and table layout can be updated as data or queries change.

[Learn More]('https://iceberg.apache.org/docs/latest/partitioning/#icebergs-hidden-partitioning')

<div class="termynal-container">
<div>
  <script src="https://unpkg.com/@lottiefiles/lottie-player@latest/dist/lottie-player.js"></script>
  <lottie-player src="https://iceberg.apache.org/lottie/hidden-partitioning-animation.json" background="transparent" speed="0.5" style="width: 600px; height: 400px;" loop="" autoplay="">
</div>
</div>

## Time Travel and Rollback

Time-travel enables reproducible queries that use exactly the same table snapshot, or lets users easily examine changes. Version rollback allows users to quickly correct problems by resetting tables to a good state.
[Learn More]('https://iceberg.apache.org/docs/latest/spark-queries/#time-travel')

<div class="termynal-container">
<div id="termynal-time-travel" data-termynal="" data-ty-startdelay="6000" data-ty-typedelay="20" data-ty-linedelay="500" style="width: 445px; min-height: 390.516px;"><span data-ty="input" data-ty-prompt="sql&gt;">SELECT count(*) FROM nyc.taxis</span><span data-ty="">2,853,020</span><span data-ty="input" data-ty-prompt="sql&gt;">SELECT count(*) FROM nyc.taxis FOR VERSION AS OF 2188465307835585443</span><span data-ty="">2,798,371</span><span data-ty="input" data-ty-prompt="sql&gt;">SELECT count(*) FROM nyc.taxis FOR TIMESTAMP AS OF TIMESTAMP '2022-01-01 00:00:00.000000 Z'</span><span data-ty="">2,798,371</span>
</div>
</div>

## Data Compaction

Data compaction is supported out-of-the-box and you can choose from different rewrite strategies such as bin-packing or sorting to optimize file layout and size.

<div class="termynal-container">
<div id="termynal-data-compaction" data-termynal="" data-ty-startdelay="8000" data-ty-typedelay="20" data-ty-linedelay="500" style="width: 445px; min-height: 192.172px;"><span data-ty="input" data-ty-prompt="sql&gt;">CALL system.rewrite_data_files("nyc.taxis");</span>
</div>
</div>

  <script src="https://cdn.jsdelivr.net/gh/apache/iceberg-docs@main/iceberg-theme/static/js/termynal.js" data-termynal-container="#termynal|#termynal-data-compaction|#termynal-expressive-sql|#termynal-time-travel"></script>
