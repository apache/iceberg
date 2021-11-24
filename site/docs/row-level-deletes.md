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

# Row-Level Deletes

Iceberg supports metadata-based deletion through the `DeleteFiles` interface.
It allows you to quickly delete a specific file or any file that might match a given expression without the need to read or write any data in the table.

Row-level deletes target more complicated use cases such as general data protection regulation (GDPR).
Copy-on-write and merge-on-read are two different approaches to handle row-level delete operations. Here are their definitions in Iceberg:

- **copy-on-write**: a delete directly rewrites all the affected data files.
- **merge-on-read**: delete information is encoded in the form of _delete files_. The table reader can apply all delete information at read time.

Overall, copy-on-write is more efficient in reading data, whereas merge-on-read is more efficient in writing deletes, but requires more maintenance and tuning to be performant in reading data with deletes.
Users can choose to use **both** copy-on-write and merge-on-read for the same Iceberg table based on different situations. 
For example, a time-partitioned table can have newer partitions maintained with the merge-on-read approach through a streaming pipeline,
and older partitions maintained with the copy-on-write approach to apply less frequent GDPR deletes from batch ETL jobs.

There are use cases that could only be supported by one approach such as change data capture (CDC).
There are also limitations for different compute engines that lead them to prefer one approach over another.
Please check out the documentation of the specific compute engines to see the details of their capabilities related to row-level deletes.
This article will focus on explaining Iceberg's core design of copy-on-write and merge-on-read.

!!!Note
    Update is modeled as a delete with an insert within the same transaction in Iceberg, so this article only explains delete-related concepts. 

## Copy-on-write

In the copy-on-write approach, given a user's delete requirement, the write process would search for all the affected data files and perform a rewrite operation.

For example, consider an unpartitioned table with schema `(id bigint, category string, data string)` that has the following files:

```
file A: (1, 'c1', 'data1'), (2, 'c1', 'data2')
file B: (3, 'c2', 'data1'), (4, 'c2', 'data2')
file C: (5, 'c3', 'data3'), (6, 'c3', 'data2')
```

A copy-on-write deletion of `data='data1'` can rewrite files A and B into a new file D. D contains the rows that were not deleted from files A and B. The table after the deletion looks like:

```
file C: (5, 'c3', 'data3'), (6, 'c3', 'data2')
file D: (2, 'c1', 'data2'), (4, 'c2', 'data2')
```

There is no effect on read side in the copy-on-write approach.

## Merge-on-read

### Definitions

Iceberg supports 2 different types of row-level delete files: **position deletes** and **equality deletes**.
The **sequence number** concept is also needed to describe the relative age of data and delete files.
If you are unfamiliar with these concepts, please read the [row-level deletes](../spec/#row-level-deletes) and [sequence numbers](../spec/#sequence-numbers) sections in the Iceberg spec for more information before proceeding.


Also note that because row-level delete files are valid Iceberg data files, all rows in each delete file must belong to the same partition.
For a partitioned table, if a delete file belongs to `Unpartitioned` (the partition has no partition field), then the delete file is called a **global delete**. 
Otherwise, it is called a **partition delete**.

### Writing delete files

From the end user's perspective, it is very rare to directly request deletion of a specific row of a specific file. 
A delete requirement usually comes as a predicate such as `id = 5` or `date < TIMESTAMP '2000-01-01'`. 

Given a predicate, a compute engine can perform a scan to know the data files and row positions affected by the predicate and then write partition position deletes.
The scan can be a table scan, or a scan of unflushed files stored in memory, local RocksDB, etc. for use cases like streaming.
It is in theory possible to write global position deletes for partitioned tables, 
but it is always preferred to write partition position deletes because the writer already knows the exact partition to use after the scan.

When performing a scan is too expensive or time-consuming, the compute engine can use equality deletes for faster write.
It can convert an input predicate to global equality deletes, or convert it to equality predicates for each affected partition and write partition equality deletes.
However, such conversion might not always be possible without scanning data (e.g. delete all data with `price > 2.33`). In those cases, using position deletes is preferred.

For example, in a CDC pipeline, both partition position deletes and partition equality deletes are used.
Consider an unpartitioned table with schema `(id bigint, category string, data string)` that has the following files with sequence numbers:

```
seq=0 file A: (1, 'c1', 'data1'), (2, 'c1', 'data2')
seq=0 file B: (3, 'c2', 'data1'), (4, 'c2', 'data2')
```

The CDC pipeline writing to the table currently contains unflushed data `(1, 'c10', 'data10')` that will be committed as file C in the table.
For a new delete predicate `id = 1`, the writer first checks the unflushed data index in memory and performs a position delete of file C at row position 0.
It then writes an equality delete row `(1, NULL, NULL)` that is applied to all the existing data files A and B in the table.
After the next commit checkpoint, the new table contains the following files:

```
seq=0 file A: (1, 'c1', 'data1'), (2, 'c1', 'data2')
seq=0 file B: (3, 'c2', 'data1'), (4, 'c2', 'data2')
seq=1 file C: (1, 'c10', 'data10')
seq=1 position delete D: ('C', 0)
seq=1 equality delete E: (1, NULL, NULL)
```

### Reading data with delete files

During Iceberg's scan file planning phase, a delete file index is constructed to filter the delete files associated with each data file using the following rules:

1. equality delete files are applied to data files of strictly lower sequence numbers
2. position delete files are applied to data files of equal or lower sequence numbers
3. further pruning is performed by comparing the partition and column statistics information of each pair of delete and data file. Therefore, for a partitioned table, partition deletes are always preferred to global deletes.

In the CDC example in the last section, position delete D is applied to file C, and equality delete E is applied to file A.

After the planning phase, each data file to read is associated with a set of delete files to merge with.
In general, position deletes are easier to merge, because they are already sorted by file path and row position when writing.
Applying position deletes to a data file can be viewed as merging two sorted lists, which can be done efficiently.
In contrast, applying equality deletes to a data files requires loading all rows to memory and checking every row in a data file against every equality predicate.

### Committing delete files

Iceberg uses the `RowDelta` interface for users to commit new delete files to a table.
The interface offers multiple validations that users can perform to ensure snapshot or serializable isolation.
The example below shows how delete file commits might conflict with other commits.
Consider the following case, where a `rewrite` and a `delete` start at the same time, and they run against the same snapshot of sequence number `100`:

```text
snapshot: data.1 data.2 data.3 delete.3_1
              \     /       \     /
rewrite:       data.4        data.5

delete:   data.1 data.2 data.3 delete.3_1    
                               delete.3_2
```

We have the following 4 possible situations:

1. If `delete` commits after `rewrite`:
    1. If `delete.3_2` is a position delete, `delete` has to be retried from beginning because the old file is removed.
    2. If `delete.3_2` is an equality delete, `delete` can succeed after retry because `delete.3_2` will gain a higher sequence number `102` and be applied to `data.5` with sequence number `101`.
2. If `rewrite` commits after `delete`:
    1. If `delete.3_2` is a position delete, `rewrite` has to be retried from beginning because it removes `data.3` which undeletes `delete.3_2`.
    2. If `delete.3_2` is an equality delete, `rewrite` can commit `data.5` with sequence number `100`, so `delete.3_2` with sequence number `101` can still be applied to `data.5`.

So overall equality deletes are less likely to cause commit conflicts comparing to position deletes.
The commit strategy described by **2-2** above is also used by Iceberg compaction to minimize commit conflict.  

## Compaction

### Data file rewrite

Iceberg compaction is mainly done through the `RewriteDataFiles` action.
The action reads data and rewrites it back using a `RewriteStrategy`.
Users can choose different rewrite strategies such as bin-packing, sorting or z-ordering to rewrite files into optimized layout and file size.

Users can tune the config `delete-file-threshold` to determine how many delete files a data file can have before it needs to be rewritten.
If the delete files exceeds the threshold, the data file is included for compaction even if the file is already optimized before.

Files compacted are committed using the sequence number of the snapshot when the compaction starts to minimize commit conflicts, as we discussed in the [committing delete files](#committing-delete-files) section.
More details of this action could be found in each engine that implements this action, including [Spark](../spark-procedures/#rewrite_data_files) and [Flink](../flink/#rewrite-files-action).

### Delete file removal

The data files produced by `RewiteDataFiles` always contain the latest data, but delete files merged in the process are not removed because they might be referenced by some other files.
During the commit phase, if iceberg finds out there is no data file associated with a delete file, the delete file is marked as `DELETED` in the snapshot.
The delete file is not physically removed because it might still be referenced by a historical snapshot.
During the [snapshot expiration process](../maintenance/#expire-snapshots), delete files that are not referenced by any snapshot are physically removed.

### Additional compaction actions

Currently `RewriteDataFiles` is able to solve most of the compaction use cases.
However, Iceberg can offer some additional actions to further optimize an Iceberg table with delete files.
These actions are not implemented yet, but would in theory provide additional flexibility for users when managing an Iceberg table.

1. **Rewrite position deletes**: this action optimizes the positions delete file layout through algorithms like bin-packing and sorting.
   This is similar to how data files are optimized. It can reduce the IO cost of opening too many small position delete files.
2. **Convert equality to position deletes**: equality deletes has the potential of causing out-of-memory error and should be removed as soon as possible.
   However, frequently rewriting data files to merge equality deletes might be costly.
   This action converts equality deletes to position deletes to improve read performance while keeping the computation cost low.
3. **Expire delete files**: information of delete files is stored in delete manifests. 
   During the Iceberg scan planning phase, all delete manifests are first read to formulate the delete file index. 
   Because `RewriteDataFiles` does not physically remove delete files, the delete file indexing phase would take long if there are too many delete manifests accumulated over time.
   Although the snapshot expiration process would ultimately remove those unused delete files,
   having an action to remove delete files more eagerly is helpful in reducing Iceberg scan planning time and minimizing storage cost.
