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
# Iceberg secondary indexes

# Motivation
Well established database systems like Oracle, MsSQL, Postgres, MySql provide a possibility for the users to create indexes above existing tables. Database indexes are powerful because they significantly reduce query latency by allowing engines to locate data without scanning entire datasets. They enable efficient filtering, point lookups, and range queries, which is critical for large-scale analytics and mixed workloads. For Apache Iceberg, integrating index support would unlock similar benefits: faster query execution, reduced I/O, and shared indexes across engines. Features like inverted indexes for deletes, auxiliary versions for selective access, and vector indexes for similarity search can make Iceberg tables more suitable for real-time analytics and AI workloads, while preserving its strong governance and snapshot-based consistency model.

# Goals
* Define the generic metadata structure for indexes
* For a few specific index types
  * Define data and metadata structure for these indexes
  * Defining the maintenance processes for these indexes
  * Defining supported usage patterns for indexes

# Non-Goals
* The mentioned index types are just examples, and far from exhaustive
* The concrete implementation of the indexes could be discussed later in more details

# Definitions
## Index
A **database index** is a specialized data structure that improves the speed of data retrieval operations on a database table. The index can be computed either synchronously and committed along the DDL/DML changes or asynchronously and updated by an index maintenance process. This proposal focuses on indexes that are maintained asynchronously.
## Index type
Index data structures and algorithms are constantly evolving, so it’s important to define a flexible metadata framework that supports adding new index types as the Iceberg specification evolves. For reference, here is a [collection](https://docs.google.com/spreadsheets/d/14cBdwsOw89ivolHtAw342YNoGmb1-Kri1E80hwWymL0) of structures referred to as indexes by various engines.

An **index type** defines the algorithm and the underlying data structure that governs the behavior of the index. Together with its configuration parameters, this specification enables both query engines and users to evaluate whether employing the index will provide performance benefits for specific workloads.

An index type specifies:
* The column types it can optimize for
* The column types it can include
* The available user-defined properties for the index
* The data layout used by the index
## Index instance
An **index instance** is a concrete, parameterized realization of an index type applied to a specific table. It represents an actual index built with defined properties, configuration settings, and scoped for the designated table.

Users can create index instances by specifying:
* The source table
* The columns to optimize
* The columns to include
* The user-defined properties for the index
Users can create multiple instances of the same index type, each configured with different properties.
## Index snapshot
An **index snapshot** is a version of the index data that corresponds to a specific snapshot of the table. When table data changes, the index maintenance process periodically generates new index snapshots, ensuring that queries against the latest table snapshot can leverage the updated index.

The maintenance process may also optimize the index layout and produce additional properties for each snapshot to enhance the index’s effectiveness.

# Layout
We evaluated the pros and cons of storing index metadata in dedicated index metadata files versus embedding it in the table metadata file.

Advantages of separate index metadata files:
* Prevents the table metadata file from growing in size.
* Aligns with common engine practices, where indexes are maintained in separate structures.
* Reduces coupling between indexes and tables, allowing for greater flexibility.

Advantages of storing base index metadata in the table metadata file:
* During query planning, the engine needs quick access to:
  * Available indexes
  * Index freshness
* If this metadata is embedded in the table metadata, only the indexes actually used need to be read. Otherwise, multiple additional file reads would be required to retrieve index metadata during planning.

To stay aligned with current metadata layout practices, we chose to store index metadata in dedicated metadata files. In some cases, retrieving this metadata may be too costly. When that happens, the catalog can use caching to serve index metadata more efficiently, or we may later decide to duplicate selected portions of it into the table metadata file to speed up access for query optimizers.

# Usage
## SQL
### Listing
It should be possible to list the available indexes for a table through SQL using a metadata tables like:

```sql
SELECT * FROM persons.indexes;
```

### Manipulation
We need to provide an API to create drop and update indexes through SQL:
* We could prepare for SQL commands for creation like this:
```sql
CREATE INDEX nat_index ON persons USING BTREE([nationality], [first_name, last_name]);
```
  or in Spark
```sql
CALL create_index(
  table => "persons",
  name => "nat_index"
  type => BTREE,
  optimized-columns => ARRAY(nationality),
  index-columns => ARRAY(first_name, last_name));
```
* We could prepare for SQL commands for dropping an index like this:
```sql
DROP INDEX persons.nat_index;
```
  or in Spark
```sql
CALL drop_index(
  table => "persons",
  name => "nat_index");
```
* We could prepare for SQL commands for altering an index like this:
```sql
ALTER INDEX persons.ivf_index SET quantizer = new_quantizer;
```
  or in Spark
```sql
CALL alter_index(
  table => "persons",
  name => "nat_index",
  properties => MAP("quantizer", "new_quantizer"));
```

## Catalog
The indexes will be stored and accessible through the Catalog.

An index instance is uniquely identified by its **IndexIdentifier**, which is constructed by combining the **TableIdentifier** with the index name. This ensures that index names are scoped to their respective tables.

**Example**:

For a table *persons* in the *company* database with an index named *nationality\_index*, the resulting **IndexIdentifier** would be:

*company.persons.nationality\_index*

This format guarantees uniqueness across tables and databases.
### Java API
We need to provide a *listIndexes* functionality which enables query optimizers to discover the indexes available for a given table. The returned list must already be filtered to include only index types supported by the engine. Each returned *BaseIndex* entry must provide all information required for the optimizer to decide whether the index is applicable to a query or should be skipped.

The **BaseIndex** fields are:

| Type | Name | Requirement | Description |
| :---- | :---- | :---- | :---- |
| IndexIdentifier | id | required | The unique identifier for the index instance. |
| IndexType | type | required | The type of the index instance. |
| int\[\] | indexColumnIds | required | The columns which are stored losslessly in the table instance. |
| int\[\] | optimizedColumnIds | required | The index is optimized for retrieval based on these columns. |
| long\[\] | availableTableSnapshots | required | The index has valid snapshots corresponding to these source table snapshots. |
We also require methods to load, create, update, and delete indexes. Each of these methods must return the complete set of index metadata, encapsulated in a DetailedIndex object.

The **DetailedIndex** is an extension of the BaseIndex and the extra fields are:

| Type | Name | Requirement | Description |
| :---- | :---- | :---- | :---- |
| String | location | required | Index’s base location; used to create index file locations. |
| Long | currentVersionId | required | ID of the current version of the index (version-id). |
| List\<Version\> | versions | required | The columns which are stored losslessly in the table instance. |
| List\<VersionLog\> | versionLog | required | A list of known versions of the index, the number of versions retained is implementation-specific. current-version-id must be present in this list. |
| List\<IndexSnapshot\> | snapshots | required | During the index maintenance a new index snapshot is generated for the specific Table snapshot and it is added to the snapshots list. |
All-in-all, we need the following new Catalog methods:

| Return type | Name | Parameters | Description |
| :---- | :---- | :---- | :---- |
| List\<BaseIndex\> | listIndexes | TableIdentifier, IndexType\[\] | Returns a list of index instances for the specified table, filtered to include only those whose type matches one of the provided types. |
| DetailedIndex | createIndex | TableIdentifier, String name, String location, IndexType, int\[\] indexColumnIds, int\[\] optimizedColumnIds, Map userProperties | Creates an index instance with the given parameters. |
| DetailedIndex | loadIndex | IndexIdentifier | Loads the details of a specific index instance. |
| DetailedIndex | updateIndex | IndexIdentifier, IndexUpdateData | Updates a given index instance. The IndexUpdateData is either: VersionUpdateData, when the user updates the index properties. This contains a single userProperties Map SnapshotUpdateData, when the index maintenance process. This contains: Map snapshotProperties Long tableSnapshotId Long indexSnapshotId |
| void | dropIndex | IndexIdentifier | Drops a specific index instance |
| boolean | indexExists | IndexIdentifier | Checks if an index instance exists with the given id. |
### Rest catalog changes
### We need to provide a way to list and create, update indexes through the rest catalog as well:
```
GET /v1/{prefix}/namespaces/{namespace}/indexes
    summary: List all indexes matching a given parameters
    parameters:
        TableIdentifier: table
        Array of IndexType: index-types
    response:
        Array of BaseIndex

POST /v1/{prefix}/namespaces/{namespace}/indexes
    summary: Create index for a given table
    parameter:
        TypedIndex: data
    response:
        TypedIndex

GET /v1/{prefix}/namespaces/{namespace}/indexes/{index}
    summary: Load index from a catalog
    parameter:
        IndexIdentifier: index
    response:
        TypedIndex

POST /v1/{prefix}/namespaces/{namespace}/indexes/{index}
    summary: Updates an index
    parameter:
        UpdateIndexData: data
    response:
        TypedIndex

DELETE /v1/{prefix}/namespaces/{namespace}/indexes/{index}
    summary: Deletes an index
    parameter:
        IndexIdentifier: index

HEAD /v1/{prefix}/namespaces/{namespace}/indexes/{index}
    summary: Check if an index exists
    parameter:
        IndexIdentifier: index
```

```yaml
BaseIndex:
    identifier:
        type: '\#/components/schemas/IndexIdentifier'
    index-type:
        type: '\#/components/schemas/IndexType'
    index-columns:
        type: array
        items: IntegerTypeValue
    optimized-columns:
        type: array
        items: IntegerTypeValue
    available-table-snapshots:
        type: array
        items: LongTypeValue

DetailedIndex:
    allOf:
        '\#/components/schemas/BaseIndex'
    location:
        type:  string
    versions:
        type: array
        items:  '\#/components/schemas/IndexVersion'
    versionLog:
        type: array
        items:  '\#/components/schemas/IndexVersionLog'
    snapshots:
        type: array
        items: '\#/components/schemas/IndexSnapshot'

IndexSnapshot:
    table-snapshot-id:
        type: array
        items: LongTypeValue
    index-snapshot-id:
        type: array
        items: LongTypeValue
    version-id:
        type: array
        items: LongTypeValue
    properties:
        type: object
    additionalProperties:
        type: string

IndexVersion:
    vesion-id:
        type: array
        items: LongTypeValue
    timestamp-ms:
        type: array
        items: LongTypeValue
    properties:
        type: object
    additionalProperties:
        type: string

IndexVersionLog:
    version-id:
        type: array
        items: LongTypeValue
    timestamp-ms:
        type: array
        items: LongTypeValue

IndexType:
  enum: [BTree, Term, IVF]
```

## Data
The index type must explicitly define all available properties \- both user-defined and maintenance-service-defined \- as well as the contents of the index location. If certain parameters cannot be represented within the properties map, the index may specify custom Puffin file layouts to store these values. In such cases, the properties map should include references to the corresponding data, such as file names and byte offsets within the Puffin file.
## In query
There are 2 main use-cases for indexes depending on the content of the index:
* **Covering index**: When all of the queried data is contained in the index then it is enough to query the index to return the query results
* **Skipping index**: The skipping index could be used to collect the rowIds which need to be read, and then the engine needs to read the original table to read the actual rows not contained in the index itself.

# Index Metadata
Every available index should be listed in the catalog. The list of the indexes could be used by the engines and the query planner to decide if an index could be useful during the query execution/scan.

The following metadata should be stored per index in the index metadata json:

| Field name         | Field Type             | Requirement | Description                                                                                                                                                                                                                   |
|:-------------------|:-----------------------|:------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| index-uuid         | String                 | required    | A UUID that identifies the index, generated when the index is created. Implementations must throw an exception if an index's UUID does not match the expected UUID after refreshing metadata                                  |
| format-version     | Integer                | required    | An integer version number for the index metadata format; format-version is 1 for current version of spec.                                                                                                                     |
| type               | String                 | required    | One of the supported index-types. For example: BTREE, TERM, IVF. Must be supplied during the creation of an index and must not be changed.                                                                                    |
| index-columns      | List of Integer        | required    | The ids of the columns contained by the index.                                                                                                                                                                                |
| optimized-columns  | List of Integer        | required    | The ids of the columns that the index is designed to optimize for retrieval.                                                                                                                                                  |
| location           | String                 | required    | index’s base location; used to create index file locations.                                                                                                                                                                   |
| current-version-id | Integer                | required    | ID of the current version of the index (version-id).                                                                                                                                                                          |
| versions           | List of \<Version\>    | required    | A list of known versions of the index, the number of versions retained is implementation-specific. current-version-id must be present in this list.                                                                          |
| version-log        | List of \<VersionLog\> | optional    | A list of version log entries with the timestamp and version-id for every change to current-version-id. The number of entries retained is implementation-specific. current-version-id may or may not be present in this list. |
| snapshots          | List of \<Snapshot\>   | optional    | During the index maintenance a new index snapshot is generated for the specific Table snapshot and it is added to the snapshots list.                                                                                         |
## Version
Each Version in versions is a struct with the following fields:

| Field name | Field type | Requirement | Description |
| :---- | :---- | :---- | :---- |
| version-id | Integer | required | ID for the version |
| timestamp-ms | Long | required | Timestamp when the version was created (ms since epoch) |
| properties | Map | optional | A map of index properties, represented as string-to-string pairs, supplied by the user. |
## Version log
The version log tracks changes to the index's current version. This is the index's history and allows reconstructing what version of the index would have been used at some point in time.

Note that this is not the version's creation time, which is stored in each version's metadata. A version can appear multiple times in the version log, indicating that the index definition was rolled back.

Each entry in version-log is a struct with the following fields:

| Field name | Type | Requirement | Description |
| :---- | :---- | :---- | :---- |
| timestamp-ms | Long | required | Timestamp when the index’s current-version-id was updated (ms from epoch) |
| version-id | String | required | ID that current-version-id was set to |
## Snapshot
Index data is versioned using snapshots, similar to table data. Each index snapshot is derived from a specific table snapshot, ensuring consistency. When an engine queries a table snapshot, it must determine whether a corresponding index snapshot exists and, if so, determine which properties should be applied for index evaluation.

This relationship is maintained in the table’s metadata file through an index-snapshot list. This list is updated whenever an index maintenance process creates a new snapshot for the index and links it to the corresponding base table snapshot, or when the index maintenance process decides to expire index snapshots.

| Field name | Type | Requirement | Description |
| :---- | :---- | :---- | :---- |
| table-snapshot-id | Long | required | The table snapshot id which is the base of the index version |
| index-snapshot-id | Long | required | The index snapshot id |
| version-id | Long | required | The index version id when the snapshot was created. |
| properties | Map | optional | A map of index properties, represented as string-to-string pairs, supplied by the Index Maintenance process. |
# Proposed implementation
To reduce complexity, we propose focusing on a few examples:
* **B-Tree index**: For tables stored in blobstores, the current sorted and partitioned tables are a prime example for B-Tree indexes.
* **Full text index:** A full-text index can be implemented as a B-Tree, where terms serve as keys and the associated values are lists of pointers to the target rows. Considering how Parquet handles lists, an alternative approach is to store term–*\_file*–*\_pos* triplets directly.
* **IVF-PQ index:** IVF-PQ is essentially a PK-Quantized value table which is partitioned based on the nearest centroid.

These examples illustrate that several key index types can be implemented as standard [Iceberg Materialized Views](https://docs.google.com/document/d/1UnhldHhe3Grz8JBngwXPA6ZZord1xMedY5ukEhZYF-A), enabling engines to reuse existing components for reading, writing, and maintaining indexes. In such cases, the index’s data content is simply a storage table of a materialized view. The column identifiers in the materialized view schema should match those in the original table schema to allow query engines to seamlessly substitute the original table’s data files with the materialized view’s data files and execute queries against the view immediately. If they do not match, a name‑mapping process must be applied.
## Example
If we have a table defined as (pseudo code):
```sql
CREATE TABLE persons (
  person_id int,
  salary int,
  last_name string,
  first_name string,
  resume string,
  nationality string
) ORDER BY person_id ASC;
```
This table is optimized to return persons when searched by person\_id, but would be very inefficient to find persons based on nationality.
### B-Tree index
When the user creates a B-Tree index, like:
```sql
CREATE INDEX nat_index
  ON persons
  USING BTREE([nationality], [person_id, last_name, first_name]);
```
We could create a B-Tree index by creating a materialized above the original table for enum columns like:
```sql
CREATE MATERIALIZED VIEW nat_index AS
  SELECT person_id, last_name, first_name, nationality
  FROM persons
  PARTITIONED BY bucket(nationality, bucket_num)
  ORDER BY nationality ASC;
``` 
We could create a B-Tree index by creating a materialized above the original table for columns containing high cardinality and contain orderable data, such as numeric or date fields:
```sql
CREATE MATERIALIZED VIEW salary_index AS
  SELECT person_id, last_name, first_name, nationality
  FROM persons
  PARTITIONED BY truncate(salary, truncate_width)
  ORDER BY salary ASC;
```
The resulting view should include all optimized columns and index columns. The index creation and maintenance logic can apply partitioning and ordering to produce an optimal layout, ensuring the view returns efficient results when queried by the optimized columns.
### Full text index
When the user creates a B-TREE index, like:
```sql
CREATE INDEX term_index ON persons USING TERM(resume, [_file, _pos]);
```
If a UDF named *get\_terms* is available to extract terms from resumes and returns *row\_id–term* pairs, we can create a Term index by defining a materialized view on top of the original table as follows:
```sql
CREATE MATERIALIZED VIEW term_index AS
  SELECT terms.term, persons._file, persons._pos
  FROM persons, TABLE(get_terms(persons.row_id, persons.resume)) terms
  WHERE persons.row_id = terms.row_id
  PARTITIONED BY truncate(term, truncate_size)
  ORDER BY term ASC;
```
```sql
CREATE MATERIALIZED VIEW term_index AS
  SELECT terms.term, persons._file, persons._pos
  FROM persons
  LATERAL VIEW get_terms(persons.resume) AS terms
  PARTITIONED BY truncate(term, truncate_size)
  ORDER BY term ASC;
```
The resulting view should include the terms, along with the *\_file* and *\_pos* columns for rows containing those terms. The index creation and maintenance logic can apply partitioning and ordering to produce an optimal layout, ensuring the view returns efficiently positions of records where a person's résumé contains a given term.
### IVF-PQ index
When the user creates a IVF index, like:
```sql
CREATE INDEX ivf_index ON persons USING IVF(resume, embedding, quantizer, [_file, _pos]);
```
If we have:
* An UDF for getting the embedding for the resume called *embedding*, and
* An UDF for quantizing the vector called *quantizer*, and
* An UDF for getting the nearest centroids for a vector called *centroid*,
then we could create a Vector index by creating a materialized view above the original table table like:
```sql
CREATE MATERIALIZED VIEW ivf_index AS
  SELECT quantizer(embedding(resume)), _file, _pos
  FROM persons
  PARTITIONED BY centroid(embedding(resume));
```
The resulting view should include the quantized vector for the résumé along with the *\_file* and *\_pos* columns for each row. The index creation and maintenance logic can apply partitioning and ordering to produce an optimal layout, ensuring the view efficiently returns the positions of records where a person's résumé is most similar to the one being searched.
## Usage
### Index properties
Certain indexes require properties provided by the user during index creation or generated by table maintenance operations. These properties are defined by the index type and stored in the index version or index snapshot metadata—for example, quantizer settings for vector indexes. For example, the quantizer properties are kept in a separate Puffin file and referenced in the snapshot properties map.
### Covering index
When an index contains all the columns referenced in a query, the Iceberg planner can substitute the original table with the corresponding materialized view during scan planning.
If there is an index that:
* Has a matching materialized view snapshot for the table snapshot referenced by the query,
* Includes all required columns,
* And has its ordering columns aligned with those referenced in the filter expression and/or the order by columns,
then the materialized view should serve as the base for the query instead of the original table.
### Skipping index
For skipping indexes, the engine may need to split the query into two phases:
* Scan the relevant index to retrieve row identifiers (filename and pos, or rowId, or primary key columns).
* Use these results to query the original table and fetch the actual rows.
The optimizer should choose this approach when:
* The query is expected to return only a small number of rows, and
* There is an index that:
  * Has a matching materialized view snapshot for the table snapshot referenced by the query,
  * Includes the most selective filtering columns,
  * Contains columns which identify a single row
    * filename and position columns, or
    * rowId column, or
    * the primary key columns for the table
  * Has ordering columns aligned with those used in the filter expression.
In such cases, the engine should first query the index, then use the returned filenames and positions to retrieve the final results from the original table.
### Example queries
#### Example table
```sql
CREATE TABLE persons (
  person_id int,
  salary int,
  last_name string,
  first_name string,
  resume string,
  nationality string
) ORDER BY person_id ASC;
```
#### Initial index definitions
```sql
CREATE INDEX nat_index
  ON persons
  USING BTREE([nationality], [person_id, last_name, first_name]);

CREATE INDEX salary_index
  ON persons
  USING BTREE([salary], [last_name, first_name]);

CREATE INDEX term_index ON persons USING TERM(resume, [_file, _pos]);

CREATE INDEX ivf_index ON persons USING IVF(resume, embedding, quantizer, [_file, _pos]);
```
#### Query behavior
Covering query:
* Query: `SELECT first_name, last_name WHERE nationality='HU'`
* Index Usage: *nat\_index* can act as a **covering index**, allowing the engine to query only the materialized view.

Range query:
* Query: `SELECT first_name, last_name WHERE salary > 100000`
* Index Usage: *salary\_index* can act as a **covering index**, allowing the engine to query only the materialized view.

Skipping query:
* Query: `SELECT first_name, last_name, resume WHERE nationality='HU'`
* Index Usage: *nat\_index* works as a **skipping index**. The engine retrieves the relevant identifier list (*person\_id* in this case) from the index, then performs an additional step to fetch rows from the base table.

Term query:
* Query: `SELECT first_name, last_name WHERE MATCH(resume, 'Iceberg')`
* Index Usage: *term\_index* works as a **skipping index**, returning *\_file* and *\_pos* references, followed by a lookup in the original table.

IVF query:
* Query: `SELECT first_name, last_name WHERE IVF_SEARCH(resume, embedding('Ryan Blue's resume'), TOP_K => 10)`
* Index Usage: *ivf\_index* works as a **skipping index**, returning *\_file* and *\_pos* references, followed by a lookup in the original table.

#### Improving Performance
If users want faster results for two-stage queries and accept the extra storage and maintenance cost, they can create specialized indexes that store additional columns:
```sql
CREATE INDEX nat_index
  ON persons
  USING BTREE([nationality], [last_name, first_name, resume]);

CREATE INDEX term_index ON persons USING TERM(persons, [last_name, first_name]);

CREATE INDEX ivf_index
  ON persons
  USING IVF(resume, embedding, quantizer, [last_name, first_name]);
```
With these enhanced indexes, all the queries above can be resolved by querying only the indexes, eliminating the need for additional lookups.
## Index Maintenance
### View Maintenance
Index maintenance would happen asynchronously, and the Materialized view maintenance procedures could be used. Index metadata contains information about the freshness of the index, and the engines/planners could decide if the index could be used, or the query should be issued against the original table.
### Index optimization
Index internal parameters could be optimized by the maintenance process for better performance, like partitioning logic, or quantizer parameters.
### Snapshot expiration
Unused index snapshots should be removed. This process can be triggered when the number of snapshots for an index exceeds the configured limit, or when the corresponding table snapshot has been deleted and the index snapshot is no longer in use.
## Missing building blocks
Many of the building blocks for the proposed solution is already available, or the specification is in progress:

| Block | Component | Status |  |
| :---- | :---- | :---- | :---- |
| **Spec changes for indexes** | Specification | New | [https://docs.google.com/document/d/1N6a2IOzC6Qsqv7NBqHKesees4N6WF49YUSIX2FrF7S0](https://docs.google.com/document/d/1N6a2IOzC6Qsqv7NBqHKesees4N6WF49YUSIX2FrF7S0) |
| **Catalog changes and example JDBC implementation** | Catalog implementation | New |  |
| **REST Catalog changes** | Catalog implementation | New |  |
| **Materialized views** | Index implementation | ⚠️- in progress | [https://docs.google.com/document/d/1UnhldHhe3Grz8JBngwXPA6ZZord1xMedY5ukEhZYF-A](https://docs.google.com/document/d/1UnhldHhe3Grz8JBngwXPA6ZZord1xMedY5ukEhZYF-A) |
| **View switching for covering indexes** | Index implementation | New |  |
| **UDF specification** | Term index | ⚠️- in progress | [https://docs.google.com/document/d/1BDvOfhrH0ZQiQv9eLBqeAu8k8Vjfmeql9VzIiW1F0vc](https://docs.google.com/document/d/1BDvOfhrH0ZQiQv9eLBqeAu8k8Vjfmeql9VzIiW1F0vc) |
| **Rust/Java UDF** | IVF index \- Quantizer | New \- as a workaround, we can register the UDF in the engine using the provided JARs and invoke it from SQL. |  |
| **UDF partitioning** | IVF index \- Centroid | New \- as a workaround, it could be an extra column |  |
| **Query optimizer changes** | Skipping index implementation | New |  |
| **Index maintenance** | Index manitenance | ⚠️- same as for materialized views |  |
| **Index optimization** | Index manitenance | New |  |
