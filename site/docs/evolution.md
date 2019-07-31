# Table Evolution

Iceberg supports **in-place table evolution**. You can [evolve a table schema](#schema-evolution) just like SQL -- even in nested structures -- or [change partition layout](#partition-evolution) when data volume changes. Iceberg does not require costly distractions, like rewriting table data or migrating to a new table.

For example, Hive table partitioning cannot change so moving from a daily partition layout to an hourly partition layout requires a new table. And because queries are dependent on partitions, queries must be rewritten for the new table. In some cases, even changes as simple as renaming a column are either not supported, or can cause [data correctness](#correctness) problems.

## Schema evolution

Iceberg supports the following schema evolution changes:

* **Add** -- add a new column to the table or to a nested struct
* **Drop** -- remove an existing column from the table or a nested struct
* **Rename** -- rename an existing column or field in a nested struct
* **Update** -- widen the type of a column, struct field, map key, map value, or list element
* **Reorder** -- change the order of columns or fields in a nested struct

Iceberg schema updates are metadata changes. Data files are not eagerly rewritten.

Note that map keys do not support adding or dropping struct fields that would change equality.

### Correctness

Iceberg guarantees that **schema evolution changes are independent and free of side-effects**:

1.  Added columns never read existing values from another column.
2.  Dropping a column or field does not change the values in any other column.
3.  Updating a column or field does not change values in any other column.
4.  Changing the order of columns or fields in a struct does not change the values associated with a column or field name.

Iceberg uses unique IDs to track each column in a table. When you add a column, it is assigned a new ID so existing data is never used by mistake.

* Formats that track columns by name can inadvertently un-delete a column if a name is reused, which violates #1.
* Formats that track columns by position cannot delete columns without changing the names that are used for each column, which violates #2.


## Partition evolution

Iceberg table partitioning can be updated in an existing table because queries do not reference partition values directly.

Iceberg uses [hidden partitioning](../partitioning), so you don't *need* to write queries for a specific partition layout to be fast. Instead, you can write queries that select the data you need, and Iceberg automatically prunes out files that don't contain matching data.

Partition evolution is a metadata operation and does not eagerly rewrite files.
