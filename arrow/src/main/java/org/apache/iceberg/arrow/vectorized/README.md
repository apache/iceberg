# Vectorized Reads of Lists from Parquet Files

## Problem Statement

Vectorized reads for primitive (scalar) columns work by bulk-copying raw page bytes directly or with bulk operations into
Arrow data buffers. Unfortunately, this approach does not work for list columns because of the way Parquet and Arrow
encodes lists using different formats.

## Parquet Three-Level List Representation and Dremel Encoding

Parquet stores lists using the standard three-level nesting:

```
optional|required group my_list (LIST) {          // level 1 — the list field itself
  repeated group list {                  // level 2 — the repeated wrapper group
    optional|required int32 element;              // level 3 — the actual element
  }
}
```

### The Dremel Encoding

Each leaf value in this structure is encoded as a **triple**: `(repetition_level, definition_level,
value)`. This is the Dremel encoding.

#### Repetition Levels

The repetition level describes where in the list nesting a value falls:

| `rep_level`               | Meaning                                                      |
|---------------------------|--------------------------------------------------------------|
| `< list_repetition_level` | Start of a new list on `rep_level`                            |
| `list_repetition_level`   | Continuation of the current list (next element in same list) |

#### Definition Levels

The definition level encodes how many optional/repeated fields along the path are actually present.
For the three-level layout above where both the list and elements are optional, the max definition level is `3` and the values mean:

| `def_level` | Meaning |
|-------------|---------|
| `0`         | The list field itself is null |
| `1`         | The list is present but empty (the repeated `list` group is absent) |
| `2`         | The list has an element, but the element is null |
| `3`         | The list has a non-null element (value is present) |

## The Arrow `ListVector` Representation

Arrow stores lists in a `ListVector`, which consists of three buffers:

```
ListVector
├── validity bitmap buffer     — 1 bit per list row; 0 = null list, 1 = non-null list
├── offsets buffer      — int32[numRows + 1]; offsets[i]..offsets[i+1] is the range of elements for row i
└── data child vector   — flat FieldVector (e.g. IntVector) holding all elements end-to-end
    ├── validity bitmap buffer — 1 bit per element; 0 = null element, 1 = non-null element
    └── data buffer     — contiguous element values
```

## Mapping between the two formats

| Concern | Parquet                        | Arrow                                                    |
|---------|--------------------------------|----------------------------------------------------------|
| List nullability | `def_level == 0` in the triple stream | Bit `0` in `ListVector` validity buffer                  |
| Empty list | `def_level == 1`, no element triples | `offsets[i] == offsets[i+1]` (zero-length range)         |
| Null element | `def_level == (maxDefLevel - 1)` | Bit `0` in child vector validity buffer                  |
| Element value | Differs based on the encoding  | Stored in child vector data buffer or dictionary encoded |
| Row boundary | `rep_level == 0` starts a new row | Encoded as an offset entry                               |

## Problems with the current vectorized reader design

List columns break current assumptions many way:

1. **List data is not interpretable without the repetition level.** The existing vectorized infrastructure (`VectorizedPageIterator`,
   `VectorizedParquetDefinitionLevelReader`) is built around flat columns: it counts definition-level
   triples one-per-row and has no concept of repetition levels
2. **List data is variable width and can span across multiple pages** Reading a fix number of row may contain unfinished lists,
   reading fix number of rows can cause memory issues
3. **The number of triplets read not equals the number of values in the vector** 
4. **Nullability has multiple levels.** The list itself may be null or empty. Each element within a
   non-null list may also independently be null (for optional element types). A single
   `NullabilityHolder` sized to the number of rows (lists) is not enough to describe element-level
   nullability — that is a separate concern tracked inside the inner element vector.

### Repetition levels are ignored by the current vectorized readers

In the current vectorized reader chain call — `VectorizedArrowReader` ->`VectorizedColumnIterator`-> 
`VectorizedPageIterator` -> `VectorizedParquetDefinitionLevelReader` repetition levels are ignored.
There are multiple options:
1. Read the primitive values and repetition values together with actual values. Then reconstruct the list on the list level.

   **Pros**
   * Can reuse the existing vectorized readers with minimal change
   * Reading from storage can be truly vectorized
   * Handles nested structures more naturally as fits current reader builder pattern

   **Cons**
   * Extra complexity around page boundaries
   * Extra memory allocation/data movement required to reconstruct the list

![sequence diagram](./vectorized_list_reader.svg)

2. Make all current vectorized readers repetition aware. Batch reads based on repetition levels instead of definition levels
and

   **Pros**
   * Can read to Arrow format in the first go, no need to move data around

   **Cons**
   * Need to take care of the higher level structure on low levels
   * Further complicates element readers
   * Needs changes in many level

+1. Don't read into arrow list vector, but read into a normal vector with extra repetition info in the `VectorHolder`.
The `ArrowVectorAccessor` should be able to reconstruct lists during reads.

   **Pros**
   * Very easy and fast read to memory

   **Cons**
   * The access for lists is slow because reconstructing happens at read time.
This can be mitigated by calculating the offsets (for non nested lists this is the same as a normal Arrow ListVector)
   * The batch size and list sizes are not aligned, so hard to manage memory and batch boundaries
   * Not clear how nested lists should work
   * Advanced readers cannot read the underlying vector

For nested lists we need to return not just the nullability information but also the repetition level of the lists like
the nullability holder

### Proposed design 

Because of the simpler approach the current implementation uses the first approach. 

1. **Read element repetition levels** These are stored in memory and can be read a same way as definition levels.
2. **Read element values with definition level information** Read the definitions and values as previously but store the explicit definition level information
3. **Build list vector** Iterate over the lists and build the list vector

### Further questions/issues
* Need new performance tests to compare the performance vs non-vectorized reads
* NullabilityHolder refactoring
  * Store definition levels only for nested fields
  * Use a bit vector instead of a byte array
* Intermediate lists doesn't have ColumnDescriptors: these are not a columns in Parquet files
* TestArrowReader doesn't support null values
* How batch size should be interpreted for list columns? Should it be the number of rows (lists) or the number of elements?
  * Triplets are not the same as rows but the current code treats them as such
* Bug?: VectorizedArrowReader always read whole row group (numValsToRead is only used to check if page size fits)
* How to find end of lists? We need to read an extra page to be sure that the list is not spilling to the next page
* Arrow Java indexes vectors with ints but Parquet allows more than Integer.MAX_VALUE values in a list:
  Arrow buffers support 64-bit addressing, so this is just inconvenience (Previously batch size could provide an upper bound)
