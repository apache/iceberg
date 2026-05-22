<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache Iceberg — Agent Instructions

Project conventions, architecture, and coding patterns synthesized from 58,000+ review comments across 4,300+ merged PRs.

## Architecture

### Module Boundaries

- **API** (`api/`): Public interfaces and types. Changes affect every engine and catalog. API breaks are almost never acceptable.
- **Core** (`core/`): Table spec implementation. Must be engine-agnostic. No Spark/Flink references. Properties should apply to all catalogs.
- **Data** (`data/`): Generic data layer (DeleteFilter, BaseDeleteLoader, readers/writers). Behavior should be general, not engine-specific.
- **Spark** (`spark/`): Spark integration only. Tests here validate integration, not core behavior.
- **Flink** (`flink/`): Same principle as Spark — integration tests only.
- **REST Catalog** (`open-api/`): OpenAPI spec for catalog interop. Precision in spec text is critical.
- **AWS/GCP/Azure**: Cloud-specific catalog implementations. Don't leak cloud-specific assumptions into core.

The `api/` module has the strongest stability guarantees — breaking changes are almost never allowed. Other modules with public APIs (`iceberg-data`, `iceberg-parquet`, and others marked in `build.gradle`) may have breaking changes in minor releases, but they must be justified and all changes are tracked via `revapi`. New interface methods in any of these modules must include default implementations.

### High-Sensitivity Areas

- **`TableMetadata`**: Changes ripple through all engines and catalogs. Use `TableMetadata.Builder`; produce proper metadata updates for REST.
- **`SnapshotProducer` / `MergingSnapshotProducer`**: The commit path. Validations must use established patterns.
- **`ManifestGroup` / `ManifestReader`**: Container reuse causes bugs in parallel code. Callers must `copyWithoutStats` if holding references.
- **Serialization** (parsers): Never use Jackson annotations. Custom `XxxParser.toJson/fromJson` only. JSON keys use kebab-case. Optional fields only written when present.
- **REST spec**: Check for ambiguity, over-constraining, missing client-side guidance. POST for deltas, PUT for full-state replacement.
- **Scan planning**: Metrics must not leak across `TableScan` refinements. Timers must be thread-safe (parallel manifest scanning).

## Design Patterns

- **Refinement**: `TableScan` methods return new independent scans. State must not leak between refinements.
- **`CloseableIterable`** over `Stream`: Iceberg's standard lazy collection. Always close iterables.
- **Null over `Optional`**: Use `null` for missing values. `Optional` is not used.
- **Builder pattern**: For complex creation. Never require passing `null` for optional parameters.
- **Package-private by default**: Only make things public with demonstrated need.
- **Postel's Law**: Accept case-insensitive input, produce canonical output.
- **`Tasks.foreach`**: For bulk operations with parallelism, retry, and error handling.
- **Immutable metadata**: `TableMetadata`, `Schema`, `PartitionSpec`, `SortOrder` produce new instances via builders.
- **Metadata updates for REST**: All mutations must produce serializable `MetadataUpdate` objects.
- **`SerializableTable`**: Wrap table references for Spark/Flink serialization. Don't serialize the catalog.
- **Validate at boundaries**: `Preconditions` at public entry points; internal methods assume invariants hold.
- **Spec version gating**: Version 2+ features must check `formatVersion >= 2` with clear errors for v1 tables.

## Coding Conventions

### API Design

- New public methods require strong justification. Prefer package-private.
- Never break APIs. Add default implementations to new interface methods.
- Don't introduce deprecated methods in brand-new interfaces.
- Use `@SuppressWarnings({"unchecked", "rawtypes"})` internally rather than widening public signatures.
- Prefer builders over multi-argument create methods.
- Keep the `Table` API small. Utility methods go in helper classes.
- Operations should be idempotent. Return final state from `apply()`, not intermediate changes.
- Minimize third-party types (JTS, Guava) in public APIs. `StructLike` equality requires `StructLikeWrapper`.

### Naming

- Method names describe specific behavior: `selectInIdOrder` not `selectOrdered`.
- Avoid `get` prefix — use `find`, `fetch`, `load`, `parse`, `create`, or drop it.
- Variable names indicate meaning, not type. Property names use kebab-case.
- Capitalize `ID` consistently. `toString()` must produce parseable output.
- Avoid `Factory` suffix unless the class is a true factory.

### Code Style

- 2 spaces indent, 4 spaces continuation. Empty newline after control flow blocks.
- Use `this.` for instance field assignment. `Preconditions` calls first in methods.
- No `final` on locals. No one-argument-per-line unless necessary.
- Magic numbers should be named constants. No personal pronouns in comments.
- `} else {` on same line. Minimize variable scope. `try-with-resources` for all `AutoCloseable`.
- Prefer method references over lambdas. Wrap lines at the highest semantic level.
- Always use imports — never use fully-qualified class names inline.

### Code Placement

- Extract reusable logic to the right utility layer (`TypeUtil`, `SchemaUtil`, `DateTimeUtil`, etc.).
- Engine-specific concepts must not leak into core. Follow existing patterns before introducing new ones.
- Parsers: `XxxParser.toJson/fromJson`. Config: `XxxProperties`. Check existing utilities first.
- Avoid expanding Guava — use JDK equivalents. Use `CloseableGroup` for multi-resource lifecycle.
- Keep things internal until proven needed.

### Serialization

- Never use Jackson annotations. Custom `XxxParser.toJson`/`fromJson` only.
- JSON keys: kebab-case. Optional fields: only write when non-null. Required fields: validate in constructors.
- `Preconditions.checkArgument` for validation. `MoreObjects.toStringHelper` for `toString()`.
- Wrap `IOException` in `UncheckedIOException`. Use `Locale.ROOT` for case conversions.
- Use `Iterable`/`CloseableIterable` over `Stream`. Use `null` not `Optional`. Chain exception causes.
- Use Iceberg's `Pair` instead of `Map.Entry`.

### Error Handling

- Messages: direct, actionable, with specific values. Capitalize first word.
- Don't swallow exceptions. Use `closeQuietly` for cleanup that shouldn't mask real failures.
- Use `ConcurrentMap` for shared mutable state. `Preconditions.checkArgument` over NPE.
- Close iterables in `finally`. Separate `Preconditions` calls for each condition.
- Forward compatibility: don't fail on unknown reserved bytes.

### Performance

- Watch for hidden materialization in streaming pipelines (copy/collect steps).
- Builders over rebuild patterns in hot paths. Lazy over eager evaluation.
- `ByteBuffer` over `byte[]`. Direct-access arrays for dense integer keys.
- Avoid streams/closures in tight loops. Cache per-class, not per-call.

### Configuration

- New features default to off. Question whether a property is even needed.
- Properties must work across all catalogs. Zero/negative disables caches/features.
- Versions go to version catalog. Engine-specific properties don't belong in `TableProperties`.

### Testing

- Minimal test setup: `PartitionSpec.unpartitioned()` when partitioning isn't needed.
- Test classes and methods should be package private unless required by inheritance.
- Compute expected values, don't hardcode. Tests belong in the module that owns the code.
- Write the most direct test for the bug. Parameterized tests for type variations.
- JUnit 5 + AssertJ: `@Test` (no `test` prefix), `assertThat`, `assertThatThrownBy`.
- `waitUntilAfter` for time-dependent tests. Separate tests over combined.

### REST / OpenAPI Spec

- Spec describes behavior, not implementation. RFC 2119: "MUST" = absolute, "SHOULD" = may reject.
- Include client-side guidance. Consistent encoding terminology. 409 for "already exists", not 400.
- Favor the client: required response fields reduce client complexity.

## Commands

- **Build (no tests):** `./gradlew build -x test -x integrationTest`
- **Single test class:** `./gradlew :iceberg-core:test --tests org.apache.iceberg.TestTableMetadata`
- **Single test method:** `./gradlew :iceberg-core:test --tests "org.apache.iceberg.TestTableMetadata.testJsonSerialization"`
- **Spark-versioned module:** `./gradlew :iceberg-spark:iceberg-spark-4.1_2.13:test --tests "org.apache.iceberg.spark.source.TestSparkReaderDeletes"`
- **Format code:** `./gradlew spotlessApply`
- **Check formatting:** `./gradlew spotlessCheck`
- **API compatibility:** `./gradlew revApiCheck`

## PR & Commit Conventions

- PR titles follow `Module: Description` format (e.g., `Core: Fix ...`, `Spark: Add ...`, `Docs: Update ...`).
- One concern per PR. Unrelated whitespace, import, or formatting changes go in separate PRs.
- Keep first version of a PR minimal — defer recovery, optimization, and edge cases to follow-ups.
- Commit messages describe the *what* and *why*, not implementation details.
- Apache License header required on all new files (enforced by spotless pre-commit hook).

## Boundaries

- **Never** modify `.asf.yaml`, `LICENSE`, `NOTICE`, or `versions.props` without explicit discussion.
- **Never** add Jackson annotations to serialization classes — always use custom `XxxParser` classes.
- **Never** break public API without an approved `revapi.yml` exception.
- **Never** add Hadoop dependencies where `FileIO` abstractions exist.
- **Never** commit secrets, credentials, or cloud-specific tokens.
- **Ask first** before adding new third-party dependencies (license compatibility matters).
- **Ask first** before promoting package-private classes/methods to public.
