## Setup JdbcCatalog
1. Use schema creation script under `sql/` to initialize database tables.

    For example, use `sql/pg.sql` to create tables with a PostgreSQL.
2. Config [jooq][jooq-doc] to use the database setup in step 1, and generate java objects under `jooqgenerated/`.
3. Create JdbcCatalog instance with the database setup in step 1.

[jooq-doc]: https://www.jooq.org/doc/3.15/manual/getting-started/tutorials/jooq-in-7-steps/jooq-in-7-steps-step3/

