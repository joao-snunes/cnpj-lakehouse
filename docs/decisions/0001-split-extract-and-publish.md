# ADR 0001 — Split ingestion into an extract stage and a join/publish stage

## Status

Accepted. Extract stage implemented (`ingestion/extract_parquet.py`). Join/publish stage not implemented yet.

## Context

The first version of the ingestion pipeline (`ingestion/transform_bronze.py`, now removed) did three things in a single pass, per file group:

1. Stream-parse the CSV out of the downloaded `.zip` shard (never extracted to disk — `zipfile.ZipFile.open()` + `pandas.read_csv(..., chunksize=...)`).
2. Filter to a single UF: either directly (Estabelecimentos has its own `uf` column) or via a semi-join against the set of `cnpj_basico` values kept from Estabelecimentos (Empresas, Sócios, Simples/MEI don't have `uf`, only `cnpj_basico`).
3. Write the filtered result to Bronze Parquet, partitioned by `data_referencia`/`uf`.

During this session we benchmarked the parse step against a real downloaded shard (`Empresas5.zip`, 93MB compressed / 336MB decompressed CSV, 634,957 rows) with pandas, PyArrow, Polars and DuckDB:

| Engine | Time | Peak RSS (delta) |
|---|---|---|
| pandas (original) | 17.35s | ~195 MB |
| PyArrow (streamed transcode) | 4.64s | ~533 MB |
| DuckDB (reading from an extracted CSV file) | 5.93s | ~205 MB |

PyArrow's CSV reader is ~3.7x faster than pandas for the parse step while keeping the same streaming-from-zip technique (no need to extract the CSV to disk first). DuckDB, on the other hand, cannot read directly from inside a `.zip` member — it needs a real file path or URL — but is a better fit for step 2 (the semi-join): it has a native out-of-core hash join, avoiding the ~500MB-1GB Python `set()` of `cnpj_basico` strings the original code built and held in memory, and it can write directly to S3 via the `httpfs` extension without an extra `boto3`/`s3fs` dependency.

Bolting DuckDB onto the existing single-pass script would have meant extracting the national CSVs to disk anyway (to hand them to DuckDB), defeating the point of the zip-streaming technique. Splitting the pipeline into two stages lets each tool do only the part it's good at, using Parquet (compact, columnar, already typed) as the hand-off format between them instead of raw CSV.

## Decision

Split the pipeline into two independently runnable stages:

1. **Extract** (`ingestion/extract_parquet.py`) — reads each shard's CSV straight out of its `.zip` with PyArrow (same streaming technique as before, just a faster parser), applies the official column layout, and writes it to `data/staging/` as Parquet. **No UF filter, no `cnpj_basico` join.** Every group (including Estabelecimentos) is processed identically — one output Parquet file per shard, nothing merged across shards or groups.
2. **Join + publish** (not implemented yet) — a future module where DuckDB reads the staged Parquet from step 1, applies the UF filter (`estabelecimentos`) and the `cnpj_basico` semi-join (`empresas`, `socios`, `simples`), and writes/uploads the final Bronze result to S3.

### Output layout

```
data/
├── raw/                                  (unchanged — downloaded .zip)
├── staging/                              (NEW — extract stage output)
│   └── <tabela>/
│       ├── data_referencia=<AAAA-MM>/shard=<N>/part-00000.parquet   (sharded groups)
│       └── data_referencia=<AAAA-MM>/part-00000.parquet             (single-file groups)
└── bronze/                               (future — join/publish stage output, UF-filtered + joined)
```

`data/staging/` (rather than `data/raw_parquet/` or `data/interim/`) was chosen as the name for this layer: it's the standard ETL term for a pre-final landing area, and it avoids overloading "raw" (already used for the downloaded `.zip` directory). `shard=<N>` mirrors the project's existing `data_referencia=`/`uf=` Hive-style partitioning convention and is what makes "extract just one shard" a real, independent, non-colliding operation — each shard gets its own file. Every partition value that's a directory (`data_referencia`, `shard`) is also written as a real column in the Parquet, matching the existing convention, so the file is self-describing without relying on Hive-partition-aware reads.

Disk is treated as cheap for this transient staging layer: keeping the full national (unfiltered) data on disk between the two stages, instead of trying to filter as early as possible, trades some temporary disk space for a much simpler separation of concerns (all filtering/join logic lives in one place, the future join/publish stage) and for the ability to (re)run extraction for a single shard/group, or later re-filter to a different UF, without re-parsing anything.

## Consequences

- `ingestion/transform_bronze.py` is removed. Its filter-strategy logic (`own_uf` / `by_cnpj_basico` / `none`) is superseded by the future join/publish stage; available in git history if needed as reference.
- The extract stage has no new dependency (PyArrow was already a project dependency). DuckDB will be added when the join/publish stage is built.
- `docs/schema_bronze.md` and `context.md` are updated to describe the two-stage flow instead of the old single-pass one (see those files for the current state of what's implemented vs. planned).
- The `data/staging/` layer is intentionally not meant to be queried directly by anything downstream of the join/publish stage — it's transient, and a future cleanup step will remove it after a successful publish to S3 (not implemented yet, out of scope for this ADR).
