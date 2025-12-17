# BigQuery → DuckDB SQL Transpilation Plan

## Overview

This document outlines the complete plan to support all SQL statements in `bigquery-graph.json` (2,416 queries) for DuckDB mock mode execution.

**Current coverage:** ~70-80%
**Target coverage:** ~99% (excluding JavaScript UDFs and `_TABLE_SUFFIX` queries)

---

## Summary Stats

| Category | Count | Status |
|----------|-------|--------|
| Total SQL statements | 2,416 | |
| Already supported | ~1,900 | ✅ |
| Needs transformer updates | ~500 | 🟡 |
| Cannot transpile (JS UDFs) | 4 | ❌ |
| Cannot transpile (`_TABLE_SUFFIX`) | 5 | ❌ |

---

## 1. Already Implemented ✅

These transformations are already working in `src/sql/transformer.rs` and `src/sql/functions.rs`:

| BigQuery | Count | DuckDB |
|----------|-------|--------|
| `SAFE_DIVIDE(a, b)` | 2,425 | `a / NULLIF(b, 0)` |
| `SAFE_CAST(x AS T)` | 1,119 | `TRY_CAST(x AS T)` |
| `SAFE_OFFSET(n)` | 4,173 | `n` (0-based index) |
| `IFNULL(a, b)` | 4,202 | `COALESCE(a, b)` |
| `ANY_VALUE(x)` | 96 | `ARBITRARY(x)` |
| `TIMESTAMP_DIFF(t1, t2, unit)` | 145 | `DATE_DIFF('unit', t2, t1)` |
| `TIMESTAMP_ADD(t, INTERVAL)` | 71 | `t + INTERVAL` |
| `TIMESTAMP_SUB(t, INTERVAL)` | 199 | `t - INTERVAL` |
| `FORMAT_TIMESTAMP(fmt, ts)` | 55 | `strftime(ts, fmt)` |
| `PARSE_TIMESTAMP(fmt, s)` | 5 | `strptime(s, fmt)` |
| `DATETIME(x)` | 761 | `CAST(x AS TIMESTAMP)` |
| `DATE(x)` | 11,364 | `CAST(x AS DATE)` |
| `ARRAY_LENGTH(arr)` | 83 | `LEN(arr)` |
| `STRUCT(...)` | 8,870 | `struct_pack(...)` |
| `REGEXP_CONTAINS(s, p)` | 112 | `regexp_matches(s, p)` |
| `REGEXP_EXTRACT(s, p)` | 185 | `regexp_extract(s, p)` |
| `REGEXP_REPLACE(s, p, r)` | 366 | `regexp_replace(s, p, r)` |
| `BYTE_LENGTH(s)` | - | `octet_length(s)` |
| `CHAR_LENGTH(s)` | 3 | `length(s)` |
| `GENERATE_UUID()` | - | `uuid()` |

---

## 2. High Priority Additions 🟡

Functions with >100 occurrences that need to be added:

### 2.1 COUNTIF (1,130 occurrences)

**BigQuery:**
```sql
SELECT COUNTIF(status = 'active') FROM users
```

**DuckDB:**
```sql
SELECT COUNT(*) FILTER (WHERE status = 'active') FROM users
```

**Transform type:** Custom AST transformation

### 2.2 JSON Functions (~1,800 occurrences)

| BigQuery | Count | DuckDB |
|----------|-------|--------|
| `JSON_EXTRACT_SCALAR(j, path)` | 1,773 | `json_extract_string(j, path)` |
| `JSON_EXTRACT(j, path)` | 37 | `json_extract(j, path)` |
| `JSON_EXTRACT_ARRAY(j, path)` | 26 | `json_extract(j, path)` |
| `TO_JSON_STRING(val)` | 175 | `to_json(val)::VARCHAR` |

**Transform type:** Rename + Custom for `TO_JSON_STRING`

### 2.3 Date/Time Truncation (~1,450 occurrences)

| BigQuery | Count | DuckDB |
|----------|-------|--------|
| `DATE_TRUNC(date, MONTH)` | 909 | `date_trunc('month', date)` |
| `DATETIME_TRUNC(dt, DAY)` | 479 | `date_trunc('day', dt)` |
| `TIMESTAMP_TRUNC(ts, HOUR)` | 62 | `date_trunc('hour', ts)` |

**Transform type:** Swap arguments + quote the unit

### 2.4 CURRENT_DATETIME (1,462 occurrences)

**BigQuery:**
```sql
SELECT CURRENT_DATETIME()
```

**DuckDB:**
```sql
SELECT CURRENT_TIMESTAMP
```

**Transform type:** Rename (remove parentheses)

### 2.5 SPLIT (2,145 occurrences)

**BigQuery:**
```sql
SELECT SPLIT(tags, ',') FROM posts
```

**DuckDB:**
```sql
SELECT string_split(tags, ',') FROM posts
```

**Transform type:** Rename

### 2.6 DATETIME_ADD / DATETIME_SUB / DATETIME_DIFF (~475 occurrences)

| BigQuery | Count | DuckDB |
|----------|-------|--------|
| `DATETIME_ADD(dt, INTERVAL 1 DAY)` | 237 | `dt + INTERVAL '1 day'` |
| `DATETIME_DIFF(dt1, dt2, HOUR)` | 208 | `DATE_DIFF('hour', dt2, dt1)` |
| `DATETIME_SUB(dt, INTERVAL 1 HOUR)` | 31 | `dt - INTERVAL '1 hour'` |

**Transform type:** Arithmetic for ADD/SUB, same as TIMESTAMP_DIFF for DIFF

### 2.7 DATE_DIFF (67 occurrences)

Same transformation as `TIMESTAMP_DIFF`:

**BigQuery:**
```sql
SELECT DATE_DIFF(end_date, start_date, DAY)
```

**DuckDB:**
```sql
SELECT DATE_DIFF('day', start_date, end_date)
```

**Transform type:** Swap args 1&2, quote unit

---

## 3. Medium Priority Additions 🟡

Functions with 10-100 occurrences:

| BigQuery | Count | DuckDB | Transform |
|----------|-------|--------|-----------|
| `FORMAT_DATETIME(fmt, dt)` | 92 | `strftime(dt, fmt)` | Swap args |
| `FORMAT_DATE(fmt, d)` | 35 | `strftime(d, fmt)` | Swap args |
| `APPROX_QUANTILES(x, 100)[OFFSET(50)]` | 47 | `approx_quantile(x, 0.5)` | Custom |
| `REGEXP_EXTRACT_ALL(s, p)` | 38 | `regexp_extract_all(s, p)` | Rename |
| `SHA256(s)` | 35 | `sha256(s)` | Rename |
| `TO_HEX(bytes)` | 30 | `encode(bytes, 'hex')` | Custom |
| `STRING_AGG(s, delim)` | 29 | `string_agg(s, delim)` | Native ✅ |
| `ARRAY_CONCAT(a1, a2)` | 28 | `list_concat(a1, a2)` | Rename |
| `DATE_SUB(d, INTERVAL)` | 22 | `d - INTERVAL` | Arithmetic |
| `DATE_ADD(d, INTERVAL)` | 16 | `d + INTERVAL` | Arithmetic |
| `GENERATE_ARRAY(start, end)` | 20 | `generate_series(start, end)` | Rename |
| `GENERATE_DATE_ARRAY(s, e, INTERVAL)` | 16 | `generate_series(s, e, INTERVAL)` | Rename |
| `LPAD(s, len, pad)` | 20 | `lpad(s, len, pad)` | Native ✅ |
| `ARRAY_TO_STRING(arr, delim)` | 16 | `array_to_string(arr, delim)` | Native ✅ |
| `STARTS_WITH(s, prefix)` | 15 | `starts_with(s, prefix)` | Native ✅ |
| `PARSE_DATETIME(fmt, s)` | 13 | `strptime(s, fmt)` | Swap args |
| `SHA1(s)` | 10 | `md5(s)` or extension | Rename |

---

## 4. Low Priority Additions 🟡

Functions with <10 occurrences:

| BigQuery | Count | DuckDB | Transform |
|----------|-------|--------|-----------|
| `UNIX_MILLIS(ts)` | 7 | `epoch_ms(ts)` | Rename |
| `TO_BASE64(bytes)` | 6 | `encode(bytes, 'base64')` | Custom |
| `FARM_FINGERPRINT(s)` | 5 | `hash(s)` | Rename (different algorithm) |
| `ARRAY_CONCAT_AGG(arr)` | 5 | `flatten(list_agg(arr))` | Custom |
| `LOGICAL_OR(x)` | 5 | `bool_or(x)` | Rename |
| `UNIX_SECONDS(ts)` | 5 | `epoch(ts)` | Rename |
| `TIMESTAMP_MILLIS(ms)` | 3 | `to_timestamp(ms / 1000.0)` | Custom |
| `STDDEV(x)` | 3 | `stddev(x)` | Native ✅ |
| `SAFE_MULTIPLY(a, b)` | 2 | `TRY_CAST(a*b AS DOUBLE)` | Custom |

---

## 5. NET.* Functions (18 occurrences)

These require DuckDB macros to be registered at session init:

| BigQuery | Count | Implementation |
|----------|-------|----------------|
| `NET.REG_DOMAIN(url)` | 5 | Regex macro |
| `NET.IP_TRUNC(ip, prefix)` | 4 | Bit manipulation macro |
| `NET.HOST(url)` | 4 | Regex macro |
| `NET.IP_TO_STRING(bytes)` | 2 | Bit shift macro |
| `NET.SAFE_IP_FROM_STRING(ip)` | 2 | TRY_CAST + parsing macro |
| `NET.IP_FROM_STRING(ip)` | 1 | Arithmetic macro |

### Macro Definitions

```sql
-- Register these at session initialization

-- NET.HOST: Extract hostname from URL
CREATE OR REPLACE MACRO net_host(url) AS
    regexp_extract(url, 'https?://([^/:]+)', 1);

-- NET.REG_DOMAIN: Extract registered domain from URL
CREATE OR REPLACE MACRO net_reg_domain(url) AS
    regexp_extract(url, 'https?://(?:[^/]*\.)?([^./]+\.[^./]+)(?:/|$)', 1);

-- NET.IP_FROM_STRING: Convert IPv4 string to integer
CREATE OR REPLACE MACRO net_ip_from_string(ip) AS (
    CAST(split_part(ip, '.', 1) AS BIGINT) * 16777216 +
    CAST(split_part(ip, '.', 2) AS BIGINT) * 65536 +
    CAST(split_part(ip, '.', 3) AS BIGINT) * 256 +
    CAST(split_part(ip, '.', 4) AS BIGINT)
);

-- NET.SAFE_IP_FROM_STRING: Same as above but returns NULL on error
CREATE OR REPLACE MACRO net_safe_ip_from_string(ip) AS (
    CASE
        WHEN ip ~ '^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$'
        THEN net_ip_from_string(ip)
        ELSE NULL
    END
);

-- NET.IP_TO_STRING: Convert integer to IPv4 string
CREATE OR REPLACE MACRO net_ip_to_string(ip_int) AS (
    CONCAT(
        CAST((ip_int >> 24) & 255 AS VARCHAR), '.',
        CAST((ip_int >> 16) & 255 AS VARCHAR), '.',
        CAST((ip_int >> 8) & 255 AS VARCHAR), '.',
        CAST(ip_int & 255 AS VARCHAR)
    )
);

-- NET.IP_TRUNC: Truncate IP to network prefix
CREATE OR REPLACE MACRO net_ip_trunc(ip_bytes, prefix_len) AS (
    net_ip_to_string(
        (net_ip_from_string(net_ip_to_string(ip_bytes)) >> (32 - prefix_len)) << (32 - prefix_len)
    )
);
```

### Transformer Mapping

```rust
// In functions.rs, add:
("NET.HOST", Rename("net_host")),
("NET.REG_DOMAIN", Rename("net_reg_domain")),
("NET.IP_FROM_STRING", Rename("net_ip_from_string")),
("NET.SAFE_IP_FROM_STRING", Rename("net_safe_ip_from_string")),
("NET.IP_TO_STRING", Rename("net_ip_to_string")),
("NET.IP_TRUNC", Rename("net_ip_trunc")),
```

---

## 6. SQL Syntax Features

### 6.1 QUALIFY (2 occurrences)

**BigQuery:**
```sql
SELECT * FROM orders
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at DESC) = 1
```

**DuckDB (rewrite as subquery):**
```sql
SELECT * EXCEPT(__qualify_rn) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at DESC) AS __qualify_rn
    FROM orders
) WHERE __qualify_rn = 1
```

**Implementation:** AST transformation in `transform_select()`

### 6.2 PIVOT (3 occurrences)

**BigQuery:**
```sql
SELECT * FROM t PIVOT(SUM(val) FOR category IN ('A', 'B', 'C'))
```

**DuckDB:**
```sql
PIVOT t ON category USING SUM(val)
```

**Implementation:** Syntax differs, needs AST transformation

### 6.3 MERGE (1 occurrence)

DuckDB supports MERGE with similar syntax - likely works as-is or needs minor adjustments.

### 6.4 EXTRACT Parts

| BigQuery | DuckDB | Notes |
|----------|--------|-------|
| `EXTRACT(DAYOFYEAR FROM d)` | `dayofyear(d)` | Or `EXTRACT(doy FROM d)` |
| `EXTRACT(DAYOFWEEK FROM d)` | `dayofweek(d)` | BQ: 1=Sun, DuckDB: 0=Sun |

---

## 7. Cannot Transpile ❌

### 7.1 JavaScript UDFs (4 queries)

These define functions using JavaScript:

```sql
CREATE TEMP FUNCTION pivotToJson(row_data ARRAY<STRUCT<...>>)
  RETURNS STRING
  LANGUAGE js AS """
    var result = {};
    row_data.forEach(function(val) {
      result[val.attribute] = val.value;
    });
    return JSON.stringify(result);
  """;
```

**Options:**
1. Skip these queries in mock mode
2. Manually rewrite as SQL/DuckDB macros
3. Use DuckDB's Python UDF support (if Python available)

### 7.2 _TABLE_SUFFIX (5 queries)

Wildcard table queries:

```sql
SELECT * FROM `project.dataset.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20240101' AND '20240131'
```

**Options:**
1. Skip these queries in mock mode
2. Pre-expand to UNION ALL if table list is known
3. Use `read_parquet('*.parquet', filename=true)` pattern

---

## 8. Implementation Plan

### Phase 1: High Priority Functions
1. Add `COUNTIF` transformation (custom AST rewrite)
2. Add JSON function renames
3. Add `CURRENT_DATETIME` rename
4. Add `DATE_TRUNC`, `DATETIME_TRUNC`, `TIMESTAMP_TRUNC` (swap args + quote)
5. Add `SPLIT` rename
6. Add `DATETIME_ADD`, `DATETIME_SUB`, `DATETIME_DIFF`
7. Add `DATE_DIFF` (same as TIMESTAMP_DIFF)

### Phase 2: Medium Priority Functions
1. Add `FORMAT_DATETIME`, `FORMAT_DATE`, `PARSE_DATETIME` (swap args)
2. Add `APPROX_QUANTILES` custom transformation
3. Add remaining renames (SHA256, ARRAY_CONCAT, GENERATE_ARRAY, etc.)
4. Add `TO_HEX`, `TO_BASE64` custom transformations

### Phase 3: Special Features
1. Implement NET.* macros and function mappings
2. Implement QUALIFY rewrite
3. Review and fix PIVOT if needed

### Phase 4: Testing
1. Run all 2,416 queries through transformer
2. Log any failures
3. Fix edge cases

---

## 9. Files to Modify

| File | Changes |
|------|---------|
| `src/sql/functions.rs` | Add new function mappings |
| `src/sql/transformer.rs` | Add custom transformations for COUNTIF, QUALIFY, etc. |
| `src/session/mod.rs` | Register NET.* macros on session init |
| `src/executor/duckdb.rs` | Ensure macros are loaded before query execution |

---

## 10. Function Mapping Reference

Complete list for `functions.rs`:

```rust
pub static FUNCTION_MAPPINGS: LazyLock<HashMap<&'static str, FunctionMapping>> = LazyLock::new(|| {
    let mut m = HashMap::new();

    // === ALREADY IMPLEMENTED ===
    m.insert("SAFE_DIVIDE", FunctionMapping { duckdb_name: "", transform: FunctionTransform::SafeDivide });
    m.insert("TIMESTAMP_DIFF", FunctionMapping { duckdb_name: "DATE_DIFF", transform: FunctionTransform::TimestampDiff });
    m.insert("TIMESTAMP_ADD", FunctionMapping { duckdb_name: "", transform: FunctionTransform::TimestampAdd });
    m.insert("TIMESTAMP_SUB", FunctionMapping { duckdb_name: "", transform: FunctionTransform::TimestampSub });
    m.insert("FORMAT_TIMESTAMP", FunctionMapping { duckdb_name: "strftime", transform: FunctionTransform::FormatTimestamp });
    m.insert("PARSE_TIMESTAMP", FunctionMapping { duckdb_name: "strptime", transform: FunctionTransform::ParseTimestamp });
    m.insert("SAFE_CAST", FunctionMapping { duckdb_name: "TRY_CAST", transform: FunctionTransform::SafeCast });
    m.insert("ANY_VALUE", FunctionMapping { duckdb_name: "ARBITRARY", transform: FunctionTransform::Rename });
    m.insert("ARRAY_LENGTH", FunctionMapping { duckdb_name: "LEN", transform: FunctionTransform::ArrayLength });
    m.insert("DATETIME", FunctionMapping { duckdb_name: "", transform: FunctionTransform::DatetimeCast });
    m.insert("DATE", FunctionMapping { duckdb_name: "", transform: FunctionTransform::DateCast });
    m.insert("IFNULL", FunctionMapping { duckdb_name: "COALESCE", transform: FunctionTransform::Rename });
    m.insert("GENERATE_UUID", FunctionMapping { duckdb_name: "uuid", transform: FunctionTransform::Rename });
    m.insert("BYTE_LENGTH", FunctionMapping { duckdb_name: "octet_length", transform: FunctionTransform::Rename });
    m.insert("CHAR_LENGTH", FunctionMapping { duckdb_name: "length", transform: FunctionTransform::Rename });
    m.insert("REGEXP_CONTAINS", FunctionMapping { duckdb_name: "regexp_matches", transform: FunctionTransform::Rename });
    m.insert("REGEXP_EXTRACT", FunctionMapping { duckdb_name: "regexp_extract", transform: FunctionTransform::Rename });
    m.insert("REGEXP_REPLACE", FunctionMapping { duckdb_name: "regexp_replace", transform: FunctionTransform::Rename });
    m.insert("STRUCT", FunctionMapping { duckdb_name: "struct_pack", transform: FunctionTransform::Struct });

    // === TO BE ADDED: High Priority ===
    m.insert("COUNTIF", FunctionMapping { duckdb_name: "", transform: FunctionTransform::CountIf });
    m.insert("JSON_EXTRACT_SCALAR", FunctionMapping { duckdb_name: "json_extract_string", transform: FunctionTransform::Rename });
    m.insert("JSON_EXTRACT", FunctionMapping { duckdb_name: "json_extract", transform: FunctionTransform::Rename });
    m.insert("JSON_EXTRACT_ARRAY", FunctionMapping { duckdb_name: "json_extract", transform: FunctionTransform::Rename });
    m.insert("TO_JSON_STRING", FunctionMapping { duckdb_name: "", transform: FunctionTransform::ToJsonString });
    m.insert("CURRENT_DATETIME", FunctionMapping { duckdb_name: "CURRENT_TIMESTAMP", transform: FunctionTransform::Rename });
    m.insert("DATE_TRUNC", FunctionMapping { duckdb_name: "date_trunc", transform: FunctionTransform::TruncSwapArgs });
    m.insert("DATETIME_TRUNC", FunctionMapping { duckdb_name: "date_trunc", transform: FunctionTransform::TruncSwapArgs });
    m.insert("TIMESTAMP_TRUNC", FunctionMapping { duckdb_name: "date_trunc", transform: FunctionTransform::TruncSwapArgs });
    m.insert("SPLIT", FunctionMapping { duckdb_name: "string_split", transform: FunctionTransform::Rename });
    m.insert("DATETIME_ADD", FunctionMapping { duckdb_name: "", transform: FunctionTransform::TimestampAdd });
    m.insert("DATETIME_SUB", FunctionMapping { duckdb_name: "", transform: FunctionTransform::TimestampSub });
    m.insert("DATETIME_DIFF", FunctionMapping { duckdb_name: "DATE_DIFF", transform: FunctionTransform::TimestampDiff });
    m.insert("DATE_DIFF", FunctionMapping { duckdb_name: "DATE_DIFF", transform: FunctionTransform::TimestampDiff });
    m.insert("DATE_ADD", FunctionMapping { duckdb_name: "", transform: FunctionTransform::TimestampAdd });
    m.insert("DATE_SUB", FunctionMapping { duckdb_name: "", transform: FunctionTransform::TimestampSub });

    // === TO BE ADDED: Medium Priority ===
    m.insert("FORMAT_DATETIME", FunctionMapping { duckdb_name: "strftime", transform: FunctionTransform::FormatTimestamp });
    m.insert("FORMAT_DATE", FunctionMapping { duckdb_name: "strftime", transform: FunctionTransform::FormatTimestamp });
    m.insert("PARSE_DATETIME", FunctionMapping { duckdb_name: "strptime", transform: FunctionTransform::ParseTimestamp });
    m.insert("APPROX_QUANTILES", FunctionMapping { duckdb_name: "", transform: FunctionTransform::ApproxQuantiles });
    m.insert("REGEXP_EXTRACT_ALL", FunctionMapping { duckdb_name: "regexp_extract_all", transform: FunctionTransform::Rename });
    m.insert("SHA256", FunctionMapping { duckdb_name: "sha256", transform: FunctionTransform::Rename });
    m.insert("SHA1", FunctionMapping { duckdb_name: "md5", transform: FunctionTransform::Rename }); // approximation
    m.insert("TO_HEX", FunctionMapping { duckdb_name: "", transform: FunctionTransform::ToHex });
    m.insert("ARRAY_CONCAT", FunctionMapping { duckdb_name: "list_concat", transform: FunctionTransform::Rename });
    m.insert("GENERATE_ARRAY", FunctionMapping { duckdb_name: "generate_series", transform: FunctionTransform::Rename });
    m.insert("GENERATE_DATE_ARRAY", FunctionMapping { duckdb_name: "generate_series", transform: FunctionTransform::Rename });

    // === TO BE ADDED: Low Priority ===
    m.insert("UNIX_MILLIS", FunctionMapping { duckdb_name: "epoch_ms", transform: FunctionTransform::Rename });
    m.insert("UNIX_SECONDS", FunctionMapping { duckdb_name: "epoch", transform: FunctionTransform::Rename });
    m.insert("TO_BASE64", FunctionMapping { duckdb_name: "", transform: FunctionTransform::ToBase64 });
    m.insert("FARM_FINGERPRINT", FunctionMapping { duckdb_name: "hash", transform: FunctionTransform::Rename });
    m.insert("ARRAY_CONCAT_AGG", FunctionMapping { duckdb_name: "", transform: FunctionTransform::ArrayConcatAgg });
    m.insert("LOGICAL_OR", FunctionMapping { duckdb_name: "bool_or", transform: FunctionTransform::Rename });
    m.insert("TIMESTAMP_MILLIS", FunctionMapping { duckdb_name: "", transform: FunctionTransform::TimestampMillis });
    m.insert("SAFE_MULTIPLY", FunctionMapping { duckdb_name: "", transform: FunctionTransform::SafeMultiply });

    // === TO BE ADDED: NET.* Functions ===
    m.insert("NET.HOST", FunctionMapping { duckdb_name: "net_host", transform: FunctionTransform::Rename });
    m.insert("NET.REG_DOMAIN", FunctionMapping { duckdb_name: "net_reg_domain", transform: FunctionTransform::Rename });
    m.insert("NET.IP_FROM_STRING", FunctionMapping { duckdb_name: "net_ip_from_string", transform: FunctionTransform::Rename });
    m.insert("NET.SAFE_IP_FROM_STRING", FunctionMapping { duckdb_name: "net_safe_ip_from_string", transform: FunctionTransform::Rename });
    m.insert("NET.IP_TO_STRING", FunctionMapping { duckdb_name: "net_ip_to_string", transform: FunctionTransform::Rename });
    m.insert("NET.IP_TRUNC", FunctionMapping { duckdb_name: "net_ip_trunc", transform: FunctionTransform::Rename });

    m
});
```

---

## 11. New Transform Types Needed

Add to `FunctionTransform` enum in `functions.rs`:

```rust
pub enum FunctionTransform {
    // Existing
    Rename,
    SafeDivide,
    TimestampDiff,
    TimestampAdd,
    TimestampSub,
    FormatTimestamp,
    ParseTimestamp,
    SafeCast,
    ArrayLength,
    DatetimeCast,
    DateCast,
    Struct,

    // New
    CountIf,           // COUNTIF(x) -> COUNT(*) FILTER (WHERE x)
    ToJsonString,      // TO_JSON_STRING(x) -> to_json(x)::VARCHAR
    TruncSwapArgs,     // DATE_TRUNC(d, MONTH) -> date_trunc('month', d)
    ApproxQuantiles,   // APPROX_QUANTILES(x, 100)[OFFSET(50)] -> approx_quantile(x, 0.5)
    ToHex,             // TO_HEX(x) -> encode(x, 'hex')
    ToBase64,          // TO_BASE64(x) -> encode(x, 'base64')
    ArrayConcatAgg,    // ARRAY_CONCAT_AGG(x) -> flatten(list_agg(x))
    TimestampMillis,   // TIMESTAMP_MILLIS(ms) -> to_timestamp(ms / 1000.0)
    SafeMultiply,      // SAFE_MULTIPLY(a, b) -> TRY_CAST(a * b AS DOUBLE)
}
```
