# `bitfilters` Extension for DuckDB Developed by [Query.Farm](https://query.farm)

A high-performance [DuckDB](https://duckdb.org) extension providing probabilistic data structures for fast set membership testing and approximate duplicate detection. This extension implements state-of-the-art filter algorithms including [Quotient filters](https://en.wikipedia.org/wiki/Quotient_filter), [XOR filters](https://arxiv.org/abs/1912.08258), [Binary Fuse filters](https://arxiv.org/abs/2201.01174), and soon [Bloom filters](https://en.wikipedia.org/wiki/Bloom_filter).

`bitfilters` provides space-efficient [probabilistic data structures](https://en.wikipedia.org/wiki/Probabilistic_data_structure) that can answer "Is element X in set S?" with:

- **No false negatives**: If the filter says an element is not present, it's definitely not there

- **Possible false positives**: If the filter says an element is present, it might be there (with configurable probability)

You may find it useful to use this module in combination with the [`hashfuncs` extension](https://query.farm/duckdb_extension_hashfuncs.html) to produce the hash outputs that these filters require.  Most filters require a `UBIGINT` or unsigned 64-bit integer value as their input type.  You can use the functions provided by `hashfuncs` or the DuckDB `hash()` function to produce those values from most DuckDB data types.

## Installation

**`bitfilters` is a [DuckDB Community Extension](https://github.com/duckdb/community-extensions).**

You can install and use this extension with the following SQL commands:

```sql
INSTALL bitfilters FROM community;
LOAD bitfilters;
```

For more information about DuckDB extensions, see the [official documentation](https://duckdb.org/docs/extensions/overview).

## What are bitfilters?

Bitfilters are [probabilistic data structures](https://en.wikipedia.org/wiki/Probabilistic_data_structure) that provide fast, memory-efficient approximate set membership testing. They are designed to answer the question "Is element X in set S?" with:

- **Space efficiency**: Use significantly less memory than storing the actual set
- **Speed**: Extremely fast lookups (typically [O(1)](https://en.wikipedia.org/wiki/Big_O_notation) or O(k) where k is small)
- **No false negatives**: If a filter says "NO", the element is definitely not in the set
- **Possible false positives**: If a filter says "YES", the element might be in the set

### Common Use Cases

- **Pre-filtering expensive operations**: Avoid costly disk I/O or network calls for non-existent data
- **Duplicate detection**: Quickly identify potential duplicates in large datasets
- **Cache optimization**: Determine if data might be in cache before expensive lookups
- **Data skipping**: Skip irrelevant data partitions in analytical queries
- **Set operations**: Approximate set intersections and unions on massive datasets
- **Database join optimization**: Pre-filter join candidates to reduce computation

### Performance Benefits

```sql
-- Without bitfilters: Expensive operation on every row
SELECT expensive_function(data)
FROM large_table
WHERE complex_condition(data);

-- With bitfilters: Pre-filter to reduce expensive operations by 90%+
SELECT expensive_function(lt.data)
FROM large_table lt
JOIN precomputed_filters pf ON lt.partition = pf.partition
WHERE filter_contains(pf.filter, lt.key)  -- Fast filter check
  AND complex_condition(lt.data);         -- Expensive check only when needed
```

## Available Filters

### 1. Quotient Filters

Space-efficient filters that support deletion and resizing operations. Learn more about [Quotient Filters](https://en.wikipedia.org/wiki/Quotient_filter).

#### Functions:

- `quotient_filter(q, r, hash_value)` - Create filter from hash values
- `quotient_filter_contains(filter, hash_value)` - Test membership

#### Characteristics:

- **Use case**: Dynamic datasets, applications requiring deletion
- **Pros**: Supports deletion, better cache locality, resizable
- **Cons**: More complex implementation, slightly slower than Bloom filters
- **Memory**: Similar to Bloom filters but with better cache performance

```sql
-- Create a quotient filter with q=16 and r=4
CREATE TABLE quotient_filters AS (
    SELECT id % 2 AS remainder,
           quotient_filter(16, 4, hash(id)) AS filter
    FROM series_data
    GROUP BY id % 2
);

-- Test membership
SELECT quotient_filter_contains(filter, hash(12345)) AS might_exist
FROM quotient_filters WHERE remainder = 1;
```

### 2. XOR Filters

Modern high-performance filters with better space efficiency than Bloom filters. Read the [XOR Filters paper](https://arxiv.org/abs/1912.08258) for technical details.

#### Functions:

- `xor16_filter(hash_value)` - Create 16-bit XOR filter from hash values
- `xor8_filter(hash_value)` - Create 8-bit XOR filter from hash values
- `xor16_filter_contains(filter, hash_value)` - Test membership in 16-bit filter
- `xor8_filter_contains(filter, hash_value)` - Test membership in 8-bit filter

#### Characteristics:

- **Use case**: Read-heavy workloads, static datasets
- **Pros**: Better space efficiency (~20% less memory), faster queries
- **Cons**: Static size, more complex construction, no incremental updates
- **Memory**: ~1.23 bits per element for 1% false positive rate

```sql
-- Create XOR filters (both 8-bit and 16-bit versions)
CREATE TABLE xor_filters AS (
    SELECT id % 2 AS remainder,
           xor16_filter(hash(id)) AS xor16_filter,
           xor8_filter(hash(id)) AS xor8_filter
    FROM series_data
    GROUP BY id % 2
);

-- Test membership
SELECT
    xor16_filter_contains(xor16_filter, hash(12345)) AS in_xor16,
    xor8_filter_contains(xor8_filter, hash(12345)) AS in_xor8
FROM xor_filters WHERE remainder = 1;
```

### 3. Binary Fuse Filters

Latest generation filters with optimal space usage and excellent performance. See the [Binary Fuse Filters paper](https://arxiv.org/abs/2201.01174) for implementation details.

#### Functions:

- `binary_fuse16_filter(hash_value)` - Create 16-bit Binary Fuse filter from hash values
- `binary_fuse8_filter(hash_value)` - Create 8-bit Binary Fuse filter from hash values
- `binary_fuse16_filter_contains(filter, hash_value)` - Test membership in 16-bit filter
- `binary_fuse8_filter_contains(filter, hash_value)` - Test membership in 8-bit filter

#### Characteristics:

- **Use case**: Applications requiring minimal memory footprint
- **Pros**: Best space efficiency, fast construction and queries
- **Cons**: Static size, newer algorithm with less production experience
- **Memory**: Significantly more space-efficient than other filters

```sql
-- Create Binary Fuse filters (both 8-bit and 16-bit versions)
CREATE TABLE binary_fuse_filters AS (
    SELECT id % 2 AS remainder,
           binary_fuse16_filter(hash(id)) AS binary_fuse16_filter,
           binary_fuse8_filter(hash(id)) AS binary_fuse8_filter
    FROM series_data
    GROUP BY id % 2
);

-- Test membership
SELECT
    binary_fuse16_filter_contains(binary_fuse16_filter, hash(12345)) AS in_fuse16,
    binary_fuse8_filter_contains(binary_fuse8_filter, hash(12345)) AS in_fuse8
FROM binary_fuse_filters WHERE remainder = 1;
```

## Usage Examples

### Basic Filter Operations

```sql
-- Create test data
CREATE TABLE series_data AS (
    SELECT * AS id FROM generate_series(1, 100000) AS id
);

-- Create quotient filters with q=16 and r=4
CREATE TABLE quotient_filters AS (
    SELECT id % 2 AS remainder,
           quotient_filter(16, 4, hash(id)) AS filter
    FROM series_data
    GROUP BY id % 2
);

-- Test membership (should find all matching elements)
SELECT remainder,
       COUNT(CASE WHEN quotient_filter_contains(filter, hash(id)) THEN 1 ELSE NULL END) AS matches
FROM series_data, quotient_filters
WHERE series_data.id % 2 = quotient_filters.remainder
GROUP BY remainder;
┌───────────┬─────────┐
│ remainder │ matches │
│   int64   │  int64  │
├───────────┼─────────┤
│         0 │   50000 │
│         1 │   50000 │
└───────────┴─────────┘

-- Check false positives (elements not in the filter that test positive)
SELECT remainder,
       COUNT(CASE WHEN quotient_filter_contains(filter, hash(id)) THEN 1 ELSE NULL END) AS false_positives
FROM series_data, quotient_filters
WHERE series_data.id % 2 != quotient_filters.remainder
GROUP BY remainder;
┌───────────┬─────────────────┐
│ remainder │ false_positives │
│   int64   │      int64      │
├───────────┼─────────────────┤
│         0 │            2264 │
│         1 │            2273 │
└───────────┴─────────────────┘
```

### XOR Filter Examples

```sql
-- Create XOR filters (both 8-bit and 16-bit)
CREATE TABLE xor_filters AS (
    SELECT id % 2 AS remainder,
           xor16_filter(hash(id)) AS xor16_filter,
           xor8_filter(hash(id)) AS xor8_filter
    FROM series_data
    GROUP BY id % 2
);

-- Verify all elements are found (no false negatives)
SELECT remainder,
       COUNT(CASE WHEN xor16_filter_contains(xor16_filter, hash(id)) THEN 1 ELSE NULL END) AS xor16_matches,
       COUNT(CASE WHEN xor8_filter_contains(xor8_filter, hash(id)) THEN 1 ELSE NULL END) AS xor8_matches
FROM series_data, xor_filters
WHERE series_data.id % 2 = xor_filters.remainder
GROUP BY remainder;
┌───────────┬───────────────┬──────────────┐
│ remainder │ xor16_matches │ xor8_matches │
│   int64   │     int64     │    int64     │
├───────────┼───────────────┼──────────────┤
│         0 │         50000 │        50000 │
│         1 │         50000 │        50000 │
└───────────┴───────────────┴──────────────┘

-- Compare filter performance
SELECT
    'XOR16' AS filter_type,
    octet_length(xor16_filter) AS size_bytes
FROM xor_filters WHERE remainder = 0
UNION ALL
SELECT
    'XOR8' AS filter_type,
    octet_length(xor8_filter) AS size_bytes
FROM xor_filters WHERE remainder = 0;
┌─────────────┬────────────┐
│ filter_type │ size_bytes │
│   varchar   │   int64    │
├─────────────┼────────────┤
│ XOR16       │     123076 │
│ XOR8        │      61546 │
└─────────────┴────────────┘
```

### Binary Fuse Filter Examples

```sql
-- Create Binary Fuse filters
CREATE TABLE binary_fuse_filters AS (
    SELECT id % 2 AS remainder,
           binary_fuse16_filter(hash(id)) AS binary_fuse16_filter,
           binary_fuse8_filter(hash(id)) AS binary_fuse8_filter
    FROM series_data
    GROUP BY id % 2
);

-- Test all elements are found
SELECT remainder,
       COUNT(CASE WHEN binary_fuse16_filter_contains(binary_fuse16_filter, hash(id)) THEN 1 ELSE NULL END) AS fuse16_matches,
       COUNT(CASE WHEN binary_fuse8_filter_contains(binary_fuse8_filter, hash(id)) THEN 1 ELSE NULL END) AS fuse8_matches
FROM series_data, binary_fuse_filters
WHERE series_data.id % 2 = binary_fuse_filters.remainder
GROUP BY remainder;
┌───────────┬────────────────┬───────────────┐
│ remainder │ fuse16_matches │ fuse8_matches │
│   int64   │     int64      │     int64     │
├───────────┼────────────────┼───────────────┤
│         0 │          50000 │         50000 │
│         1 │          50000 │         50000 │
└───────────┴────────────────┴───────────────┘

-- Check false positive rates
SELECT remainder,
       COUNT(CASE WHEN binary_fuse16_filter_contains(binary_fuse16_filter, hash(id)) THEN 1 ELSE NULL END) AS fuse16_false_positives,
       COUNT(CASE WHEN binary_fuse8_filter_contains(binary_fuse8_filter, hash(id)) THEN 1 ELSE NULL END) AS fuse8_false_positives
FROM series_data, binary_fuse_filters
WHERE series_data.id % 2 != binary_fuse_filters.remainder
GROUP BY remainder;
┌───────────┬────────────────────────┬───────────────────────┐
│ remainder │ fuse16_false_positives │ fuse8_false_positives │
│   int64   │         int64          │         int64         │
├───────────┼────────────────────────┼───────────────────────┤
│         0 │                      1 │                   171 │
│         1 │                      1 │                   199 │
└───────────┴────────────────────────┴───────────────────────┘
```

### Practical Use Case: User Activity Tracking

```sql
-- Track active users by day using quotient filters
CREATE TABLE user_activity AS (
    SELECT
        (CURRENT_DATE - (random() * 30)::INTEGER) AS activity_date,
        (random() * 1000000)::INTEGER AS user_id
    FROM generate_series(1, 5000000)
);

-- Create daily quotient filters for active users
CREATE TABLE daily_user_filters AS (
    SELECT
        activity_date,
        quotient_filter(16, 4, hash(user_id)) AS user_filter,
        COUNT(DISTINCT user_id) AS actual_unique_users
    FROM user_activity
    GROUP BY activity_date
);

-- Check if specific users were active on specific dates
SELECT
    activity_date,
    quotient_filter_contains(user_filter, hash(12345)) AS user_12345_active,
    quotient_filter_contains(user_filter, hash(67890)) AS user_67890_active,
    actual_unique_users
FROM daily_user_filters
ORDER BY activity_date;

-- Find days when specific users might have been active
WITH target_users AS (
    SELECT unnest([12345, 67890, 11111, 99999]) AS user_id
)
SELECT
    tu.user_id,
    duf.activity_date,
    quotient_filter_contains(duf.user_filter, hash(tu.user_id)) AS possibly_active,
    EXISTS(
        SELECT 1 FROM user_activity ua
        WHERE ua.user_id = tu.user_id
        AND ua.activity_date = duf.activity_date
    ) AS actually_active
FROM target_users tu
CROSS JOIN daily_user_filters duf
WHERE quotient_filter_contains(duf.user_filter, hash(tu.user_id))
ORDER BY tu.user_id, duf.activity_date;
```

### Filter Comparison Example

```sql
-- Compare all filter types side by side
WITH sample_data AS (
    SELECT hash(id) AS hash_value
    FROM generate_series(1, 1000000) AS id
),
all_filters AS (
    SELECT
        quotient_filter(20, 4, hash_value) AS qf,
        xor16_filter(hash_value) AS xor16,
        xor8_filter(hash_value) AS xor8,
        binary_fuse16_filter(hash_value) AS bf16,
        binary_fuse8_filter(hash_value) AS bf8
    FROM sample_data
)
SELECT
    'Quotient Filter' AS filter_type,
    octet_length(qf) AS size_bytes,
    octet_length(qf) / 1000000.0 AS bytes_per_element
FROM all_filters
UNION ALL
SELECT
    'XOR16 Filter',
    octet_length(xor16),
    octet_length(xor16) / 1000000.0
FROM all_filters
UNION ALL
SELECT
    'XOR8 Filter',
    octet_length(xor8),
    octet_length(xor8) / 1000000.0
FROM all_filters
UNION ALL
SELECT
    'Binary Fuse16 Filter',
    octet_length(bf16),
    octet_length(bf16) / 1000000.0
FROM all_filters
UNION ALL
SELECT
    'Binary Fuse8 Filter',
    octet_length(bf8),
    octet_length(bf8) / 1000000.0
FROM all_filters;
┌──────────────────────┬────────────┬───────────────────┐
│     filter_type      │ size_bytes │ bytes_per_element │
│       varchar        │   int64    │      double       │
├──────────────────────┼────────────┼───────────────────┤
│ Quotient Filter      │     917544 │          0.917544 │
│ XOR16 Filter         │    2460076 │          2.460076 │
│ XOR8 Filter          │    1230046 │          1.230046 │
│ Binary Fuse16 Filter │    2261024 │          2.261024 │
│ Binary Fuse8 Filter  │    1130524 │          1.130524 │
└──────────────────────┴────────────┴───────────────────┘
```


## Best Practices

### ✅ Do's

- Use a hash function to create consistent hash values for filter operations
- Store filters in materialized views for reuse across queries
- Combine filters with exact checks for final results
- Monitor actual false positive rates in production
- Choose appropriate bit-width (8 vs 16) based on memory/accuracy tradeoffs

### ❌ Don'ts

- Don't rely on filters for exact results without confirmation
- Don't mix hash values from different sources in the same filter
- Don't rebuild filters frequently for dynamic data
- Don't use filters for very small datasets (overhead not worth it)
- Don't forget that quotient filter parameters (q, r) affect capacity and accuracy

```sql
-- Good: Use filter to pre-screen, then exact check
SELECT expensive_operation(data)
FROM large_table lt
JOIN precomputed_filter pf ON lt.partition = pf.partition
WHERE quotient_filter_contains(pf.filter, hash(lt.key))  -- Fast pre-filter
  AND exact_expensive_condition(lt.data);                -- Exact check

-- Bad: Using filter as final arbiter
SELECT data
FROM large_table lt
JOIN precomputed_filter pf ON lt.partition = pf.partition
WHERE quotient_filter_contains(pf.filter, hash(lt.key)); -- May have false positives!
```

## API Reference

### Quotient Filter Functions

#### `quotient_filter(q, r, hash_value)`
Creates a quotient filter with 2^q slots and r remainder bits.

##### Parameters:

- `q` (INTEGER): Log2 of the number of slots (capacity = 2^q)
- `r` (INTEGER): Number of remainder bits (affects accuracy)
- `hash_value` (UBIGINT): Hash values to insert into the filter

##### Returns:

BLOB containing the serialized quotient filter

##### Example

```sql
-- Create filter with 2^16 = 65536 slots and 4 remainder bits
SELECT quotient_filter(16, 4, hash(user_id)) FROM users;
```
---

#### `quotient_filter_contains(filter, hash_value)`
Tests if a quotient filter may contain a hash value.

##### Parameters:
- `filter` (BLOB): Serialized quotient filter
- `hash_value` (UBIGINT): Hash value to test

##### Returns:

`BOOLEAN`

- `true`: Hash value might be in the set (possible false positive)
- `false`: Hash value is definitely not in the set (no false negatives)

### XOR Filter Functions

#### `xor16_filter(hash_value)` / `xor8_filter(hash_value)`

Creates XOR filters with 16-bit or 8-bit fingerprints.

##### Parameters:

- `hash_value` (BIGINT): Hash values to insert into the filter

##### Returns:

`BLOB` containing the serialized XOR filter

##### Example:

```sql
SELECT xor16_filter(hash(product_id)) FROM products;
SELECT xor8_filter(hash(product_id)) FROM products;  -- Smaller but higher FPR
```

#### `xor16_filter_contains(filter, hash_value)` / `xor8_filter_contains(filter, hash_value)`

Tests if an XOR filter may contain a hash value.

##### Parameters:

- `filter` (`BLOB`): Serialized XOR filter
- `hash_value` (`UBIGINT`): Hash value to test

##### Returns:

`BOOLEAN` (same semantics as quotient filters)

### Binary Fuse Filter Functions

#### `binary_fuse16_filter(hash_value)` / `binary_fuse8_filter(hash_value)`

Creates Binary Fuse filters with 16-bit or 8-bit fingerprints.

##### Parameters:

- `hash_value` (`UBIGINT`): Hash values to insert into the filter

##### Returns:

`BLOB` containing the serialized Binary Fuse filter

##### Example:

```sql
SELECT binary_fuse16_filter(hash(transaction_id)) FROM transactions;
SELECT binary_fuse8_filter(hash(transaction_id)) FROM transactions;
```

#### `binary_fuse16_filter_contains(filter, hash_value)` / `binary_fuse8_filter_contains(filter, hash_value)`

Tests if a Binary Fuse filter may contain a hash value.

##### Parameters:

- `filter` (`BLOB`): Serialized Binary Fuse filter
- `hash_value` (`BIGINT`): Hash value to test

##### Returns:

`BOOLEAN` (same semantics as other filters)

### Filter Characteristics Summary

| Filter Type | Create Function | Contains Function | Bits per Element | False Positive Rate | Notes |
|-------------|----------------|-------------------|------------------|-------------------|-------|
| Quotient | `quotient_filter(q,r,hash)` | `quotient_filter_contains(filter,hash)` | Variable | Depends on q,r | Supports deletion |
| XOR16 | `xor16_filter(hash)` | `xor16_filter_contains(filter,hash)` | ~9-10 bits | ~0.4% | High performance |
| XOR8 | `xor8_filter(hash)` | `xor8_filter_contains(filter,hash)` | ~9-10 bits | ~0.4% | Smaller size |
| Binary Fuse16 | `binary_fuse16_filter(hash)` | `binary_fuse16_filter_contains(filter,hash)` | ~9-10 bits | ~0.4% | Best space efficiency |
| Binary Fuse8 | `binary_fuse8_filter(hash)` | `binary_fuse8_filter_contains(filter,hash)` | ~9-10 bits | ~1.5% | Smallest size |

## Integration with Other Extensions

### Using with `hashfuncs` Extension

The [`hashfuncs` extension](https://query.farm/duckdb_extension_hashfuncs.html) provides additional hash functions that can improve filter performance and distribution.

```sql
-- Load both extensions
LOAD hashfuncs;
LOAD bitfilters;

-- Use specialized hash functions for better distribution
SELECT quotient_filter(16, 4, xxh64(complex_key || salt))
FROM my_table;

```

### Hash Function Recommendations

For optimal performance, consider these hash functions from the [`hashfuncs` extension](https://query.farm/duckdb_extension_hashfuncs.html):

## Limitations

1. **Hash-based Input**: All filters require hash values as input
2. **Static Size**: XOR and Binary Fuse filters cannot be resized after creation
3. **No Direct Deletion**: Only quotient filters support element removal
4. **False Positives**: All filters may return false positives but never false negatives
5. **Type Consistency**: Hash values must be consistent between creation and testing
6. **Memory Usage**: Larger filters provide better accuracy but use more memory

## Performance Characteristics

For detailed performance analysis, see the respective research papers: [Quotient Filters](https://dl.acm.org/doi/10.1145/2213977.2214006), [XOR Filters](https://arxiv.org/abs/1912.08258), and [Binary Fuse Filters](https://arxiv.org/abs/2201.01174).

| Operation | Quotient Filter | XOR Filter | Binary Fuse Filter |
|-----------|----------------|------------|-------------------|
| **Construction** | [O(n)](https://en.wikipedia.org/wiki/Big_O_notation) | O(n) | O(n) |
| **Query** | [O(1)](https://en.wikipedia.org/wiki/Big_O_notation) average | O(1) | O(1) |
| **Space** | Variable | ~9.84 bits/element | ~9.1 bits/element |
| **False Positive Rate** | Configurable | ~0.39% | 8-bit: ~1.56%, 16-bit: ~0.39% |
| **Supports Deletion** | ✅ | ❌ | ❌ |
| **Resizable** | ✅ | ❌ | ❌ |

## Contributing

This extension is developed and maintained by **[Query.Farm](https://query.farm)**.

For bug reports, feature requests, or contributions:
- Visit our [GitHub repository](https://github.com/Query-farm/bitfilters)
- Submit issues with reproducible examples
- Follow our coding standards for pull requests
- Check the [DuckDB extension development guide](https://duckdb.org/docs/extensions/overview) for technical details

## License

[MIT Licensed](https://github.com/Query-farm/bitfilters/blob/main/LICENSE)

## Related Resources

- **Academic Papers:**
  - [Quotient Filters](https://dl.acm.org/doi/10.1145/2213977.2214006) - Original quotient filter paper
  - [XOR Filters](https://arxiv.org/abs/1912.08258) - XOR filter research
  - [Binary Fuse Filters](https://arxiv.org/abs/2201.01174) - Latest filter technology


- **Compatible Extensions:**
  - [`hashfuncs`](https://query.farm/duckdb_extension_hashfuncs.html) - Additional hash functions

---

**[Query.Farm](https://query.farm)** - Advanced data processing solutions for modern analytics workloads.
