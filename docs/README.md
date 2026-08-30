<p align="center">
  <a href="https://query.farm">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://query.farm/media-kit/logo/wordmark-dark.svg">
      <img alt="Query.Farm" src="https://query.farm/media-kit/logo/wordmark-light.svg" height="64">
    </picture>
  </a>
</p>

# `bitfilters` Extension for DuckDB Developed by [Query.Farm](https://query.farm)

[![DuckDB](https://img.shields.io/badge/DuckDB-community_extension-fdf1e0?logo=duckdb&logoColor=fff000)](https://duckdb.org/community_extensions/extensions/bitfilters.html)
[![v1.5 build](https://github.com/Query-farm/bitfilters/actions/workflows/MainDistributionPipeline.yml/badge.svg?branch=v1.5)](https://github.com/Query-farm/bitfilters/actions/workflows/MainDistributionPipeline.yml?query=branch%3Av1.5)

A high-performance [DuckDB](https://duckdb.org) extension providing probabilistic data structures for fast set membership testing and approximate duplicate detection. This extension implements state-of-the-art filter algorithms including [Quotient filters](https://en.wikipedia.org/wiki/Quotient_filter), [XOR filters](https://arxiv.org/abs/1912.08258), [Binary Fuse filters](https://arxiv.org/abs/2201.01174), and soon [Bloom filters](https://en.wikipedia.org/wiki/Bloom_filter).

## Documentation

Full documentation, including installation, usage, the function reference, and cookbook examples, is available at:

**[https://query.farm/products/extensions/bitfilters](https://query.farm/products/extensions/bitfilters)**

## Installation

```sql
INSTALL bitfilters FROM community;
LOAD bitfilters;
```
