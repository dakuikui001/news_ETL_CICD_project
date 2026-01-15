## Table Structures

This document describes the logical schema of all main tables used in the news ELT pipeline, including column names, data types, and comments.

Tables are implemented in ClickHouse and follow the Medallion architecture: **Bronze → Silver → Gold**, plus a **data quality quarantine** table.

---

## 1. Bronze Layer – `news.straitstimes_news_bz`

Raw ingested data from CSV files, with minimal processing. Created in `include/pyspark/setup.py`.

**Engine**: `MergeTree`  
**Order by**: `(url)`

| Column        | Type              | Description                                                                 |
|--------------|-------------------|-----------------------------------------------------------------------------|
| `title`      | `Nullable(String)`| Article title as scraped from the website.                                 |
| `publish_date` | `Nullable(String)` | Original published date/time as string (e.g. `"Jan 05, 2026, 08:30 PM"`). |
| `update_date`  | `Nullable(String)` | Original updated date/time as string, if available.                        |
| `img_url`    | `Nullable(String)`| URL of the main image associated with the article.                         |
| `caption_text` | `Nullable(String)` | Caption or short description for the image/article.                       |
| `tags_list`  | `Nullable(String)`| Raw list of tags as a string (e.g. `"['Politics', 'Asia']"`).              |
| `full_article` | `Nullable(String)` | Full article body text (HTML stripped by scraper).                        |
| `url`        | `String`          | Canonical article URL, used as primary business key.                       |
| `load_time`  | `DateTime`        | Ingestion timestamp when the record was loaded into Bronze.                |

---

## 2. Silver Layer – `news.straitstimes_news_sl`

Cleaned and standardized data with proper types and incremental updates. Created in `include/pyspark/setup.py` and populated by `include/pyspark/silver.py`.

**Engine**: `ReplacingMergeTree(load_time)`  
**Order by**: `(url)`

| Column        | Type               | Description                                                                                          |
|--------------|--------------------|------------------------------------------------------------------------------------------------------|
| `title`      | `Nullable(String)` | Cleaned article title.                                                                              |
| `publish_date` | `Nullable(DateTime)` | Parsed publish datetime from Bronze `publish_date` string.                                       |
| `update_date`  | `Nullable(DateTime)` | Parsed update datetime from Bronze `update_date` string.                                         |
| `img_url`    | `Nullable(String)` | Image URL carried over from Bronze.                                                                  |
| `caption_text` | `Nullable(String)` | Cleaned caption text.                                                                              |
| `tags_list`  | `Nullable(String)` | Standardized tag list string; still stored as a single string at Silver layer.                      |
| `full_article` | `Nullable(String)` | Cleaned article body, after basic null/duplicate handling.                                         |
| `url`        | `String`           | Article URL (business key), same as in Bronze.                                                       |
| `load_time`  | `DateTime`         | Original ingestion time from Bronze, preserved for traceability.                                     |
| `update_time`| `DateTime`         | Processing timestamp when this row was last updated in Silver.                                      |

**Notes:**
- Silver uses **incremental upserts** based on `load_time` watermark and deduplicates using `ReplacingMergeTree`.
- `DBSetupManager.preprocessing` handles duplicates, all-null rows, and null value filling before writing.

---

## 3. Gold Layer – Fact Table `news.fact_news_gl`

Analytics-ready fact table for news articles, with date and time keys for reporting. Created in `include/pyspark/setup.py` and populated by `include/pyspark/gold.py`.

**Engine**: `ReplacingMergeTree(update_time)`  
**Order by**: `(url)`

| Column           | Type               | Description                                                                                   |
|-----------------|--------------------|-----------------------------------------------------------------------------------------------|
| `url`           | `String`           | Article URL (fact table primary business key).                                                |
| `title`         | `Nullable(String)` | Article title.                                                                                |
| `publish_date`  | `Nullable(Date)`   | Publication date (date only), extracted from Silver `publish_date`.                           |
| `update_date`   | `Nullable(Date)`   | Last update date (date only), extracted from Silver `update_date`.                            |
| `img_url`       | `Nullable(String)` | Main article image URL.                                                                       |
| `caption_text`  | `Nullable(String)` | Short caption/summary text.                                                                   |
| `full_article`  | `Nullable(String)` | Full article text (for deep analysis / NLP use cases).                                       |
| `publish_timekey` | `Nullable(String)` | Time-of-day key (e.g. `"08:30"`), derived from Silver `publish_date` string.                 |
| `update_timekey`  | `Nullable(String)` | Time-of-day key for last update, derived from Silver `update_date` string.                   |
| `update_time`   | `DateTime`         | Gold layer processing timestamp; used as versioning column in `ReplacingMergeTree`.          |

**Notes:**
- Gold fact table is designed for BI tools (e.g. Power BI), making it easy to slice by date and time of day.
- Incremental loads are driven by the maximum `update_time` already present in `fact_news_gl`.

---

## 4. Gold Layer – Dimension Table `news.dim_tags_gl`

Normalized tag dimension table with one row per `(url, tag)` combination. Created in `include/pyspark/setup.py` and populated by `include/pyspark/gold.py`.

**Engine**: `ReplacingMergeTree(update_time)`  
**Order by**: `(url, tag)`

| Column       | Type       | Description                                                                |
|-------------|------------|----------------------------------------------------------------------------|
| `url`       | `String`   | Article URL, foreign key to `fact_news_gl.url`.                           |
| `tag`       | `String`   | Single normalized tag (e.g. `"Politics"`, `"Asia"`).                      |
| `update_time` | `DateTime` | Timestamp when this tag row was last processed in the Gold pipeline.     |

**Notes:**
- Tags are derived from Silver `tags_list` by cleaning (`[]`, quotes) and splitting on commas, then exploding arrays.
- Supports many-to-many relationships between articles and tags for dimensional analysis.

---

## 5. Data Quality Quarantine – `news.data_quality_quarantine`

Holds rows that failed Great Expectations validation in the Bronze streaming pipeline. Created and used in `include/pyspark/setup.py` and `include/pyspark/great_expectations_common.py`.

**Engine**: `MergeTree`  
**Order by**: `(ingestion_time)`

| Column          | Type               | Description                                                                                       |
|----------------|--------------------|---------------------------------------------------------------------------------------------------|
| `table_name`   | `Nullable(String)` | Logical target table name where the row was originally intended to be inserted (e.g. `straitstimes_news_bz`). |
| `gx_batch_id`  | `Nullable(String)` | Great Expectations batch identifier generated per micro-batch.                                   |
| `violated_rules` | `Nullable(String)` | Concatenated list of validation rules violated for this row (e.g. column-level or table-level). |
| `raw_data`     | `Nullable(String)` | Full raw row serialized as JSON for inspection and reprocessing.                                 |
| `ingestion_time` | `DateTime`         | Server-side timestamp when the quarantined record was stored (default `now()`).                  |

**Notes:**
- Used by `validate_and_insert_process_batch` to capture failed rows from Bronze streaming ingestion.
- Enables offline inspection, reprocessing, and improvement of data quality rules.

---

## 6. Summary

- **Bronze** (`straitstimes_news_bz`): Raw ingested data with minimal transformation and `load_time` tracking.  
- **Silver** (`straitstimes_news_sl`): Cleaned, typed data with `update_time` for incremental upserts.  
- **Gold** (`fact_news_gl`, `dim_tags_gl`): Analytics-ready star schema for reporting and dashboards.  
- **Quarantine** (`data_quality_quarantine`): Central store for all rows that fail Great Expectations validation.


