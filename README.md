# Module 1 — Data Pipeline (`/data_pipeline`)


Scrapes book catalogue data from [books.toscrape.com](http://books.toscrape.com/),
cleans and type-converts it, enriches it with a fixed GBP→INR conversion, loads it
into a normalized two-table SQLite database, and queries it with both SQL and
pandas.


## Setup


```bash
cd data_pipeline
pip install -r requirements.txt
```


(If you're using one consolidated `requirements.txt` at the repo root instead,
install from there — same packages: `requests`, `beautifulsoup4`, `pandas`, `lxml`.)


## Run


```bash
python pipeline.py
```


This requires outbound internet access to `books.toscrape.com` for the scraping
step. Everything after scraping (cleaning, loading, querying, pandas read-back)
only needs `books_raw.csv`, so if you need to re-run offline after a first
successful run, set `SKIP_SCRAPE = True` at the top of `pipeline.py` to reuse the
cached raw CSV.


Running it produces, in this folder:


- `books_raw.csv` — raw scraped rows (title, price, star_rating text, availability
  text, category), one row per book, before any cleaning.
- `books_clean.csv` — the cleaned/typed rows actually loaded into the database.
- `zepto_books.db` — the SQLite database (schema below).
- `query_outputs.txt` — all 5 required SQL queries with their printed output.
- `readback_comparison.txt` — the `pd.read_sql` read-back of two queries, plus the
  SQL JOIN query's result shown side by side with its `pd.merge` reproduction.


## Scraping approach


Scoped to **all books across the first 3+ sidebar categories** (in the order the
site lists them), paginating each category until it runs out of pages. If 3
categories don't reach 60 books, the scraper keeps adding the next category in
sidebar order until both minimums (≥3 categories, ≥60 books) are satisfied — so
the exact category count in the output can be 3 or slightly more, depending on
how many books each category happens to have. All four required fields
(`title`, `price`, `star_rating`, `availability`) are available directly on each
category's listing page, so no per-book detail-page requests are needed.


## Cleaning & type conversion


- `price` (e.g. `"£51.77"`) → `price_gbp` (float), via regex-stripping the
  currency symbol.
- `star_rating` (e.g. `"Three"`) → `rating` (int 1–5), via a fixed word→int
  lookup table.
- `availability` (e.g. `"In stock (22 available)"` / `"Out of stock"`) →
  `in_stock` (bool), based on whether the text contains "in stock".


**Row-failure handling (stated/justified per the task):**
- If `price_gbp` or `rating` fails to parse for a row, that single value is
  **median-imputed** from the successfully parsed values of the same field —
  both are numeric and a single missing value doesn't invalidate the rest of
  that row's fields.
- If `availability` fails to parse (or title/category is missing), the row is
  **dropped** instead of imputed — `in_stock` is a boolean identity field, not a
  measurement, so there's no sensible "average" to fall back on.
- The pipeline never crashes on a messy row; it logs how many rows were
  imputed vs. dropped (see the console output / `Cleaning summary` block).


## Currency conversion


`price_inr = price_gbp * 105.50` — **1 GBP = 105.50 INR**, the fixed,
project-defined baseline conversion rate required by the assignment (no live
lookup, no date reference). This is the only path that is graded.


## Database schema (normalized, 2 tables, PK/FK)


```sql
CREATE TABLE categories (
    category_id   INTEGER PRIMARY KEY,
    category_name TEXT UNIQUE
);


CREATE TABLE books (
    book_id     INTEGER PRIMARY KEY,
    title       TEXT,
    price_gbp   REAL,
    price_inr   REAL,
    rating      INTEGER,
    in_stock    INTEGER,
    category_id INTEGER REFERENCES categories(category_id)
);
```


## SQL queries (5, in `query_outputs.txt`)


| # | Query | Clauses demonstrated |
|---|-------|----------------------|
| 1 | Top-priced books rated ≥4 that are in stock | `SELECT` / `WHERE`, `ORDER BY`, `LIMIT` |
| 2 | Distinct category names | `DISTINCT` |
| 3 | Books priced between £20 and £40 | `BETWEEN` |
| 4 | Books rated 4 or 5 | `IN` |
| 5 | Top-10 rated books per category | `JOIN` (`books` ⋈ `categories`) + window function |


## Pandas read-back (`readback_comparison.txt`)


- Queries 1 and 3 are read back into DataFrames with `pd.read_sql(...)`.
- Query 5 (the JOIN) is additionally reproduced with **no SQL** — `books` and
  `categories` are pulled into DataFrames via `pd.read_sql("SELECT * ...")` and
  combined with `pd.merge(..., on="category_id")`, with an equivalent
  `groupby().cumcount()` standing in for the SQL `ROW_NUMBER() OVER (PARTITION
  BY ...)`. Both the SQL result and the pandas-only result are printed side by
  side and asserted equal (`sql_join_result.equals(pandas_join_result)`); the
  script prints `True`/`False` for this check.


## Git workflow note


Per the project-wide requirement, this repository's commit history includes a
feature branch created for this module, committed to at least twice, and
merged back into `main` (visible via `git log --graph --all`).

