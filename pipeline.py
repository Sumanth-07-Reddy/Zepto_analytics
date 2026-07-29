"""
Module 1 - Data Pipeline  (/data_pipeline)
Zepto Capstone Project

End-to-end pipeline:
    1. Scrape book catalogue data from http://books.toscrape.com (>= 3 categories, >= 60 books)
    2. Clean & type-convert the scraped fields (price_gbp, rating, in_stock)
    3. Convert price_gbp -> price_inr using the fixed project baseline rate (1 GBP = 105.50 INR)
    4. Load into a normalized two-table SQLite schema (categories <-1---N-> books)
    5. Run >= 5 SQL queries (SELECT/WHERE, ORDER BY, LIMIT, DISTINCT, IN/BETWEEN, JOIN)
    6. Read back >= 2 query results with pd.read_sql, and reproduce the JOIN query result
       with pd.merge on in-memory DataFrames, showing both approaches match.

Run with:
    python pipeline.py

Outputs (written into this folder):
    books_raw.csv          - raw scraped rows before cleaning
    books_clean.csv         - cleaned / typed rows actually loaded into SQLite
    zepto_books.db          - SQLite database (2-table normalized schema)
    query_outputs.txt       - all 5+ SQL queries with their printed output
    readback_comparison.txt - pd.read_sql vs pd.merge side-by-side comparison

NOTE ON NETWORK ACCESS:
    This script requires outbound internet access to books.toscrape.com to run the
    scraping step (Task 1). Everything downstream (cleaning, SQLite loading, SQL
    queries, pandas read-back) only depends on books_raw.csv and will run offline
    once that file exists. If you re-run without network access after the first
    successful run, set SKIP_SCRAPE = True below to reuse the cached books_raw.csv.
"""

import os
import re
import sqlite3
import statistics

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE_URL = "http://books.toscrape.com/"
GBP_TO_INR = 105.50          # fixed, project-defined baseline conversion rate (required, keyless)
MIN_CATEGORIES = 3
MIN_BOOKS = 60

HERE = os.path.dirname(os.path.abspath(__file__))
RAW_CSV = os.path.join(HERE, "books_raw.csv")
CLEAN_CSV = os.path.join(HERE, "books_clean.csv")
DB_PATH = os.path.join(HERE, "zepto_books.db")
QUERY_OUT = os.path.join(HERE, "query_outputs.txt")
READBACK_OUT = os.path.join(HERE, "readback_comparison.txt")

SKIP_SCRAPE = False   # flip to True to reuse an existing books_raw.csv without hitting the network

STAR_WORD_TO_INT = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}


# --------------------------------------------------------------------------- #
# 1. SCRAPE
# --------------------------------------------------------------------------- #

def get_category_urls():
    """Return a list of (category_name, category_listing_url) for every category
    on the site's sidebar, in the order they appear."""
    resp = requests.get(BASE_URL + "index.html", timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    links = soup.select("div.side_categories ul li ul li a")
    categories = []
    for a in links:
        name = a.get_text(strip=True)
        # hrefs look like "catalogue/category/books/travel_2/index.html"
        href = a["href"].replace("../../../", "").replace("../../", "").replace("../", "")
        if not href.startswith("catalogue/"):
            href = "catalogue/" + href
        url = BASE_URL + href
        categories.append((name, url))
    return categories


def scrape_category(name, start_url):
    """Scrape every book across every paginated page of a single category."""
    books = []
    page_url = start_url

    while True:
        resp = requests.get(page_url, timeout=15)
        if resp.status_code != 200:
            break
        soup = BeautifulSoup(resp.text, "html.parser")
        pods = soup.select("article.product_pod")
        if not pods:
            break

        for pod in pods:
            title = pod.h3.a["title"].strip()
            price_text = pod.select_one("p.price_color").get_text(strip=True)
            rating_classes = pod.select_one("p.star-rating")["class"]
            star_word = next((c for c in rating_classes if c != "star-rating"), None)
            availability_text = pod.select_one("p.instock.availability").get_text(strip=True)

            books.append({
                "title": title,
                "price": price_text,
                "star_rating": star_word,
                "availability": availability_text,
                "category": name,
            })

        next_link = soup.select_one("li.next a")
        if not next_link:
            break
        # page_url's directory + the relative "page-N.html" href
        page_url = page_url.rsplit("/", 1)[0] + "/" + next_link["href"]

    return books


def scrape_all(min_categories=MIN_CATEGORIES, min_books=MIN_BOOKS):
    """Scrape categories one at a time (in sidebar order) until BOTH the minimum
    number of categories and the minimum number of total books are satisfied."""
    all_categories = get_category_urls()
    collected = []
    used_categories = 0

    for name, url in all_categories:
        collected.extend(scrape_category(name, url))
        used_categories += 1
        if used_categories >= min_categories and len(collected) >= min_books:
            break

    if len(collected) < min_books or used_categories < min_categories:
        raise RuntimeError(
            f"Could not reach the required minimums: got {len(collected)} books "
            f"across {used_categories} categories."
        )
    return pd.DataFrame(collected)


# --------------------------------------------------------------------------- #
# 2. CLEAN
# --------------------------------------------------------------------------- #

def parse_price(price_text):
    """'£51.77' -> 51.77 (float). Raises ValueError if it can't be parsed."""
    match = re.search(r"[\d.]+", price_text)
    if not match:
        raise ValueError(f"Unparseable price: {price_text!r}")
    return float(match.group())


def parse_rating(star_word):
    """'Three' -> 3 (int). Raises ValueError if not a recognised word."""
    if star_word not in STAR_WORD_TO_INT:
        raise ValueError(f"Unparseable star rating: {star_word!r}")
    return STAR_WORD_TO_INT[star_word]


def parse_in_stock(availability_text):
    """'In stock (22 available)' -> True, 'Out of stock' -> False."""
    text = availability_text.lower()
    if "in stock" in text:
        return True
    if "out of stock" in text:
        return False
    raise ValueError(f"Unparseable availability: {availability_text!r}")


def clean_dataframe(df_raw):
    """
    Clean/typecast the raw scrape into: price_gbp (float), rating (int 1-5),
    in_stock (bool), category (str), title (str).

    Parsing strategy for rows that fail to parse a field:
      - price_gbp: median-impute using the median of the successfully parsed
        price_gbp values (price is numeric and a missing single value doesn't
        invalidate the row for other analysis).
      - rating: median-impute (rounded to nearest int) for the same reason -
        it's numeric and low-stakes to approximate.
      - in_stock / title / category: these aren't safely imputable (booleans /
        identifiers), so a row that fails to parse availability, or that is
        missing a title/category, is DROPPED instead.
    This is stated explicitly here and in the README per the task's requirement
    to "state and justify" the chosen approach.
    """
    rows = []
    parse_failures = {"price_gbp": [], "rating": [], "in_stock": [], "dropped": []}

    # First pass: parse everything we can, remember which rows/fields failed.
    parsed_rows = []
    for i, row in df_raw.iterrows():
        entry = {"title": row["title"], "category": row["category"]}

        try:
            entry["price_gbp"] = parse_price(row["price"])
        except ValueError:
            entry["price_gbp"] = None
            parse_failures["price_gbp"].append(i)

        try:
            entry["rating"] = parse_rating(row["star_rating"])
        except ValueError:
            entry["rating"] = None
            parse_failures["rating"].append(i)

        try:
            entry["in_stock"] = parse_in_stock(row["availability"])
        except ValueError:
            entry["in_stock"] = None
            parse_failures["in_stock"].append(i)

        parsed_rows.append(entry)

    # Compute medians from successfully parsed numeric values.
    good_prices = [r["price_gbp"] for r in parsed_rows if r["price_gbp"] is not None]
    good_ratings = [r["rating"] for r in parsed_rows if r["rating"] is not None]
    price_median = statistics.median(good_prices) if good_prices else 0.0
    rating_median = round(statistics.median(good_ratings)) if good_ratings else 3

    # Second pass: impute numeric fields, drop rows with un-imputable failures.
    for entry in parsed_rows:
        if entry["in_stock"] is None or not entry["title"] or not entry["category"]:
            parse_failures["dropped"].append(entry["title"])
            continue
        if entry["price_gbp"] is None:
            entry["price_gbp"] = price_median
        if entry["rating"] is None:
            entry["rating"] = rating_median
        rows.append(entry)

    df_clean = pd.DataFrame(rows)
    df_clean["price_inr"] = (df_clean["price_gbp"] * GBP_TO_INR).round(2)
    df_clean["in_stock"] = df_clean["in_stock"].astype(bool)
    df_clean["rating"] = df_clean["rating"].astype(int)

    print("Cleaning summary:")
    print(f"  price_gbp median-imputed for {len(parse_failures['price_gbp'])} row(s)")
    print(f"  rating     median-imputed for {len(parse_failures['rating'])} row(s)")
    print(f"  rows dropped (unparseable availability/title/category): "
          f"{len(parse_failures['dropped'])}")

    return df_clean


# --------------------------------------------------------------------------- #
# 3. LOAD INTO SQLITE (normalized 2-table schema)
# --------------------------------------------------------------------------- #

def load_into_sqlite(df_clean, db_path=DB_PATH):
    if os.path.exists(db_path):
        os.remove(db_path)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE categories (
            category_id   INTEGER PRIMARY KEY,
            category_name TEXT UNIQUE
        )
    """)
    cur.execute("""
        CREATE TABLE books (
            book_id     INTEGER PRIMARY KEY,
            title       TEXT,
            price_gbp   REAL,
            price_inr   REAL,
            rating      INTEGER,
            in_stock    INTEGER,
            category_id INTEGER REFERENCES categories(category_id)
        )
    """)

    category_names = sorted(df_clean["category"].unique())
    category_to_id = {}
    for name in category_names:
        cur.execute("INSERT INTO categories (category_name) VALUES (?)", (name,))
        category_to_id[name] = cur.lastrowid

    for _, row in df_clean.iterrows():
        cur.execute(
            """INSERT INTO books (title, price_gbp, price_inr, rating, in_stock, category_id)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                row["title"],
                float(row["price_gbp"]),
                float(row["price_inr"]),
                int(row["rating"]),
                int(bool(row["in_stock"])),
                category_to_id[row["category"]],
            ),
        )

    conn.commit()
    return conn


# --------------------------------------------------------------------------- #
# 4. SQL QUERIES (>=5, covering SELECT/WHERE, ORDER BY, LIMIT, DISTINCT,
#    IN/BETWEEN, and at least one JOIN)
# --------------------------------------------------------------------------- #

QUERIES = {
    "q1_top_rated_expensive_in_stock": """
        SELECT title, rating, price_gbp, price_inr
        FROM books
        WHERE rating >= 4 AND in_stock = 1
        ORDER BY price_gbp DESC
        LIMIT 10;
    """,
    "q2_distinct_categories": """
        SELECT DISTINCT category_name
        FROM categories
        ORDER BY category_name;
    """,
    "q3_midrange_priced_books": """
        SELECT title, price_gbp
        FROM books
        WHERE price_gbp BETWEEN 20 AND 40
        ORDER BY price_gbp ASC;
    """,
    "q4_specific_ratings": """
        SELECT title, rating, price_gbp
        FROM books
        WHERE rating IN (4, 5)
        ORDER BY rating DESC, price_gbp ASC;
    """,
    "q5_join_top10_rated_per_category": """
        SELECT category_name, title, rating, price_gbp
        FROM (
            SELECT
                c.category_name AS category_name,
                b.title         AS title,
                b.rating        AS rating,
                b.price_gbp     AS price_gbp,
                ROW_NUMBER() OVER (
                    PARTITION BY c.category_id
                    ORDER BY b.rating DESC, b.price_gbp ASC
                ) AS rn
            FROM books b
            JOIN categories c ON b.category_id = c.category_id
        )
        WHERE rn <= 10
        ORDER BY category_name, rating DESC, price_gbp ASC;
    """,
}


def run_queries(conn, out_path=QUERY_OUT):
    lines = []
    results = {}
    for name, sql in QUERIES.items():
        cur = conn.cursor()
        cur.execute(sql)
        col_names = [d[0] for d in cur.description]
        rows = cur.fetchall()
        results[name] = (col_names, rows)

        lines.append(f"--- {name} ---")
        lines.append(sql.strip())
        lines.append(f"columns: {col_names}")
        for r in rows:
            lines.append(str(r))
        lines.append(f"({len(rows)} rows)\n")

    with open(out_path, "w") as f:
        f.write("\n".join(lines))

    print(f"Wrote {len(QUERIES)} query results to {out_path}")
    return results


# --------------------------------------------------------------------------- #
# 5. PANDAS READ-BACK: pd.read_sql for >=2 queries, and pd.merge reproduction
#    of the JOIN query, compared side by side.
# --------------------------------------------------------------------------- #

def readback_and_compare(conn, out_path=READBACK_OUT):
    lines = []

    # (a) read back at least two query results via pd.read_sql
    df_q1 = pd.read_sql(QUERIES["q1_top_rated_expensive_in_stock"], conn)
    df_q3 = pd.read_sql(QUERIES["q3_midrange_priced_books"], conn)
    lines.append("=== pd.read_sql: q1_top_rated_expensive_in_stock ===")
    lines.append(df_q1.to_string(index=False))
    lines.append("\n=== pd.read_sql: q3_midrange_priced_books ===")
    lines.append(df_q3.to_string(index=False))

    # (b) reproduce the JOIN query's result purely with pandas (no SQL)
    books_df = pd.read_sql("SELECT * FROM books", conn)
    categories_df = pd.read_sql("SELECT * FROM categories", conn)

    merged = books_df.merge(categories_df, on="category_id")
    merged["rn"] = (
        merged.sort_values(["rating", "price_gbp"], ascending=[False, True])
        .groupby("category_id")
        .cumcount() + 1
    )
    pandas_join_result = (
        merged[merged["rn"] <= 10][["category_name", "title", "rating", "price_gbp"]]
        .sort_values(["category_name", "rating", "price_gbp"], ascending=[True, False, True])
        .reset_index(drop=True)
    )

    sql_join_result = pd.read_sql(QUERIES["q5_join_top10_rated_per_category"], conn).reset_index(drop=True)

    lines.append("\n=== SQL JOIN query result (top 10 rated per category) ===")
    lines.append(sql_join_result.to_string(index=False))
    lines.append("\n=== pd.merge reproduction of the same result ===")
    lines.append(pandas_join_result.to_string(index=False))

    are_equal = sql_join_result.equals(pandas_join_result)
    lines.append(f"\nSQL result and pandas-merge result identical: {are_equal}")

    with open(READBACK_OUT, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"SQL vs pandas-merge join results match: {are_equal}")
    print(f"Wrote read-back comparison to {out_path}")
    return are_equal


# --------------------------------------------------------------------------- #
# MAIN
# --------------------------------------------------------------------------- #

def main():
    if SKIP_SCRAPE and os.path.exists(RAW_CSV):
        print(f"SKIP_SCRAPE=True: reusing cached {RAW_CSV}")
        df_raw = pd.read_csv(RAW_CSV)
    else:
        print("Scraping books.toscrape.com ...")
        df_raw = scrape_all()
        df_raw.to_csv(RAW_CSV, index=False)
        print(f"Scraped {len(df_raw)} books across "
              f"{df_raw['category'].nunique()} categories -> {RAW_CSV}")

    df_clean = clean_dataframe(df_raw)
    df_clean.to_csv(CLEAN_CSV, index=False)
    print(f"Cleaned dataset: {len(df_clean)} rows -> {CLEAN_CSV}")

    conn = load_into_sqlite(df_clean)
    print(f"Loaded into SQLite -> {DB_PATH}")

    run_queries(conn)
    readback_and_compare(conn)

    conn.close()
    print("Pipeline complete.")


if __name__ == "__main__":
    main()
# TEST CHANGE