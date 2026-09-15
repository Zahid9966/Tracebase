# TraceBase – Scalable Record Search Platform

TraceBase is a search system built to make large CSV and Parquet datasets easy to look up and browse. Instead of manually scanning spreadsheets or running slow, unindexed SQL queries, TraceBase imports the data, organizes it into indexed, partitioned MySQL tables, and exposes it through a simple search dashboard.

## Why This Project

Raw CSV/Parquet files are hard to search once they grow large — opening them in a spreadsheet or running `LIKE '%term%'` queries on a single big table gets slow fast. TraceBase was built to solve that: import once, index properly, and search quickly by mobile number, name, or pincode.

## Features

- **Bulk data import** — ingests CSV and Parquet files into the system
- **Parquet conversion** — uses DuckDB to convert raw CSV into Parquet for more efficient storage and faster loading
- **Partitioned MySQL tables** — data is split across multiple tables instead of one large table, keeping indexes smaller and easier to maintain
- **Indexed search** — BTREE indexes for exact-match lookups (mobile number, pincode) and FULLTEXT indexes for keyword search
- **Smart query routing** — the search input is inspected and routed automatically:
  - 10-digit input → mobile number lookup
  - 6-digit input → pincode lookup
  - text input → FULLTEXT keyword search
- **Dashboard UI** — dataset selection, pagination, filtering, and a loading state
- **Downloadable results** — search results can be exported from the dashboard

## Tech Stack

| Layer | Technology |
|---|---|
| Backend | Django |
| Database | MySQL (partitioned tables, BTREE + FULLTEXT indexes) |
| ETL | DuckDB |
| Frontend | React |
| Storage | AWS S3 |

## Architecture

```
Raw CSV/Parquet files (AWS S3)
        ↓
DuckDB ETL — converts CSV to Parquet
        ↓
MySQL partitioned tables
        ↓
BTREE + FULLTEXT indexes
        ↓
Django search API
        ↓
React dashboard
```

## Search Logic (simplified)

```python
if len(query) == 10:
    # mobile number lookup
    Record.objects.filter(mobile=query)
elif len(query) == 6:
    # pincode lookup
    Record.objects.filter(pincode=query)
else:
    # keyword search
    Record.objects.filter(search_blob__search=query)
```

## Getting Started

1. Clone the repository
   ```
   git clone https://github.com/Zahid9966/tracebase.git
   cd tracebase
   ```
2. Set up the backend
   ```
   pip install -r requirements.txt
   python manage.py migrate
   python manage.py runserver
   ```
3. Set up the frontend
   ```
   cd frontend
   npm install
   npm start
   ```
4. Configure your MySQL and AWS S3 credentials in the environment/config file before running imports.

## Limitations

- Very common keyword searches can still be slower than exact-match lookups
- Partition routing is currently rule-based rather than fully automatic across all tables
- Not yet deployed with auto-scaling; tested primarily on local/single-server MySQL

## Possible Future Improvements

- Unified search across all partitioned tables without manual routing
- Cloud deployment with auto-scaling
- Basic admin/analytics panel
- Role-based access control

## Author

**Zahid Shaikh**
[GitHub](https://github.com/Zahid9966) · [LinkedIn](https://linkedin.com/in/zahid-shaikh-195624259)
