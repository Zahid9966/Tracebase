import os
import time
import shutil
import django
import duckdb
import boto3
import mysql.connector
from django.conf import settings

# =====================================================
# DJANGO SETUP
# =====================================================
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "core.settings")
django.setup()

print("\n🚀 STARTING FULL REBUILD PIPELINE (BINLOG SAFE FIXED)\n")

# =====================================================
# CONFIG
# =====================================================
BASE_TABLE = "laptop_search"
TRACK_TABLE = "processed_files"
LOG_TABLE = "file_size_log"
STORAGE_TABLE = "storage_monitor"
MAX_ROWS_PER_TABLE = 10000000
BATCH_SIZE = 2000
COMMIT_EVERY = 10
STOP_DISK_GB = 10

db = settings.DATABASES["default"]

# =====================================================
# MYSQL CONNECT
# =====================================================
def mysql_connect():
    return mysql.connector.connect(
        host=db["HOST"],
        user=db["USER"],
        password=db["PASSWORD"],
        database=db["NAME"],
        port=int(db.get("PORT", 3306)),
        autocommit=False,
        connection_timeout=600
    )

# =====================================================
# SESSION SETUP (FIXED)
# =====================================================
def setup_mysql_session():
    global conn, cursor
    cursor = conn.cursor()

    # 🔥 SAFE BINLOG DISABLE
    try:
        cursor.execute("SET SESSION sql_log_bin = 0")
    except Exception as e:
        print("⚠️ No permission for sql_log_bin, skipping...")

    cursor.execute("SET SESSION innodb_lock_wait_timeout = 300")
    cursor.execute("SET SESSION max_execution_time = 0")

# =====================================================
# INIT
# =====================================================
conn = mysql_connect()
setup_mysql_session()
print("✅ MySQL Connected")

# =====================================================
# SUPPORT TABLES
# =====================================================
cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {TRACK_TABLE} (
    file_name VARCHAR(700) PRIMARY KEY
)
""")

cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
    file_name VARCHAR(700) PRIMARY KEY,
    rows_inserted BIGINT,
    approx_blob_mb FLOAT,
    table_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {STORAGE_TABLE} (
    id INT AUTO_INCREMENT PRIMARY KEY,
    db_size_mb FLOAT,
    free_disk_gb FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

conn.commit()

# =====================================================
# LOAD PROCESSED FILES
# =====================================================
cursor.execute(f"SELECT file_name FROM {TRACK_TABLE}")
processed_files = set(x[0] for x in cursor.fetchall())
print("📌 Already Processed:", len(processed_files))

# =====================================================
# HELPERS
# =====================================================
def get_db_size_mb():
    cursor.execute("""
        SELECT ROUND(SUM(data_length + index_length)/1024/1024,2)
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
    """)
    r = cursor.fetchone()[0]
    return float(r or 0)

def get_disk_free_gb():
    total, used, free = shutil.disk_usage("D:/")
    return free / (1024 ** 3)

# =====================================================
# S3
# =====================================================
S3_REGION = settings.AWS_S3_REGION_NAME
S3_ACCESS_KEY = settings.AWS_ACCESS_KEY_ID
S3_SECRET_KEY = settings.AWS_SECRET_ACCESS_KEY
BUCKET = settings.AWS_STORAGE_BUCKET_NAME

s3 = boto3.client(
    "s3",
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    region_name=S3_REGION
)

# =====================================================
# LOAD FILES
# =====================================================
files = []

for folder in ["parquet_part1", "parquet_part2"]:
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=BUCKET, Prefix=f"{folder}/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if not key.endswith(".parquet"):
                continue

            parts = key.split("/")
            if len(parts) < 4:
                continue

            if parts[1] != "laptop_data_csv":
                continue

            full = f"s3://{BUCKET}/{key}"

            if full not in processed_files:
                files.append(full)

print("📂 Pending Files:", len(files))

# =====================================================
# DUCKDB
# =====================================================
con = duckdb.connect()
con.execute("INSTALL httpfs")
con.execute("LOAD httpfs")

os.makedirs("D:/duck_temp", exist_ok=True)

con.execute("SET temp_directory='D:/duck_temp'")
con.execute("SET memory_limit='4GB'")
con.execute("SET threads=4")

con.execute(f"SET s3_region='{S3_REGION}'")
con.execute(f"SET s3_access_key_id='{S3_ACCESS_KEY}'")
con.execute(f"SET s3_secret_access_key='{S3_SECRET_KEY}'")

print("✅ DuckDB Ready")

# =====================================================
# GLOBALS
# =====================================================
table_row_cache = {}
commit_counter = 0
total_inserted = 0

# =====================================================
# ACTIVE TABLE
# =====================================================
def get_active_table():
    cursor.execute(f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
        AND table_name LIKE '{BASE_TABLE}_%'
        ORDER BY table_name DESC
        LIMIT 1
    """)

    row = cursor.fetchone()

    if not row:
        return f"{BASE_TABLE}_1"

    table_name = row[0]

    if table_name not in table_row_cache:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        table_row_cache[table_name] = cursor.fetchone()[0]

    if table_row_cache[table_name] >= MAX_ROWS_PER_TABLE:
        nxt = int(table_name.split("_")[-1]) + 1
        return f"{BASE_TABLE}_{nxt}"

    return table_name

# =====================================================
# CREATE TABLE
# =====================================================
def ensure_table(table_name, cols_count):
    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")

    if cursor.fetchone():
        return

    print("🧱 Creating", table_name)

    cols = [f"`col_{i}` TEXT" for i in range(cols_count)]
    cols.append("`search_blob` MEDIUMTEXT")

    cursor.execute(f"""
        CREATE TABLE {table_name} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            {",".join(cols)}
        )
    """)

    conn.commit()

# =====================================================
# SAFE EXECUTE
# =====================================================
def safe_execute(query, batch):
    global conn, cursor, commit_counter

    for attempt in range(3):
        try:
            cursor.executemany(query, batch)

            commit_counter += 1

            if commit_counter >= COMMIT_EVERY:
                conn.commit()
                commit_counter = 0

            return True

        except Exception as e:
            print("⚠️ Retry:", attempt + 1, e)

            try:
                conn.rollback()
            except:
                pass

            time.sleep(2)

            try:
                conn = mysql_connect()
                setup_mysql_session()
            except Exception as x:
                print("Reconnect failed:", x)

    return False

# =====================================================
# MAIN LOOP
# =====================================================
for idx, file_path in enumerate(files, 1):

    if get_disk_free_gb() < STOP_DISK_GB:
        print("🛑 LOW DISK SPACE STOPPED")
        break

    table_name = get_active_table()

    print(f"\n🚀 [{idx}/{len(files)}] {file_path}")
    print("📦 Using:", table_name)

    try:
        res = con.execute(f"SELECT * FROM read_parquet('{file_path}')")
        col_count = len(res.description)

        ensure_table(table_name, col_count)

        cols = [f"`col_{i}`" for i in range(col_count)]
        cols.append("`search_blob`")

        insert_cols = ",".join(cols)
        placeholders = ",".join(["%s"] * (col_count + 1))

        insert_query = f"""
        INSERT INTO {table_name} ({insert_cols})
        VALUES ({placeholders})
        """

        rows_done = 0

        while True:
            rows = res.fetchmany(BATCH_SIZE)

            if not rows:
                break

            batch = []

            for row in rows:
                row = list(row)

                row = [
                    None if v is None else str(v).strip()
                    for v in row
                ]

                seen = set()
                blob_parts = []

                for v in row:
                    if not v:
                        continue

                    low = v.lower()

                    if low in ("null", "none", "nan"):
                        continue

                    if low in seen:
                        continue

                    seen.add(low)
                    blob_parts.append(v)

                blob = " ".join(blob_parts)

                row.append(blob)
                batch.append(row)

            if safe_execute(insert_query, batch):
                rows_done += len(batch)
                total_inserted += len(batch)
                print("   inserted", rows_done)

        cursor.execute(f"""
            INSERT IGNORE INTO {TRACK_TABLE}(file_name)
            VALUES (%s)
        """, (file_path,))

        conn.commit()

        print("💾 DB Size:", get_db_size_mb(), "MB")
        print("💽 Free Disk:", round(get_disk_free_gb(), 2), "GB")

    except Exception as e:
        print("❌ FAILED:", e)

        try:
            conn.rollback()
        except:
            pass

# =====================================================
# FINAL
# =====================================================
try:
    conn.commit()
except:
    pass

print("\n🎯 TOTAL INSERTED:", total_inserted)

try:
    cursor.close()
    conn.close()
except:
    pass

con.close()

print("🚀 PIPELINE COMPLETE")