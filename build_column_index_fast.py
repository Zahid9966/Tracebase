import os
import django
import time

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "core.settings")
django.setup()

import boto3
import duckdb
from django.conf import settings

# -----------------------------
# CONFIG
# -----------------------------
bucket = settings.AWS_STORAGE_BUCKET_NAME
prefix = "parquet/"
output_path = f"s3://{bucket}/search_index/master_index_v2.parquet"

temp_dir = "D:/duckdb_temp"

batch_size = 20
MAX_THREADS = 8

os.makedirs(temp_dir, exist_ok=True)

# -----------------------------
# S3 CLIENT
# -----------------------------
s3 = boto3.client(
    "s3",
    aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
    region_name=settings.AWS_S3_REGION_NAME,
)

paginator = s3.get_paginator("list_objects_v2")

# -----------------------------
# FETCH FILES
# -----------------------------
print("🔍 Fetching parquet files...")

files = []

for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
    for obj in page.get("Contents", []):
        key = obj["Key"]

        if not key.endswith(".parquet"):
            continue
        if "search_index" in key:
            continue
        if obj["Size"] == 0:
            continue

        files.append(f"s3://{bucket}/{key}")

print(f"✅ Total files: {len(files)}")

# -----------------------------
# DUCKDB SETUP
# -----------------------------
con = duckdb.connect()

con.execute("INSTALL httpfs")
con.execute("LOAD httpfs")

con.execute(f"SET threads={MAX_THREADS}")
con.execute("SET memory_limit='4GB'")
con.execute("SET preserve_insertion_order=false")   # ✅ FIXED
con.execute(f"SET temp_directory='{temp_dir}'")     # ✅ FIXED

con.execute(f"SET s3_region='{settings.AWS_S3_REGION_NAME}'")
con.execute(f"SET s3_access_key_id='{settings.AWS_ACCESS_KEY_ID}'")
con.execute(f"SET s3_secret_access_key='{settings.AWS_SECRET_ACCESS_KEY}'")

# -----------------------------
# BUILD INDEX
# -----------------------------
print("\n🚀 Building column index...\n")

start = time.time()

batches = [files[i:i+batch_size] for i in range(0, len(files), batch_size)]

partial_paths = []
failed_batches = []

for idx, batch in enumerate(batches):

    print(f"📦 Batch {idx+1}/{len(batches)}")

    file_list = ",".join([f"'{f}'" for f in batch])
    temp_out = f"s3://{bucket}/search_index/col_partial_{idx}.parquet"

    try:
        con.execute(f"""
            COPY (
                SELECT
                    filename AS dataset,
                    row_number() OVER (PARTITION BY filename) AS row_id,

                    -- ✅ FIXED: no filename pollution
                    lower(concat_ws(' ', COLUMNS(* EXCLUDE (filename)))) AS search_blob

                FROM read_parquet(
                    [{file_list}],
                    union_by_name=true,
                    filename=true
                )
            )
            TO '{temp_out}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        partial_paths.append(temp_out)

    except Exception as e:
        print(f"❌ Batch {idx+1} FAILED → {e}")
        failed_batches.append(idx + 1)

# -----------------------------
# SAFE GUARD
# -----------------------------
if not partial_paths:
    print("❌ No batches succeeded. Exiting.")
    con.close()
    exit(1)

# -----------------------------
# MERGE FINAL
# -----------------------------
print("\n⚡ Merging...")

file_list = ",".join([f"'{p}'" for p in partial_paths])

con.execute(f"""
    COPY (
        SELECT * FROM read_parquet([{file_list}], union_by_name=true)
    )
    TO '{output_path}'
    (FORMAT PARQUET, COMPRESSION ZSTD)
""")

print("🎉 COLUMN INDEX CREATED")

# -----------------------------
# CLEANUP
# -----------------------------
print("🧹 Cleaning partials...")

for p in partial_paths:
    key = p.replace(f"s3://{bucket}/", "")
    s3.delete_object(Bucket=bucket, Key=key)

print("✅ Cleanup done")

# -----------------------------
# SUMMARY
# -----------------------------
print("\n📊 Summary:")
print(f"✔ Success : {len(partial_paths)}")
print(f"❌ Failed : {len(failed_batches)}")

if failed_batches:
    print(f"⚠️ Failed batches: {failed_batches}")

print(f"\n🏁 TOTAL TIME: {(time.time()-start)/60:.2f} min")

con.close()