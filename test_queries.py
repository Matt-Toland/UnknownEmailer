"""Test script to debug the BigQuery data structure."""
import json
from google.cloud import bigquery
from app.config import config

client = bigquery.Client(project=config.BQ_PROJECT_ID)
table_id = config.get_full_table_id()

# Test 1: Check actual structure of JSON fields
query1 = f"""
SELECT
    meeting_id,
    JSON_VALUE(client_info, '$.client') as extracted_client,
    client_info as raw_client_info,
    JSON_VALUE(now, '$.qualified') as now_qualified,
    JSON_VALUE(fit, '$.qualified') as fit_qualified,
    now as raw_now,
    fit as raw_fit
FROM `{table_id}`
WHERE DATE(scored_at) = '2025-10-03'
LIMIT 5
"""

print("Testing JSON field structure...")
result1 = client.query(query1).result()
for row in result1:
    print(f"\nMeeting: {row.meeting_id}")
    print(f"  Client extracted: {row.extracted_client}")
    print(f"  Client raw: {str(row.raw_client_info)[:100] if row.raw_client_info else 'None'}")
    print(f"  NOW qualified: {row.now_qualified}")
    print(f"  FIT qualified: {row.fit_qualified}")

    # Parse and inspect NOW structure
    if row.raw_now:
        try:
            now_obj = json.loads(row.raw_now)
            print(f"  NOW keys: {list(now_obj.keys())}")
        except:
            print(f"  NOW parse failed")

    # Parse and inspect FIT structure
    if row.raw_fit:
        try:
            fit_obj = json.loads(row.raw_fit)
            print(f"  FIT keys: {list(fit_obj.keys())}")
            if 'services' in fit_obj:
                print(f"  FIT services: {fit_obj['services']}")
        except:
            print(f"  FIT parse failed")

# Test 2: Count qualified meetings by criteria
query2 = f"""
SELECT
    COUNT(*) as total,
    COUNTIF(qualified = TRUE) as qualified,
    COUNTIF(JSON_VALUE(now, '$.qualified') = 'true') as now_true,
    COUNTIF(JSON_VALUE(next, '$.qualified') = 'true') as next_true,
    COUNTIF(JSON_VALUE(measure, '$.qualified') = 'true') as measure_true,
    COUNTIF(JSON_VALUE(blocker, '$.qualified') = 'true') as blocker_true,
    COUNTIF(JSON_VALUE(fit, '$.qualified') = 'true') as fit_true
FROM `{table_id}`
WHERE DATE(scored_at) BETWEEN '2025-10-01' AND '2025-11-30'
"""

print("\n\nCriteria qualification counts:")
result2 = client.query(query2).result()
for row in result2:
    print(f"Total meetings: {row.total}")
    print(f"Qualified: {row.qualified}")
    print(f"NOW qualified: {row.now_true}")
    print(f"NEXT qualified: {row.next_true}")
    print(f"MEASURE qualified: {row.measure_true}")
    print(f"BLOCKER qualified: {row.blocker_true}")
    print(f"FIT qualified: {row.fit_true}")

# Test 3: Check client_info structure
query3 = f"""
SELECT DISTINCT
    JSON_VALUE(client_info, '$.client') as client,
    COUNT(*) as count
FROM `{table_id}`
WHERE DATE(scored_at) BETWEEN '2025-10-01' AND '2025-11-30'
    AND client_info IS NOT NULL
GROUP BY client
ORDER BY count DESC
LIMIT 10
"""

print("\n\nTop clients:")
result3 = client.query(query3).result()
for row in result3:
    print(f"  {row.client}: {row.count} meetings")