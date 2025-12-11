from airflow import DAG
from airflow.decorators import task
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago

import json
from collections import defaultdict


# ======================================================
#  CONNECTION VARIABLES (EDIT THESE ONLY)
# ======================================================

DB_CONN_ID = "stg_postgres_conn_catalog"     # Your actual Postgres connection

AWS_CONN_ID = "oci_s3_conn"                  # OCI S3-compatible connection

# One bucket per region in OCI:
S3_BUCKET_PREFIX = "balady_business_"        # Creates: balady_business_{region_id}

# Path inside each bucket:
S3_KEY_TEMPLATE = "regions/{region_id}/data.json"

TABLE_NAME = "branches"
JSON_COLUMN = "json_data"
# ======================================================


# ===================== HELPERS ========================

def safe_json_load(v):
    if isinstance(v, str):
        try:
            return json.loads(v)
        except:
            return None
    return None


def get_latest_schema():
    hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    sql = """
        SELECT nspname AS schema_name
        FROM pg_namespace
        WHERE nspname ~ '^[0-9]+$'
        ORDER BY nspname::bigint DESC
        LIMIT 1;
    """
    res = hook.get_first(sql)
    return res[0]


def get_region_ids(schema_name):
    hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    sql = f"""
        SELECT DISTINCT region_id
        FROM {schema_name}.{TABLE_NAME}
        WHERE region_id IS NOT NULL;
    """
    rows = hook.get_records(sql)
    return [r[0] for r in rows]


def get_rows_for_region(schema_name, region_id):
    hook = PostgresHook(postgres_conn_id=DB_CONN_ID)

    sql = f"""
        SELECT *
        FROM {schema_name}.{TABLE_NAME}
        WHERE region_id = {region_id};
    """

    df = hook.get_pandas_df(sql)
    records = df.to_dict(orient="records")

    for r in records:
        r["_json"] = safe_json_load(r.get(JSON_COLUMN))

    return records


def group_ar_en(rows):
    """
    Groups rows by ID → { "ar": row, "en": row }
    """
    grouped = defaultdict(lambda: {"ar": None, "en": None})

    for r in rows:
        lang_val = str(r.get("lang") or "").lower()
        lang = "en"
        if lang_val.startswith("ar"):
            lang = "ar"

        rec_id = str(r.get("id"))
        grouped[rec_id][lang] = r

    return grouped


def get_region_name_from_json(json_obj):
    if not json_obj:
        return None
    for div in json_obj.get("adm_div", []):
        if div.get("type") == "region":
            return div.get("name")
    return None


def collect_contacts(json_obj, contact_type):
    result = []
    if not json_obj:
        return result
    for grp in json_obj.get("contact_groups", []):
        for c in grp.get("contacts", []):
            if c.get("type") == contact_type:
                txt = c.get("text") or c.get("value")
                if txt:
                    result.append(txt)
    return result


def build_shifts(json_obj):
    if not json_obj:
        return []

    schedule = json_obj.get("schedule", {})
    day_map = {
        "Sun": ("sunday", "الأحد"),
        "Mon": ("monday", "الاثنين"),
        "Tue": ("tuesday", "الثلاثاء"),
        "Wed": ("wednesday", "الأربعاء"),
        "Thu": ("thursday", "الخميس"),
        "Fri": ("friday", "الجمعة"),
        "Sat": ("saturday", "السبت"),
    }

    shifts = []
    for key, (day_id, day_name) in day_map.items():
        entry = schedule.get(key)
        if not entry:
            continue

        wh = entry.get("working_hours", [])
        if not wh:
            continue

        wh0 = wh[0]
        shifts.append({
            "day": {"id": day_id, "name": day_name},
            "status": {"id": "opened", "name": "مفتوح"},
            "period": {"id": "first", "name": "الاولى"},
            "from": wh0.get("from"),
            "to": wh0.get("to"),
        })

    return shifts


def classify_object(json_obj, rubric_ids_col):
    """
    Very simple stub mapping — expand later if needed.

    If rubric contains "207" => pharmacy
    If rubric contains "162" => restaurants & cafes
    """
    rubrics = (json_obj or {}).get("rubrics", [])

    ids = {str(r.get("id")) for r in rubrics}

    if "207" in ids:
        return "pharmacy"

    if "162" in ids:
        return "restaurantAndCafe"

    return None


def build_dest_record(rec_id, lang_group):
    ar = lang_group["ar"]
    en = lang_group["en"]

    any_row = ar or en or {}
    json_ar = ar["_json"] if ar else {}
    json_en = en["_json"] if en else {}

    base_json = json_ar or json_en

    # -------------------------------------------
    # Basic fields
    # -------------------------------------------
    syn_code = any_row.get("id")

    # License
    trade_license = (base_json or {}).get("trade_license", {})
    commercial_license_number = trade_license.get("license")

    # Classification
    classification_id = classify_object(base_json, any_row.get("rubric_ids"))
    classification = {
        "id": classification_id,
        "name": ""
    } if classification_id else None

    # Names
    name_ar = json_ar.get("name") if json_ar else None
    name_en = json_en.get("name") if json_en else None

    # Rubrics
    rubrics = base_json.get("rubrics", [])

    showed_name = [
        {"id": "nameInArabic", "name": name_en},
        {"id": "nameInEnglish", "name": name_ar},
    ]

    region_id = any_row.get("region_id")
    region_name = get_region_name_from_json(base_json)

    city_name = (ar or en or {}).get("city_name")
    district_name = (ar or en or {}).get("district_name")

    # Location
    lat = any_row.get("lat")
    lon = any_row.get("lon")

    # Contacts
    phone = collect_contacts(base_json, "phone")
    whatsapp = collect_contacts(base_json, "whatsapp")
    email = collect_contacts(base_json, "email")
    website = collect_contacts(base_json, "website")
    facebook = collect_contacts(base_json, "facebook")
    twitter = collect_contacts(base_json, "twitter")
    instagram = collect_contacts(base_json, "instagram")
    other = collect_contacts(base_json, "youtube")

    # Shifts
    shifts = build_shifts(base_json)

    # Building ID
    building_id = (base_json or {}).get("building_id")

    record = {
        "synCode": syn_code,
        "commercialLicenseNumber": commercial_license_number,
        "classification": classification,
        "generalTemplateData": {
            "nameAr": name_ar,
            "nameEn": name_en,
            "rubrics": rubrics,
            "showedName": showed_name,
            "regionId": str(region_id) if region_id is not None else None,
            "regionName": region_name,
            "cityId": None,
            "cityName": city_name,
            "district": {"id": None, "name": district_name},
            "street": None,
            "address": None,
            "location": {
                "latitude": str(lat) if lat else None,
                "longitude": str(lon) if lon else None,
            },
            "phoneNo": phone,
            "mobileNo": None,
            "unifiedPhoneNo": None,
            "whatsappNo": whatsapp,
            "email": email,
            "otherNumber": None,
            "website": website,
            "facebook": facebook,
            "twitter": twitter,
            "snapchat": None,
            "instagram": instagram,
            "otherAccount": other,
            "shifts": shifts,
        },
        "additionalAttributes": {
            "buildingId": building_id
        },
        "siteNotes": [],
    }

    return record


# ================= S3 WRITE =====================

def write_to_bucket(region_id, json_list):
    bucket_name = f"{S3_BUCKET_PREFIX}{region_id}"
    key = S3_KEY_TEMPLATE.format(region_id=region_id)

    hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    # ensure bucket exists
    if not hook.check_for_bucket(bucket_name):
        hook.create_bucket(bucket_name=bucket_name)

    hook.load_string(
        string_data=json.dumps(json_list, ensure_ascii=False),
        bucket_name=bucket_name,
        key=key,
        replace=True,
    )

    return bucket_name


# ===================== DAG ========================

with DAG(
    dag_id="balady_business_dag",
    start_date=days_ago(1),
    schedule_interval=None,      # manual trigger
    catchup=False,
):

    @task
    def latest_schema():
        return get_latest_schema()

    @task
    def list_regions(schema):
        return get_region_ids(schema)

    @task
    def pull_region(schema, region_id):
        return get_rows_for_region(schema, region_id)

    @task
    def merge_rows(rows):
        if not rows:
            return []

        grouped = group_ar_en(rows)
        out = []

        for rec_id, lang_group in grouped.items():
            out.append(build_dest_record(rec_id, lang_group))

        return out

    @task
    def upload(region_id, json_array):
        bucket = write_to_bucket(region_id, json_array)
        return {"region_id": region_id, "bucket": bucket}

    schema = latest_schema()
    region_ids = list_regions(schema)

    raw = pull_region.expand(schema=schema, region_id=region_ids)
    merged = merge_rows.expand(rows=raw)

    upload.expand(region_id=region_ids, json_array=merged)
