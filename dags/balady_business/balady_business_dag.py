import json
from collections import defaultdict

from airflow import DAG
from airflow.decorators import task
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago


# ======================================================
#  CONNECTION VARIABLES (EDIT THESE ONLY)
# ======================================================

DB_CONN_ID = "stg_postgres_conn_catalog"   # Postgres catalog connection
AWS_CONN_ID = "oci_s3_conn"               # OCI S3-compatible conn (Amazon type)

# Single OCI bucket, one file per region under a prefix
S3_BUCKET_NAME = "balady_business"
S3_KEY_TEMPLATE = "regions/{region_id}/data.json"

TABLE_NAME = "branches"
JSON_COLUMN = "json_data"

# ======================================================


# ===================== HELPERS ========================

def safe_json_load(v):
    if isinstance(v, str) and v:
        try:
            return json.loads(v)
        except Exception:
            return None
    return None


def get_latest_schema():
    """
    Returns the latest numeric schema name, e.g. '20251207'
    """
    hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    sql = """
        SELECT nspname AS schema_name
        FROM pg_namespace
        WHERE nspname ~ '^[0-9]+$'
        ORDER BY nspname::bigint DESC
        LIMIT 1;
    """
    row = hook.get_first(sql)
    if not row or not row[0]:
        raise ValueError("No numeric schema found in pg_namespace")
    return row[0]


def get_region_ids(schema_name):
    """
    Returns list of distinct region_id values from {schema}.branches
    """
    hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    sql = f"""
        SELECT DISTINCT region_id
        FROM {schema_name}.{TABLE_NAME}
        WHERE region_id IS NOT NULL;
    """
    rows = hook.get_records(sql)
    return [r[0] for r in rows]


def get_rows_for_region(schema_name, region_id):
    """
    Returns list[dict] of all rows for the given region, with parsed JSON.
    """
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
    Group rows by 'id', splitting into Arabic/English.

    Returns:
      {
        "<id>": {"ar": row_or_None, "en": row_or_None}
      }
    """
    grouped = defaultdict(lambda: {"ar": None, "en": None})

    for r in rows:
        lang_val = (r.get("lang") or r.get("language") or r.get("locale") or "").lower()
        if lang_val.startswith("ar"):
            lang = "ar"
        else:
            lang = "en"

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
    """
    Extract contact_groups[].contacts[].text for given type.
    """
    out = []
    if not json_obj:
        return out

    for grp in json_obj.get("contact_groups", []) or []:
        for c in grp.get("contacts", []) or []:
            if c.get("type") == contact_type:
                val = c.get("text") or c.get("value")
                if val:
                    out.append(val)
    return out


def build_shifts(json_obj):
    """
    Build `shifts` array based on schedule, with hardcoded day/status/period structure.
    """
    if not json_obj:
        return []

    schedule = json_obj.get("schedule", {}) or {}

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

        wh_list = entry.get("working_hours", []) or []
        if not wh_list:
            continue

        wh = wh_list[0]
        from_time = wh.get("from")
        to_time = wh.get("to")

        if not (from_time and to_time):
            continue

        shifts.append({
            "day": {"id": day_id, "name": day_name},
            "status": {"id": "opened", "name": "مفتوح"},
            "period": {"id": "first", "name": "الاولى"},
            "from": from_time,
            "to": to_time,
        })

    return shifts


def classify_object(json_obj, rubric_ids_col):
    """
    Basic rubric → classification mapping.
    If we can't determine, we return None.
    """
    rubrics = (json_obj or {}).get("rubrics", []) or []
    ids = {str(r.get("id")) for r in rubrics if r.get("id") is not None}

    # Example rules, extend later if needed:
    if "207" in ids:
        return "pharmacy"
    if "162" in ids:
        return "restaurantAndCafe"

    return None  # leave classification null otherwise


def build_dest_record(rec_id, lang_group):
    """
    Build one destination JSON object for a single business id.
    """
    row_ar = lang_group.get("ar")
    row_en = lang_group.get("en")
    any_row = row_ar or row_en or {}

    json_ar = (row_ar or {}).get("_json") or {}
    json_en = (row_en or {}).get("_json") or {}
    base_json = json_ar or json_en

    # synCode
    syn_code = any_row.get("id")

    # commercialLicenseNumber from trade_license.license
    trade_license = (base_json or {}).get("trade_license") or {}
    commercial_license_number = trade_license.get("license")

    # classification
    classification_id = classify_object(base_json, any_row.get("rubric_ids"))
    classification = {
        "id": classification_id,
        "name": ""
    } if classification_id else None

    # names from JSON by lang
    name_ar = json_ar.get("name")
    name_en = json_en.get("name")

    # rubrics - full array
    rubrics = base_json.get("rubrics", []) or []

    # showedName mapping (note the cross-language mapping per your spec)
    showed_name = [
        {"id": "nameInArabic", "name": name_en},
        {"id": "nameInEnglish", "name": name_ar},
    ]

    # region info
    region_id = any_row.get("region_id")
    region_name = get_region_name_from_json(base_json)

    # city/district from AR row with fallback to EN
    city_name = (row_ar or row_en or {}).get("city_name")
    district_name = (row_ar or row_en or {}).get("district_name")

    # location from columns
    lat = any_row.get("lat")
    lon = any_row.get("lon")

    # contacts
    phone = collect_contacts(base_json, "phone")
    whatsapp = collect_contacts(base_json, "whatsapp")
    email = collect_contacts(base_json, "email")
    website = collect_contacts(base_json, "website")
    facebook = collect_contacts(base_json, "facebook")
    twitter = collect_contacts(base_json, "twitter")
    instagram = collect_contacts(base_json, "instagram")
    other_account = collect_contacts(base_json, "youtube")

    # shifts
    shifts = build_shifts(base_json)

    # building id from JSON
    building_id = (base_json or {}).get("building_id")

    general_template_data = {
        "nameAr": name_ar,
        "nameEn": name_en,
        "rubrics": rubrics,
        "showedName": showed_name,
        # buildingId placed BEFORE regionId as requested
        "buildingId": building_id,
        "regionId": str(region_id) if region_id is not None else None,
        "regionName": region_name,
        "cityId": None,
        "cityName": city_name,
        "district": {
            "id": None,
            "name": district_name,
        },
        "street": None,
        "address": None,
        "location": {
            "latitude": str(lat) if lat is not None else None,
            "longitude": str(lon) if lon is not None else None,
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
        "otherAccount": other_account,
        "shifts": shifts,
    }

    additional_attributes = {
        "buildingId": building_id,
    }

    return {
        "synCode": syn_code,
        "commercialLicenseNumber": commercial_license_number,
        "classification": classification,
        "generalTemplateData": general_template_data,
        "additionalAttributes": additional_attributes,
        "siteNotes": [],
    }


def upload_region_json(region_id, json_array):
    """
    Upload the array of JSONs for a region to the single OCI bucket.
    """
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    # ensure main bucket exists
    if not hook.check_for_bucket(S3_BUCKET_NAME):
        hook.create_bucket(bucket_name=S3_BUCKET_NAME)

    key = S3_KEY_TEMPLATE.format(region_id=region_id)

    hook.load_string(
        string_data=json.dumps(json_array, ensure_ascii=False),
        bucket_name=S3_BUCKET_NAME,
        key=key,
        replace=True,
    )

    return {"bucket": S3_BUCKET_NAME, "key": key, "region_id": region_id}


# ===================== DAG ========================

with DAG(
    dag_id="catalog_numeric_schema_to_oci_per_region",
    start_date=days_ago(1),
    schedule_interval=None,   # run manually
    catchup=False,
):

    @task
    def latest_schema():
        return get_latest_schema()

    @task
    def list_regions(schema_name: str):
        return get_region_ids(schema_name)

    @task
    def fetch_region_data(schema_name: str, region_id: int):
        return get_rows_for_region(schema_name, region_id)

    @task
    def merge_region_rows(rows: list):
        if not rows:
            return []
        grouped = group_ar_en(rows)
        out = []
        for rec_id, lang_group in grouped.items():
            out.append(build_dest_record(rec_id, lang_group))
        return out

    @task
    def write_region(region_id: int, json_array: list):
        return upload_region_json(region_id, json_array)

    schema = latest_schema()
    region_ids = list_regions(schema)

    raw_rows = fetch_region_data.expand(schema_name=schema, region_id=region_ids)
    merged = merge_region_rows.expand(rows=raw_rows)
    write_region.expand(region_id=region_ids, json_array=merged)
