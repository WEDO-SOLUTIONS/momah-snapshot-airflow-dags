from typing import Dict, Any

# This dictionary maps DB column names to their display properties.
ATTRIBUTE_MAPPER: Dict[str, Dict[str, Any]] = {
    # Mandatory displayable attributes
    "latitude":              {"en": "Latitude",               "ar": "خط العرض",             "type": "number",   "mandatory": True},
    "longitude":             {"en": "Longitude",              "ar": "خط الطول",             "type": "number",   "mandatory": True},
    "date":                  {"en": "Date",                   "ar": "التاريخ",              "type": "date_time",     "mandatory": True},

    # Amana (governorate) fields
    "amana_id":              {"en": "Amana ID",               "ar": "معرف الأمانة",         "type": "string",   "mandatory": False},
    "amana_name_ar":         {"en": "Amana Name (AR)",        "ar": "اسم الأمانة",         "type": "string",   "mandatory": False},
    "amana_name_en":         {"en": "Amana Name (EN)",        "ar": "اسم الأمانة بالإنجليزية",  "type": "string",   "mandatory": False},

    # Municipality fields
    "municipality_id":       {"en": "Municipality ID",        "ar": "معرف البلدية",         "type": "string",   "mandatory": False},
    "municipality_name_ar":  {"en": "Municipality Name (AR)", "ar": "اسم البلدية",         "type": "string",   "mandatory": False},
    "municipality_name_en":  {"en": "Municipality Name (EN)", "ar": "اسم البلدية بالإنجليزية","type": "string",   "mandatory": False},

    # Priority and metrics
    "priority_level":        {"en": "Priority Level",         "ar": "مستوى الأولوية",       "type": "string",   "mandatory": False},
    "vpi":                   {"en": "VPI",                    "ar": "مؤشر التشوه البصري",            "type": "number",   "mandatory": False},
    "coverage":              {"en": "Coverage",               "ar": "مؤشر التغطية",             "type": "number",   "mandatory": False},
    "ttr":                   {"en": "TTR",                    "ar": "TTR",                 "type": "string",   "mandatory": False},
    "repeat":                {"en": "Repeat",                 "ar": "إعادة",               "type": "string",   "mandatory": False},

    # Aggregated areas and values
    "street_inspected_area": {"en": "Street Inspected Area",  "ar": "مساحة الشوارع المفحوصة","type": "string",   "mandatory": False},
    "street_area":           {"en": "Street Area",            "ar": "مساحة الشوارع",        "type": "string",   "mandatory": False},
    "unit_value":            {"en": "Unit Value",             "ar": "قيمة الوحدة",         "type": "string",   "mandatory": False},
}