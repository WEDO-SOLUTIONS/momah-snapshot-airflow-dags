from typing import Dict, Any

# This dictionary maps DB column names to their display properties.

ATTRIBUTE_MAPPER: Dict[str, Dict[str, Any]] = {

    # Core location fields (fixed position)
    "latitude": {"en": "Latitude", "ar": "خط العرض", "type": "number", "mandatory": True},
    "longitude": {"en": "Longitude", "ar": "خط الطول", "type": "number", "mandatory": True},
    "date": {"en": "Date", "ar": "التاريخ", "type": "date_time", "mandatory": True},

    "cluster_id": {"en": "Cluster ID", "ar": "معرف العنقود", "type": "string", "mandatory": False},
    "amana_name_ar": {"en": "Amana Name (AR)", "ar": "اسم الأمانة", "type": "string", "mandatory": False},
    "municipality_name_ar": {"en": "Municipality Name (AR)", "ar": "اسم البلدية", "type": "string", "mandatory": False},
    "element_name": {"en": "Element Name", "ar": "اسم العنصر", "type": "string", "mandatory": False},
    "vp_category": {"en": "VP Category", "ar": "فئة التشوه البصري", "type": "string", "mandatory": False},
    "crm_case_id": {"en": "CRM Case ID", "ar": "معرف البلاغ", "type": "string", "mandatory": False},
    "crm_case_createdatetime": {"en": "Case Created", "ar": "تاريخ الإنشاء", "type": "string", "mandatory": False},
    "crm_case_status": {"en": "Case Status", "ar": "حالة البلاغ", "type": "string", "mandatory": False},
    "resolution_date_crm": {"en": "Resolution Date", "ar": "تاريخ الحل", "type": "string", "mandatory": False},
    "area_type": {"en": "Area Type", "ar": "نوع المنطقة", "type": "string", "mandatory": False},
    "num_incidents": {"en": "Incident Count", "ar": "عدد البلاغات", "type": "number", "mandatory": False},
    "contracting_company_ar": {"en": "Contracting Company", "ar": "الشركة المتعاقدة", "type": "string", "mandatory": False},
    "inspector_ar_name": {"en": "Inspector Name", "ar": "اسم المفتش", "type": "string", "mandatory": False},
    "track_email": {"en": "Track Email", "ar": "البريد الإلكتروني", "type": "string", "mandatory": False},

}