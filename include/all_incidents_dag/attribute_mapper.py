from typing import Dict, Any

# This dictionary maps DB column names to their display properties.
ATTRIBUTE_MAPPER: Dict[str, Dict[str, Any]] = {

    # DB Column Name: { en: "English Caption", ar: "Arabic Caption", type: "api_type", mandatory: bool }
    "latitude": {"en": "Latitude", "ar": "خط العرض", "type": "number", "mandatory": True},
    "longitude": {"en": "Longitude", "ar": "خط الطول", "type": "number", "mandatory": True},
    "date": {"en": "Date", "ar": "التاريخ", "type": "date_time", "mandatory": True},

    "amana_id": {"en": "Amana ID", "ar": "معرف الأمانة", "type": "string", "mandatory": False},
    "amana_name_ar": {"en": "Amana Name (AR)", "ar": "اسم الأمانة", "type": "string", "mandatory": False},
    "municipality_id": {"en": "Municipality ID", "ar": "معرف البلدية", "type": "string", "mandatory": False},
    "municipality_name_ar": {"en": "Municipality Name (AR)", "ar": "اسم البلدية", "type": "string", "mandatory": False},
    "cluster_id": {"en": "Cluster ID", "ar": "معرف العنقود", "type": "string", "mandatory": False},
    "incident_id": {"en": "Incident ID", "ar": "معرف البلاغ", "type": "string", "mandatory": False},
    "district_id": {"en": "District ID", "ar": "معرف المنطقة", "type": "string", "mandatory": False},
    "district_name": {"en": "District Name", "ar": "اسم المنطقة", "type": "string", "mandatory": False},
    "priority_id": {"en": "Priority ID", "ar": "معرف الأولوية", "type": "string", "mandatory": False},
    "area_type": {"en": "Area Type", "ar": "نوع المنطقة", "type": "string", "mandatory": False},
    "area_name": {"en": "Area Name", "ar": "اسم المنطقة", "type": "string", "mandatory": False},
    "classification_id": {"en": "Classification ID", "ar": "معرف التصنيف", "type": "string", "mandatory": False},
    "vp_category": {"en": "VP Category", "ar": "فئة التشوه البصري", "type": "string", "mandatory": False},
    "element_name": {"en": "Element Name", "ar": "اسم العنصر", "type": "string", "mandatory": False},
    "fixing_cost": {"en": "Fixing Cost", "ar": "تكلفة الإصلاح", "type": "number", "mandatory": False},
    "case_closure_status": {"en": "Case Closure Status", "ar": "حالة إغلاق البلاغ", "type": "string", "mandatory": False},

}