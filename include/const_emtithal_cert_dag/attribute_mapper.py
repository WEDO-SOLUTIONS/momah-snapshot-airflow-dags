from typing import Dict, Any

# This dictionary maps DB column names to their display properties.
ATTRIBUTE_MAPPER: Dict[str, Dict[str, Any]] = {

    # Core location fields (fixed position)
    "LATITUDE": {"en": "Latitude", "ar": "خط العرض", "type": "number", "mandatory": True},
    "LONGITUDE": {"en": "Longitude", "ar": "خط الطول", "type": "number", "mandatory": True},
    
    # Request information
    "REQUEST_ID": {"en": "Request ID", "ar": "معرف الطلب", "type": "string", "mandatory": False},
    "CERTIFICATE_NUMBER": {"en": "Certificate Number", "ar": "رقم الشهادة", "type": "string", "mandatory": False},
    "CERTIFICATE_ISSUE_DATE": {"en": "Certificate Issue Date", "ar": "تاريخ إصدار الشهادة", "type": "date_time", "mandatory": True},
    "CERTIFICATE_END_DATE": {"en": "Certificate End Date", "ar": "تاريخ انتهاء الشهادة", "type": "date_time", "mandatory": True},
    
    # Location information
    "AMANA_NAME": {"en": "Amana Name", "ar": "اسم الأمانة", "type": "string", "mandatory": False},
    "MUNICIPALITY_NAME": {"en": "Municipality Name", "ar": "اسم البلدية", "type": "string", "mandatory": False},
    "DISTRICT_NAME": {"en": "District Name", "ar": "اسم الحي", "type": "string", "mandatory": False},
    "STREET_NAME": {"en": "Street Name", "ar": "اسم الشارع", "type": "string", "mandatory": False},
    
    # License information
    "LICENSE_ID": {"en": "License ID", "ar": "معرف الرخصة", "type": "string", "mandatory": False},
    
    # Ownership information
    "OWNERSHIP_DOCUMENT_NUMBER": {"en": "Ownership Document Number", "ar": "رقم وثيقة الملكية", "type": "string", "mandatory": False},
    "OWNERSHIP_DOCUMENT_TYPE": {"en": "Ownership Document Type", "ar": "نوع وثيقة الملكية", "type": "string", "mandatory": False},
    
    # Submitter information
    "SUBMITTER_ID_NUMBER": {"en": "Submitter ID Number", "ar": "رقم هوية مقدم الطلب", "type": "string", "mandatory": False},
    "SUBMITTER_FULL_NAME": {"en": "Submitter Full Name", "ar": "اسم مقدم الطلب كاملاً", "type": "string", "mandatory": False},
    "SUBMITTER_MOBILE": {"en": "Submitter Mobile", "ar": "جوال مقدم الطلب", "type": "string", "mandatory": False},
    "SUBMITTER_ID_TYPE": {"en": "Submitter ID Type", "ar": "نوع هوية مقدم الطلب", "type": "string", "mandatory": False},
    
    # Owner information
    "OWNER_ID": {"en": "Owner ID", "ar": "معرف المالك", "type": "string", "mandatory": False},
    "OWNER_FULL_NAME": {"en": "Owner Full Name", "ar": "اسم المالك كاملاً", "type": "string", "mandatory": False},
    "OWNER_MOBILE": {"en": "Owner Mobile", "ar": "جوال المالك", "type": "string", "mandatory": False},
    "OWNER_ID_TYPE": {"en": "Owner ID Type", "ar": "نوع هوية المالك", "type": "string", "mandatory": False},
    
    # Request details
    "REQUEST_SOURCE": {"en": "Request Source", "ar": "مصدر الطلب", "type": "string", "mandatory": False},
    "SUBMISSION_TYPE": {"en": "Submission Type", "ar": "نوع التقديم", "type": "string", "mandatory": False},
    "REQUEST_STATUS": {"en": "Request Status", "ar": "حالة الطلب", "type": "string", "mandatory": False},
    "ENTRY_MODES": {"en": "Entry Modes", "ar": "طرق الدخول", "type": "string", "mandatory": False},
    "NOTES": {"en": "Notes", "ar": "ملاحظات", "type": "string", "mandatory": False},
    "INTERSECTS_WITH_TARGET_ROADS": {"en": "Intersects With Target Roads", "ar": "يتقاطع مع الطرق المستهدفة", "type": "string", "mandatory": False}

}