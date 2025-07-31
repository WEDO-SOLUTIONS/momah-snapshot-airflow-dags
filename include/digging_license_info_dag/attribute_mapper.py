from typing import Dict, Any

# This dictionary maps DB column names to their display properties.
ATTRIBUTE_MAPPER: Dict[str, Dict[str, Any]] = {

    # Core identification fields
    "request_id": {"en": "Request ID", "ar": "معرف الطلب", "type": "string", "mandatory": True},
    "license_id": {"en": "License ID", "ar": "معرف الرخصة", "type": "string", "mandatory": True},

    # License dates and status
    "issue_date": {"en": "Issue Date", "ar": "تاريخ الإصدار", "type": "date_time", "mandatory": True},
    "expiration_date": {"en": "Expiration Date", "ar": "تاريخ الانتهاء", "type": "date_time", "mandatory": True},
    "license_status_code": {"en": "License Status Code", "ar": "كود حالة الرخصة", "type": "string", "mandatory": False},
    "license_status": {"en": "License Status", "ar": "حالة الرخصة", "type": "string", "mandatory": False},
    
    # Digging information
    "digging_status_id": {"en": "Digging Status ID", "ar": "معرف حالة الحفر", "type": "string", "mandatory": False},
    "digging_status": {"en": "Digging Status", "ar": "حالة الحفر", "type": "string", "mandatory": False},
    "digging_method": {"en": "Digging Method", "ar": "طريقة الحفر", "type": "string", "mandatory": False},
    "digging_duration": {"en": "Digging Duration", "ar": "مدة الحفر", "type": "string", "mandatory": False},
    "digging_late_status": {"en": "Digging Late Status", "ar": "حالة تأخر الحفر", "type": "string", "mandatory": False},

    # Project details
    "site_name": {"en": "Site Name", "ar": "اسم الموقع", "type": "string", "mandatory": False},
    "project_name": {"en": "Project Name", "ar": "اسم المشروع", "type": "string", "mandatory": False},
    "project_description": {"en": "Project Description", "ar": "وصف المشروع", "type": "string", "mandatory": False},
    "work_nature": {"en": "Work Nature", "ar": "طبيعة العمل", "type": "string", "mandatory": False},
    "path_length_sum": {"en": "Path Length Sum", "ar": "إجمالي طول المسار", "type": "string", "mandatory": False},

    # Project dates
    "project_start_date": {"en": "Project Start Date", "ar": "تاريخ بدء المشروع", "type": "string", "mandatory": False},
    "work_start_date": {"en": "Work Start Date", "ar": "تاريخ بدء العمل", "type": "string", "mandatory": False},
    "project_end_date": {"en": "Project End Date", "ar": "تاريخ انتهاء المشروع", "type": "string", "mandatory": False},

    # Location information
    "amana_name": {"en": "Amana Name", "ar": "اسم الأمانة", "type": "string", "mandatory": False},
    "city_name": {"en": "City Name", "ar": "اسم المدينة", "type": "string", "mandatory": False},
    "municipality_name": {"en": "Municipality Name", "ar": "اسم البلدية", "type": "string", "mandatory": False},
    "amana_code": {"en": "Amana Code", "ar": "كود الأمانة", "type": "string", "mandatory": False},
    "municipality_code": {"en": "Municipality Code", "ar": "كود البلدية", "type": "string", "mandatory": False},
    "worksite_code": {"en": "Worksite Code", "ar": "كود موقع العمل", "type": "string", "mandatory": False},
    "path_code": {"en": "Path Code", "ar": "كود المسار", "type": "string", "mandatory": False},
    
    # Coordinates
    "license_coordinates": {"en": "License Coordinates", "ar": "إحداثيات الرخصة", "type": "string", "mandatory": False},
    "office_coordinates": {"en": "Office Coordinates", "ar": "إحداثيات المكتب", "type": "string", "mandatory": False},
    "start_point_longitude": {"en": "Start Point Longitude", "ar": "خط طول نقطة البداية", "type": "number", "mandatory": True},
    "start_point_latitude": {"en": "Start Point Latitude", "ar": "خط عرض نقطة البداية", "type": "number", "mandatory": True},
    "end_point_longitude": {"en": "End Point Longitude", "ar": "خط طول نقطة النهاية", "type": "number", "mandatory": True},
    "end_point_latitude": {"en": "End Point Latitude", "ar": "خط عرض نقطة النهاية", "type": "number", "mandatory": True},

    # Additional details
    "name_arabic": {"en": "Arabic Name", "ar": "الاسم العربي", "type": "string", "mandatory": False},
    "map_number": {"en": "Map Number", "ar": "رقم الخريطة", "type": "string", "mandatory": False},
    "heavy_equipment_permission": {"en": "Heavy Equipment Permission", "ar": "تصريح المعدات الثقيلة", "type": "string", "mandatory": False},
    "camping_room_count": {"en": "Camping Room Count", "ar": "عدد غرف المعسكر", "type": "string", "mandatory": False},
    
    # Contractor/Consultant information
    "consultant_name": {"en": "Consultant Name", "ar": "اسم الاستشاري", "type": "string", "mandatory": False},
    "consultant_cr_number": {"en": "Consultant CR Number", "ar": "رقم السجل التجاري للاستشاري", "type": "string", "mandatory": False},
    "contractor_name": {"en": "Contractor Name", "ar": "اسم المقاول", "type": "string", "mandatory": False},
    "contractor_cr_number": {"en": "Contractor CR Number", "ar": "رقم السجل التجاري للمقاول", "type": "string", "mandatory": False}

}