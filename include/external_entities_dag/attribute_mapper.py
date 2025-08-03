from typing import Dict, Any

# This dictionary maps DB column names to their display properties for incident management system
ATTRIBUTE_MAPPER: Dict[str, Dict[str, Any]] = {

    # Core incident identification
    "incident_id": {"en": "Incident ID", "ar": "معرف البلاغ", "type": "string", "mandatory": True},
    "case_id_x": {"en": "Case ID", "ar": "معرف البلاغ", "type": "string", "mandatory": False},
    "case_id_y": {"en": "Resolved Case ID", "ar": "معرف البلاغ المغلق", "type": "string", "mandatory": False},
    "py_guid": {"en": "Process GUID", "ar": "معرف العملية", "type": "string", "mandatory": False},
    
    # Incident priority and status
    "priority": {"en": "Priority", "ar": "الأولوية", "type": "string", "mandatory": False},
    "py_status_work": {"en": "Work Status", "ar": "حالة العمل", "type": "string", "mandatory": False},
    "sla_track": {"en": "SLA Track", "ar": "مسار SLA", "type": "string", "mandatory": False},
    "agreement_status": {"en": "Agreement Status", "ar": "حالة الاتفاقية", "type": "string", "mandatory": False},
    
    # Date and time tracking
    "px_create_datetime": {"en": "Creation Date", "ar": "تاريخ الإنشاء", "type": "date_time", "mandatory": True},
    "py_resolved_timestamp": {"en": "Resolution Date", "ar": "تاريخ الحل", "type": "date_time", "mandatory": False},
    "ext_entity_essigned_date": {"en": "Assignment Date", "ar": "تاريخ التعيين", "type": "date_time", "mandatory": False},
    "ext_entity_resolved_date": {"en": "External Resolution Date", "ar": "تاريخ الحل الخارجي", "type": "date_time", "mandatory": False},
    "inc_date": {"en": "Incident Date", "ar": "تاريخ البلاغ", "type": "date_time", "mandatory": False},
    "inc_date_month": {"en": "Incident Month", "ar": "شهر البلاغ", "type": "string", "mandatory": False},

    # Location information
    "sub_municipality_id": {"en": "Sub-Municipality ID", "ar": "معرف البلدية الفرعية", "type": "string", "mandatory": False},
    "municipality_id": {"en": "Municipality ID", "ar": "معرف البلدية", "type": "string", "mandatory": False},
    "latitude": {"en": "Latitude", "ar": "خط العرض", "type": "number", "mandatory": True},
    "longitude": {"en": "Longitude", "ar": "خط الطول", "type": "number", "mandatory": True},
    "sub_sub_municipality_id": {"en": "Sub-Sub-Municipality ID", "ar": "معرف البلدية الفرعية الفرعية", "type": "string", "mandatory": False},
    
    # Classification information
    "main_classification_id": {"en": "Main Classification ID", "ar": "معرف التصنيف الرئيسي", "type": "string", "mandatory": False},
    "sub_classification_id_x": {"en": "Sub Classification ID", "ar": "معرف التصنيف الفرعي", "type": "string", "mandatory": False},
    "sub_classification_id_y": {"en": "Resolved Sub Classification ID", "ar": "معرف التصنيف الفرعي المغلق", "type": "string", "mandatory": False},
    "spl_classification_id_x": {"en": "Special Classification ID", "ar": "معرف التصنيف الخاص", "type": "string", "mandatory": False},
    "spl_classification_id_y": {"en": "Resolved Special Classification ID", "ar": "معرف التصنيف الخاص المغلق", "type": "string", "mandatory": False},
    "spl_classification_id_conf": {"en": "Confirmed Special Classification ID", "ar": "معرف التصنيف الخاص المؤكد", "type": "string", "mandatory": False},
    "special_classification": {"en": "Special Classification", "ar": "تصنيف خاص", "type": "string", "mandatory": False},
    "special_classification_txt": {"en": "Special Classification Text", "ar": "نص التصنيف الخاص", "type": "string", "mandatory": False},
    "name": {"en": "Classification Name", "ar": "اسم التصنيف", "type": "string", "mandatory": False},
    
    # SLA and agreement information
    "agreement_id": {"en": "Agreement ID", "ar": "معرف الاتفاقية", "type": "string", "mandatory": False},
    "agreement_name": {"en": "Agreement Name", "ar": "اسم الاتفاقية", "type": "string", "mandatory": False},
    "agreement_sla": {"en": "Agreement SLA", "ar": "اتفاقية مستوى الخدمة", "type": "string", "mandatory": False},
    "ee_processing_sla": {"en": "Processing SLA", "ar": "مستوى خدمة المعالجة", "type": "string", "mandatory": False},
    "sla_breached": {"en": "SLA Breached", "ar": "مخالفة SLA", "type": "number", "mandatory": False},
    "sla_breach_text": {"en": "SLA Breach Text", "ar": "نص مخالفة SLA", "type": "string", "mandatory": False},
    "critical_sla_breached": {"en": "Critical SLA Breached", "ar": "مخالفة SLA الحرجة", "type": "string", "mandatory": False},
    "ttp_sla_breached": {"en": "TTP SLA Breached", "ar": "مخالفة SLA TTP", "type": "string", "mandatory": False},
    "pc_sla_breached": {"en": "PC SLA Breached", "ar": "مخالفة SLA PC", "type": "string", "mandatory": False},
    "completed_sla_breached": {"en": "Completed SLA Breached", "ar": "مخالفة SLA المكتملة", "type": "string", "mandatory": False},
    "processing_sla_breached": {"en": "Processing SLA Breached", "ar": "مخالفة SLA المعالجة", "type": "string", "mandatory": False},
    
    # Incident metrics and statistics
    "received": {"en": "Received Count", "ar": "عدد الوارد", "type": "string", "mandatory": False},
    "closed_completed": {"en": "Closed Completed", "ar": "المغلقة المكتملة", "type": "string", "mandatory": False},
    "closed_rejected": {"en": "Closed Rejected", "ar": "المغلقة المرفوضة", "type": "string", "mandatory": False},
    "p_open": {"en": "Percentage Open", "ar": "النسبة المفتوحة", "type": "string", "mandatory": False},
    "p_reopen": {"en": "Percentage Reopened", "ar": "النسبة المعادة", "type": "string", "mandatory": False},
    "p_late": {"en": "Percentage Late", "ar": "النسبة المتأخرة", "type": "string", "mandatory": False},
    "critical": {"en": "Critical Count", "ar": "عدد الحالات الحرجة", "type": "string", "mandatory": False},
    "ttc": {"en": "Time to Close", "ar": "وقت الإغلاق", "type": "string", "mandatory": False},
    "ttp": {"en": "Time to Process", "ar": "وقت المعالجة", "type": "string", "mandatory": False},
    "completed": {"en": "Completed Count", "ar": "عدد المكتملة", "type": "string", "mandatory": False},
    "processing": {"en": "Processing Count", "ar": "عدد قيد المعالجة", "type": "string", "mandatory": False},
    
    # Additional attributes
    "source_system": {"en": "Source System", "ar": "نظام المصدر", "type": "string", "mandatory": False},
    "attribute_name": {"en": "Attribute Name", "ar": "اسم السمة", "type": "string", "mandatory": False},
    "attribute_name_ar": {"en": "Attribute Name (Arabic)", "ar": "اسم السمة (عربي)", "type": "string", "mandatory": False}

}