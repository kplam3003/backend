-- CHART HRA 1
hra_1 AS (
  SELECT
    empl.case_study_id,
    empl.case_study_name,
    empl.source_name,
    empl.company_name,
    empl.nlp_type,
    empl.nlp_pack,
    empl.dimension_config_name,
    empl.coresignal_employee_id,
    edu.education_id,
    edu.date_from AS education_date_from,
    edu.date_to AS education_date_to,
    expr.title,
    expr.experience_id,
    expr.date_from AS experience_date_from,
    expr.date_to AS experience_date_to
  FROM employee_summary_table empl
  LEFT JOIN experience_summary_table expr
  ON empl.company_datasource_id = expr.company_datasource_id
    AND empl.coresignal_employee_id = expr.coresignal_employee_id
  LEFT JOIN education_summary_table edu
  ON empl.company_datasource_id = edu.company_datasource_id
    AND empl.coresignal_employee_id = edu.coresignal_employee_id
),
