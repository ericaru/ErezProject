import os

# Get project root (one level up from backend/)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Data files
PATIENTS_FILE = os.path.join(PROJECT_ROOT, 'data', 'project_db.xlsx')
LOINC_CODES_ZIP = os.path.join(PROJECT_ROOT, 'data', 'Loinc_2.80.zip')
DB_PATH = os.path.join(PROJECT_ROOT, 'data', 'cdss.db')

# DDL Queries
INITIATE_PATIENTS_TABLE_DDL = os.path.join(PROJECT_ROOT, 'backend', 'queries', 'create_patients_tables.sql')
INITIATE_LOINC_TABLE_DDL = os.path.join(PROJECT_ROOT, 'backend', 'queries', 'create_loinc_table.sql')

# DML Queries
INSERT_PATIENT_QUERY = os.path.join(PROJECT_ROOT, 'backend', 'queries', 'insert_patient.sql')
INSERT_MEASUREMENT_QUERY = os.path.join(PROJECT_ROOT, 'backend', 'queries', 'insert_measurement.sql')
INSET_LOINC_CODE_QUERY = os.path.join(PROJECT_ROOT, 'backend', 'queries', 'insert_loinc.sql')
INSERT_ABSTRACTED_MEASUREMENT_QUERY = os.path.join(PROJECT_ROOT, 'backend', 'queries', 'insert_abstracted_measurement.sql')

# SQL Queries
CHECK_PATIENT_BY_NAME_QUERY = os.path.join(PROJECT_ROOT, 'backend', 'queries', 'check_patient_by_name.sql')
CHECK_PATIENT_BY_ID_QUERY = os.path.join(PROJECT_ROOT, 'backend', 'queries', 'check_patient_by_id.sql')
CHECK_LOINC_QUERY = os.path.join(PROJECT_ROOT, 'backend', 'queries', 'check_lonic.sql')
CHECK_RECORD_QUERY = os.path.join(PROJECT_ROOT, 'backend', 'queries', 'check_record.sql')
CHECK_FUTURE_RECORD_QUERY = os.path.join(PROJECT_ROOT, 'backend', 'queries', 'check_future_record.sql')
CHECK_LOINC_QUERY = os.path.join(PROJECT_ROOT, 'backend', 'queries', 'check_lonic.sql')
CHECK_RECORD_QUERY = os.path.join(PROJECT_ROOT, 'backend', 'queries', 'check_record.sql')
CHECK_FUTURE_RECORD_QUERY = os.path.join(PROJECT_ROOT, 'backend', 'queries', 'check_future_record.sql')
CHECK_TABLE_EXISTS_QUERY = os.path.join(PROJECT_ROOT, 'backend', 'queries', 'check_table_exists.sql')
SEARCH_HISTORY_QUERY = os.path.join(PROJECT_ROOT, 'backend', 'queries', 'get_history.sql')
UPDATE_MEASUREMENT_QUERY = os.path.join(PROJECT_ROOT, 'backend', 'queries', 'update_measurement.sql') # In-place value update, currently irrelevant.
UPDATE_DELETION_TIME_QUERY = os.path.join(PROJECT_ROOT, 'backend', 'queries', 'update_record_deletion_time.sql')
GET_ALL_PATIENTS_QUERY = os.path.join(PROJECT_ROOT, 'backend', 'queries', 'get._all_patients.sql')
GET_EXISTING_UNIT_QUERY = os.path.join(PROJECT_ROOT, 'backend', 'queries', 'get_existing_unit.sql')
GET_LOINC_BY_COMPONENT_FL_QUERY = os.path.join(PROJECT_ROOT, 'backend', 'queries', 'get_loinc_by_component_from_loinc.sql')
GET_LOINC_BY_COMPONENT_FM_QUERY = os.path.join(PROJECT_ROOT, 'backend', 'queries', 'get_loinc_by_component_from_measurements.sql')
GET_LOINC_ALLOWED_VALUES = os.path.join(PROJECT_ROOT, 'backend', 'queries', 'get_loinc_allowed_values.sql') # From LOINC table
GET_LATEST_VALIDTIME_FOR_DAY_QUERY = os.path.join(PROJECT_ROOT, 'backend', 'queries', 'select_latest_validtime_for_day.sql')
GET_PATIENT_PARAMS_QUERY = os.path.join(PROJECT_ROOT, 'backend', 'queries', 'get_patient_params.sql')

# TAK Folder
TAK_FOLDER = os.path.join(PROJECT_ROOT, 'backend', 'taks')

# rules
RULES_FOLDER = os.path.join(PROJECT_ROOT, 'backend', 'rules')
HEMATOLOGICAL_RULES = os.path.join(RULES_FOLDER, 'hematological_rules.json')