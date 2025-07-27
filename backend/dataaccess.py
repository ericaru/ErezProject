
import sqlite3
import os
import pandas as pd
import zipfile
import shutil

# Local Code
from backend.backend_config import *


class DataAccess:
    def __init__(self, db_path=DB_PATH):
        '''
        Initialize the DataAccess class, connecting to the SQLite database and creating tables if they don't exist.
        :param db_path: The path to the SQLite database file.

        Args:
            db_path (str): The path to the SQLite database file. Configured in backend_config.py.
        '''
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.cur = self.conn.cursor()

        if not self.__check_tables_exist():
            print('[Info]: Building a DB instance. This might take a few minutes...')
            self.__execute_script(INITIATE_LOINC_TABLE_DDL)
            self.__execute_script(INITIATE_PATIENTS_TABLE_DDL)
            self.__load_loinc_from_zip()
            self.__load_patients_from_excel()
            self.__print_db_info()
    
    def check_record(self, query_or_path, params):
        """
        A general function supposed to return a bool value if a searched record (based on params) exists in the snapshot of the DB.
        The operation is determined by the query, that should return 0 or 1.

        Args:
            query_or_path (str): str (describing the query) or .sql file path
            params (tuple): A tuple of size <0 with the input parameters needed to run the query, based on it's placeholders (?) 
        """
        if os.path.isfile(query_or_path):
            with open(query_or_path, 'r') as file:
                query = file.read()
        else:
            query = query_or_path  # assume raw SQL
        result = self.fetch_records(query, params)
        return bool(result)
        
    def get_attr(self, query_or_path, params):
        """
        A general function supposed to return a specific value like unit, date etc.
        The operation is determined by the query, that should have 1 item in the SELECT section.

        Args:
            query_or_path (str): str (describing the query) or .sql file path
            params (tuple): A tuple of size <0 with the input parameters needed to run the query, based on it's placeholders (?)
        """
        if os.path.isfile(query_or_path):
            with open(query_or_path, 'r') as file:
                query = file.read()
        else:
            query = query_or_path  # assume raw SQL
        result = self.fetch_records(query, params)
        return result[0][0] if result else None

    
    def execute_query(self, query_or_path, params):
        """
        Executes an INSERT/UPDATE/DELETE query.
        Accepts either a path to a .sql file or a raw SQL string.

        Args:
            query_or_path (str): str (describing the query) or .sql file path
            params (tuple): A tuple of size <0 with the input parameters needed to run the query, based on it's placeholders (?)
        """
        if os.path.isfile(query_or_path):
            with open(query_or_path, 'r') as file:
                query = file.read()
        else:
            query = query_or_path  # assume raw SQL

        self.cur.execute(query, params)
        self.conn.commit()
    
    def fetch_records(self, query_or_path, params):
        """
        Executes a SELECT query.
        Accepts either a path to a .sql file or a raw SQL string.
        Returns all rows.

        Args:
            query_or_path (str): str (describing the query) or .sql file path
            params (tuple): A tuple of size <0 with the input parameters needed to run the query, based on it's placeholders (?)
        """
        if os.path.isfile(query_or_path):
            with open(query_or_path, 'r') as file:
                query = file.read()
        else:
            query = query_or_path  # assume raw SQL

        return self.cur.execute(query, params).fetchall()
    
    def __execute_script(self, script_path):
        """
        Execute a DDL script from a file.
        Used to initialize tables in the DB.

        Args:
            query_or_path (str): str (describing the query) or .sql file path
            params (tuple): A tuple of size <0 with the input parameters needed to run the query, based on it's placeholders (?)
        """
        with open(script_path, 'r') as file:
            script = file.read()
        self.cur.executescript(script)
        self.conn.commit()

    def __check_tables_exist(self):
        """
        Ensuring DB was initialized.
        """
        result = self.fetch_records(CHECK_TABLE_EXISTS_QUERY, ())
        return bool(result)

    def __load_patients_from_excel(self):
        patients_df = pd.read_excel(PATIENTS_FILE, sheet_name='Patients')
        measurements_df = pd.read_excel(PATIENTS_FILE, sheet_name='Measurements')

        if patients_df.empty:
            print('[Info]: No patients data found to load.')
            return
        
        # Convert datetime columns to ISO 8601 format: 'YYYY-MM-DD HH:MM:SS'
        # Drop rows where datetime conversion failed
        for col in ['Valid start time', 'Transaction time']:
            measurements_df[col] = pd.to_datetime(measurements_df[col], errors='coerce', dayfirst=True).dt.strftime('%Y-%m-%d %H:%M:%S')
        measurements_df.dropna(subset=['Valid start time', 'Transaction time'], inplace=True)

        # Deduplicate patients
        unique_patients = patients_df[['PatientId', 'First name', 'Last name', 'Sex']].drop_duplicates()

        # Deduplicate measures
        measurements_df = measurements_df.sort_values(by='Transaction time', ascending=False)
        measurements_df = measurements_df.drop_duplicates(
            subset=['PatientId', 'LOINC-NUM', 'Valid start time'],
            keep='first'
        )
        measurements_df = measurements_df.sort_values(by=['PatientId', 'Valid start time'])

        # Insert unique patients
        for _, row in unique_patients.iterrows():
            self.execute_query(
                INSERT_PATIENT_QUERY,
                (row['PatientId'], row['First name'], row['Last name'], row['Sex'])
            )

        # Insert measurements
        for _, row in measurements_df.iterrows():
            self.execute_query(
                INSERT_MEASUREMENT_QUERY,
                (
                    row['PatientId'],
                    row['LOINC-NUM'],
                    row['Value'],
                    row['Unit'],
                    str(row['Valid start time']),
                    str(row['Transaction time'])
                )
            )

        print(
            f'[Info]: Loaded {len(measurements_df)} measurement records and {len(unique_patients)} unique patients from Excel file to DB tables.')
    
    def __load_loinc_from_zip(self):
        """
        Load LOINC codes from a ZIP file and insert them into the Loinc table in the DB for future use.
        Assumes the existance of the .zip file which is publically available.
        Looks for the Loinc.csv file which is the relevant one for this task.
        """
        extract_path = 'data/loinc_extracted'
        with zipfile.ZipFile(LOINC_CODES_ZIP, 'r') as zip_ref:
            zip_ref.extractall(extract_path)

        loinc_file = os.path.join(extract_path, 'LoincTable', 'Loinc.csv')

        if not os.path.exists(loinc_file):
            print('[Info]: LOINC file not found in extracted ZIP.')
            return

        df = pd.read_csv(loinc_file, dtype=str)

        if df.empty:
            print('[Info]: No LOINC codes found to load.')
        else:
            for _, row in df.iterrows():
                self.execute_query(
                    INSET_LOINC_CODE_QUERY,
                    (
                        str(row['LOINC_NUM']).strip(), 
                        str(row['COMPONENT']).strip(), 
                        str(row['PROPERTY']).strip(), 
                        str(row['TIME_ASPCT']).strip(),
                        str(row['SYSTEM']).strip(), 
                        str(row['SCALE_TYP']).strip(), 
                        str(row['METHOD_TYP']).strip(),
                        str(row['ALLOWED_VALUES']).strip(),
                    )
                )
            print(f'[Info]: Loaded {len(df)} LOINC codes from ZIP.')

        shutil.rmtree(extract_path)
        
    def __print_db_info(self):
        """
        Printing DB information, including the total number of tables created
        and the number of rows in each table.
        """
        print("[Info]: DB initiated successfully!")

        tables = self.fetch_records("SELECT name FROM sqlite_master WHERE type='table';", ())

        print(f"[Info]: Total tables created: {len(tables)}")

        for (table_name,) in tables:
            count = self.fetch_records(f"SELECT COUNT(*) FROM {table_name};", ())[0][0]
            print(f"[Info]: Table '{table_name}' - Rows: {count}")

if __name__ == '__main__':
    da = DataAccess()

