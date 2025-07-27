import os
import glob
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import pandas as pd
from dateutil import parser as dateparser

# Local Code
from backend.dataaccess import DataAccess
from backend.backend_config import * 


def parse_duration(duration_str):
    """
    Convert a compact duration string (e.g. '72h', '2d', '15m') into a timedelta object.

    Args:
        duration_str (str): A string with a number followed by a unit character:
                            - 'h' for hours
                            - 'd' for days
                            - 'm' for minutes

    Returns:
        timedelta: A timedelta representing the duration.

    Raises:
        ValueError: If the input format is invalid or unit is unsupported.
    """
    unit_map = {'h': 'hours', 'd': 'days', 'm': 'minutes'}
    value = int(duration_str[:-1])
    unit = unit_map[duration_str[-1]]
    return timedelta(**{unit: value})


class TAKRule:
    """
    A single temporal abstraction rule derived from a TAK XML file.

    Attributes:
        abstraction_name (str): The name of the abstract concept (e.g., 'Hemoglobin State').
        loinc_code (str): The LOINC code this rule applies to.
        filters (dict): Key-value constraints to check against patient parameters (e.g., {'sex': 'Male'}).
        good_before (timedelta): Time window before the measurement to consider the interval valid.
        good_after (timedelta): Time window after the measurement to consider the interval valid.
        rules (list of dict): List of abstraction thresholds, each with:
            - 'label': Name of the category (e.g., 'Low', 'Normal')
            - 'min': Minimum numeric value (inclusive) or None
            - 'max': Maximum numeric value (exclusive) or None
    """
    def __init__(self, abstraction_name, loinc_code, filters, persistence, rules):
        self.abstraction_name = abstraction_name
        self.loinc_code = loinc_code
        self.filters = filters  # e.g., {'sex': 'Male'} or {'age_group': 'Adult'}
        self.good_before = parse_duration(persistence['before'])
        self.good_after = parse_duration(persistence['after'])
        self.rules = rules

    def applies_to(self, patient_params):
        """
        Checks whether this rule is applicable to the patient based on filters.

        Args:
            patient_params (dict): Any number of demographic or contextual attributes.

        Returns:
            bool: True if all filters match patient parameters; False otherwise.
        """
        for key, value in self.filters.items():
            if key not in patient_params or str(patient_params[key]).lower() != str(value).lower():
                return False
        return True

    def apply(self, df):
        """
        Applies abstraction to raw measurement data based on defined rules.

        Args:
            df (pd.DataFrame): Must contain measurements with 'Value' and 'Valid start time' columns.

        Returns:
            dict:
                - 'abstracted': List of abstracted interval records
                - 'used_indices': Set of indices of original rows that were abstracted
        """
        used_indices = set()
        abstracted_records = []

        for _, row in df.iterrows():
            val = float(row['Value'])
            match = None
            # Find the discrete value for this row, store in match
            for rule in self.rules:
                if ((rule['min'] is None or val >= rule['min']) and
                    (rule['max'] is None or val < rule['max'])):
                    match = rule['label']
                    break
            # Create a perimiter around the matched value based on good_before, good_after
            if match:
                start_time = dateparser.parse(row['Valid start time']) - self.good_before
                end_time = dateparser.parse(row['Valid start time']) + self.good_after
                abstracted_records.append({
                    "LOINC-Code": self.loinc_code,
                    "Concept Name": self.abstraction_name,
                    "Value": match,
                    "StartDateTime": start_time.strftime('%Y-%m-%d %H:%M:%S'),
                    "EndDateTime": end_time.strftime('%Y-%m-%d %H:%M:%S')
                })
                used_indices.add(row.name)

        return {"abstracted": abstracted_records, "used_indices": used_indices}


class TAKParser:
    """
    Parses TAK XML files into executable abstraction rules (TAKRule).
    Each TAK file can contain multiple conditions for different demographics.

    Attributes:
        tak_folder (str): Folder path containing TAK XML rule files.
    """
    def __init__(self, tak_folder):
        self.tak_folder = tak_folder

    def load_all_taks(self):
        """
        Load all the TAK files into a list of TAKRule objects that can be applied to patient data.

        Returns:
            list of TAKRule: Fully constructed abstraction rules with conditions, thresholds, and persistence.
        """
        rules = []
        for path in glob.glob(os.path.join(self.tak_folder, '*.xml')):
            tree = ET.parse(path)
            root = tree.getroot()
            abstraction_name = root.attrib['name']
            loinc_code = root.attrib['loinc']

            for cond in root.findall('condition'):
                # Extract all condition-level attributes dynamically
                filters = {k: v for k, v in cond.attrib.items()}

                # Parse persistence window
                persistence = cond.find('persistence')
                p_before = persistence.attrib['good-before']
                p_after = persistence.attrib['good-after']

                # Parse rule thresholds
                rule_objs = []
                for r in cond.findall('rule'):
                    rule_objs.append({
                        'label': r.attrib['value'],
                        'min': float(r.attrib['min']) if 'min' in r.attrib else None,
                        'max': float(r.attrib['max']) if 'max' in r.attrib else None
                    })

                # Create TAKRule for this condition
                rules.append(TAKRule(
                    abstraction_name,
                    loinc_code,
                    filters,
                    {'before': p_before, 'after': p_after},
                    rule_objs
                ))

        return rules


class Mediator:
    """
    The Mediator engine orchestrates the temporal abstraction process.
    It:
    - Loads abstraction rules (TAK files)
    - Retrieves raw measurement data for a patient
    - Applies all applicable abstraction rules
    - Merges overlapping intervals with the same label
    - Returns both abstracted intervals and untouched raw measurements

    Attributes:
        parser (TAKParser): Loads TAK rules from XML files.
        tak_rules (list of TAKRule): All loaded rules.
        db (DataAccess): Database interface for patient and measurement retrieval.
    """
    def __init__(self, tak_folder=TAK_FOLDER):
        self.parser = TAKParser(tak_folder)
        self.tak_rules = self.parser.load_all_taks()
        self.db = DataAccess()
    

    def _get_patient_records(self, patient_id, snapshot_date):
        """
        Retrieves patient measurement records and patient-level attributes for abstraction.
        
        Args:
            patient_id (str): The ID of the patient.
            snapshot_date (str or datetime): The point-in-time view of the DB. Assumed to already be a parsed date.

        Returns:
            tuple: (list of measurement rows, dict of patient demographic params)
        """
        if not self.db.check_record(CHECK_PATIENT_BY_ID_QUERY, (patient_id,)):
            return [], {}

        # Construct query
        filters = [
            "m.PatientId = ?",
            "m.TransactionInsertionTime <= ?",
            "(m.TransactionDeletionTime IS NULL OR m.TransactionDeletionTime > ?)"
        ]
        params = [patient_id, snapshot_date, snapshot_date]

        with open(SEARCH_HISTORY_QUERY, 'r') as f:
            base_query = f.read()

        final_query = base_query.replace("{where_clause}", " AND ".join(filters))
        patient_records = self.db.fetch_records(final_query, params)

        # --- Fetch patient demographic parameters ---
        patient_info = self.db.fetch_records(GET_PATIENT_PARAMS_QUERY, (patient_id,))
        param_dict = {}
        if patient_info:
            columns = ['sex']
            param_dict = dict(zip(columns, patient_info[0]))

        return patient_records, param_dict

    def _merge_abstracted_intervals(self, patient_id, df, relevance_hours):
        """
        Merge overlapping abstracted intervals row-by-row, extending each edge by a 'relevance' duration (e.g., 24h),
        but prevent overlaps between different intervals of the same LOINC code and different values.

        Args:
            patient_id (str): The patient ID to which the abstracted records belong to.
            df (pd.DataFrame): Abstracted intervals with columns ['LOINC-Code', 'Value', 'StartDateTime', 'EndDateTime']
            relevance_hours (int): Number of hours to extend interval edges (default: 24)

        Returns:
            pd.DataFrame: Merged intervals with extended relevance windows, no overlap between distinct values.
        """
        df['StartDateTime'] = pd.to_datetime(df['StartDateTime'])
        df['EndDateTime'] = pd.to_datetime(df['EndDateTime'])

        # Sort by LOINC-Code, StartDateTime, then Value for deterministic behavior
        df = df.sort_values(by=['LOINC-Code', 'StartDateTime', 'Value'])

        merged_records = []
        current = None

        for _, row in df.iterrows():
            row = row.copy()
            # Extend the row's end by relevance hours
            row['EndDateTime'] += timedelta(hours=relevance_hours)

            if current is None:
                current = row
                continue

            same_code = row['LOINC-Code'] == current['LOINC-Code']
            same_value = row['Value'] == current['Value']
            overlap_or_touching = row['StartDateTime'] <= current['EndDateTime']

            if same_code and same_value and overlap_or_touching:
                # Merge by extending current interval
                current['EndDateTime'] = max(current['EndDateTime'], row['EndDateTime'])
            else:
                # No merge: truncate current to avoid overlap if needed
                if same_code and row['StartDateTime'] < current['EndDateTime']:
                    current['EndDateTime'] = row['StartDateTime']
                merged_records.append(current)
                current = row

        if current is not None:
            merged_records.append(current)

        df_merged = pd.DataFrame(merged_records)
        if not df_merged.empty:
            df_merged['PatientId'] = patient_id
            df_merged['Source'] = 'abstracted'
        return df_merged


    def run(self, patient_id, snapshot_date=None, relevance=24):
        """
        Run the temporal abstraction engine for a single patient.

        Retrieves raw measurement records, applies applicable abstraction rules,
        merges overlapping abstracted intervals, and returns both abstracted and untouched
        measurement records in a unified format.
        Will set "relevance" duration of 24h for raw records as well as to abstracted records, 
        which may have a longer duration, depends on the intervals.

        Args:
            patient_id (str or int): Patient identifier in the database.
            snapshot_date (str, optional): View of the DB up to this date (default: today).
            relevance (int, optional): Number of hours each measure is relevant for (default: 24 hours).

        Returns:
            pd.DataFrame: All records in unified format:
                ['PatientId', 'LOINC-Code', 'Concept Name', 'Value', 'StartDateTime', 'EndDateTime', 'Source']
        """
        patient_id = str(patient_id).strip()
        snapshot_date = snapshot_date or datetime.today().strftime('%Y-%m-%d')

        # Step 1: Retrieve raw measurements + patient attributes (e.g., sex)
        raw_rows, params = self._get_patient_records(patient_id, snapshot_date)
        if not raw_rows:
            return pd.DataFrame(columns=[
                "PatientId", "LOINC-Code", "Concept Name", "Value", "StartDateTime", "EndDateTime", "Source"
            ])

        # Step 2: Convert raw rows to DataFrame
        raw_df = pd.DataFrame(raw_rows, columns=[
            'LOINC-NUM', 'ConceptName', 'Value', 'Unit',
            'Valid start time', 'Transaction time'
        ])
        required_fields = {'LOINC-NUM', 'ConceptName', 'Value', 'Valid start time'}
        assert required_fields.issubset(raw_df.columns), "Missing required columns in measurement data"

        used_indices = set()
        abstracted_records = []

        # Step 3: Apply each applicable abstraction rule
        for rule in self.tak_rules:
            if not rule.applies_to(params):
                continue

            rule_df = raw_df[raw_df['LOINC-NUM'] == rule.loinc_code]
            if rule_df.empty:
                continue

            result = rule.apply(rule_df)
            for row in result['abstracted']:
                abstracted_records.append({
                    "LOINC-Code": rule.loinc_code,
                    "Concept Name": rule.abstraction_name,
                    "Value": row["Value"],
                    "StartDateTime": row["StartDateTime"],
                    "EndDateTime": row["EndDateTime"]
                })
            used_indices.update(result['used_indices'])

        # Step 4: Merge abstracted intervals (safely across all LOINC codes)
        if abstracted_records:
            merged_records = self._merge_abstracted_intervals(
                patient_id, pd.DataFrame(abstracted_records), relevance_hours=relevance)
        else:
            merged_records = pd.DataFrame(columns=[
                "PatientId", "LOINC-Code", "Concept Name", "Value", "StartDateTime", "EndDateTime", "Source"
            ])

        # Step 5: Process untouched raw records
        untouched = raw_df[~raw_df.index.isin(used_indices)].copy()
        untouched['Concept Name'] = untouched['ConceptName']
        untouched['StartDateTime'] = untouched['Valid start time']
        untouched['EndDateTime'] = pd.to_datetime(untouched['Valid start time']) + timedelta(hours=relevance)
        untouched['Source'] = 'raw'
        untouched['PatientId'] = patient_id
        untouched = untouched.rename(columns={"LOINC-NUM": "LOINC-Code"})

        # Step 6: Fix types before merge
        if not merged_records.empty:
            merged_records['StartDateTime'] = pd.to_datetime(merged_records['StartDateTime'], errors='coerce')
            merged_records['EndDateTime'] = pd.to_datetime(merged_records['EndDateTime'], errors='coerce')

        if not untouched.empty:
            untouched['StartDateTime'] = pd.to_datetime(untouched['StartDateTime'], errors='coerce')
            untouched['EndDateTime'] = pd.to_datetime(untouched['EndDateTime'], errors='coerce')

        # Step 7: Combine all and return
        frames = [
            merged_records,
            untouched[["PatientId", "LOINC-Code", "Concept Name", "Value", "StartDateTime", "EndDateTime", "Source"]]
        ]

        final_df = pd.concat([df for df in frames if not df.empty], ignore_index=True)

        return final_df.sort_values(by="StartDateTime").reset_index(drop=True)
    


if __name__ == "__main__":

    # --- CONFIGURABLE INPUT ---
    patient_id = "123456782"
    snapshot_date = "2025-06-20 12:00:00"

    # --- RUN MEDIATOR ---
    engine = Mediator()
    result_df = engine.run(patient_id=patient_id, snapshot_date=snapshot_date)

    # --- DISPLAY RESULT ---
    if result_df.empty:
        print(f"[Info] No data available for Patient {patient_id} on snapshot {snapshot_date}")
    else:
        print(f"[Info] Abstracted records for Patient {patient_id} on {snapshot_date}:")
        print(result_df.to_string(index=False))