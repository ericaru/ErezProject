import pandas as pd
from datetime import datetime
from backend.dataaccess import DataAccess
from backend.mediator import Mediator
from backend.rule_processor import RuleProcessor
from backend.backend_config import *


class SimpleRuleEngine:
    """
    Simple rule engine implementing only the 2:1 AND table for Hematological state.
    Uses JSON-based rules instead of hard-coded dictionaries.
    Combines Hemoglobin_State + WBC_Level → Hematological_State
    """

    def __init__(self):
        self.db = DataAccess()
        self.mediator = Mediator()
        self.rule_processor = RuleProcessor()

    def get_hematological_state(self, hemoglobin_state, wbc_level):
        """
        Apply 2:1 AND rule to get hematological state using JSON rules.

        Args:
            hemoglobin_state (str): Abstracted hemoglobin state
            wbc_level (str): Abstracted WBC level

        Returns:
            str: Hematological state or None if no match or missing data
        """
        if not hemoglobin_state or not wbc_level:
            return None

        # Prepare input for rule processor
        input_values = {
            "hemoglobin_state": hemoglobin_state,
            "wbc_level": wbc_level
        }

        return self.rule_processor.apply_rule(HEMATOLOGICAL_RULES, input_values)

    def get_latest_abstracted_value(self, df, concept_name, snapshot_time):
        """
        Extract the latest abstracted value for a specific concept.

        Args:
            df (pd.DataFrame): Abstracted measurements dataframe
            concept_name (str): Name of the concept to extract
            snapshot_time (datetime): Current time for filtering

        Returns:
            str or None: Latest value or None if not found
        """
        if df.empty:
            return None

        # Filter for the specific concept and current time
        concept_data = df[
            (df['Concept Name'] == concept_name) &
            (pd.to_datetime(df['StartDateTime']) <= snapshot_time) &
            (pd.to_datetime(df['EndDateTime']) >= snapshot_time)
            ]

        if concept_data.empty:
            return None

        # Get the most recent one by StartDateTime
        latest = concept_data.loc[concept_data['StartDateTime'].idxmax()]
        return latest['Value']

    def analyze_patient_hematological_state(self, patient_id, snapshot_date=None):
        """
        Analyze patient's hematological state using the 2:1 AND rule.

        Args:
            patient_id (str): Patient identifier
            snapshot_date (str, optional): Snapshot date for analysis

        Returns:
            dict: Analysis results with individual states and compound hematological state
        """
        patient_id = str(patient_id).strip()
        snapshot_date = snapshot_date or datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Convert to datetime for filtering
        if isinstance(snapshot_date, str):
            snapshot_time = pd.to_datetime(snapshot_date)
        else:
            snapshot_time = snapshot_date

        # Step 1: Get abstracted data from existing Mediator
        abstracted_df = self.mediator.run(patient_id, snapshot_date)  # to change

        if abstracted_df.empty:
            return {
                'patient_id': patient_id,
                'snapshot_date': snapshot_date,
                'hemoglobin_state': None,
                'wbc_level': None,
                'hematological_state': None,
                'error': 'No abstracted data available for this patient'
            }

        # Step 2: Extract latest abstracted values
        hemoglobin_state = self.get_latest_abstracted_value(abstracted_df, "Hemoglobin_Level", snapshot_time)
        wbc_level = self.get_latest_abstracted_value(abstracted_df, "WBC_Level", snapshot_time)

        # Step 3: Apply 2:1 AND rule using JSON rules
        hematological_state = self.get_hematological_state(hemoglobin_state, wbc_level)

        return {
            'patient_id': patient_id,
            'snapshot_date': snapshot_date,
            'individual_states': {
                'hemoglobin_state': hemoglobin_state,
                'wbc_level': wbc_level
            },
            'hematological_state': hematological_state,
            'abstracted_data': abstracted_df
        }

    def analyze_all_patients_hematological_state(self, snapshot_date=None):
        """
        Analyze hematological state for all patients using existing single-patient function.

        Args:
            snapshot_date (str, optional): Snapshot date for analysis

        Returns:
            list: List of analyses for all patients
        """
        from backend_config import GET_ALL_PATIENTS_QUERY
        import os

        print("Absolute path:", os.path.abspath(GET_ALL_PATIENTS_QUERY))
        print("Exists?", os.path.isfile(GET_ALL_PATIENTS_QUERY))

        print(f"GET_ALL_PATIENTS_QUERY = {GET_ALL_PATIENTS_QUERY}")
        print("Is file:", os.path.isfile(GET_ALL_PATIENTS_QUERY))
        print("Path:", GET_ALL_PATIENTS_QUERY)
        with open(GET_ALL_PATIENTS_QUERY, 'r', encoding='utf-8') as f:
            print("SQL content:\n", f.read())
        # Use existing fetch_records with the query file
        all_patients = self.db.fetch_records(GET_ALL_PATIENTS_QUERY, ())

        if not all_patients:
            return []

        results = []
        for patient_id, first_name, last_name, _ in all_patients:  # Ignore sex with _
            try:
                # Call existing single patient function - no changes needed
                analysis = self.analyze_patient_hematological_state(patient_id, snapshot_date)
                # Enhance with patient info from the query
                analysis['first_name'] = first_name
                analysis['last_name'] = last_name
                results.append(analysis)
            except Exception as e:
                # Handle individual patient errors gracefully
                results.append({
                    'patient_id': patient_id,
                    'first_name': first_name,
                    'last_name': last_name,
                    'error': str(e)
                })

        return results




