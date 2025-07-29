import pandas as pd
from datetime import datetime
from backend.dataaccess import DataAccess
from backend.mediator import Mediator
from backend.rule_processor import RuleProcessor
from backend.backend_config import *


class SimpleRuleEngine:
    """
    Simple rule engine implementing:
    - 2:1 AND table for Hematological state
    - 4:1 Table Lookup for Systemic Toxicity
    Uses AbstractedMeasurements table and temporal overlap validation.
    """

    def __init__(self):
        self.db = DataAccess()
        self.mediator = Mediator()
        self.rule_processor = RuleProcessor()

    def get_hematological_state(self, hemoglobin_state, wbc_level):
        """
        Apply 2:1 AND rule to get hematological state using JSON rules.
        """
        if not hemoglobin_state or not wbc_level:
            return None

        input_values = {
            "hemoglobin_state": hemoglobin_state,
            "wbc_level": wbc_level
        }

        return self.rule_processor.apply_rule(HEMATOLOGICAL_RULES, input_values)

    def get_systemic_toxicity_grade(self, fever_level, chills, skin_look, allergic_state):
        """
        Apply 4:1 Table Lookup rule for systemic toxicity grade.

        Uses the rule processor to perform exact table lookup.

        Args:
            fever_level (str): Abstracted fever level ("Normal-Elevated", "High", "Very High")
            chills (str): Abstracted chills value ("None", "Shaking", "Rigor")
            skin_look (str): Abstracted skin-look value ("Erythema", "Vesiculation", "Desquamation", "Exfoliation")
            allergic_state (str): Abstracted allergic state ("Edema", "Bronchospasm", "Sever-Bronchospasm", "Anaphylactic-Shock")

        Returns:
            str: Toxicity grade ("GRADE I", "GRADE II", "GRADE III", "GRADE IV") or None if no exact match
        """
        # All 4 parameters must have values
        if not all([fever_level, chills, skin_look, allergic_state]):
            return None

        # Prepare input values for rule processor
        input_values = {
            "fever_level": fever_level,
            "chills": chills,
            "skin_look": skin_look,
            "allergic_state": allergic_state
        }

        # Use rule processor to apply the systemic toxicity rule
        return self.rule_processor.apply_rule(SYSTEMIC_TOXICITY_RULES, input_values)

    def get_latest_abstracted_value(self, patient_id, concept_name):
        """
        Get the latest abstracted value for a specific concept from AbstractedMeasurements table.

        Args:
            patient_id (str): Patient identifier
            concept_name (str): Name of the abstracted concept to search for

        Returns:
            str or None: The latest abstracted value, or None if no record found.
        """
        # Use the query from the queries folder
        result = self.db.fetch_records(GET_LATEST_ABSTRACTED_VALUE_QUERY, (patient_id, concept_name))

        if not result:
            return None

        # Return just the value
        return result[0][0]

    def check_temporal_overlap(self, records):
        """
        Check if multiple abstracted records have temporal overlap.

        Args:
            records (list): List of record dictionaries with start_time and end_time

        Returns:
            bool: True if all records have overlapping time periods, False otherwise
        """
        if not records or len(records) < 2:
            return True

        # Convert all times to datetime objects for comparison
        datetime_records = []
        for record in records:
            if record is None:
                return False

            try:
                start_dt = pd.to_datetime(record['start_time'])
                end_dt = pd.to_datetime(record['end_time'])
                datetime_records.append({'start': start_dt, 'end': end_dt})
            except:
                return False

        # Find the latest start time and earliest end time
        latest_start = max(record['start'] for record in datetime_records)
        earliest_end = min(record['end'] for record in datetime_records)

        # There's overlap if latest start is before earliest end
        return latest_start <= earliest_end

    def analyze_patient_hematological_state(self, patient_id):
        """
        Analyze patient's hematological state using the 2:1 AND rule with temporal validation.

        Gets latest abstracted values for Hemoglobin_Level and WBC_Level from AbstractedMeasurements table,
        checks for temporal overlap, and applies the hematological rule if valid.

        Args:
            patient_id (str): Patient identifier

        Returns:
            dict: Hematological analysis results
        """

        # Get latest abstracted values for required concepts
        hemoglobin_record = self.get_latest_abstracted_value(patient_id, "Hemoglobin_Level")
        wbc_record = self.get_latest_abstracted_value(patient_id, "WBC_Level")

        # Check if both records exist
        if not hemoglobin_record or not wbc_record:
            return {
                'patient_id': patient_id,
                'individual_states': {
                    'hemoglobin_state': hemoglobin_record['value'] if hemoglobin_record else None,
                    'wbc_level': wbc_record['value'] if wbc_record else None
                },
                'hematological_state': None,
                'error': 'Missing required abstracted data for hematological analysis'
            }

        # Check temporal overlap between the records
        if not self.check_temporal_overlap([hemoglobin_record, wbc_record]):
            return {
                'patient_id': patient_id,
                'individual_states': {
                    'hemoglobin_state': hemoglobin_record['value'],
                    'wbc_level': wbc_record['value']
                },
                'hematological_state': None,
                'error': 'No temporal overlap between hemoglobin and WBC measurements'
            }

        # Extract values and apply rule
        hemoglobin_state = hemoglobin_record['value']
        wbc_level = wbc_record['value']
        hematological_state = self.get_hematological_state(hemoglobin_state, wbc_level)

        return {
            'patient_id': patient_id,
            'individual_states': {
                'hemoglobin_state': hemoglobin_state,
                'wbc_level': wbc_level
            },
            'hematological_state': hematological_state,
            'temporal_overlap': True
        }

    def analyze_patient_systemic_toxicity(self, patient_id):
        """
        Analyze patient's systemic toxicity using 4:1 Table Lookup rule with temporal validation.

        Gets latest abstracted values for all 4 required concepts from AbstractedMeasurements table,
        checks for temporal overlap, and applies the systemic toxicity rule if valid.

        Args:
            patient_id (str): Patient identifier

        Returns:
            dict: Systemic toxicity analysis results
        """

        # Get latest abstracted values for all 4 required concepts
        fever_record = self.get_latest_abstracted_value(patient_id, "Fever_Level")
        chills_record = self.get_latest_abstracted_value(patient_id, "Chills")
        skin_record = self.get_latest_abstracted_value(patient_id, "Skin-Look")
        allergic_record = self.get_latest_abstracted_value(patient_id, "Allergic-State")

        # Collect all records for validation
        all_records = [fever_record, chills_record, skin_record, allergic_record]
        concept_names = ["Fever_Level", "Chills", "Skin-Look", "Allergic-State"]

        # Check if all records exist
        missing_concepts = []
        for i, record in enumerate(all_records):
            if not record:
                missing_concepts.append(concept_names[i])

        if missing_concepts:
            return {
                'patient_id': patient_id,
                'individual_states': {
                    'fever_level': fever_record['value'] if fever_record else None,
                    'chills': chills_record['value'] if chills_record else None,
                    'skin_look': skin_record['value'] if skin_record else None,
                    'allergic_state': allergic_record['value'] if allergic_record else None
                },
                'systemic_toxicity_grade': None,
                'error': f'Missing required abstracted data for: {", ".join(missing_concepts)}'
            }

        # Check temporal overlap between all records
        if not self.check_temporal_overlap(all_records):
            return {
                'patient_id': patient_id,
                'individual_states': {
                    'fever_level': fever_record['value'],
                    'chills': chills_record['value'],
                    'skin_look': skin_record['value'],
                    'allergic_state': allergic_record['value']
                },
                'systemic_toxicity_grade': None,
                'error': 'No temporal overlap between all required measurements'
            }

        # Extract values and apply rule
        fever_level = fever_record['value']
        chills = chills_record['value']
        skin_look = skin_record['value']
        allergic_state = allergic_record['value']

        systemic_toxicity_grade = self.get_systemic_toxicity_grade(
            fever_level, chills, skin_look, allergic_state
        )

        return {
            'patient_id': patient_id,
            'individual_states': {
                'fever_level': fever_level,
                'chills': chills,
                'skin_look': skin_look,
                'allergic_state': allergic_state
            },
            'systemic_toxicity_grade': systemic_toxicity_grade,
            'temporal_overlap': True
        }

    def analyze_treatment(self, patient_id, gender, hematological_analysis, systemic_toxicity_analysis):
        """
        Analyze treatment recommendations using the 4:1 treatment lookup table.

        Args:
            patient_id (str): Patient identifier
            gender (str): Patient gender ("Male" or "Female")
            hematological_analysis (dict): Results from hematological analysis
            systemic_toxicity_analysis (dict): Results from systemic toxicity analysis

        Returns:
            dict: Treatment analysis results
        """
        # Check for errors in input analyses
        if 'error' in hematological_analysis:
            return {
                'patient_id': patient_id,
                'treatment_recommendations': None,
                'error': f'Hematological analysis failed: {hematological_analysis["error"]}'
            }

        if 'error' in systemic_toxicity_analysis:
            return {
                'patient_id': patient_id,
                'treatment_recommendations': None,
                'error': f'Systemic toxicity analysis failed: {systemic_toxicity_analysis["error"]}'
            }

        # Extract required values
        hemoglobin_state = hematological_analysis['individual_states']['hemoglobin_state'] #to_check
        hematological_state = hematological_analysis['hematological_state']
        systemic_toxicity_grade = systemic_toxicity_analysis['systemic_toxicity_grade']

        # Check if all required values are available
        if not all([gender, hemoglobin_state, hematological_state, systemic_toxicity_grade]):
            missing = []
            if not gender: missing.append('gender')
            if not hemoglobin_state: missing.append('hemoglobin_state')
            if not hematological_state: missing.append('hematological_state')
            if not systemic_toxicity_grade: missing.append('systemic_toxicity_grade')

            return {
                'patient_id': patient_id,
                'treatment_recommendations': None,
                'error': f'Missing required parameters for treatment analysis: {", ".join(missing)}'
            }

        # Prepare input values for rule processor
        input_values = {
            "gender": gender,
            "hemoglobin_state": hemoglobin_state,
            "hematological_state": hematological_state,
            "systemic_toxicity_grade": systemic_toxicity_grade
        }

        # Use rule processor to get treatment recommendations
        treatment_recommendations = self.rule_processor.apply_rule(TREATMENT_RULES, input_values)

        return {
            'patient_id': patient_id,
            'clinical_inputs': {
                'gender': gender,
                'hemoglobin_state': hemoglobin_state,
                'hematological_state': hematological_state,
                'systemic_toxicity_grade': systemic_toxicity_grade
            },
            'treatment_recommendations': treatment_recommendations
        }

