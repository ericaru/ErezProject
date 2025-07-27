import json
import os
from backend.backend_config import RULES_FOLDER

class RuleProcessor:
    """
    Rule processor for loading and applying JSON-based clinical rules.
    """

    def __init__(self, rules_folder=RULES_FOLDER):
        self.rules_folder = rules_folder

    def load_rule(self, rule_path):
        """
        Load rule configuration from JSON file.

        Args:
            rule_path (str): Path to JSON rule file

        Returns:
            dict: Complete rule configuration including parameters and lookup table
        """
        if os.path.isfile(rule_path):
            with open(rule_path, 'r') as file:
                return json.load(file)
        else:
            raise FileNotFoundError(f"Rule file not found: {rule_path}")

    def apply_rule(self, rule_path, input_values):
        """
        Execute rule logic against provided input values.

        Args:
            rule_path (str): Path to JSON rule file
            input_values (dict): Dictionary of parameter names and their values

        Returns:
            str or None: Rule outcome or None if no matching condition found
        """
        rule_data = self.load_rule(rule_path)

        # Build lookup key by combining input values in parameter order
        key = ",".join(str(input_values.get(param, ""))
                       for param in rule_data["input_parameters"])

        return rule_data["rules"].get(key, None)


if __name__ == "__main__":
    # Test the rule processor
    processor = RuleProcessor()

    # Test hematological rules
    test_input = {
        "hemoglobin_state": "Severe Anemia",
        "wbc_level": "Low"
    }

    try:
        from backend.backend_config import HEMATOLOGICAL_RULES

        result = processor.apply_rule(HEMATOLOGICAL_RULES, test_input)
        print(f"Test input: {test_input}")
        print(f"Result: {result}")
    except Exception as e:
        print(f"Error testing rule processor: {e}")