import yaml
import pandas as pd


class DataModelValidator:
    def __init__(self, yaml_config_path: str, dataframe: pd.DataFrame):
        """
        Initialize the validator with a YAML configuration file.
        :param yaml_config_path: Path to the YAML file containing model definitions.
        """
        with open(yaml_config_path, "r") as file:
            self.config = yaml.safe_load(file)

        self.columns = self.config["model"]["output_trino_columns"]
        self.dataframe = dataframe

    def validate(self):
        """
        Validate a given pandas DataFrame against the model configuration.
        :param dataframe: Pandas DataFrame to validate.
        :return: A report dictionary with validation results.
        """
        report = {"valid": True, "errors": []}
        df_dtypes = dict(self.dataframe.dtypes)
        df_columns = [str(col_name).lower() for col_name in self.dataframe.columns]

        for column_config in self.columns:
            col_name = str(column_config["name"]).lower()
            col_type = str(column_config["type"]).lower()
            tests = column_config.get("tests", [])

            # Check if the column exists in the dataframe
            if col_name not in self.dataframe.columns:
                report["valid"] = False
                report["errors"].append(f"Column '{col_name}' is missing.")
                continue

            # Validate data type
            if not self._validate_type(df_dtypes.get(col_name), col_type):
                report["valid"] = False
                report["errors"].append(
                    f"Column '{col_name}' has incorrect type. Expected '{col_type}'."
                )

            # Apply additional tests
            for test in tests:
                check = test["check"]
                if not self._apply_test(col_name=col_name, check=check):
                    report["valid"] = False
                    report["errors"].append(
                        f"Column '{col_name}' failed the '{check}' test."
                    )

        is_valid = report.get("valid")
        return is_valid, report

    @staticmethod
    def _validate_type(column_dtype: str, expected_type: str) -> bool:
        """
        Validate the data type of a column.
        :param column: Pandas Series representing a column.
        :param expected_type: Expected type as string (e.g., "string", "int").
        :return: True if the column matches the type, False otherwise.
        """
        type_mapping = {
            "varchar": "string",
            "integer": "integer",
            "bigint": "long",
            "real": "float",
            "double": "double",
            "boolean": "boolean",
            "date": "date",
            "timestamp": "timestamp",
            "varbinary": "binary",
            "decimal": "decimal",
            "array": "array",
            "map": "map",
            "row": "struct",
        }
        pandas_type = type_mapping.get(expected_type.lower())
        return column_dtype == pandas_type

    def _apply_test(self, col_name: str, check: str) -> bool:
        """
        Apply a specific test to a column.
        :param col_name: Name of the column to apply the test to.
        :param check: The test to apply (e.g., "isComplete", "isUnique").
        :return: True if the test passes, False otherwise.
        """
        column = self.dataframe[col_name]

        if check == "isComplete":
            # Check if the column has no null values
            return self.dataframe.filter(column.isNull()).count() == 0
        elif check == "isUnique":
            # Check if the column values are unique
            distinct_count = self.dataframe.select(col_name).distinct().count()
            total_count = self.dataframe.count()

            return distinct_count == total_count

        return True
