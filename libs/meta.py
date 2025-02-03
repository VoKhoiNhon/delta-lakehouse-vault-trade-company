from pyspark.sql.types import StructType
from pyspark.sql import DataFrame
import yaml
from jinja2 import Template
import json
import argparse
from typing import Union, List
from dataclasses import dataclass, field

# Register the custom representer
def double_quote_representer(dumper, data):
    return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='"')


yaml.add_representer(str, double_quote_representer)


@dataclass
class TableMetaModel:
    """
    model = TableMetaModel(**meta['model'])
    """

    table_name: str
    database_name: str
    data_location: str
    unique_key: Union[str, List] = None
    columns: list = field(default_factory=list)
    partition_by: list = field(default_factory=list)
    data_format: str = "delta"
    checkpoint_location: str = None  # for streaming
    options: dict = field(default_factory=dict)
    load_date: str = None
    env: str = None

    def __post_init__(self):
        if self.env == "test":
            self.data_location = (
                self.data_location.replace("s3a://", "tests/resources/")
                + "__datatest_rs"
            )

        self.unique_key = (
            [self.unique_key] if isinstance(self.unique_key, str) else self.unique_key
        )

    @property
    def struct_type(self):
        if not self.columns or self.columns == []:
            return None
        fields = []
        for item in self.columns:
            if not item.get("nullable"):
                item["nullable"] = True
            if not item.get("metadata"):
                item["metadata"] = None
            fields.append(item)

        return StructType.fromJson({"fields": fields})


@dataclass
class TableMetaInputResource:
    """
    input_ = TableMetaModel(**meta['input_resources'])
    """

    table_name: str
    data_location: str
    record_source: str
    process_job: str = None  # A Python script that utilizes these resources
    format: str = "delta"
    filter: str = None  # sql
    options: dict = field(default_factory=dict)
    load_date: str = ""
    spark_resources: dict = field(default_factory=dict)
    dv_source_version: str = ""
    dataframe: DataFrame = None
    env: str = None

    def __post_init__(self):
        if self.env == "test":
            self.data_location = self.data_location.replace(
                "s3a://", "tests/resources/"
            )


def concat_yaml_files(file_paths):
    """Concatenates multiple YAML files by concatenating arrays and replacing simple values."""
    combined_data = {}

    for file_path in file_paths:
        data = yaml.safe_load(open(file_path, "r"))

        for key, value in data.items():
            if key in combined_data:
                if isinstance(value, list) and isinstance(combined_data[key], list):
                    # If both values are lists, concatenate them
                    combined_data[key].extend(value)
                else:
                    # If the value is not a list, replace the value
                    combined_data[key] = value
            else:
                # If the key does not exist, simply add it
                combined_data[key] = value

    return combined_data


def render_yaml_from_object(yaml_object, payload=None):
    """
    Render a Jinja template from a YAML object with a given context.
    Supports both argument parsing via `argparse` and direct `payload` parameter.
    """
    # Determine context from payload or argparse
    if payload is None:
        # Use argparse to fetch payload
        parser = argparse.ArgumentParser(
            description="Render a Jinja template from a YAML object with a given context",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser.add_argument(
            "--payload", help="Context for rendering, as a JSON string", default="{}"
        )
        args = parser.parse_args()
        try:
            context = json.loads(args.payload)
            print("context: ",context)
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON format in --payload argument : {args.payload}")
    else:
        # Process the provided payload directly
        if isinstance(payload, str):
            try:
                context = json.loads(payload)
            except json.JSONDecodeError:
                raise ValueError("Invalid JSON format in payload parameter")
        elif isinstance(payload, dict):
            context = payload
        else:
            raise TypeError("Payload must be a JSON string or dictionary")

    # Convert the YAML object to a string
    yaml_string = yaml.dump(yaml_object)
    # print("yaml_string", yaml_string)
    # Use Jinja to render the string
    template = Template(yaml_string)
    rendered_content = template.render(context)
    # print("rendered_content", rendered_content)

    # Convert the rendered string back to a YAML object
    rendered_yaml = yaml.safe_load(rendered_content)
    return rendered_yaml, context


class TableMeta:
    """
    This class is dedicated to storing and parsing metadata.
    It does not implement any execution, reading, or writing functionalities.

    here is the yaml file: tests/libs/hub_account.yaml
    """

    def __init__(
        self, from_text=None, from_key=None, from_files=None, env="None", payload=None
    ):
        if from_text:
            self.meta = yaml.load(from_text, Loader=yaml.loader.SafeLoader)
        elif from_key:
            import boto3
            s3_client = boto3.client("s3")
            response = s3_client.get_object(Bucket="artifact", Key=from_key)
            self.meta = yaml.load(response["Body"], Loader=yaml.loader.SafeLoader)
        elif from_files:
            if isinstance(from_files, list):
                self.meta = concat_yaml_files(from_files)
            else:
                self.meta = yaml.safe_load(open(from_files, "r"))
        else:
            raise Exception(
                "must define source of metadata : from_text | from_key | from_files"
            )

        # Render Template with UserConfig
        self.meta, self.context = render_yaml_from_object(
            yaml_object=self.meta, payload=payload
        )
        self.model = TableMetaModel(**self.meta["model"])

        self.input_resources = [
            TableMetaInputResource(**item) for item in self.meta["input_resources"]
        ]
        self.spark_resources = self.meta.get("spark_resources", {})

        self.sla = self.meta.get("sla")
        self.reconciles = self.meta.get("reconciles")

    @property
    def deequ_quality_params(self):
        check_suite = []

        for item in self.model.columns:
            name = item["name"]
            tests = item.get("tests")
            if tests:
                for _test in tests:
                    check_suite.append(
                        {
                            "check": _test["check"],
                            "params": (
                                [name, *_test.get("params")]
                                if _test.get("params")
                                else [name]
                            ),
                        }
                    )

        return check_suite
