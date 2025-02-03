import yaml
import json
import os
import re
from pyspark.sql import SparkSession
from libs.utils.logger import DriverLogger


class SparkSessionBuilder:
    def __init__(
        self, config_path: str = "configs/connection.yaml", user_config: dict = {}
    ):
        """
        Initialize the Trino connection from a YAML configuration file or direct parameters.

        :param config_path: Path to the YAML configuration file
        :param user_config: Custom SparkApplication Config from User
        """
        self.config_path = config_path
        self.user_config = user_config
        self.default_config = self._load_config()
        self.driver_logger = DriverLogger().get_logger()

    def _load_config(self):
        """
        Load configuration from a YAML file.
        :return: Configuration dictionary
        """
        try:
            with open(self.config_path, "r") as file:
                spark_config = yaml.safe_load(file)["spark"]
                spark_config.update(self._load_priv_key())
            return spark_config
        except Exception as e:
            raise Exception(f"Error loading configuration: {e}")

    def _load_priv_key(self):
        priv_keys = {
            "spark.hadoop.fs.s3a.endpoint": os.environ.get(
                "spark_hadoop_fs_s3a_endpoint", "..."
            ),
            "spark.hadoop.fs.s3a.access.key": os.environ.get(
                "spark_hadoop_fs_s3a_access_key", "..."
            ),
            "spark.hadoop.fs.s3a.secret.key": os.environ.get(
                "spark_hadoop_fs_s3a_secret_key", "..."
            ),
            "spark.master": os.environ.get("spark_master", "spark://spark-master:7077"),
        }
        return priv_keys

    def create_spark_session(self, log_level: str = "WARN"):
        """
        Create a Spark session using the merged configuration of the default and user-provided settings.
        Returns: SparkSession: The created Spark session.
        """
        try:
            # Merge the default configuration with the user-provided configuration
            merged_config = self.default_config.copy()
            merged_config.update(self.user_config)

            # Create the Spark session
            spark_builder = SparkSession.builder.appName(
                merged_config.get("spark.app.name")
            ).master(merged_config.get("spark.master"))
            # Set log level to DEBUG
            for key, value in merged_config.items():
                spark_builder = spark_builder.config(key, value)

            spark_session = spark_builder.getOrCreate()
            spark_session.sparkContext.setLogLevel(log_level)
            self.driver_logger.info(
                f"Created SparkSession: {spark_session.sparkContext}"
            )
            return spark_session
        except Exception as e:
            raise f"Failed to create Spark session: {e}"


class S3Storage:
    def __init__(
        self,
        config_path: str = "configs/connection.yaml",
        bucket_name: str = "datalake",
    ):
        """
        Initialize the S3Storage class with the given bucket name.

        Parameters:
        bucket_name (str): The name of the S3 bucket.
        """
        if config_path:
            self.config_path = config_path
            self.bucket_name = bucket_name
            self.default_config = self._load_config()
            import boto3

            self.s3_client = boto3.client("s3", **self.default_config)
        else:
            raise ValueError("Configuration file path is required.")

    def upload_file(self, file_path, s3_path):
        """
        Upload a file to the specified S3 bucket.

        Parameters:
        file_path (str): The local path to the file to upload.
        s3_path (str): The path in the S3 bucket where the file will be stored.
        """
        try:
            self.s3_client.upload_file(file_path, self.bucket_name, s3_path)
            print(
                f"File {file_path} uploaded to s3://{self.bucket_name}/{s3_path} successfully."
            )
        except Exception as e:
            print(f"Failed to upload file: {e}")

    def download_file(self, s3_path, file_path):
        """
        Download a file from the specified S3 bucket.

        Parameters:
        s3_path (str): The path in the S3 bucket to download from.
        file_path (str): The local path where the file will be stored.
        """
        try:
            self.s3_client.download_file(self.bucket_name, s3_path, file_path)
            print(
                f"File s3://{self.bucket_name}/{s3_path} downloaded to {file_path} successfully."
            )
        except Exception as e:
            print(f"Failed to download file: {e}")

    def list_files_with_regex(self, path_regex):
        """
        List files in the S3 bucket that match a given regex pattern.

        Parameters:
        path_regex (str): The regex pattern to match file paths.

        Returns:
        list: A list of matching file paths.
        """
        try:
            bucket_name = re.match(r"s3[a-z]?://([^/]+)/", path_regex).group(1)
            prefix = re.match(r"s3[a-z]?://[^/]+/(.*)", path_regex).group(1)
            prefix = prefix.split("*")[0] if "*" in prefix else prefix

            paginator = self.s3_client.get_paginator("list_objects_v2")
            operation_parameters = {"Bucket": bucket_name, "Prefix": prefix}

            matching_files = []
            for page in paginator.paginate(**operation_parameters):
                if "Contents" in page:
                    for obj in page["Contents"]:
                        matching_regex = path_regex.replace("*", ".*")
                        search_s3_filepath = f"s3a://{bucket_name}/{obj['Key']}"
                        if re.match(matching_regex, search_s3_filepath):
                            matching_files.append(
                                {"path": search_s3_filepath, "size": obj["Size"]}
                            )

            matching_files.sort(key=lambda x: x["size"])
            return [file["path"] for file in matching_files]

        except Exception as e:
            print(f"Failed to list files with regex {path_regex}: {e}")
            return []

    def _load_config(self):
        """
        Implicitly Load and Transform (if need) configuration from a YAML file.

        :param config_path: Path to the YAML file
        :return: Configuration dictionary
        """
        try:
            with open(self.config_path, "r") as file:
                s3_config = yaml.safe_load(file)["s3"]
                s3_config.update(self._load_priv_key())
                return s3_config
        except Exception as e:
            raise Exception(f"Error loading configuration: {e}")

    def _load_priv_key(self):
        priv_keys = {
            "endpoint_url": os.environ.get("s3_endpoint_url", "..."),
            "aws_access_key_id": os.environ.get("s3_access_key", "..."),
            "aws_secret_access_key": os.environ.get("s3_secret_key", "..."),
        }
        return priv_keys
