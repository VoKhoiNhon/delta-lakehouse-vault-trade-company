from libs.utils.connection import SparkSessionBuilder
from libs.meta import TableMeta
import os
from pyspark.sql.types import StructType, StructField


def capture_schemas(df, file_name: str = ""):
    """
    Writes the schema of a Spark DataFrame to a Python file in a way that can be imported.

    Parameters:
        df (pyspark.sql.DataFrame): The Spark DataFrame whose schema is to be written.
        file_name (str): The name of the Python file where the schema will be saved.
    """

    def generate_schema_code(schema, indent=0):
        """
        Recursively generates Python code for a StructType schema.

        Parameters:
            schema (StructType): The schema to convert into code.
            indent (int): Current indentation level for nested structures.

        Returns:
            str: Python code representing the schema.
        """
        indent_space = "    " * indent
        schema_code = "StructType([\n"
        for field in schema.fields:
            field_type = field.dataType
            if isinstance(field_type, StructType):
                # Nested StructType
                nested_schema = generate_schema_code(field_type, indent + 1)
                schema_code += f"{indent_space}    StructField('{field.name}', {nested_schema}, {field.nullable}),\n"
            else:
                # Primitive type
                schema_code += f"{indent_space}    StructField('{field.name}', {type(field_type).__name__}(), {field.nullable}),\n"
        schema_code = schema_code.rstrip(",\n") + f"\n{indent_space}])"
        return schema_code

    try:
        # Create directory if it does not exist
        directory = os.path.dirname(file_name)
        if directory and not os.path.exists(directory):
            os.makedirs(directory)
            print(f"Directory '{directory}' created.")

        # Generate schema Python code
        full_schema_code = generate_schema_code(df.schema)

        # Full Python code to write into the file
        python_code = (
            f"from pyspark.sql.types import StructType, StructField, {', '.join(set(type(f.dataType).__name__ for f in df.schema.fields))}"
            + f"\nschema = {full_schema_code}"
        )

        # Write the schema to the Python file
        with open(file_name, "w") as file:
            file.write(python_code)

        print(f"Schema successfully written to {file_name}")
    except Exception as e:
        print(f"An error occurred: {e}")


def write_sample_data(df, file_name: str, num_rows: int = 5):
    """
    Writes a sample of the DataFrame to a file in a vertical format.

    Parameters:
        df (pyspark.sql.DataFrame): The Spark DataFrame to sample data from.
        file_name (str): The name of the file where the sample data will be written.
        num_rows (int): The number of rows to write. Default is 5.
    """
    try:
        # Limit and collect sample data
        sample_data = df.limit(num_rows).collect()

        # Create directory if it does not exist
        directory = os.path.dirname(file_name)
        if directory and not os.path.exists(directory):
            os.makedirs(directory)
            print(f"Directory '{directory}' created.")

        # Write sample data to file
        with open(file_name, "w") as file:
            for idx, row in enumerate(sample_data):
                file.write(f"Row {idx + 1}:\n")
                for col, value in row.asDict().items():
                    file.write(f"{col}: {value}\n")
                file.write("-" * 40 + "\n")

        print(f"Sample data successfully written to {file_name}")
    except Exception as e:
        print(f"An error occurred while writing sample data: {e}")


def create_spark_session(spark_app_name: str = "default"):
    user_config = {
        "spark.app.name": spark_app_name,
        "spark.driver.memory": "2g",
        "spark.executor.memory": "20g",
        "spark.executor.memoryOverhead": "1g",
        "spark.executor.cores": "10",
        "spark.executor.instances": "1",
        "spark.executor.heartbeatInterval": "60s",
        "spark.network.timeout": "900s",
        "spark.dynamicAllocation.enabled": "true",
        "spark.dynamicAllocation.minExecutors": "1",
        "spark.dynamicAllocation.maxExecutors": "1",
        "spark.sql.autoBroadcastJoinThreshold": "100MB",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.files.openCostInBytes": "134217728",
        "spark.sql.files.maxPartitionBytes": "256MB",
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": "/opt/bitnami/spark/logs",
        "spark.history.fs.logDirectory": "/opt/bitnami/spark/logs",
        "spark.sql.parquet.enableVectorizedReader": "false",
        ## ELS
        # "spark.es.nodes":"10.1.0.88",
        # "spark.es.port":"9200",
        # "spark.es.nodes.wan.only": "true",
    }
    spark = SparkSessionBuilder(user_config=user_config).create_spark_session(
        log_level="WARN"
    )
    # Test SparkContext
    if spark.sparkContext._jsc.sc().isStopped():
        print("SparkContext is NOT running!")
    else:
        print("SparkContext is running. Connection Successful!")
    return spark


def capture_all_schemas(mapping: dict):
    spark = create_spark_session(spark_app_name="capture_schemas")
    for table_meta_file, payload in mapping.items():
        table_meta = TableMeta(from_files=[table_meta_file], payload=payload)
        filter_input_resources = payload.get("filter_input_resources", [])
        input_resources = table_meta.input_resources
        for source in input_resources:
            if getattr(source, "table_name") in filter_input_resources:
                try:
                    df = (
                        spark.read.format(source.format)
                        .options(**source.options)
                        .load(source.data_location)
                    )
                    output_filename_schemas = (
                        "input_resources." + getattr(source, "table_name") + ".py"
                    )
                    output_filename_sample = (
                        "samples.input_resources."
                        + getattr(source, "table_name")
                        + ".txt"
                    )
                    capture_schemas(
                        df=df, file_name=f"schemas/{output_filename_schemas}"
                    )
                    write_sample_data(
                        df=df, file_name=f"samples/{output_filename_sample}"
                    )
                except Exception as e:
                    print(e)
                    pass

    for table_meta_file, payload in mapping.items():
        table_meta = TableMeta(from_files=[table_meta_file], payload=payload)
        model = table_meta.model
        try:
            df = spark.read.format(model.data_format).load(model.data_location)
            output_filename_schemas = "model." + getattr(model, "table_name") + ".py"
            output_filename_sample = (
                "samples.model." + getattr(source, "table_name") + ".txt"
            )
            capture_schemas(df=df, file_name=f"schemas/{output_filename_schemas}")
            write_sample_data(df=df, file_name=f"samples/{output_filename_sample}")
        except Exception as e:
            print(e)
            pass

    spark.stop()
