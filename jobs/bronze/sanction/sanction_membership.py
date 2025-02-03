# Example run:
# python3 jobs/bronze/sanction/sanction_membership.py
# --payload '{"load_date":"20241128", "env":"/test_on_s3"}'
from libs.executers.bronze_executer import BronzeExecuter
from libs.meta import TableMeta
from libs.utils.commons import (
    clean_column_names,
    clean_string_columns,
    add_load_date_columns,
    clean_jurisdiciton_1tm,
    add_pure_company_name,
)
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, ArrayType


class Executer(BronzeExecuter):
    def flatten_company(self, df):
        # Flatten the `properties` struct
        properties_columns = [
            F.col(f"properties.{col}").alias(col)
            for col in df.select("properties.*").columns
        ]

        # Explode and flatten arrays if necessary (optional)
        flatten_columns = [
            F.explode_outer(F.col(column)).alias(column)
            if "array" in str(df.schema[column].dataType)
            else F.col(column)
            for column in df.columns
            if column != "properties"
        ]

        # Create a new DataFrame with flattened columns
        flattened_df = df.select(flatten_columns + properties_columns)

        return flattened_df

    def transform(self):
        df = self.input_dataframe_dict["raw.sanction.entity_link.membership"].dataframe
        df = self.flatten_company(df)
        df = add_load_date_columns(df=df, date_value=self.meta_table_model.load_date)
        df = clean_column_names(df)
        df.createOrReplaceTempView("sanction_membership")

        # Spark SQL query to filter and drop columns
        query = """
            SELECT
                schema,
                member[0] AS from_entity_id,
                organization[0] AS to_entity_id,
                CASE WHEN REGEXP_EXTRACT(start_date[0], '\\d{4}-\\d{2}-\\d{2}') != '' THEN DATE(start_date[0]) WHEN REGEXP_EXTRACT(start_date[0], '\\d{4}-\\d{2}') != '' THEN DATE(CONCAT(start_date[0], '-01')) ELSE DATE(CONCAT(start_date[0], '-01-01')) END AS start_date,
                CASE WHEN REGEXP_EXTRACT(end_date[0], '\\d{4}-\\d{2}-\\d{2}') != '' THEN DATE(end_date[0]) WHEN REGEXP_EXTRACT(end_date[0], '\\d{4}-\\d{2}') != '' THEN DATE(CONCAT(end_date[0], '-01')) ELSE DATE(CONCAT(end_date[0], '-01-01')) END AS end_date,
                CASE
                    WHEN LOWER(caption) LIKE '%director%' THEN 'Director'
                    WHEN LOWER(caption) LIKE '%owner%' THEN 'Beneficial Owner'
                    WHEN LOWER(caption) LIKE '%employee%' THEN 'Employee'
                    WHEN LOWER(caption) LIKE '%agent%' THEN 'Registered Agent'
                    ELSE caption
                END AS position,
                CASE
                    WHEN LOWER(caption) LIKE '%director%' THEN 1
                    WHEN LOWER(caption) LIKE '%owner%' THEN 2
                    WHEN LOWER(caption) LIKE '%employee%' THEN 5
                    WHEN LOWER(caption) LIKE '%agent%' THEN 4
                    ELSE 0
                END AS position_code,
                load_date
            FROM sanction_membership
            WHERE schema = 'Membership'
        """

        # Execute the query and create a new DataFrame
        df = self.spark.sql(query)
        # df.show(n=5)
        return df


def run(payload=None):
    table_meta = TableMeta(
        from_files="metas/bronze/sanction/sanction_entitylink.yaml", payload=payload
    )
    executer = Executer(
        app_name="bronze.sanction.entity_link.membership",
        meta_table_model=table_meta.model,
        meta_input_resource=table_meta.input_resources,
        spark_resources=table_meta.spark_resources,
        filter_input_resources=['raw.sanction.entity_link.membership']
    )
    executer.execute()


if __name__ == "__main__":
    run()
