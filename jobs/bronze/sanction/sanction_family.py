# Example run:
# python3 jobs/bronze/sanction/sanction_family.py
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
        df = self.input_dataframe_dict["raw.sanction.entity_link.family"].dataframe
        df = self.flatten_company(df)
        df = add_load_date_columns(df=df, date_value=self.meta_table_model.load_date)
        df = clean_column_names(df)
        df.createOrReplaceTempView("sanction_family")

        # Spark SQL query to filter and drop columns
        query = """
            select
                sanction_family_alias.*,
                to_date(CASE WHEN raw_start_date RLIKE '\\d{4}-\\d{2}-\\d{2}' THEN raw_start_date WHEN raw_start_date RLIKE '\\d{4}-\\d{2}' THEN CONCAT(raw_start_date, '-01') ELSE CONCAT(raw_start_date, '-01-01') END, 'yyyy-MM-dd' ) AS start_date,
                to_date(CASE WHEN raw_end_date RLIKE '\\d{4}-\\d{2}-\\d{2}' THEN raw_end_date WHEN raw_end_date RLIKE '\\d{4}-\\d{2}' THEN CONCAT(raw_end_date, '-01') ELSE CONCAT(raw_end_date, '-01-01') END, 'yyyy-MM-dd' ) AS end_date
            from (
                SELECT
                    datasets,
                    first_seen,
                    id,
                    last_change,
                    last_seen,
                    referents,
                    date,
                    end_date[0] AS raw_end_date,
                    modified_at,
                    person[0] AS from_entity_id,
                    relationship,
                    relative[0] AS to_entity_id,
                    source_url,
                    start_date[0] AS raw_start_date,
                    schema,
                    load_date
                FROM sanction_family
                WHERE schema = 'Family'
            ) as sanction_family_alias
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
        app_name="bronze.sanction.entity_link.family",
        meta_table_model=table_meta.model,
        meta_input_resource=table_meta.input_resources,
        spark_resources=table_meta.spark_resources,
        filter_input_resources=['raw.sanction.entity_link.family']
    )
    executer.execute()


if __name__ == "__main__":
    run()
