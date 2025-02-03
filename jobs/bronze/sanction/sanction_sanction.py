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
        df = self.input_dataframe_dict["raw.sanction.sanction"].dataframe
        df = self.flatten_company(df)
        df = add_load_date_columns(df=df, date_value=self.meta_table_model.load_date)
        df = clean_column_names(df)
        df.createOrReplaceTempView("sanction_sanction")

        # Spark SQL query to filter and drop columns
        query = """
            SELECT
                caption,
                datasets,
                first_seen,
                id,
                last_change,
                last_seen,
                CONCAT_WS('-', authority) AS authority,
                authority_id,
                country[0] AS country,
                date,
                description,
                duration,
                end_date[0] AS end_date,
                entity[0] AS entity,
                listing_date,
                modified_at,
                program,
                provisions,
                publisher,
                record_id,
                CONCAT_WS('-', reason) AS reason,
                source_url[0] AS source_url,
                start_date[0] AS start_date,
                status,
                summary,
                unsc_id,
                schema,
                load_date
            FROM sanction_sanction
            WHERE schema = 'Sanction'
        """

        # Execute the query and create a new DataFrame
        df = self.spark.sql(query)
        # df.show(n=5)
        return df


def run(payload=None):
    table_meta = TableMeta(
        from_files="metas/bronze/sanction/sanction_sanction.yaml", payload=payload
    )
    executer = Executer(
        app_name="bronze.sanction.sanction",
        meta_table_model=table_meta.model,
        meta_input_resource=table_meta.input_resources,
        spark_resources=table_meta.spark_resources,
        filter_input_resources=['raw.sanction.sanction']
    )
    executer.execute()


if __name__ == "__main__":
    run()
