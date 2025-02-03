# Example run:
# python3 jobs/bronze/sanction/sanction_person.py
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
        df = self.input_dataframe_dict["raw.sanction.person"].dataframe
        df = self.flatten_company(df)
        df = add_load_date_columns(df=df, date_value=self.meta_table_model.load_date)
        df = clean_column_names(df)
        df.createOrReplaceTempView("sanction_person")

        # Spark SQL query to filter and drop columns
        query = """
            select
                sanction_person_alias.*,
                CONCAT_WS('\\n',CONCAT('description: ', COALESCE(description, '')),CONCAT('Note: ', COALESCE(notes, '')),CONCAT('Summary: ', COALESCE(summary, '')),CONCAT('Position: ', COALESCE(position, '')),CONCAT('Education: ', COALESCE(education, ''))) AS description_full,
                CASE WHEN gender = 'female' THEN 1 ELSE 0 END AS gender_numeric
            from (
                select
                    array_distinct(array_union(array_union(array_union(COALESCE(alias, ARRAY()),COALESCE(name, ARRAY())),COALESCE(previous_name, ARRAY())),COALESCE(second_name, ARRAY()))) AS other_names,
                    caption AS person_name,
                    birth_date[0] AS dob,
                    birth_place[0] AS birthplace,
                    address[0] AS address,
                    incorporation_date[0] AS date_incorporated,
                    COALESCE(citizenship[0], nationality[0]) AS nationality,
                    CASE WHEN inn_code IS NOT NULL THEN CONCAT_WS('-', inn_code) ELSE NULL END AS inn_code,
                    CASE WHEN lei_code IS NOT NULL THEN CONCAT_WS('-', lei_code) ELSE NULL END AS lei_code,
                    CASE WHEN ogrn_code IS NOT NULL THEN CONCAT_WS('-', ogrn_code) ELSE NULL END AS ogrn_code,
                    CASE WHEN icij_id IS NOT NULL THEN CONCAT_WS('-', icij_id) ELSE NULL END AS icij_id,
                    CASE WHEN id_number IS NOT NULL THEN CONCAT_WS('-', id_number) ELSE NULL END AS id_number,
                    CASE WHEN npi_code IS NOT NULL THEN CONCAT_WS('-', npi_code) ELSE NULL END AS npi_code,
                    CASE WHEN passport_number IS NOT NULL THEN CONCAT_WS('-', passport_number) ELSE NULL END AS passport_number,
                    CASE WHEN registration_number IS NOT NULL THEN CONCAT_WS('-', registration_number) ELSE NULL END AS registration_number,
                    CASE WHEN tax_number IS NOT NULL THEN CONCAT_WS('-', tax_number) ELSE NULL END AS tax_number,
                    status[0] AS status,
                    CASE WHEN position IS NOT NULL THEN CONCAT_WS(' - ', position) ELSE NULL END AS position,
                    CASE WHEN education IS NOT NULL THEN CONCAT_WS(' - ', education) ELSE NULL END AS education,
                    CASE WHEN description IS NOT NULL THEN CONCAT_WS(' - ', description) ELSE NULL END AS description,
                    CASE WHEN notes IS NOT NULL THEN CONCAT_WS(' - ', notes) ELSE NULL END AS notes,
                    CASE WHEN summary IS NOT NULL THEN CONCAT_WS(' - ', summary) ELSE NULL END AS summary,
                    CASE WHEN array_contains(topics, 'sanction') THEN TRUE ELSE FALSE END AS is_sanctioned,
                    email AS emails,
                    phone AS phone_numbers,
                    website AS websites,
                    program[0] AS program,
                    title[0] AS title,
                    CASE WHEN first_name IS NOT NULL THEN CONCAT_WS(' - ', first_name) ELSE NULL END AS first_name,
                    CASE WHEN last_name IS NOT NULL THEN CONCAT_WS(' - ', last_name) ELSE NULL END AS last_name,
                    CASE WHEN middle_name IS NOT NULL THEN CONCAT_WS(' - ', middle_name) ELSE NULL END AS middle_name,
                    CONCAT_WS(' ', title, name) AS full_name,
                    gender[0] AS gender,
                    CASE WHEN country[0] IS NULL THEN 'Unknown' ELSE country[0] END AS country_name,
                    schema,
                    load_date
                FROM sanction_person
                WHERE schema = 'Person'
            ) as sanction_person_alias
        """

        # Execute the query and create a new DataFrame
        df = self.spark.sql(query)
        # df.show(n=5)
        return df


def run(payload=None):
    table_meta = TableMeta(
        from_files="metas/bronze/sanction/sanction_person.yaml", payload=payload
    )
    executer = Executer(
        app_name="bronze.sanction.person",
        meta_table_model=table_meta.model,
        meta_input_resource=table_meta.input_resources,
        spark_resources=table_meta.spark_resources,
        filter_input_resources=['raw.sanction.person']
    )
    executer.execute()


if __name__ == "__main__":
    run()
