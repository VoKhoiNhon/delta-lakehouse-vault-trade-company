# Example run:
# python3 jobs/bronze/sanction/sanction_organization.py
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
        df = self.input_dataframe_dict["raw.sanction.company.organization"].dataframe
        df = self.flatten_company(df)
        df = add_load_date_columns(df=df, date_value=self.meta_table_model.load_date)
        df = clean_column_names(df)
        df.createOrReplaceTempView("sanction_organization")

        # Spark SQL query to filter and drop columns
        query = """
            select
                sanction_organization_alias.*,
                COALESCE(country, jurisdiction,'Unknown') AS country_name,
                CONCAT_WS('\\n',CONCAT('description: ', description),CONCAT('Note: ', notes),CONCAT('Summary: ', summary),CONCAT('Sector: ', sector_combined)) AS description_full
            from (select
                jurisdiction[0] AS jurisdiction,
                ARRAY_DISTINCT(ARRAY_UNION(ARRAY_UNION(COALESCE(alias, ARRAY()),COALESCE(name, ARRAY())),COALESCE(previous_name, ARRAY()))) AS other_names,
                address[0] AS address,
                dissolution_date[0] AS date_struck_off,
                incorporation_date[0] AS date_incorporated,
                CASE WHEN inn_code IS NOT NULL THEN CONCAT_WS('-', inn_code) ELSE NULL END AS inn_code,
                CASE WHEN lei_code IS NOT NULL THEN CONCAT_WS('-', lei_code) ELSE NULL END AS lei_code,
                CASE WHEN ogrn_code IS NOT NULL THEN CONCAT_WS('-', ogrn_code) ELSE NULL END AS ogrn_code,
                CASE WHEN duns_code IS NOT NULL THEN CONCAT_WS('-', duns_code) ELSE NULL END AS duns_code,
                CASE WHEN icij_id IS NOT NULL THEN CONCAT_WS('-', icij_id) ELSE NULL END AS icij_id,
                CASE WHEN id_number IS NOT NULL THEN CONCAT_WS('-', id_number) ELSE NULL END AS id_number,
                npi_code,
                CASE WHEN okpo_code IS NOT NULL THEN CONCAT_WS('-', okpo_code) ELSE NULL END AS okpo_code,
                CASE WHEN vat_code IS NOT NULL THEN CONCAT_WS('-', vat_code) ELSE NULL END AS vat_code,
                CASE WHEN registration_number IS NOT NULL THEN CONCAT_WS('-', registration_number) ELSE NULL END AS registration_number,
                CASE WHEN tax_number IS NOT NULL THEN CONCAT_WS('-', tax_number) ELSE NULL END AS tax_number,
                sector[0] AS sector,
                status[0] AS status,
                program[0] AS program,
                CASE WHEN description IS NOT NULL THEN CONCAT_WS(' - ', description) ELSE NULL END AS description,
                CASE WHEN notes IS NOT NULL THEN CONCAT_WS(' - ', notes) ELSE NULL END AS notes,
                CASE WHEN summary IS NOT NULL THEN CONCAT_WS(' - ', summary) ELSE NULL END AS summary,
                CASE WHEN sector IS NOT NULL THEN CONCAT_WS(' - ', sector) ELSE NULL END AS sector_combined,
                CASE WHEN ARRAY_CONTAINS(topics, 'sanction') AND NOT ARRAY_CONTAINS(topics, 'linked') THEN TRUE ELSE FALSE END AS is_sanctioned,
                email AS emails,
                phone AS phone_numbers,
                website AS websites,
                country[0] as country,
                caption as company_name,
                schema,
                load_date
            FROM sanction_organization
            WHERE schema = 'Organization') as sanction_organization_alias
        """

        # Execute the query and create a new DataFrame
        df = self.spark.sql(query)
        df = add_pure_company_name(df=df, name="company_name", return_col="pure_name")
        # df.show(n=5)
        return df


def run(payload=None):
    table_meta = TableMeta(
        from_files="metas/bronze/sanction/sanction_company.yaml", payload=payload
    )
    executer = Executer(
        app_name="bronze.sanction.company.organization",
        meta_table_model=table_meta.model,
        meta_input_resource=table_meta.input_resources,
        spark_resources=table_meta.spark_resources,
        filter_input_resources=['raw.sanction.company.organization']
    )
    executer.execute()


if __name__ == "__main__":
    run()
