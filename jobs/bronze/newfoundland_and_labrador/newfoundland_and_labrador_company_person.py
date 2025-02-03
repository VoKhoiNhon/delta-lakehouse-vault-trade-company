from libs.executers.bronze_executer import BronzeExecuter
from libs.meta import TableMeta
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from libs.utils.commons import (
    clean_column_names,
    clean_string_columns,
    add_load_date_columns,
    add_pure_company_name,
)


class Executer(BronzeExecuter):
    def fill_null(self, df_company: DataFrame) -> DataFrame:
        values = ["null", "N/A", "NONE", "x x", "d d"]
        return df_company.select(
            [
                F.when(F.col(column).isin(values), None)
                .otherwise(F.col(column))
                .alias(column)
                for column in df_company.columns
            ]
        )

    def transform(self):
        df = self.input_dataframe_dict[
            "raw_newfoundland_and_labrador_company"
        ].dataframe
        df = clean_column_names(df)
        df = df.select(
            "business_type",
            "company_name",
            "company_number",
            "corporation_type",
            "director_1",
            "director_2",
            "director_3",
            "director_4",
            "director_5",
            "director_6",
            "director_7",
            "director_8",
            "incorporation_date",
            "registered_office_address_1",
            "registered_office_address_2",
            "registered_office_address_3",
            "registered_office_city",
            "registered_office_postal_zip_code",
            "registered_office_province_state",
            "registered_office_outside_nl_address_1",
            "registered_office_outside_nl_address_2",
            "registered_office_outside_nl_city",
            "registered_office_outside_nl_country",
            "registered_office_outside_nl_postal_zip_code",
            "registered_office_outside_nl_province_state",
            "registration_date",
            "status",
        )
        df = self.fill_null(df)
        df = df.withColumn(
            "legal_form",
            F.when(
                F.col("corporation_type").isNotNull(),
                F.concat_ws(" - ", F.col("corporation_type"), F.col("business_type")),
            ).otherwise(F.col("business_type")),
        )
        df = df.withColumn(
            "is_branch",
            F.when(F.col("legal_form").contains("Foreign"), False).otherwise(True),
        )
        df = df.filter(F.col("status") != "System Error")
        df = df.withColumn(
            "main_full_address",
            F.concat_ws(
                " ",
                F.col("registered_office_address_1"),
                F.col("registered_office_address_2"),
                F.col("registered_office_address_3"),
                F.col("registered_office_postal_zip_code"),
                F.col("registered_office_province_state"),
            ),
        )
        df = df.withColumn(
            "mailing_full_address",
            F.concat_ws(
                " ",
                F.col("registered_office_outside_nl_address_1"),
                F.col("registered_office_outside_nl_address_2"),
                F.col("registered_office_outside_nl_city"),
                F.col("registered_office_outside_nl_postal_zip_code"),
                F.col("registered_office_outside_nl_country"),
            ),
        )
        df = clean_string_columns(df)
        df = df.withColumn(
            "date_incorporated",
            F.when(
                F.col("incorporation_date").isNull(), F.col("registration_date")
            ).otherwise(F.col("incorporation_date")),
        )

        df = df.withColumn(
            "date_incorporated", F.to_date(F.col("date_incorporated"), "YYYY-MM-DD")
        )
        df = (
            df.select(
                "*",
                F.explode(
                    F.array(
                        F.col("director_1"),
                        F.col("director_2"),
                        F.col("director_3"),
                        F.col("director_4"),
                        F.col("director_5"),
                        F.col("director_6"),
                        F.col("director_7"),
                        F.col("director_8"),
                    )
                ).alias("officer_name"),
            )
            .withColumn("position", F.lit("Director"))
            .withColumn("position_code", F.lit(1))
        )
        df = df.withColumn(
            "officer_name", F.regexp_replace(F.col("officer_name"), ".*Elected.*", "")
        )

        df = df.withColumn(
            "officer_name", F.regexp_replace(F.col("officer_name"), "\\s+$", "")
        )
        df = add_pure_company_name(df=df, name="company_name")
        df = df.withColumn(
            "status_code",
            F.when(
                F.col("status").isin(["Active", "Active (See Note)"]),
                0,
            )
            .when(
                F.col("status").isin(
                    [
                        "Dissolved (Prior To June 2004)",
                        "Dissolved - Involuntary",
                        "Dissolved - Voluntary",
                        "Cancelled - Voluntary",
                        "Cancelled - Involuntary",
                        "Cancelled",
                        "Liquidated",
                        "Discontinued",
                        "Inactive",
                    ]
                ),
                1,
            )
            .otherwise(3),
        )
        df = add_load_date_columns(df=df, date_value=self.meta_table_model.load_date)

        df.show(n=2)
        df.printSchema()
        return df


def run():
    table_meta = TableMeta(
        from_files="metas/bronze/newfoundland_and_labrador/newfoundland_and_labrador_company_person.yaml"
    )
    executer = Executer(
        app_name="newfoundland_and_labrador_company_raw_to_bronze",
        meta_table_model=table_meta.model,
        meta_input_resource=table_meta.input_resources,
    )
    executer.execute()


if __name__ == "__main__":
    run()
