from libs.executers.bronze_executer import BronzeExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F

# from pyspark.sql import DataFrame
# from pyspark.sql.types import StructType
from libs.utils.support import flatten_df
from libs.utils.commons import (
    clean_column_names,
    clean_string_columns,
    add_load_date_columns,
)


class Executer(BronzeExecuter):
    def transform(self):
        df_company_detail = self.input_dataframe_dict["raw_turkey_company_detail"]
        df_company_branch_address = self.input_dataframe_dict[
            "raw_turkey_company_branch_address"
        ]
        df_company_officals = self.input_dataframe_dict["raw_turkey_company_officials"]

        df_company_detail = df_company_detail.dataframe
        df_company_branch_address = df_company_branch_address.dataframe
        df_company_officals = df_company_officals.dataframe

        df_company_detail = flatten_df(df_company_detail)
        df_company_branch_address = flatten_df(df_company_branch_address)
        df_company_officals = flatten_df(df_company_officals)

        df_company_branch_address = df_company_branch_address.select(
            [
                F.col(c).alias("branch_" + c) if c != "key" else F.col(c)
                for c in df_company_branch_address.columns
            ]
        )

        df_company_officals = df_company_officals.select(
            [
                F.col(c).alias("officials_" + c) if c != "key" else F.col(c)
                for c in df_company_officals.columns
            ]
        )

        merged_df = df_company_detail
        merged_df = merged_df.join(df_company_branch_address, on="key", how="left")
        merged_df = merged_df.join(df_company_officals, on="key", how="left")

        merged_df = merged_df.withColumn(
            "data_Alerts",
            F.when(
                F.regexp_replace(
                    F.lower(
                        F.col("data_Alerts").getItem(F.size(F.col("data_Alerts")) - 1)
                    ),
                    r"\.",
                    "",
                ).contains("kapalı"),
                "inactive",
            ).otherwise("active"),
        )

        df = merged_df.select(
            F.col("key").alias("registration_number"),
            F.col("data_CompanyTitle").alias("name"),
            F.col("data_BusinessIssueOfTheFirm").alias("description"),
            F.col("data_DateOfEstablishmentReg").alias("date_incorporated"),
            F.col("data_Alerts").alias("status"),
            F.col("data_OfficeAddress").alias("address"),
            F.col("data_NaceCodes").alias("industry"),
            F.col("data_ProfessionalGroup").alias("category"),
            F.col("data_FaxNumber").alias("fax_numbers"),
            F.col("data_PhoneNumber").alias("phone_number"),
            F.col("data_WebPageLink").alias("websites"),
            F.col("officials_data").alias("employee"),
        ).drop_duplicates()
        df = add_load_date_columns(df)
        df = clean_column_names(df)
        df.show(n=10)
        return df


def run():
    table_meta = TableMeta(from_files="metas/bronze/turkey/turkey.yaml")
    executer = Executer(
        app_name="turkey_raw_to_bronze",
        meta_table_model=table_meta.model,
        meta_input_resource=table_meta.input_resources,
    )
    executer.execute()


if __name__ == "__main__":
    run()
# def run(env="pro", params={}):
#     table_meta_link_position = TableMeta(
#         from_files="metas/bronze/turkey/turkey.yaml", env=env
#     )
#     executer_link_position = Executer(
#         "bronze_turkey",
#         table_meta_link_position.model,
#         table_meta_link_position.input_resources,
#         params,
#     )
#     executer_link_position.execute()


# if __name__ == "__main__":
#     import argparse

#     parser = argparse.ArgumentParser()
#     args = parser.parse_args()
#     env = args.env
#     params = {"dv_source_version": args.dv_source_version}
#     run(env=args, params=params)
