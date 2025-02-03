import os
import json

from libs.executers.bronze_executer import BronzeExecuter
from libs.meta import TableMeta
from libs.utils.base_transforms import transform_column_from_json

from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.window import Window


class Executer(BronzeExecuter):
    def transform(self):
        df = self.input_dataframe_dict["raw_tanzania"]

        # Drop redundant columns
        df = df.drop(
            *[
                "id",
                "object_type",
                "company_subtype",
                "ba_category",
                "reg_status",
                "has_charges",
                "last_change_tracking_no",
                "update_date",
            ]
        )

        # Remove NULL and dots in col cert_number
        df = df.filter(F.col("cert_number").isNotNull())
        df = df.dropDuplicates(["cert_number"])
        df = df.withColumn("cert_number", regexp_replace(col("cert_number"), "\.", ""))
        df = df.orderBy(F.col("cert_number").asc())

        # Rename col
        df = (
            df.withColumnRenamed("cert_number", "registration_number")
            .withColumnRenamed("reg_date", "registration_date")
            .withColumnRenamed("legal_name", "company_name")
            .withColumnRenamed("subtype_name", "company_type")
            .withColumnRenamed("reg_status_name", "status")
        )

        return df.withColumn("load_date", F.current_date())


def run():
    table_meta = TableMeta(from_files="./metas/bronze/tanzania/tanzania.yaml")

    executer = Executer(
        "bronze_tanzania",
        table_meta.model,
        table_meta.input_resources,
    )

    executer.execute()


if __name__ == "__main__":
    run()
