from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
import sys
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import flatten


class SSanctionExecuter(SatelliteVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["bronze.sanction"]
        dv_source_version = self.params.get("dv_source_version", "")
        record_source = source.record_source
        df = source.dataframe
        df = df.withColumn("reason", F.array(F.col("reason")))

        array_columns = [
            "emails",
            "legal_form",
            "phone_numbers",
            "category",
            "websites",
            "father_name",
            "mother_name",
            "other_names",
        ]

        # Create arrays
        for column in array_columns:
            df = df.withColumn(column, F.array(F.col(column)))

        # Then flatten them
        for column in array_columns:
            df = df.withColumn(column, flatten(F.col(column)))
        df = df.filter(F.col("is_sanction") == True)
        df = df.selectExpr(
            "md5(id) as dv_hashkey_sanction",
            f"'{record_source}' as dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "1 as dv_status",
            "DATE_SUB(CAST(FROM_UTC_TIMESTAMP(NOW(), 'UTC') AS DATE), 1) as dv_valid_from",
            "NULL as dv_valid_to",
            f"'{dv_source_version}' as dv_source_version",
            "id",
            "jurisdiction",
            "type",
            "country",
            "authority",
            "program",
            "reason",
            "start_date",
            "end_date",
            "source_link",
        )
        return df


def run():
    table_meta_hub = TableMeta(from_files=["metas/silver/s_sanction.yaml"])
    executer_hub = SSanctionExecuter(
        sys.argv[0],
        table_meta_hub.model,
        table_meta_hub.input_resources,
    )
    executer_hub.execute()


if __name__ == "__main__":
    run()
