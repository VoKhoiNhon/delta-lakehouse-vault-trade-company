import sys
import pyspark.sql.functions as F
from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
from libs.utils.vault_hashfuncs import DV_HASHKEY_COMPANY


class HCompanyExecuter(RawVaultExecuter):
    def transform(self):
        source_sanction = self.input_dataframe_dict["bronze.sanction.company"]
        record_source = str(getattr(source_sanction, "record_source")).replace(
            "'", "\\'"
        )
        dv_source_version = str(getattr(source_sanction, "dv_source_version")).replace(
            "'", "\\'"
        )
        sanction_df = source_sanction.dataframe
        sanction_df = sanction_df.withColumnRenamed("company_name", "name")
        return sanction_df.selectExpr(
            f"{DV_HASHKEY_COMPANY} as dv_hashkey_company",
            f"'{record_source}' as dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            f"'{dv_source_version}' as dv_source_version",
            "jurisdiction",
            "registration_number",
            "name",
            "pure_name",
            """
            case
                when registration_number is null then 'reg_num_not_exists'
                else 'reg_num_exists' end as has_regnum
            """,
        )


def run(payload=None):
    table_meta = TableMeta(from_files=["metas/silver/h_company.yaml"], payload=payload)

    executer = HCompanyExecuter(
        app_name="bronze.h_company.sanction",
        meta_table_model=table_meta.model,
        meta_input_resource=table_meta.input_resources,
        spark_resources=table_meta.spark_resources,
        filter_input_resources=["bronze.sanction.company"],
    )
    executer.execute()


if __name__ == "__main__":
    run()
