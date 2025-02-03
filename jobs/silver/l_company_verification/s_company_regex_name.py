from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
from libs.utils.company_verifications import add_pure_company_name_regex
from pyspark.sql.types import StructType
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
import pyspark.sql.functions as F


class Executer(RawVaultExecuter):
    def transform(self, filter=None):
        source = self.input_dataframe_dict["silver.h_company"]
        df = source.dataframe

        # if self.query:
        #     df = df.where(self.query)
        #     print(f"filter: {self.query} | df.count(): {df.count():,}")

        df = add_pure_company_name_regex(df)
        df = df.selectExpr(
            "dv_hashkey_company",
            "dv_recsrc",
            "dv_source_version",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "jurisdiction",
            "registration_number",
            "name",
            "pure_name",
            "regexed_name",
            "pure_name_replaced_by",
        )
        return df


def run(spark=None, payload=None):
    # params={},
    import sys

    table_meta = TableMeta(
        from_files="metas/silver/s_company_regex_name.yaml", payload=payload
    )

    executer = Executer(
        app_name=sys.argv[0],
        meta_table_model=table_meta.model,
        meta_input_resource=table_meta.input_resources,
        params=table_meta.context,
        spark=spark,
    )
    executer.execute()


if __name__ == "__main__":
    run()
