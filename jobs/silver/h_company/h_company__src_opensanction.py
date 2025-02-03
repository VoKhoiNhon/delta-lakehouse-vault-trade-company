import pyspark.sql.functions as F
from pyspark.sql import DataFrame
import sys

from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
from libs.utils.vault_hashfuncs import DV_HASHKEY_COMPANY


class HCompanyExecuter(RawVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["bronze.opensanctions"]
        record_source = source.record_source
        df = source.dataframe
        df = df.filter(F.col("type") == "COMPANY")
        dv_source_version = self.params.get("dv_source_version", "")
        return df.selectExpr(
            f"{DV_HASHKEY_COMPANY} as dv_hashkey_company",
            f"'{record_source}' as dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            f"'{dv_source_version}' as dv_source_version",
            "jurisdiction",
            "registration_number",
            "name",
            """
            case
                when registration_number is null then 'reg_num_not_exists'
                else 'reg_num_exists' end as type
            """,
        )


def run(env="pro", params={}):
    table_meta_link_position = TableMeta(
        from_files="metas/silver/h_company.yaml", env=env
    )

    executer_link_position = HCompanyExecuter(
        sys.argv[0],
        table_meta_link_position.model,
        table_meta_link_position.input_resources,
        params,
    )
    executer_link_position.execute()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    args = parser.parse_args()
    env = args.env
    params = {"dv_source_version": args.dv_source_version}
    run(env=args, params=params)
