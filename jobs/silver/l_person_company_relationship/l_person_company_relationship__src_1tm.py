from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
from libs.utils.vault_hashfuncs import (
    DV_HASHKEY_FROM_PERSON,
    DV_HASHKEY_TO_COMPANY,
    DV_HASHKEY_L_PERSON_COMPANY_POSITION,
)


class LinkPositionExecuter(RawVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["1tm_2412.entity_link_personvscomp"]
        df = source.dataframe
        record_source = source.record_source

        df = (
            df.selectExpr(
                f"{DV_HASHKEY_FROM_PERSON} as dv_hashkey_person",
                f"{DV_HASHKEY_TO_COMPANY} as dv_hashkey_company",
                f"'{record_source}' as dv_recsrc",
                "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
                "'1tm' as dv_source_version",
                "from_jurisdiction",
                "to_jurisdiction",
                "position",
                "position_code",
                "start_date",
                "end_date",
            )
            .withColumn(
                "dv_hashkey_l_person_company_relationship",
                F.expr(DV_HASHKEY_L_PERSON_COMPANY_POSITION),
            )
            .where("position is not null")
        )
        return df


def run(env="pro", params={}, spark=None):
    table_meta_link_company_company_relationship = TableMeta(
        from_files="metas/silver/l_person_company_relationship.yaml", env=env
    )
    import sys

    executer_link_relationship = LinkPositionExecuter(
        sys.argv[0],
        table_meta_link_company_company_relationship.model,
        table_meta_link_company_company_relationship.input_resources,
        params,
        spark,
    )
    executer_link_relationship.execute()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--env", type=str, help="env", default="pro")
    parser.add_argument(
        "--dv_source_version", type=str, help="dv_source_version", default=""
    )
    args = parser.parse_args()
    print(args)
    env = args.env
    params = {"dv_source_version": args.dv_source_version}
    run(env=env, params=params)  # Changed from env=args to env=env
