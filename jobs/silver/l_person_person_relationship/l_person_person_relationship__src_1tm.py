from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
from libs.utils.vault_hashfuncs import (
    DV_HASHKEY_L_PERSON_PERSON_RELATED,
    DV_HASHKEY_FROM_PERSON,
    DV_HASHKEY_TO_PERSON,
)


class LinkPositionExecuter(RawVaultExecuter):
    def transform(self):
        # Get person data

        source = self.input_dataframe_dict["1tm_2412.entity_link_personvsperson"]
        df = source.dataframe
        record_source = source.record_source

        df = (
            df.selectExpr(
                f"{DV_HASHKEY_FROM_PERSON} as dv_hashkey_from_person",
                f"{DV_HASHKEY_TO_PERSON} as dv_hashkey_to_person",
                f"'{record_source}' as dv_recsrc",
                "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
                "'1tm' as dv_source_version",
                "from_jurisdiction",
                "to_jurisdiction",
                "related_type",
                "start_date",
                "end_date",
            )
            .withColumn(
                "dv_hashkey_l_person_person_relationship",
                F.expr(DV_HASHKEY_L_PERSON_PERSON_RELATED),
            )
            .where("related_type is not null")
        )
        return df


def run(env="pro", params={}, spark=None):
    table_meta_link_person_person_relationship = TableMeta(
        from_files="metas/silver/l_person_person_relationship.yaml", env=env
    )
    import sys

    executer_link_relationship = LinkPositionExecuter(
        sys.argv[0],
        table_meta_link_person_person_relationship.model,
        table_meta_link_person_person_relationship.input_resources,
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
