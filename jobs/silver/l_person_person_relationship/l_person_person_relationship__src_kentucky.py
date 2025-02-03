from libs.executers.raw_vault_executer import LinkVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
from libs.utils.vault_hashfuncs import (
    dv_hashkey_to_person,
    dv_hashkey_from_person,
    DV_HASHKEY_L_PERSON_PERSON_RELATED,
)


class LinkPersonPersonExecuter(LinkVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["bronze.kentucky"]
        record_source = source.record_source
        df = source.dataframe
        dv_source_version = self.params.get("dv_source_version", "")

        df = df.filter(F.col("full_name_from").isNotNull())
        df = df.filter(F.col("full_name_to").isNotNull())
        df = df.filter(F.col("related_type").isNotNull())

        df = df.withColumn(
            "dv_hashkey_to_person", F.expr(dv_hashkey_to_person)
        ).withColumn("dv_hashkey_from_person", F.expr(dv_hashkey_from_person))
        print(df.columns)

        df = df.selectExpr(
            f"{DV_HASHKEY_L_PERSON_PERSON_RELATED} as dv_hashkey_related_type",
            f"'{record_source}' as dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            f"'{dv_source_version}' as dv_source_version",
            "dv_hashkey_to_person",
            "dv_hashkey_from_person",
            "related_type",
            "jurisdiction",
        )
        df = df.filter(F.col("dv_hashkey_related_type").isNotNull())
        df.show(truncate=False)
        return df


def run(env="pro", params={}):
    table_meta_link_person_person_relationship = TableMeta(
        from_files="metas/silver/l_person_person_relationship.yaml", env=env
    )
    executer_link_person_person_relationship = LinkPersonPersonExecuter(
        "silver/l_person_person_relationship__src_kentucky",
        table_meta_link_person_person_relationship.model,
        table_meta_link_person_person_relationship.input_resources,
        params,
    )
    executer_link_person_person_relationship.execute()


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
