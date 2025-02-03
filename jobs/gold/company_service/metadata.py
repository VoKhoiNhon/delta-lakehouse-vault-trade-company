from pyspark.sql import functions as F

from libs.executers.gold_executer import GoldExecuter
from libs.meta import TableMeta


class MetadataExecuter(GoldExecuter):
    def transform(self):
        company_demographic = self.input_dataframe_dict[
            "silver.bridge_company_demographic"
        ].dataframe

        if self.query:
            company_demographic = company_demographic.where(self.query)
            print(
                f"filter {self.query} \n"
                f"metadata.count(): {company_demographic.count():,}"
            )

        metadata = company_demographic.withColumn(
            "data_source",
            F.when(
                F.col("dv_recsrc").contains(";"),
                F.regexp_extract(F.col("dv_recsrc"), "^([^;]*)", 1),
            ).otherwise(F.col("dv_recsrc")),
        )

        metadata = metadata.withColumn(
            "link",
            F.when(
                F.col("dv_recsrc").contains(";"),
                F.regexp_extract(F.col("dv_recsrc"), ";(.*?)$", 1),
            ).otherwise(F.lit(None)),
        )

        metadata = metadata.select(
            F.md5(
                F.concat_ws(";", F.col("dv_hashkey_company"), F.col("dv_recsrc"))
            ).alias("id"),
            "dv_recsrc",
            "dv_loaddts",
            "dv_source_version",
            F.col("dv_hashkey_company").alias("entity_id"),
            "data_source",
            "link",
        ).filter(F.col("data_source") != "")

        return metadata


def run(env="env", params={}, spark=None, payload={}):
    import sys

    table_meta = TableMeta(
        from_files=["metas/gold/company_service/metadata.yaml"],
        env=env,
        payload=payload,
    )

    executer_metadata = MetadataExecuter(
        sys.argv[0],
        table_meta.model,
        table_meta.input_resources,
        params,
        spark,
    )

    executer_metadata.execute()


if __name__ == "__main__":
    run()
