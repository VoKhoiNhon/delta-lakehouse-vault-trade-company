from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
from libs.utils.vault_hashfuncs import DV_HASHKEY_COMPANY
from libs.utils.commons import add_pure_company_name


class SCompanyDemographicExecuter(SatelliteVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["Dat_trade_172"]
        df = source.dataframe
        record_source = source.record_source
        # record_source = source.record_source
        import_df = df.select(
            F.col("import_country").alias("jurisdiction"),
            F.col("importer").alias("name"),
            "dv_recsrc",
            "data_source",
        )

        export_df = df.select(
            F.col("export_country_or_area").alias("jurisdiction"),
            F.col("exporter").alias("name"),
            "dv_recsrc",
            "data_source",
        )
        df = import_df.union(export_df).dropDuplicates()
        df = df.withColumn(
            "jurisdiction",
            F.when(F.col("jurisdiction").isNull(), F.lit("unspecified")).otherwise(
                F.col("jurisdiction")
            ),
        )
        df = add_pure_company_name(df).withColumn("registration_number", F.lit(None))
        df = df.selectExpr(
            f"{DV_HASHKEY_COMPANY} as dv_hashkey_company",
            f"'{record_source}' as dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "concat_ws(';',data_source , '20240108') as dv_source_version",
            "jurisdiction",
            "name",
            "pure_name",
        )
        # df.show(5)
        return df


def run(env="pro", params={}, spark=None):
    import sys

    table_meta = TableMeta(
        from_files="metas/silver/s_company_demographic.yaml", env=env
    )
    executer = SCompanyDemographicExecuter(
        sys.argv[0],
        table_meta.model,
        table_meta.input_resources,
        params,
        spark,
    )
    executer.execute()


if __name__ == "__main__":
    run()
