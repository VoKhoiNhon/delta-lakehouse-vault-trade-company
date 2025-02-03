from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
from libs.utils.company_verifications import verify_regex_name_flow  # pure_name_verify
import pyspark.sql.functions as F
from libs.utils.delta_utils import delta_insert_for_hubnlink


class Executer(RawVaultExecuter):
    def transform(self):
        # target = self.meta_table_model
        # target = self.spark.read.format("delta").load(target.data_location)
        # target.createOrReplaceTempView("l_company_verification")
        source = self.input_dataframe_dict["silver.s_company_regex_name"]
        df = source.dataframe
        # if self.query:
        #     df = df.where(self.query)
        #     print(f"filter: {self.query} | df.count(): {df.count():,}")
        # df.createOrReplaceTempView("h_company")

        verified_df = verify_regex_name_flow(
            df, tmp_path="s3a://lakehouse-silver/tmp/l_company_verification"
        )
        verified_df = verified_df.where("verify_method is not null").selectExpr(
            "jurisdiction",
            "dv_hashkey_company",
            "dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "registration_number",
            "name",
            "pure_name",
            "regexed_name",
            "verified_registration_number",
            "verified_dv_hashkey_company",
            "verified_name",
            "verified_regexed_name",
            "verify_method",
        )

        return verified_df

    def execute(self):
        df = self.transform()
        if df.count() > 0:
            print(
                f"Write to {self.meta_table_model.table_name} - {self.meta_table_model.data_location}"
            )
            from delta.tables import DeltaTable

            DeltaTable.forPath(self.spark, self.meta_table_model.data_location).delete(
                "1=1"
            )

            delta_insert_for_hubnlink(
                spark=self.spark,
                df=df,
                data_location=self.meta_table_model.data_location,
                base_cols=self.meta_table_model.unique_key,
                struct_type=self.meta_table_model.struct_type,
                partition_by=self.meta_table_model.partition_by,
            )


def run(spark=None, payload={}):
    # run full, no query
    import sys

    table_meta = TableMeta(
        from_files="metas/silver/l_company_verification.yaml", payload=payload
    )

    model = table_meta.model
    model.data_location = "s3a://lakehouse-silver/company_verification/regex"
    executer = Executer(
        app_name=sys.argv[0],
        meta_table_model=model,
        meta_input_resource=table_meta.input_resources,
        params={},
        spark=spark,
    )
    executer.execute()


if __name__ == "__main__":
    run()
