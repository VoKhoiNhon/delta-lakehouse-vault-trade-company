from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
from libs.utils.company_verifications import levenshtein_verify
from libs.utils.delta_utils import delta_insert_for_hubnlink
from libs.utils.commons import run_with_juris_splited
from pyspark.sql import functions as F


class Executer(RawVaultExecuter):
    def write_each_batch(self, to_verify_df, trusted_company_df):
        verified_df = levenshtein_verify(
            trusted_company_df,
            to_verify_df,
            base_col="pure_name",
            verify_method="levenshtein_pure_name_no_regis_vs_has_regis",
            threshold=95,
            num_firt_chars=8,
            num_diff_length=2,
            num_diff_space=2,
        )

        levenshtein_path = "s3a://lakehouse-silver/company_verification/levenshtein"

        delta_insert_for_hubnlink(
            spark=self.spark,
            df=verified_df,
            data_location=levenshtein_path,
            # data_location=self.meta_table_model.data_location,
            base_cols=self.meta_table_model.unique_key,
            struct_type=self.meta_table_model.struct_type,
            partition_by=self.meta_table_model.partition_by,
        )

    def execute(self, n_group, to_verify_filter=""):
        target = self.meta_table_model
        target = self.spark.read.format("delta").load(target.data_location)
        target.createOrReplaceTempView("l_company_verification")

        source = self.input_dataframe_dict["silver.h_company"]
        df = source.dataframe

        df.createOrReplaceTempView("company")

        # and jurisdiction in ('US.Texas', 'US.Pennsylvania')
        trusted_company_df = self.spark.sql(
            """
        select * from company h
        where registration_number is not null
        and not exists (
            select 1 from l_company_verification l
            where verify_method is not null  -- chưa bị thành slave
            and h.dv_hashkey_company = l.dv_hashkey_company)
        """
        )

        to_verify_df = self.spark.sql(
            """
            select * from company h
            where registration_number is  null
            and NOT EXISTS (
                SELECT 1 FROM l_company_verification l
                WHERE verify_method IS NOT NULL
                AND h.dv_hashkey_company = l.dv_hashkey_company -- chưa bị thành slave, master thì vẫn ok
                AND verify_method <> 'regexed_name_no_regis_vs_no_regis'
            )
        """
        )

        # if self.query:
        #     to_verify_df = to_verify_df.where(self.query)
        #     print(
        #         f"filter to_verify: {self.query} | df.count(): {to_verify_df.count():,}"
        #     )

        if n_group > 1:
            run_with_juris_splited(
                to_verify_df, n_group, self.write_each_batch, trusted_company_df
            )
        else:
            self.write_each_batch(to_verify_df, trusted_company_df)


def runnb(env="pro", params={}, spark=None, n_group=15, payload={}):

    import sys

    table_meta = TableMeta(
        from_files="metas/silver/l_company_verification.yaml", env=env, payload=payload
    )
    model = table_meta.model
    model.data_location = "s3a://lakehouse-silver/company_verification/levenshtein"
    executer = Executer(
        sys.argv[0],
        model,
        table_meta.input_resources,
        params,
        spark,
    )
    executer.execute(n_group)


def run(spark=None, n_group=2, payload=None):

    import sys

    table_meta = TableMeta(
        from_files="metas/silver/l_company_verification.yaml", payload=payload
    )
    model = table_meta.model
    model.data_location = "s3a://lakehouse-silver/company_verification/levenshtein"
    executer = Executer(
        app_name=sys.argv[0],
        meta_table_model=model,
        meta_input_resource=table_meta.input_resources,
        params=table_meta.context,
        spark=spark,
    )
    executer.execute(n_group)


if __name__ == "__main__":
    run()
