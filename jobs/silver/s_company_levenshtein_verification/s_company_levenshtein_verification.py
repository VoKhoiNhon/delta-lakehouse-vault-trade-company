from libs.executers.raw_vault_executer import SatelliteSCD2Executer
from libs.meta import TableMeta
from libs.utils.company_verifications import levenshtein_verify


class SCompanyLevenshteinVerificationExecuter(SatelliteSCD2Executer):
    def transform(self):
        source = self.input_dataframe_dict["silver.h_company"]
        df = source.dataframe

        trusted_company_df = df.where("has_regnum = 1")
        to_verify_df = df.where("has_regnum = 0")
        print("trusted_company_df", trusted_company_df.count())
        print("to_verify_df", to_verify_df.count())

        verified_df = levenshtein_verify(trusted_company_df, to_verify_df, 95)

        verified_df = verified_df.selectExpr(
            "dv_hashkey_company",
            "dv_recsrc",
            "jurisdiction",
            "name",
            "pure_name",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "1 as dv_status",
            "DATE_SUB(CAST(FROM_UTC_TIMESTAMP(NOW(), 'UTC') AS DATE), 1) as dv_valid_from",
            "NULL as dv_valid_to",
            "verified_registration_number",
            "verified_dv_hashkey_company",
            "verified_name",
            "verified_pure_name",
            "verified_recsrc",
            "distance",
            "similarity_percentage",
        )
        verified_df.show()
        return verified_df


def run(env="pro", params={}, spark=None):
    import sys

    table_meta = TableMeta(
        from_files="metas/silver/s_company_levenshtein_verification.yaml", env=env
    )
    executer = SCompanyLevenshteinVerificationExecuter(
        sys.argv[0],
        table_meta.model,
        table_meta.input_resources,
        params,
        spark,
    )
    executer.execute()


if __name__ == "__main__":
    run()
