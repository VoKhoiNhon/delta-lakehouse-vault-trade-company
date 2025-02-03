import pyspark.sql.functions as F
from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
import libs.utils.vault_hashfuncs as H


class LbolBangladeshImportExecuter(RawVaultExecuter):
    def transform(self):
        input_dataframe_dict = self.input_dataframe_dict
        bangladesh_trade_import = input_dataframe_dict[
            "silver.bangladesh.import"
        ].dataframe
        company_bangladesh = input_dataframe_dict[
            "silver.h_company.bangladesh"
        ].dataframe
        company_all = input_dataframe_dict["silver.h_company.all"].dataframe
        record_source = input_dataframe_dict[
            "silver.bangladesh.import"
        ].record_source.replace("'", "\\'")
        dv_source_version = input_dataframe_dict[
            "silver.bangladesh.import"
        ].dv_source_version.replace("'", "\\'")
        bangladesh_trade_import.createOrReplaceTempView("bangladesh_trade_import")
        company_bangladesh.createOrReplaceTempView("company_bangladesh")
        company_all.createOrReplaceTempView("company_all")
        query = f"""
         select
            {H.DV_HASHKEY_BOL} as dv_hashkey_bol,
            {H.DV_HASHKEY_L_BOL} as dv_hashkey_l_bol,
            '{record_source}' as dv_recsrc,
            '{dv_source_version}' as dv_source_version,
            FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts,
            supplier_dv_hashkey_company,
            buyer_dv_hashkey_company,
            import_port,
            export_port,
            bol,
            actual_arrival_date,
            jurisdiction
         from (select
                coalesce(ca.dv_hashkey_company,'CANT MAP') as supplier_dv_hashkey_company,
                coalesce(cb.dv_hashkey_company,'CANT MAP') as buyer_dv_hashkey_company,
                NULL as import_port,
                NULL as export_port,
                bte.*
            from bangladesh_trade_import  bte
            left join company_bangladesh cb
                on bte.pure_buyer = cb.pure_name
                and bte.buyer_country = cb.jurisdiction
            left join company_all ca
                on bte.pure_seller = ca.pure_name
                and bte.seller_country = ca.jurisdiction) joined_df
        """
        df = self.spark.sql(query)
        return df


def run(payload=None):

    table_meta = TableMeta(from_files="metas/silver/l_bol.yaml", payload=payload)
    executer = LbolBangladeshImportExecuter(
        app_name="silver_l_bol_bangladesh_import",
        meta_table_model=table_meta.model,
        meta_input_resource=table_meta.input_resources,
        spark_resources=table_meta.spark_resources,
        filter_input_resources=[
            "silver.bangladesh.import",
            "silver.h_company.bangladesh",
            "silver.h_company.all",
        ],
    )
    executer.execute()


if __name__ == "__main__":
    run()
