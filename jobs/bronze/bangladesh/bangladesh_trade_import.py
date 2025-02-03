from libs.executers.bronze_executer import BronzeExecuter
from libs.meta import TableMeta
from libs.utils.commons import (
    clean_column_names,
    clean_string_columns,
    add_load_date_columns,
    clean_jurisdiciton_1tm,
    add_pure_company_name,
)
from pyspark.sql.functions import col, explode, split


class Executer(BronzeExecuter):
    def transform(self):
        df = self.input_dataframe_dict["raw_bangladesh_trade_import"].dataframe
        df = df.select(
            col("data.detail.amount").alias("amount"),
            col("data.detail.assessed_unit_price_in_fc").alias(
                "assessed_unit_price_in_fc"
            ),
            col("data.detail.assessed_value_in_bdt").alias("assessed_value_in_bdt"),
            col("data.detail.be_date").alias("be_date"),
            col("data.detail.be_no").alias("be_no"),
            col("data.detail.bill_id").alias("bill_id"),
            col("data.detail.billid").alias("billid"),
            col("data.detail.bol_number").alias("bol_number"),
            col("data.detail.buyer").alias("buyer"),
            col("data.detail.customs_office_code").alias("customs_office_code"),
            col("data.detail.date").alias("date"),
            col("data.detail.declared_unit_price_in_fc").alias(
                "declared_unit_price_in_fc"
            ),
            col("data.detail.descript").alias("descript"),
            col("data.detail.exchange_rate").alias("exchange_rate"),
            col("data.detail.foreign_currency").alias("foreign_currency"),
            col("data.detail.g_weight_in_kg").alias("g_weight_in_kg"),
            col("data.detail.hs").alias("hs"),
            col("data.detail.hs_codd_desc").alias("hs_codd_desc"),
            col("data.detail.importer_address").alias("importer_address"),
            col("data.detail.importer_id").alias("importer_id"),
            col("data.detail.manifest_no").alias("manifest_no"),
            col("data.detail.n_weight_in_kg").alias("n_weight_in_kg"),
            col("data.detail.origin_country").alias("origin_country"),
            col("data.detail.quantity").alias("quantity"),
            col("data.detail.seller").alias("seller"),
            col("data.detail.total_customs_duties_cd_rd_sd_vat_fp_tk").alias(
                "total_customs_duties_cd_rd_sd_vat_fp_tk"
            ),
            col("data.detail.type").alias("type"),
            col("data.detail.type_of_package").alias("type_of_package"),
            col("data.detail.unit_of_quantity").alias("unit_of_quantity"),
            col("data.raw").alias("raw"),
        )
        df = df.withColumn("name", col("seller"))
        df = add_load_date_columns(df=df, date_value=self.meta_table_model.load_date)
        df = clean_column_names(df)
        df = add_pure_company_name(df=df, name="name", return_col="pure_name")
        # df = clean_string_columns(df)
        # df.show(n=5)
        return df


def run(payload=None):
    table_meta = TableMeta(
        from_files="metas/bronze/bangladesh/bangladesh_trade_import.yaml",
        payload=payload,
    )
    executer = Executer(
        app_name="bangladesh_trade_import_raw_to_bronze",
        meta_table_model=table_meta.model,
        meta_input_resource=table_meta.input_resources,
        spark_resources=table_meta.spark_resources,
    )
    executer.execute()


if __name__ == "__main__":
    run()
