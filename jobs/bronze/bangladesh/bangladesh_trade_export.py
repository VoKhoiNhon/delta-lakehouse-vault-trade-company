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
        df = self.input_dataframe_dict["raw_bangladesh_trade_export"].dataframe
        df = df.select(
            col("data.detail.bill_id").alias("bill_id"),
            col("data.detail.bill_no").alias("bill_no"),
            col("data.detail.billid").alias("billid"),
            col("data.detail.buyer").alias("buyer"),
            col("data.detail.buyer_country").alias("buyer_country"),
            col("data.detail.cargo_value_local_currency").alias(
                "cargo_value_local_currency"
            ),
            col("data.detail.customs_office_code").alias("customs_office_code"),
            col("data.detail.customs_office_name").alias("customs_office_name"),
            col("data.detail.date").alias("date"),
            col("data.detail.descript").alias("descript"),
            col("data.detail.exporter_address").alias("exporter_address"),
            col("data.detail.g_weight_kg").alias("g_weight_kg"),
            col("data.detail.hs").alias("hs"),
            col("data.detail.hs_code_desc").alias("hs_code_desc"),
            col("data.detail.n_weight_kg").alias("n_weight_kg"),
            col("data.detail.origin_country").alias("origin_country"),
            col("data.detail.qty").alias("qty"),
            col("data.detail.qty_unit").alias("qty_unit"),
            col("data.detail.seller").alias("seller"),
            col("data.detail.seller_country").alias("seller_country"),
            col("data.detail.seller_port").alias("seller_port"),
            col("data.detail.weight").alias("weight"),
            col("data.detail.weight_unit").alias("weight_unit"),
        )
        df = df.withColumn("name", col("seller"))
        df = add_load_date_columns(df=df, date_value=self.meta_table_model.load_date)
        df = clean_column_names(df)
        df = add_pure_company_name(df=df, name="name", return_col="pure_name")
        # df = clean_string_columns(df)
        return df


def run(payload=None):
    table_meta = TableMeta(
        from_files="metas/bronze/bangladesh/bangladesh_trade_export.yaml",
        payload=payload,
    )
    executer = Executer(
        app_name="bangladesh_trade_export_raw_to_bronze",
        meta_table_model=table_meta.model,
        meta_input_resource=table_meta.input_resources,
        spark_resources=table_meta.spark_resources,
    )
    executer.execute()


if __name__ == "__main__":
    run()
