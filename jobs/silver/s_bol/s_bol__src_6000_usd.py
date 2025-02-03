from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta
from libs.utils.vault_hashfuncs import DV_HASHKEY_BOL
import pyspark.sql.functions as F
from libs.utils.base_transforms import transform_column_from_json


class Sbol6kUSDExecuter(SatelliteVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["6k_usd.bol"]
        data = source.dataframe
        data = (
            data.withColumn("unit_of_weight", F.upper(F.col("unit_of_weight")))
            .withColumn(
                "actual_arrival_date",
                F.concat(
                    F.substring("date", 1, 4),  # yyyy
                    F.lit("-"),
                    F.substring("date", 5, 2),  # mm
                    F.lit("-"),
                    F.substring("date", 7, 2),  # dd
                ),
            )
            .withColumn(
                "dv_recsrc",
                F.lit(
                    "NBD Data;https://data.nbd.ltd/en/login?r=%2Fcn%2Ftransaction%2Fsearch"
                ),
            )
            .withColumn("bol", F.lit(None).cast("string"))
            .withColumn("teu_number", F.lit(None).cast("string"))
            .withColumn("invoice_value", F.lit(None).cast("string"))
            .withColumn("value_usd", F.lit(None).cast("string"))
            .withColumn("estimated_arrival_date", F.lit(None).cast("string"))
            .withColumn("vessel_name", F.lit(None).cast("string"))
            .withColumn("exchange_rate", F.lit(None).cast("string"))
            .withColumnRenamed("unit_of_quantity", "quantity_unit")
            .withColumnRenamed("unit_of_weight", "weight_unit")
            .withColumnRenamed("products", "description")
        )
        source_translate = {
            "土耳其出口": "Turkey Export",
            "埃塞俄比亚出口": "Ethiopia Export",
            "乌兹别克出口": "Uzbekistan Export",
            "厄瓜多尔进口": "Ecuador Import",
            "巴拉圭出口": "Paraguay Export",
            "喀麦隆进口": "Cameroon Import",
            "哥伦比亚出口": "Colombia Export",
            "肯尼亚进口": "Kenya Import",
            "哈萨克出口": "Kazakhstan Export",
            "秘鲁出口": "Peru Export",
            "印度尼西亚出口": "Indonesia Export",
            "哥斯达黎加出口": "Costa Rica Export",
            "土耳其进口": "Turkey Import",
            "莱索托进口": "Lesotho Import",
            "埃塞俄比亚进口": "Ethiopia Import",
            "巴拿马出口": "Panama Export",
            "越南进口": "Vietnam Import",
            "乌拉圭进口": "Uruguay Import",
            "印度进口": "India Import",
            "哥伦比亚进口": "Colombia Import",
            "菲律宾出口": "Philippines Export",
            "莱索托出口": "Lesotho Export",
            "巴基斯坦进口": "Pakistan Import",
            "乌干达进口": "Uganda Import",
            "墨西哥出口": "Mexico Export",
            "智利出口": "Chile Export",
            "印度尼西亚进口": "Indonesia Import",
            "科特迪瓦出口": "Côte d'Ivoire Export",
            "科特迪瓦进口": "Côte d'Ivoire Import",
            "美国出口": "USA Export",
            "尼日利亚进口": "Nigeria Import",
            "俄罗斯进口": "Russia Import",
            "委内瑞拉进口": "Venezuela Import",
            "乌兹别克进口": "Uzbekistan Import",
            "墨西哥进口": "Mexico Import",
            "巴拉圭进口": "Paraguay Import",
            "纳米比亚进口": "Namibia Import",
            "印度出口": "India Export",
            "哥斯达黎加进口": "Costa Rica Import",
            "美国进口": "USA Import",
            "越南出口": "Vietnam Export",
            "坦桑尼亚出口": "Tanzania Export",
            "巴西出口": "Brazil Export",
            "俄罗斯出口": "Russia Export",
            "阿根廷进口": "Argentina Import",
            "博兹瓦纳出口": "Botswana Export",
            "孟加拉进口": "Bangladesh Import",
            "英国进口": "UK Import",
            "哈萨克进口": "Kazakhstan Import",
            "委内瑞拉出口": "Venezuela Export",
            "秘鲁进口": "Peru Import",
            "乌干达出口": "Uganda Export",
            "菲律宾进口": "Philippines Import",
            "智利进口": "Chile Import",
            "乌拉圭出口": "Uruguay Export",
            "巴基斯坦出口": "Pakistan Export",
            "博兹瓦纳进口": "Botswana Import",
            "坦桑尼亚进口": "Tanzania Import",
            "巴西进口": "Brazil Import",
            "亚非欧贸易": "Asia-Africa-Europe Trade",
            "斯里兰卡进口": "Sri Lanka Import",
            "乌克兰进口": "Ukraine Import",
            "厄瓜多尔出口": "Ecuador Export",
            "巴拿马进口": "Panama Import",
            "加纳进口": "Ghana Import",
        }
        data = transform_column_from_json(data, "data_source", source_translate, False)
        data = data.withColumn(
            "dv_source_version",
            F.concat_ws(";", F.col("data_source"), F.lit("20241231")),
        )
        return data.selectExpr(
            f"{DV_HASHKEY_BOL} as dv_hashkey_bol",
            "dv_recsrc",
            "dv_source_version",
            "bol",
            "hs_code",
            "teu_number",
            "invoice_value",
            "value_usd",
            "exchange_rate",
            "description",
            "actual_arrival_date",
            "estimated_arrival_date",
            "vessel_name",
            "quantity",
            "quantity_unit",
            "weight",
            "weight_unit",
        )


def run(env="pro", params={}, spark=None):
    import sys

    table_meta_s_bol = TableMeta(from_files="metas/silver/s_bol.yaml", env=env)
    sbol = Sbol6kUSDExecuter(
        sys.argv[0],
        table_meta_s_bol.model,
        table_meta_s_bol.input_resources,
        params,
        spark,
    )
    sbol.execute()


if __name__ == "__main__":
    run()
