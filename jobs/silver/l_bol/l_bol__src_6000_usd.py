import pyspark.sql.functions as F

from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
import libs.utils.vault_hashfuncs as H
from libs.utils.base_transforms import transform_column_from_json
from libs.utils.commons import add_pure_company_name


class Lbol6kUSDExecuter(RawVaultExecuter):
    def transform(self):
        data = self.input_dataframe_dict["6k_usd.bol"].dataframe
        data = data.filter(F.col("load_date") == "20241229")
        lookup_port = self.input_dataframe_dict["lookup_port"].dataframe
        data = data.withColumn(
            "import_country", F.trim(F.initcap(F.col("import_country")))
        ).withColumn(
            "export_country_or_area", F.trim(F.initcap(F.col("export_country_or_area")))
        )
        country_mapping = {
            "Venezuela": "Venezuela, Bolivarian Republic of",
            "Salvador": "El Salvador",
            "New Ginea": "Papua New Guinea",
            "Turkey": "Türkiye",
            "International Waters": "unspecified",
            "Zaire": "Congo, The Democratic Republic of the",
            "Trinidad And Tobago": "Trinidad and Tobago",
            "Syria": "Syrian Arab Republic",
            "Polinesia": "French Polynesia",
            "Yugoslavia": "unspecified",
            "Saint Martin": "Saint Martin (French part)",
            "Gaziantep Free Zone": "unspecified",
            "Izmir Free Zone": "unspecified",
            "Bursa Free Zone": "unspecified",
            "Dominic": "Dominica",
            "Korea": "South Korea",
            "Other": "unspecified",
            "Czech": "Czechia",
            "Vietnam": "Viet Nam",
            "Swaziland": "Eswatini",
            "Islas Vrgenes, Ee.uu.": "Virgin Islands, U.S.",
            "Paises Bajos (reino De Los)": "Netherlands",
            "Macao (china)": "Macao",
            "Cote D'ivoire": "Côte d'Ivoire",
            "Norfolk": "Norfolk Island",
            "Reunion": "Réunion",
            "Tanzania": "Tanzania",
            "Serbia And Montenegro": "unspecified",
            "Taiwan (china)": "Taiwan",
            "Macedonia": "North Macedonia",
            "Kocaeli Free Zone": "unspecified",
            "Thrace Free Zone": "unspecified",
            "Myanmar": "Myanmar",
            "Hong Kong (china)": "Hong Kong",
            "Saint Vincent And The Grenadines": "Saint Vincent and the Grenadines",
            "Unknown": "unspecified",
            "Kayseri Free Zone": "unspecified",
            "Turkish Republic Of Northern Cyprus": "unspecified",
            "Bosnia And Herzegovina": "Bosnia and Herzegovina",
            "Mersin Free Zone": "unspecified",
            "Union Europea": "unspecified",
            "British East Africa": "unspecified",
            "Istanbul Industrial And Commercial Free Zone": "unspecified",
            "Federated States Of Micronesia": "Micronesia",
            "Aegean Free Zone": "unspecified",
            "British Virgin Islands": "Virgin Islands, British",
            "Pitcairn Islands": "Pitcairn",
            "Brunei": "Brunei",
            "Wallis And Futuna": "Wallis and Futuna",
            "Tubitak Mam Technology Free Zone": "unspecified",
            "Samsun Free Zone": "unspecified",
            "Bonaire St. Eustatio Isaba": "Bonaire, Sint Eustatius and Saba",
            "Turcas Is.": "unspecified",
            "Saint Barthelemy": "Saint Barthélemy",
            "Adana Yumurtalik Free Zone": "unspecified",
            "European Free Zone": "unspecified",
            "Denizli Free Zone": "unspecified",
            "Area Of The Canal": "unspecified",
            "Ahl Free Zone": "unspecified",
            "Antalya Free Zone": "unspecified",
            "Guadalupe": "Guadeloupe",
            "Saint Kitts And Nevis": "Saint Kitts and Nevis",
            "St. Lucia": "Saint Lucia",
            "Palestina": "Palestine",
            "Sao Tome And Principe": "São Tomé and Príncipe",
            "San Vicente": "Saint Vincent and the Grenadines",
            "West Africa": "unspecified",
            "Cape Verde": "Cabo Verde",
            "Russia": "Russian Federation",
            "Monserrate": "Montserrat",
            "Ivory Coast": "Côte d'Ivoire",
            "Turks And Caicos Islands": "Turks and Caicos Islands",
            "Congo Dr": "Congo, The Democratic Republic of the",
            "Guinea-bissau": "Guinea-Bissau",
            "Isle Of Man": "Isle of Man",
            "Falkland Island": "Falkland Islands (Malvinas)",
            "Curacao": "Curaçao",
            "Romany": "Romania",
            "Virgin Islands": "Virgin Islands, U.S.",
            "Timor-leste": "Timor-Leste",
            "Netherlands Antilles": "unspecified",
            "Northern Ireland": "unspecified",
            "Vatican City": "Holy See (Vatican City State)",
            "Antigua And Barbuda": "Antigua and Barbuda",
            "Нидерланды, Королевство": "Netherlands",
            "Republica De Corea, Popular Democratica De": "North Korea",
            "Espa?'a": "Spain",
            "Mixed Origin": "unspecified",
            "Cocos (keeling) Islands": "Cocos (Keeling) Islands",
            "Macedonia, The Former Yugoslav": "North Macedonia",
            "Territorio Britanico": "unspecified",
            "Comunidad Europea": "unspecified",
            "Zona Franca Coronel Rosales": "unspecified",
            "San Pedro": "unspecified",
            "South Georgia": "South Georgia and the South Sandwich Islands",
            "Zona Franca Río Gallegos": "unspecified",
            "Minor And Remote Islands D Usa": "United States Minor Outlying Islands",
            "Aland Islands": "Åland Islands",
            "Soviet Republic": "unspecified",
            "Питкэрн": "Pitcairn",
            "Timor Oriental": "Timor-Leste",
            "Z.f. Fray Bentos": "unspecified",
            "Zona Franca Concepción Del Uruguay": "unspecified",
            "Bosnia - Herzegovina": "Bosnia and Herzegovina",
            "Zona Franca Pacifico": "unspecified",
            "Hurd And Mcdonald Islands": "Heard Island and McDonald Islands",
            "Zona Franca Puerto Galvan": "unspecified",
            "Zona Franca Justo Daract": "unspecified",
            "#value!": "unspecified",
            "Zona Franca Salta": "unspecified",
            "Abkhazia": "unspecified",
            "Z.f. Villa Constitución": "unspecified",
            "Moldova, Republica De": "Moldova",
            "Zona Franca Mendoza": "unspecified",
            "NULL": "unspecified",
            "Espa?‘a": "Spain",
        }
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
        data = transform_column_from_json(data, "import_country", country_mapping, True)
        data = transform_column_from_json(
            data, "export_country_or_area", country_mapping, True
        )
        data = transform_column_from_json(data, "data_source", source_translate, False)
        data = data.filter(F.col("data_source") != "Argentina Import")
        data = data.withColumn("weight_unit", F.upper(F.col("unit_of_weight")))
        data = (
            data.withColumnRenamed("date", "actual_arrival_date")
            .withColumnRenamed("products", "description")
            .withColumnRenamed("unit_of_quantity", "quantity_unit")
        )

        lookup_port_test = lookup_port.withColumn(
            "new_port_name", F.explode_outer(F.split("previous_port_name", "; "))
        )
        port_for_map = (
            lookup_port_test.select("port_name", "port_code")
            .union(
                lookup_port_test.select("new_port_name", "port_code").withColumnRenamed(
                    "new_port_name",
                    "port_name",
                )
            )
            .distinct()
        )
        data = (
            data.alias("data")
            .join(
                port_for_map.withColumnRenamed("port_name", "port_name_export")
                .withColumnRenamed("port_code", "port_code_export")
                .alias("export_port"),
                on=F.lower(F.trim(F.col("data.loading_port")))
                == F.lower(F.trim(F.col("export_port.port_name_export"))),
                how="left",
            )
            .join(
                port_for_map.withColumnRenamed("port_name", "port_name_import")
                .withColumnRenamed("port_code", "port_code_import")
                .alias("import_port"),
                on=F.lower(F.trim(F.col("data.unloading_port")))
                == F.lower(F.trim(F.col("import_port.port_name_import"))),
                how="left",
            )
        )
        data = (
            data.withColumn(
                "port_name_export",
                F.when(
                    F.col("port_code_export").isNull(),
                    F.concat_ws(", ", F.lit("unspecified"), F.col("import_country")),
                ).otherwise(F.col("port_name_export")),
            )
            .withColumn(
                "port_name_import",
                F.when(
                    F.col("port_code_import").isNull(),
                    F.concat_ws(
                        ", ", F.lit("unspecified"), F.col("export_country_or_area")
                    ),
                ).otherwise(F.col("port_name_import")),
            )
            .withColumn(
                "port_code_export",
                F.when(
                    F.col("port_code_export").isNull(),
                    F.concat_ws(", ", F.lit("unspecified"), F.col("import_country")),
                ).otherwise(F.col("port_code_export")),
            )
            .withColumn(
                "port_code_import",
                F.when(
                    F.col("port_code_import").isNull(),
                    F.concat_ws(
                        ", ", F.lit("unspecified"), F.col("export_country_or_area")
                    ),
                ).otherwise(F.col("port_code_import")),
            )
        )
        data = (
            data.alias("data")
            .join(
                lookup_port.select("dv_hashkey_port", "port_code")
                .withColumnRenamed("dv_hashkey_port", "export_port")
                .alias("export_port"),
                on=F.col("data.port_code_export") == F.col("export_port.port_code"),
                how="left",
            )
            .drop("port_code")
            .join(
                lookup_port.select("dv_hashkey_port", "port_code")
                .withColumnRenamed("dv_hashkey_port", "import_port")
                .alias("import_port"),
                on=F.col("data.port_code_import") == F.col("import_port.port_code"),
                how="left",
            )
            .drop("port_code")
        )
        data = data.filter(
            (F.col("import_country").isNotNull())
            | (F.col("export_country_or_area").isNotNull())
        )
        data = add_pure_company_name(data, "importer")
        data = add_pure_company_name(data, "exporter")
        data = (
            data.withColumn(
                "buyer_dv_hashkey_company",
                F.md5(F.concat_ws(";", "import_country", F.lit(None), "pure_importer")),
            )
            .withColumn(
                "supplier_dv_hashkey_company",
                F.md5(
                    F.concat_ws(
                        ";", "export_country_or_area", F.lit(None), "pure_exporter"
                    )
                ),
            )
            .withColumn("shipper_dv_hashkey_company", F.lit(None))
        )
        data = (
            data.withColumn(
                "actual_arrival_date",
                F.concat(
                    F.substring("actual_arrival_date", 1, 4),  # yyyy
                    F.lit("-"),
                    F.substring("actual_arrival_date", 5, 2),  # mm
                    F.lit("-"),
                    F.substring("actual_arrival_date", 7, 2),  # dd
                ),
            )
            .withColumn(
                "dv_recsrc",
                F.lit(
                    "NBD Data;https://data.nbd.ltd/en/login?r=%2Fcn%2Ftransaction%2Fsearch"
                ),
            )
            .withColumn(
                "dv_source_version",
                F.concat_ws(";", F.col("data_source"), F.lit("20241231")),
            )
            .withColumn("bol", F.lit(None).cast("string"))
            .withColumn("teu_number", F.lit(None).cast("string"))
            .withColumn("invoice_value", F.lit(None).cast("string"))
            .withColumn("value_usd", F.lit(None).cast("string"))
            .withColumn("estimated_arrival_date", F.lit(None).cast("string"))
            .withColumn("vessel_name", F.lit(None).cast("string"))
            .withColumn("exchange_rate", F.lit(None).cast("string"))
            .withColumn(
                "dv_hashkey_bol",
                F.md5(
                    F.concat_ws(
                        ";",
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
                ),
            )
            .withColumn(
                "dv_hashkey_l_bol",
                F.md5(
                    F.concat_ws(
                        ";",
                        "dv_hashkey_bol",
                        "buyer_dv_hashkey_company",
                        "supplier_dv_hashkey_company",
                        "export_port",
                        "import_port",
                        "actual_arrival_date",
                    )
                ),
            )
        )

        return data.select(
            "dv_hashkey_l_bol",
            "dv_hashkey_bol",
            "dv_source_version",
            "buyer_dv_hashkey_company",
            "supplier_dv_hashkey_company",
            "import_port",
            "export_port",
            "bol",
            "actual_arrival_date",
        )


def run(env="pro", params={}, spark=None):
    import sys

    table_meta = TableMeta(from_files="metas/silver/l_bol.yaml", env=env)
    executer = Lbol6kUSDExecuter(
        sys.argv[0],
        table_meta.model,
        table_meta.input_resources,
        params,
        spark,
    )
    executer.execute()


if __name__ == "__main__":
    run()
