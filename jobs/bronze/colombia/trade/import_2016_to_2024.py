from libs.executers.bronze_executer import BronzeExecuter
from libs.meta import TableMeta
from libs.utils.commons import add_load_date_columns, clean_string_columns
from libs.utils.base_transforms import transform_column_from_json
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
import pandas as pd


# Example run:
# python3 jobs/bronze/colombia/trade/import.py


# def translate_unit(unit):
#     return dictionary_unit.get(unit, unit)

# def translate_country_export(country_export):
#     if country_export in dictionary_juris.keys():
#         return dictionary_juris[country_export]
#     return None


class Executer(BronzeExecuter):
    def select_column(self, df: DataFrame) -> DataFrame:
        feature_df = df.select(
            "NOMBRE_IMPORTADOR",
            "NIT_IMPORTADOR",
            "DIRECCION_IMPORTADOR",
            "TELEFONO_IMPORTADOR",
            "NOMBRE_EXPORTADOR",
            "DIRECCION_EXPORTADOR",
            "PAIS_EXPORTADOR",
            "EMAIL_EXPORTADOR",
            "EMPRESA_TRANSPORTADORA",
            "TASA_CAMBIO",
            "PESO_BRUTO",
            "DESCRIPCION_MERCANCIA",
            "SUBPARTIDA_ARANCELARIA",
            "VALOR_CIF_USD",
            "FECHA_MANIFIESTO",
            "CANTIDAD",
            "UNIDAD_COMERCIAL",
        )

        feature_df = feature_df.withColumnsRenamed(
            {
                "NOMBRE_IMPORTADOR": "name_import",
                "NIT_IMPORTADOR": "regis_import",
                "DIRECCION_IMPORTADOR": "address_import",
                "TELEFONO_IMPORTADOR": "tele_import",
                "NOMBRE_EXPORTADOR": "name_export",
                "DIRECCION_EXPORTADOR": "address_export",
                "PAIS_EXPORTADOR": "country_export",
                "EMAIL_EXPORTADOR": "email_export",
                "EMPRESA_TRANSPORTADORA": "shipper_company",
                "TASA_CAMBIO": "exchange_rate",
                "PESO_BRUTO": "weight",
                "DESCRIPCION_MERCANCIA": "description",
                "SUBPARTIDA_ARANCELARIA": "hs_code",
                "VALOR_CIF_USD": "value_usd",
                "FECHA_MANIFIESTO": "actual_arrival_date",
                "CANTIDAD": "quantity",
                "UNIDAD_COMERCIAL": "quantity_unit",
            }
        )
        return feature_df

    def process_email(self, df: DataFrame) -> DataFrame:
        email_regex = (
            r"^[a-zA-Z0-9._%+-]+(\@|\(at\)|\(AT\))[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        )

        feature_df = df.withColumn(
            "email_export_std",
            F.when(
                F.col("email_export").rlike(email_regex), F.col("email_export")
            ).otherwise(F.lit(None)),
        )
        feature_df = feature_df.withColumn(
            "email_export_std",
            F.regexp_replace(F.col("email_export_std"), r"(?i)\(at\)", "@"),
        )

        url_regex = "(?i)^(https?:\/\/)?([\da-z\.-]+)\.([a-z\.]{2,6})([\/\w \.-]*)*\/?$"
        feature_df = feature_df.withColumn(
            "url_export",
            F.when(
                F.col("email_export").rlike(url_regex)
                & F.col("email_export_std").isNull(),
                F.col("email_export"),
            ).otherwise(F.lit(None)),
        )

        feature_df = feature_df.withColumn(
            "fax_export",
            F.when(
                F.col("email_export").rlike(r"(?i)FAX[^\d]?\d+"), F.col("email_export")
            ).otherwise(F.lit(None)),
        )

        feature_df = feature_df.withColumn(
            "fax_export", F.regexp_replace("fax_export", r"(FAX|fax)\s?", "")
        )

        feature_df = feature_df.withColumn(
            "phone_export",
            F.when(
                F.col("email_export_std").isNull()
                & F.col("url_export").isNull()
                & F.col("fax_export").isNull()
                & ~F.col("email_export").rlike(r"^[^\d]*$"),
                F.col("email_export"),
            ).otherwise(F.lit(None)),
        )

        feature_df = feature_df.withColumn(
            "phone_export",
            F.when(F.length(F.col("phone_export")) < 3, F.lit(None)).otherwise(
                F.col("phone_export")
            ),
        )

        feature_df = feature_df.withColumn(
            "phone_export",
            F.when(F.col("phone_export").rlike(r"[a-zA-Z]"), F.lit(None)).otherwise(
                F.col("phone_export")
            ),
        )
        return feature_df

    def standardize_jurisdiction(self, df: DataFrame) -> DataFrame:
        trans_dict = {
            "BAHAMAS": "Bahamas",
            "ARMENIA": "Armenia",
            "UZBEKISTÁN": "Uzbekistan",
            "CROACIA": "Croatia",
            "EMIRATOS ÁRABES UNIDOS": "United Arab Emirates",
            "LÍBANO": "Lebanon",
            "UGANDA": "Uganda",
            "BANGLADESH": "Bangladesh",
            "COSTA DE MARFIL": "Côte d'Ivoire",
            "SANTA LUCÍA": "Saint Lucia",
            "TURQUÍA": "Türkiye",
            "COREA (SUR) REPÚBLICA DE": "South Korea",
            "LETONIA": "Latvia",
            "OMÁN": "Oman",
            "BÉLGICA": "Belgium",
            "MALTA": "Malta",
            "ESLOVAQUIA": "Slovakia",
            "ISLANDIA": "Iceland",
            "SAN CRISTÓBAL Y NIEVES": "Saint Kitts and Nevis",
            "KUWAIT": "Kuwait",
            "NORUEGA": "Norway",
            "ESPAÑA": "Spain",
            "ANDORRA": "Andorra",
            "PERÚ": "Peru",
            "BHUTÁN": "Bhutan",
            "SAMOA": "Samoa",
            "AZERBAIYÁN": "Azerbaijan",
            "ESTONIA": "Estonia",
            "TÚNEZ": "Tunisia",
            "FIJI": "Fiji",
            "COSTA RICA": "Costa Rica",
            "SUIZA": "Switzerland",
            "DINAMARCA": "Denmark",
            "BÉLICE": "Belize",
            "PUERTO RICO": "Puerto Rico",
            "SRI LANKA": "Sri Lanka",
            "COLOMBIA": "Colombia",
            "GUATEMALA": "Guatemala",
            "CAMBOYA (KAMPUCHEA)": "Cambodia",
            "MARSHALL, ISLAS": "Marshall Islands",
            "MARRUECOS": "Morocco",
            "PAKISTÁN": "Pakistan",
            "GIBRALTAR": "Gibraltar",
            "FRANCIA": "France",
            "VENEZUELA (REPÚBLICA BOLIVARIANA DE)": "Venezuela, Bolivarian Republic of",
            "HONDURAS": "Honduras",
            "ARUBA": "Aruba",
            "DOMINICA": "Dominica",
            "EL SALVADOR": "El Salvador",
            "BAHREIN": "Bahrain",
            "PANAMÁ": "Panama",
            "TAIWÁN": "Taiwan",
            "GRECIA": "Greece",
            "PAÍSES BAJOS": "Netherlands",
            "KAZAJSTÁN": "Kazakhstan",
            "CURAZAO": "Curaçao",
            "MALASIA": "Malaysia",
            "NIGERIA": "Nigeria",
            "CHINA": "China",
            "TRINIDAD Y TOBAGO": "Trinidad and Tobago",
            "AUSTRIA": "Austria",
            "NICARAGUA": "Nicaragua",
            "ITALIA": "Italy",
            "CANADÁ": "Canada",
            "FEDERACIÓN DE RUSIA": "Russian Federation",
            "LUXEMBURGO": "Luxembourg",
            "UCRANIA": "Ukraine",
            "LITUANIA": "Lithuania",
            "FINLANDIA": "Finland",
            "SURINAME": "Suriname",
            "BULGARIA": "Bulgaria",
            "ARGENTINA": "Argentina",
            "RUMANIA": "Romania",
            "HONG KONG": "Hong Kong",
            "EGIPTO": "Egypt",
            "SERBIA": "Serbia",
            "ESLOVENIA": "Slovenia",
            "ESTADOS UNIDOS DE AMÉRICA": "United States",
            "REPÚBLICA CHECA": "Czechia",
            "CHIPRE": "Cyprus",
            "POLONIA": "Poland",
            "MACAO": "Macao",
            "ANGOLA": "Angola",
            "GUYANA": "Guyana",
            "NORFOLK, ISLA": "Norfolk Island",
            "MÓNACO": "Monaco",
            "NUEVA ZELANDA": "New Zealand",
            "ECUADOR": "Ecuador",
            "IRLANDA (EIRE)": "Ireland",
            "BOLIVIA": "Bolivia",
            "ARABIA SAUDITA": "Saudi Arabia",
            "TAILANDIA": "Thailand",
            "MAURICIO": "Mauritius",
            "REINO UNIDO DE GRAN BRETAÑA E IRLANDA DEL NORTE": "United Kingdom",
            "PARAGUAY": "Paraguay",
            "MOLDOVA": "Moldova",
            "SUECIA": "Sweden",
            "REPÚBLICA DOMINICANA": "Dominican Republic",
            "BERMUDAS": "Bermuda",
            "ANGUILA": "Anguilla",
            "INDIA": "India",
            "ALEMANIA": "Germany",
            "HUNGRÍA": "Hungary",
            "SUDÁFRICA, REPÚBLICA DE": "South Africa",
            "FILIPINAS": "Philippines",
            "SINGAPUR": "Singapore",
            "LIECHTENSTEIN": "Liechtenstein",
            "BRASIL": "Brazil",
            "URUGUAY": "Uruguay",
            "AUSTRALIA": "Australia",
            "CHILE": "Chile",
            "VIRGENES ISLAS (BRITÁNICAS)": "Virgin Islands, British",
            "KENIA": "Kenya",
            "IRAQ": "Iraq",
            "QATAR": "Qatar",
            "JAMAICA": "Jamaica",
            "SAN VICENTE Y LAS GRANADINAS": "Saint Vincent and the Grenadines",
            "ESTADO DE PALESTINA": "Palestine",
            "VIET NAM": "Viet Nam",
            "SAN MARINO": "San Marino",
            "ISRAEL": "Israel",
            "PORTUGAL": "Portugal",
            "INDONESIA": "Indonesia",
            "CAIMÁN ISLAS": "Cayman Islands",
            "BARBADOS": "Barbados",
            "MÉXICO": "Mexico",
            "CUBA": "Cuba",
            "JAPÓN": "Japan",
            "GEORGIA": "Georgia",
        }
        column_name = "country_export"

        tran_map = F.create_map(
            *[x for k, v in trans_dict.items() for x in (F.lit(k), F.lit(v))]
        )
        new_key = [
            key[column_name]
            for key in df.select(column_name).distinct().collect()
            if key[column_name] and key[column_name] not in trans_dict.keys()
        ]

        df = df.withColumn(
            "correct_country_export",
            F.when(
                (F.col(column_name).isNotNull()) & (~F.col(column_name).isin(new_key)),
                tran_map[F.col(column_name)],
            ).otherwise(F.lit(None)),
        )

        df = df.withColumn(
            "correct_country_export",
            F.when(
                F.col("country_export").contains("ZONA"), F.lit("Colombia")
            ).otherwise(F.col("correct_country_export")),
        )

        lookup_jurisdiction = self.spark.createDataFrame(
            pd.read_csv("resources/lookup_jurisdiction.csv", na_filter=False)
        )
        df = (
            df.alias("c")
            .join(
                F.broadcast(lookup_jurisdiction).alias("l"),
                lookup_jurisdiction.jurisdiction == df.correct_country_export,
                "left",
            )
            .selectExpr(
                "c.*",
                "l.jurisdiction",
                "l.country_code as lookup_country_code",
                "l.country_name as lookup_country_name",
            )
        )

        return df

    def other_process(self, df: DataFrame) -> DataFrame:
        df = df.withColumn(
            "actual_arrival_date", F.split("actual_arrival_date", "\.")[0]
        )

        df = df.withColumn(
            "actual_arrival_date",
            F.when(F.length(F.col("actual_arrival_date")) < 8, F.lit(None)).otherwise(
                F.col("actual_arrival_date")
            ),
        )

        df = df.withColumn(
            "actual_arrival_date", F.to_date("actual_arrival_date", "yyyyMMdd")
        )

        # translate_unit_udf = F.udf(translate_unit, T.StringType())

        # df = df.withColumn("quantity_unit", translate_unit_udf(F.col("quantity_unit")))
        dictionary_unit = {
            "Metro cuadrado": "Square meter",
            "Metro cúbico": "Cubic meter",
            "Kilovatio hora": "Kilowatt-hour",
            "Par": "Pair",
            "Litro": "Liter",
            "Unidades": "Units",
            "Kilogramo": "Kilogram",
            "Quilate": "Carat",
            "Miles de unidades": "Thousands of units",
            "Centímetro Cúbico": "Cubic centimeter",
        }

        df = transform_column_from_json(df, "quantity_unit", dictionary_unit, True)

        for column_name in ["exchange_rate", "weight", "value_usd", "quantity"]:
            df = df.withColumn(column_name, F.col(column_name).cast("float"))

        df = df.withColumn(
            "quantity_unit",
            F.when(
                F.col("quantity_unit").isin("Pair", "Units", "Thousands of units"),
                F.col("quantity_unit"),
            ).otherwise(F.lit(None)),
        )

        df = df.withColumn(
            "quantity",
            F.when(F.col("quantity_unit").isNotNull(), F.col("quantity")).otherwise(
                F.lit(None)
            ),
        )

        df = df.withColumn("weight_unit", F.lit("Kilogram"))

        df = df.withColumn(
            "tele_import",
            F.when(F.length(F.col("tele_import")) < 3, F.lit(None)).otherwise(
                F.col("tele_import")
            ),
        )

        df = df.withColumn(
            "regis_import",
            F.when(F.length(F.col("regis_import")) < 3, F.lit(None)).otherwise(
                F.col("regis_import")
            ),
        )
        return df

    def transform(self):

        df_dict = self.input_dataframe_dict

        df = self.spark.createDataFrame([], T.StructType([]))

        for i in range(2016, 2025):
            df_year = df_dict[f"colombia.import_{i}"].dataframe
            df_year = df_year.dropDuplicates()
            df_year = self.select_column(df_year)
            df = df.unionByName(df_year, allowMissingColumns=True)

        # df_23 = df_dict["colombia.import_2023"].dataframe
        # # df_23 = df_23.dropDuplicates()
        # df_23 = self.select_column(df_23)

        # df_24 = df_dict["colombia.import_2024"].dataframe
        # # df_24 = df_24.dropDuplicates()
        # df_24 = self.select_column(df_24)

        # df = df_23.unionByName(df_24, allowMissingColumns=True)

        df = self.process_email(df)
        df = self.standardize_jurisdiction(df)
        df = self.other_process(df)

        df = add_load_date_columns(df=df, date_value=self.meta_table_model.load_date)
        df = clean_string_columns(df)
        df = df.withColumn(
            "dv_recsrc",
            F.lit(
                "gov_colombia_trade;https://www.dian.gov.co/dian/cifras/Paginas/Bases-Estadisticas-de-Comercio-Exterior-Importaciones-y-Exportaciones.aspx"
            ),
        )

        return df


def run(env="pro", params={}, spark=None):
    table_meta = TableMeta(
        from_files="metas/bronze/colombia/trade/import.yaml", env=env
    )

    import sys

    executer = Executer(
        sys.argv[0],
        table_meta.model,
        table_meta.input_resources,
        params,
        # spark,
    )
    executer.execute()


if __name__ == "__main__":

    # from pyspark.sql import SparkSession

    # spark = (
    #     SparkSession.builder.appName("pytest")
    #     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    #     .config(
    #         "spark.sql.catalog.spark_catalog",
    #         "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    #     )
    #     .getOrCreate()
    # )
    run(env="test", params={"dv_source_version": "colombiaTrade;20241227"})
