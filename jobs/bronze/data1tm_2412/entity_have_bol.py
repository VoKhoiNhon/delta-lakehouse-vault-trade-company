from libs.executers.bronze_executer import BronzeExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
from libs.utils.base_transforms import transform_column_from_json
from libs.utils.commons import (
    clean_jurisdiciton_1tm,
    add_pure_company_name,
    add_load_date_columns,
)


class Executer(BronzeExecuter):
    def transform(self):
        df = self.input_dataframe_dict["1tm_2412.entity_have_bol"].dataframe
        df = clean_jurisdiciton_1tm(self.spark, df)
        df = add_pure_company_name(df, "name")
        df = add_load_date_columns(df=df, date_value=self.meta_table_model.load_date)
        # df = df.filter(
        #     (F.col("jurisdiction").isNotNull()) | (F.col("country_name") != "")
        # )
        # jurisdiction_fix = {
        #     "Minnesota": "US.Minnesota",
        #     "Venezuela (bolivarian Republic Of)": "Venezuela",
        #     "North Carolina": "US.North Carolina",
        #     "Iran (islamic Republic Of)": "Iran, Islamic Republic Of",
        #     "East Timor": "Timor-Leste",
        #     "Türkiye": "Turkey",
        #     "Maine": "US.Maine",
        #     "Colorado": "US.Colorado",
        #     "Missouri": "US.Missouri",
        #     "Ohio": "US.Ohio",
        #     "Trinidad": "Trinidad And Tobago",
        #     "Oregon": "US.Oregon",
        #     "Moldova, Republic Of": "Moldova",
        #     "Texas": "US.Texas",
        #     "Maryland": "US.Maryland",
        #     'Cote D"Ivoire': '"Cote D""Ivoire"',
        #     "Formerly Yugoslavia Region": "Yugoslavia",
        #     "BC": "CA.British Columbia",
        #     "Vermont": "US.Vermont",
        #     "Massachusetts": "US.Massachusetts",
        #     "North Macedonia": "Macedonia",
        #     "Kentucky": "US.Kentucky",
        #     "Bolivia (plurinational State Of)": "Bolivia",
        #     "Viet Nam": "Vietnam",
        #     "Unknown": None,  # No mapping found
        #     "New York": "US.New York",
        #     "Alaska": "US.Alaska",
        #     "Iran": "Iran, Islamic Republic Of",
        #     "Bosnia And": "Bosnia And Herzegovina",
        #     "NS": "CA.Nova Scotia",
        #     "Korea, Republic Of": "Korea",
        #     "South Korea": "Korea",
        #     "Côte D'ivoire": '"Cote D""Ivoire"',
        #     "Western Samoa": "Samoa",
        #     "Virginia": "US.Virginia",
        #     "Connecticut": "US.Connecticut",
        #     "Republic Of Korea": "Korea",
        #     "South Carolina": "US.South Carolina",
        #     "Virgin Islands,british": "Virgin Islands, British",
        #     "Utah": "US.Utah",
        #     "Lao": '"Lao People""s Democratic Republic"',
        #     "Alabama": "US.Alabama",
        #     "Vatican": "Holy See (Vatican City State)",
        #     'Lao People"s Democratic Republic': '"Lao People""s Democratic Republic"',
        #     "St. Lucia": "Saint Lucia",
        #     "Central Africa Republic": "Central African Republic",
        #     "Washington": "US.Washington",
        #     "Florida": "US.Florida",
        #     "Indiana": "US.Indiana",
        #     "ONT": "CA.Ontario",
        #     "Micronesia": "Micronesia, Federated States Of",
        #     "Hawaii": "US.Hawaii",
        #     "Russia": "Russian Federation",
        #     "Palestine": "Palestinian Territory, Occupied",
        #     "Rhode Island": "US.Rhode Island",
        #     "Fiji Islands": "Fiji",
        #     "Michigan": "US.Michigan",
        #     "New Jersey": "US.New Jersey",
        #     "Democratic Republic Of Congo": "Congo, Democratic Republic",
        #     "The Former Yugoslav Republic Of Macedonia": "Macedonia",
        #     "Virgin": "Virgin Islands, U.S.",
        #     "Pennsylvania": "US.Pennsylvania",
        #     "Cote Divoire": '"Cote D""Ivoire"',
        #     "Louisiana": "US.Louisiana",
        #     "Mississippi": "US.Mississippi",
        #     "Tennessee": "US.Tennessee",
        #     "New Hampshire": "US.New Hampshire",
        #     "Illinois": "US.Illinois",
        #     "United States Minor Outlying Islands": "United States Outlying Islands",
        #     "Republic Of Moldova": "Moldova",
        #     "Virgin Islands": "Virgin Islands, U.S.",
        #     "California": "US.California",
        #     "NB": "CA.New Brunswick",
        #     "Delaware": "US.Delaware",
        #     "Wisconsin": "US.Wisconsin",
        #     "Northern Marianas Islands": "Northern Mariana Islands",
        #     "Syria": "Syrian Arab Republic",
        #     "Arizona": "US.Arizona",
        #     "Oklahoma": "US.Oklahoma",
        #     "Paises Bajos (reino De Los)": "Netherlands",
        #     "Turkmenstan": "Turkmenistan",
        #     "Democratic Republic Of The Congo": "Congo, Democratic Republic",
        #     "British Virgin Islands": "Virgin Islands, British",
        #     "QUE": "CA.Quebec",
        #     "Kansas": "US.Kansas",
        #     "Virgin Islands, U.s.": "Virgin Islands, U.S.",
        #     "West Virginia": "US.West Virginia",
        #     "Ivory Coast": '"Cote D""Ivoire"',
        #     "North Dakota": "US.North Dakota",
        #     "Brunei": "Brunei Darussalam",
        #     "Nevada": "US.Nevada",
        #     "Lao People's Democratic Republic": '"Lao People""s Democratic Republic"',
        #     "Guinea-bissau": "Guinea-Bissau",
        #     "Tanzania, United Republic Of": "Tanzania",
        #     "United States Virgin Islands": "Virgin Islands, U.S.",
        #     "Libya": "Libyan Arab Jamahiriya",
        #     "District of Columbia": "US.District of Columbia",
        #     "Saint Vincent And The Grenadines": "Saint Vincent And Grenadines",
        #     "United Republic Of Tanzania": "Tanzania",
        #     "Congo, The Democratic Republic Of The": "Congo, Democratic Republic",
        #     "All Other Guadeloupe Ports": "Guadeloupe",
        #     "Bahamas Gun Cay": "Bahamas",
        #     "Heard Islands And Mcdonald Islands": "Heard Island & Mcdonald Islands",
        #     "New Mexico": "US.New Mexico",
        #     "Falkland Islands": "Falkland Islands (Malvinas)",
        #     "Idaho": "US.Idaho",
        #     "Eswatini": "Swaziland",
        #     "Svalbard And Jan Mayen Islands": "Svalbard And Jan Mayen",
        #     "Iowa": "US.Iowa",
        #     "Palestine, State Of": "Palestinian Territory, Occupied",
        #     "Micronesia Federated States Of": "Micronesia, Federated States Of",
        #     "Coccs(keeling) Islands": "Cocos (Keeling) Islands",
        #     "Montana": "US.Montana",
        #     "Maine to Cape Hatteras": "US.Maine",
        #     "Côte D'Ivoire": '"Cote D""Ivoire"',
        #     "North Macedonia": "Macedonia",
        #     "Lao People's Democratic Republic": '"Lao People""s Democratic Republic"',
        #     "NULL": None,
        # }
        # country_fix = {
        #     "Venezuela (bolivarian Republic Of)": "Venezuela",
        #     "Iran (islamic Republic Of)": "Iran, Islamic Republic Of",
        #     "East Timor": "Timor-Leste",
        #     "Türkiye": "Turkey",
        #     "Trinidad": "Trinidad And Tobago",
        #     "Moldova, Republic Of": "Moldova",
        #     'Cote D"Ivoire': '"Cote D""Ivoire"',
        #     "Formerly Yugoslavia Region": "Yugoslavia",
        #     "BC": "Canada",
        #     "North Macedonia": "Macedonia",
        #     "Bolivia (plurinational State Of)": "Bolivia",
        #     "Viet Nam": "Vietnam",
        #     "Iran": "Iran, Islamic Republic Of",
        #     "Bosnia And": "Bosnia And Herzegovina",
        #     "NS": "Canada",
        #     "Korea, Republic Of": "Korea",
        #     "South Korea": "Korea",
        #     "Côte D'ivoire": '"Cote D""Ivoire"',
        #     "Western Samoa": "Samoa",
        #     "Republic Of Korea": "Korea",
        #     "Virgin Islands,british": "Virgin Islands, British",
        #     "Lao": '"Lao People""s Democratic Republic"',
        #     "Vatican": "Holy See (Vatican City State)",
        #     'Lao People"s Democratic Republic': '"Lao People""s Democratic Republic"',
        #     "St. Lucia": "Saint Lucia",
        #     "Central Africa Republic": "Central African Republic",
        #     "ONT": "Canada",
        #     "Micronesia": "Micronesia, Federated States Of",
        #     "Russia": "Russian Federation",
        #     "Palestine": "Palestinian Territory, Occupied",
        #     "Fiji Islands": "Fiji",
        #     "Democratic Republic Of Congo": "Congo, Democratic Republic",
        #     "The Former Yugoslav Republic Of Macedonia": "Macedonia",
        #     "Virgin": "Virgin Islands, U.S.",
        #     "Cote Divoire": '"Cote D""Ivoire"',
        #     "United States Minor Outlying Islands": "United States Outlying Islands",
        #     "Republic Of Moldova": "Moldova",
        #     "NB": "Canada",
        #     "Northern Marianas Islands": "Northern Mariana Islands",
        #     "Syria": "Syrian Arab Republic",
        #     "Turkmenstan": "Turkmenistan",
        #     "Democratic Republic Of The Congo": "Congo, Democratic Republic",
        #     "British Virgin Islands": "Virgin Islands, British",
        #     "QUE": "Canada",
        #     "Virgin Islands, U.s.": "Virgin Islands, U.S.",
        #     "Ivory Coast": '"Cote D""Ivoire"',
        #     "Brunei": "Brunei Darussalam",
        #     "Lao People's Democratic Republic": '"Lao People""s Democratic Republic"',
        #     "Guinea-bissau": "Guinea-Bissau",
        #     "Tanzania, United Republic Of": "Tanzania",
        #     "United States Virgin Islands": "Virgin Islands, U.S.",
        #     "Libya": "Libyan Arab Jamahiriya",
        #     "Saint Vincent And The Grenadines": "Saint Vincent And Grenadines",
        #     "United Republic Of Tanzania": "Tanzania",
        #     "Congo, The Democratic Republic Of The": "Congo, Democratic Republic",
        #     "Bahamas Gun Cay": "Bahamas",
        #     "Heard Islands And Mcdonald Islands": "Heard Island & Mcdonald Islands",
        #     "Falkland Islands": "Falkland Islands (Malvinas)",
        #     "Eswatini": "Swaziland",
        #     "Svalbard And Jan Mayen Islands": "Svalbard And Jan Mayen",
        #     "Palestine, State Of": "Palestinian Territory, Occupied",
        #     "Micronesia Federated States Of": "Micronesia, Federated States Of",
        #     "Coccs(keeling) Islands": "Cocos (Keeling) Islands",
        #     "All Other Guadeloupe Ports": "Guadeloupe",
        #     "Virgin Islands": "Virgin Islands, British",
        #     "Maine to Cape Hatteras": "United States",
        #     "Paises Bajos (reino De Los)": "Netherlands",
        #     "Virgin Islands": "Virgin Islands, U.S.",
        # }
        # df = transform_column_from_json(df, "jurisdiction", jurisdiction_fix, True)
        # df = transform_column_from_json(df, "country_name", country_fix, True)
        # df = df.withColumn(
        #     "jurisdiction",
        #     F.when(F.col("jurisdiction").isNull(), F.col("country_name")).otherwise(
        #         F.col("jurisdiction")
        #     ),
        # )
        return df


def run(env="pro", params={}, spark=None):
    table_meta_link_position = TableMeta(
        from_files="metas/bronze/data1tm_2412/entity_have_bol.yaml", env=env
    )

    import sys

    executer = Executer(
        sys.argv[0],
        table_meta_link_position.model,
        table_meta_link_position.input_resources,
        params,
        spark,
    )
    executer.execute(
        options={
            "delta.autoOptimize.optimizeWrite": "true"
            # "delta.autoOptimize.autoCompact": "true"
        }
    )


if __name__ == "__main__":

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("pytest")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    run(env="test", params={"dv_source_version": "init"}, spark=spark)
