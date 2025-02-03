from libs.executers.bronze_executer import BronzeExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
from libs.utils.base_transforms import transform_column_from_json
from libs.utils.commons import (
    clean_column_names,
    clean_string_columns,
    add_load_date_columns,
)


class Executer(BronzeExecuter):
    def transform(self):
        port_world = self.input_dataframe_dict["raw.world_port"]
        port_us = self.input_dataframe_dict["raw.us_port"]
        jurisdiction = self.input_dataframe_dict["raw.jurisdiction"]
        df_port_world = port_world.dataframe
        df_port_us = port_us.dataframe
        df_jurisdiction = jurisdiction.dataframe

        df_processed = (
            df_port_world.withColumn(
                "Official Name",
                F.when(
                    F.col("Official Port Name/Alias") == "Official",
                    F.col("Foreign Port Name"),
                ).otherwise(None),
            )
            .withColumn(
                "Alias Name",
                F.when(
                    F.col("Official Port Name/Alias") == "Alias",
                    F.col("Foreign Port Name"),
                ).otherwise(None),
            )
            .withColumnRenamed("Schedule K Code", "port_code")
        )
        df_processed = (
            df_processed.groupBy("port_code")
            .agg(
                F.first("Official Name", ignorenulls=True).alias("port_name"),
                F.concat_ws(
                    "; ",
                    F.collect_list(
                        F.when(
                            F.col("Alias Name").isNotNull(), F.col("Alias Name")
                        ).otherwise(None)
                    ),
                ).alias("previous_port_name"),
                F.first("Country Name").alias("country_name"),
                F.first("Latitude").alias("latitude"),
                F.first("Longitude").alias("longitude"),
            )
            .withColumn(
                "country_name",
                F.when(
                    F.col("country_name").contains("China Taiwan"), "Taiwan"
                ).otherwise(F.col("country_name")),
            )
        )
        df_processed = df_processed.withColumn(
            "previous_port_name",
            F.when(F.col("previous_port_name") == "", None).otherwise(
                F.col("previous_port_name")
            ),
        )
        merged_df = (
            df_processed.alias("a")
            .join(
                df_jurisdiction.alias("b"),
                on=F.col("a.country_name") == F.col("b.country"),
                how="left",
            )
            .select("a.*", "b.country_code")
            .withColumn("jurisdiction", F.lit(F.col("country_name")))
            .dropDuplicates()
        )
        df_port_us = df_port_us.filter(
            F.col("Code").isNotNull() & F.col("Port").isNotNull()
        ).withColumn(
            "Code",
            F.when(
                (F.length(F.col("Code")) == 3) & (F.col("Code").isNotNull()),
                F.concat(F.lit("0"), F.col("Code")),
            ).otherwise(F.col("Code")),
        )
        df_port_us = df_port_us.withColumn(
            "jurisdiction",
            F.split(F.col("Port"), ", ").getItem(
                F.size(F.split(F.col("Port"), ", ")) - 1
            ),
        )

        df_port_us = (
            df_port_us.withColumn(
                "jurisdiction",
                F.when(F.col("jurisdiction") == "DC", "District of Columbia")
                .when(F.col("jurisdiction") == "Boston Massachusetts", "Massachusetts")
                .when(F.col("jurisdiction") == "Billings Montana", "Montana")
                .when(
                    F.col("jurisdiction") == "Minnesota User Fee Airport ", "Minnesota"
                )
                .when(
                    F.col("jurisdiction") == "including O’Hare International Airport",
                    "Illinois",
                )
                .when(F.col("jurisdiction") == "KY", "Kentucky")
                .when(
                    F.col("jurisdiction")
                    == "Mississippi (Include Jackson Municipal Airport)",
                    "Mississippi",
                )
                .when(F.col("jurisdiction") == "Area Port of Ysleta", "Texas")
                .when(F.col("jurisdiction") == "NV", "Nevada")
                .otherwise(F.col("jurisdiction")),
            )
            .withColumn("country_name", F.lit("United States"))
            .withColumn("country_code", F.lit("US"))
            .withColumnRenamed("Code", "port_code")
            .withColumnRenamed("Port", "port_name")
        )

        merged_columns = merged_df.columns
        us_port_columns = df_port_us.columns
        for col in merged_columns:
            if col not in us_port_columns:
                df_port_us = df_port_us.withColumn(col, F.lit(None))
        df_port_us = df_port_us.select(merged_columns)

        true_port = merged_df.union(df_port_us)

        df_port_us = (
            true_port.select("jurisdiction", "country_name", "country_code")
            .withColumn(
                "port_code", F.concat(F.lit("unspecified, "), F.col("jurisdiction"))
            )
            .withColumn(
                "port_name", F.concat(F.lit("unspecified, "), F.col("jurisdiction"))
            )
            .dropDuplicates()
        )
        # df_port_us.filter(F.col("port_code") == "2795").show(n=100)

        merged_columns = true_port.columns
        us_port_columns = df_port_us.columns
        for col in merged_columns:
            if col not in us_port_columns:
                df_port_us = df_port_us.withColumn(col, F.lit(None))
        df_port_us = df_port_us.select(merged_columns)
        true = true_port.union(df_port_us)

        mapped_values = {
            "Venezuela": "Venezuela",
            "Ukraine": "Ukraine",
            "Falk Is": "Falkland Islands (Malvinas)",
            "Oman": "Oman",
            "North Korea": "North Korea",
            "Guyana": "Guyana",
            "Cyprus": "Cyprus",
            "Turkey": "Turkey",
            "Guatemala": "Guatemala",
            "Syria": "Syrian Arab Republic",
            "British Virgin": "Virgin Islands, British",
            "Jamaica": "Jamaica",
            "Latvia": "Latvia",
            "Argentina": "Argentina",
            "Brunei": "Brunei Darussalam",
            "Angola": "Angola",
            "Canada": "Canada",
            "Albania": "Albania",
            "Mozambique": "Mozambique",
            "Northern Marian": "Northern Mariana Islands",
            "Nicaragua": "Nicaragua",
            "Belize": "Belize",
            "Peru": "Peru",
            "Kenya": "Kenya",
            "China": "China",
            "Japan": "Japan",
            "Somalia": "Somalia",
            "Martinique": "Martinique",
            "Equatorial Gui": "Equatorial Guinea",
            "Haiti": "Haiti",
            "Croatia": "Croatia",
            "St. Kitts Nevis": "Saint Kitts And Nevis",
            "Suriname": "Suriname",
            "Norway": "Norway",
            "Bermuda": "Bermuda",
            "Colombia": "Colombia",
            "Guadeloupe": "Guadeloupe",
            "Vanuatu": "Vanuatu",
            "Guinea Bissau": "Guinea-Bissau",
            "Sv Jm Islands": "Svalbard And Jan Mayen",
            "Ireland": "Ireland",
            "Vietnam": "Vietnam",
            "Hong Kong": "Hong Kong",
            "Israel": "Israel",
            "Senegal": "Senegal",
            "St. Vincent": "Saint Vincent And Grenadines",
            "Gibraltar": "Gibraltar",
            "Mexico": "Mexico",
            "Trinidad": "Trinidad And Tobago",
            "Iraq": "Iraq",
            "Guam": "Guam",
            "Cambodia": "Cambodia",
            "Palau": "Palau",
            "Sri Lanka": "Sri Lanka",
            "Reunion": "Reunion",
            "Macau": "Macao",
            "Belgium": "Belgium",
            "Ecuador": "Ecuador",
            "Madagascar": "Madagascar",
            "Bahamas": "Bahamas",
            "Lebanon": "Lebanon",
            "Kuwait": "Kuwait",
            "Slovenia": "Slovenia",
            "American Samoa": "American Samoa",
            "New Zealand": "New Zealand",
            "Poland": "Poland",
            "Portugal": "Portugal",
            "Denmark": "Denmark",
            "Brit Ind Ocean": "British Indian Ocean Territory",
            "Barbados": "Barbados",
            "Iran": "Iran, Islamic Republic Of",
            "Mauritius": "Mauritius",
            "Netherlands": "Netherlands",
            "Monaco": "Monaco",
            "Solomon Is": "Solomon Islands",
            "Anguilla": "Anguilla",
            "Nauru": "Nauru",
            "Iceland": "Iceland",
            "South Korea": "Korea",
            "Tonga": "Tonga",
            "Djibouti": "Djibouti",
            "Fiji": "Fiji",
            "Estonia": "Estonia",
            "Germany": "Germany",
            "Tunisia": "Tunisia",
            "Honduras": "Honduras",
            "Saudi Arabia": "Saudi Arabia",
            "United Arab Em": "United Arab Emirates",
            "Algeria": "Algeria",
            "Togo": "Togo",
            "French So and Art": "French Southern Territories",
            "Finland": "Finland",
            "Samoa": "Samoa",
            "Sierra Leone": "Sierra Leone",
            "Benin": "Benin",
            "United States": "United States",
            "India": "India",
            "Sao Tome and Principe": "Sao Tome And Principe",
            "Tuvalu": "Tuvalu",
            "Chile": "Chile",
            "Neth Antilles": "Netherlands Antilles",
            "Greenland": "Greenland",
            "Nigeria": "Nigeria",
            "Papua New Guinea": "Papua New Guinea",
            "Bulgaria": "Bulgaria",
            "Serbia": "Serbia",
            "Lithuania": "Lithuania",
            "St. Helena": "Saint Helena",
            "Spain": "Spain",
            "Pitcairn": "Pitcairn",
            "Thailand": "Thailand",
            "Yemen": "Yemen",
            "Sweden": "Sweden",
            "Tokelau": "Tokelau",
            "Kiribati": "Kiribati",
            "Eritrea": "Eritrea",
            "Burma": "Burma",
            "Malaysia": "Malaysia",
            "Aruba": "Aruba",
            "Western Sahara": "Western Sahara",
            "Comoros": "Comoros",
            "France": "France",
            "French Guiana": "French Guiana",
            "Taiwan": "Taiwan",
            "Seychelles": "Seychelles",
            "Brazil": "Brazil",
            "St. Lucia": "Saint Lucia",
            "French Polyn": "French Polynesia",
            "Malta": "Malta",
            "Tanzania": "Tanzania",
            "Antigua": "Antigua And Barbuda",
            "Marshall Is": "Marshall Islands",
            "Gabon": "Gabon",
            "Italy": "Italy",
            "US Outlying Is": "United States Outlying Islands",
            "Costa Rica": "Costa Rica",
            "Bahrain": "Bahrain",
            "Micronesia": "Micronesia, Federated States Of",
            "Cuba": "Cuba",
            "Mauritania": "Mauritania",
            "Pakistan": "Pakistan",
            "Niue": "Niue",
            "United Kingdom": "United Kingdom",
            "St. Pierre/Miq": "Saint Pierre And Miquelon",
            "Morocco": "Morocco",
            "Panama": "Panama",
            "Cape Verde": "Cape Verde",
            "Russia": "Russian Federation",
            "Paraguay": "Paraguay",
            "Congo Kinshasha": "Congo, Democratic Republic",
            "Philippines": "Philippines",
            "Uruguay": "Uruguay",
            "Singapore": "Singapore",
            "Georgia": "Georgia",
            "Indonesia": "Indonesia",
            "Turks Is": "Turks And Caicos Islands",
            "US Virgin Is": "Virgin Islands, U.S.",
            "Jordan": "Jordan",
            "Ivory Coast": '"Cote D""Ivoire"',
            "Libya": "Libyan Arab Jamahiriya",
            "Maldives": "Maldives",
            "Sudan": "Sudan",
            "Grenada": "Grenada",
            "Liberia": "Liberia",
            "Greece": "Greece",
            "Montserrat": "Montserrat",
            "Dominica": "Dominica",
            "Namibia": "Namibia",
            "Qatar": "Qatar",
            "Guinea": "Guinea",
            "New Caledonia": "New Caledonia",
            "Ghana": "Ghana",
            "Gambia": "Gambia",
            "Wallis": "Wallis And Futuna",
            "Cayman Isl": "Cayman Islands",
            "Dominican Republic": "Dominican Republic",
            "Fedrated States of Micronesia": "Micronesia, Federated States Of",
            "Congo Brazzaville": "Congo",
            "Cameroon": "Cameroon",
            "Australia": "Australia",
            "Romania": "Romania",
            "Egypt": "Egypt",
            "El Salvador": "El Salvador",
            "South Africa": "South Africa",
            "Bangladesh": "Bangladesh",
            "Faroe": "Faroe Islands",
        }
        true = transform_column_from_json(true, "country_name", mapped_values, True)
        df = add_load_date_columns(df=true, date_value=self.meta_table_model.load_date)
        df = clean_column_names(df)
        return df


def run():
    table_meta = TableMeta(from_files="metas/bronze/port/port.yaml")
    executer = Executer(
        app_name="port_raw_to_bronze",
        meta_table_model=table_meta.model,
        meta_input_resource=table_meta.input_resources,
    )
    executer.execute()


if __name__ == "__main__":
    run()
