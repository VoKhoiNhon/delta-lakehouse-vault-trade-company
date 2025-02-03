from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
from libs.utils.vault_hashfuncs import DV_HASHKEY_COMPANY, DV_HASHKEY_INDUSTRY
from libs.utils.commons import add_pure_company_name
from libs.utils.base_transforms import transform_column_from_json


class SCompanyDemographicBangladeshExecuter(SatelliteVaultExecuter):
    def transform(self):
        # Transform DF
        source = self.input_dataframe_dict["bronze.colombia"]
        # record_source = source.record_source
        # dv_source_version = source.dv_source_version
        df = source.dataframe
        status = {
            "INACTIVA": "Inactive",
            "ACTIVA": "Active",
            "NO MATRICULADO": "Not Registered",
            "MATRÍCULA CANCELADA POR TRASLADO DE DOMICILIO": "Registration Canceled Due to Change of Address",
            "MATRICULA INACTIVA POR PERDIDA DE CALIDAD DE COMERCIANTE": "Registration Inactive Due to Loss of Merchant Status",
            "NO ASIGNADO": "Not Assigned",
            "MATRÍCULA NUEVA, CONSTITUCIÓN POR TRASLADO": "New Registration, Constitution Due to Transfer",
            "MATRÍCULA CANCELADA LEY 1429": "Registration Canceled by Law 1429",
            "NO COMPETENCIA DE LA CAMARA": "No Competence of the Chamber",
            "MATRÍCULA CANCELADA POR FUSIÓN": "Registration Canceled Due to Merger",
            "MATRÍCULA CANCELADA POR RECONSTITUCIÓN.": "Registration Canceled Due to Reconstitution",
            "CANCELADA": "Canceled",
            "SIN ESTADO": "No Status",
        }
        company_type = {
            "ENTIDAD SIN ANIMO DE LUCRO": "Non-Profit Entity",
            "VENDEDORES DE JUEGOS DE SUERTE Y AZAR": "Lottery and Gambling Vendors",
            "SOCIEDAD COMERCIAL": "Commercial Society",
            "VEEDURIAS CIUDADANAS": "Citizen Oversight Committees",
            "EMPRESAS INDUSTRIALES Y COMERCIALES DEL ESTADO": "State Industrial and Commercial Enterprises",
            "ENTIDAD SIN ANIMO DE LUCRO EXTRANJERAS": "Foreign Non-Profit Entities",
            "ECONOMIA SOLIDARIA": "Solidarity Economy",
            "SOCIEDAD CIVIL": "Civil Society",
            "NO APLICA": "Not Applicable",
        }
        df = df.filter(F.col("first_lastname").isNull())
        df = df.filter(
            F.col("business_name").isNotNull() | F.col("business_name").isNotNull()
        ).orderBy("business_name", ascending=True)
        df = df.withColumn(
            "date_incorporated",
            F.concat_ws(
                "-",
                F.col("registration_date").substr(1, 4),  # Year
                F.col("registration_date").substr(5, 2),  # Month
                F.col("registration_date").substr(7, 2),  # Day
            ),
        )
        df = transform_column_from_json(df, "registration_status", status, True)
        df = transform_column_from_json(df, "company_type", company_type, True)
        df = df.withColumnRenamed("business_name", "name")
        df = df.withColumn("jurisdiction", F.lit("Colombia"))
        df = add_pure_company_name(df, "name")
        s_company = df.withColumn("currency_code", F.lit("COP")).withColumn(
            "is_branch", F.lit(False)
        )
        s_company = (
            s_company.withColumnRenamed("abbreviation", "other_names")
            .withColumnRenamed("company_type", "legal_form")
            .withColumnRenamed("registration_status", "status")
            .withColumnRenamed("registration_status_code", "status_code")
        )
        s_company = s_company.withColumn(
            "other_names",
            F.when(
                F.col("other_names").isNotNull(), F.split(F.col("other_names"), ";")
            ).otherwise(F.array()),
        )
        df = s_company.selectExpr(
            f"{DV_HASHKEY_COMPANY} as dv_hashkey_company",
            "concat_ws(';', 'Colombia', 'https://www.datos.gov.co/en/Comercio-Industria-y-Turismo/Personas-Naturales-Personas-Jur-dicas-y-Entidades-/c82u-588k/about_data') as dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "'Colombia-20241231' as dv_source_version",
            "jurisdiction",
            "registration_number",
            "name",
            "pure_name",
            "NULL as lei_code",
            "NULL as description",
            "date_incorporated",
            "NULL as date_struck_off",
            "legal_form",
            "NULL as category",
            "NULL as phone_numbers",
            "NULL as emails",
            "NULL as websites",
            "NULL as linkedin_url",
            "NULL as twitter_url",
            "NULL as facebook_url",
            "NULL as fax_numbers",
            "other_names",
            "NULL as no_of_employees",
            "NULL as image_url",
            "NULL as authorised_capital",
            "NULL as paid_up_capital",
            "status",
            "is_branch",
            "currency_code",
            "NULL as status_desc",
            "status_code",
        )
        df.show(5)
        return df


def run():

    table_meta_sat_company_demographic = TableMeta(
        from_files=["metas/silver/s_company_demographic.yaml"]
    )
    executer_sat_company_demographic = SCompanyDemographicBangladeshExecuter(
        app_name="bangladesh_bronze_to_silver_sat_company_demographic",
        meta_table_model=table_meta_sat_company_demographic.model,
        meta_input_resource=table_meta_sat_company_demographic.input_resources,
    )
    executer_sat_company_demographic.execute()


if __name__ == "__main__":
    run()
