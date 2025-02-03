from libs.executers.bronze_executer import BronzeExecuter
from pyspark.sql import functions as F
from libs.meta import TableMeta
from libs.utils.commons import (
    clean_column_names,
    clean_string_columns,
    add_load_date_columns,
)


class Executer(BronzeExecuter):
    def transform(self):
        df = self.input_dataframe_dict["raw_colombia_com_per"].dataframe
        df = (
            df.withColumnRenamed("codigo_camara", "chamber_code")
            .withColumnRenamed("camara_comercio", "chamber_commerce")
            .withColumnRenamed("matricula", "registration")
            .withColumnRenamed("inscripcion_proponente", "proponent_registration")
            .withColumnRenamed("razon_social", "business_name")
            .withColumnRenamed("primer_apellido", "first_lastname")
            .withColumnRenamed("segundo_apellido", "second_lastname")
            .withColumnRenamed("primer_nombre", "first_name")
            .withColumnRenamed("segundo_nombre", "second_name")
            .withColumnRenamed("sigla", "abbreviation")
            .withColumnRenamed(
                "codigo_clase_identificacion", "identification_type_code"
            )
            .withColumnRenamed("clase_identificacion", "identification_type")
            .withColumnRenamed("numero_identificacion", "identification_number")
            .withColumnRenamed("nit", "tax_id")
            .withColumnRenamed("digito_verificacion", "verification_digit")
            .withColumnRenamed(
                "cod_ciiu_act_econ_pri", "ciiu_primary_economic_activity_code"
            )
            .withColumnRenamed(
                "cod_ciiu_act_econ_sec", "ciiu_secondary_economic_activity_code"
            )
            .withColumnRenamed("ciiu3", "ciiu_level_3")
            .withColumnRenamed("ciiu4", "ciiu_level_4")
            .withColumnRenamed("fecha_matricula", "registration_date")
            .withColumnRenamed("fecha_renovacion", "renewal_date")
            .withColumnRenamed("ultimo_ano_renovado", "last_renewed_year")
            .withColumnRenamed("fecha_vigencia", "validity_date")
            .withColumnRenamed("fecha_cancelacion", "cancellation_date")
            .withColumnRenamed("codigo_tipo_sociedad", "company_type_code")
            .withColumnRenamed("tipo_sociedad", "company_type")
            .withColumnRenamed(
                "codigo_organizacion_juridica", "legal_organization_code"
            )
            .withColumnRenamed("organizacion_juridica", "legal_organization")
            .withColumnRenamed(
                "codigo_categoria_matricula", "registration_category_code"
            )
            .withColumnRenamed("categoria_matricula", "registration_category")
            .withColumnRenamed("codigo_estado_matricula", "registration_status_code")
            .withColumnRenamed("estado_matricula", "registration_status")
            .withColumnRenamed(
                "clase_identificacion_RL", "representative_legal_identification_type"
            )
            .withColumnRenamed(
                "Num Identificacion Representante Legal",
                "representative_legal_identification_number",
            )
            .withColumnRenamed("Representante Legal", "legal_representative")
            .withColumnRenamed("fecha_actualizacion", "update_date")
        )
        df = df.withColumn(
            "registration_number",
            F.concat(F.col("registration"), F.lit("-"), F.col("chamber_code")),
        )
        df = add_load_date_columns(df=df, date_value=self.meta_table_model.load_date)
        df = clean_column_names(df)
        # df = clean_string_columns(df)
        df.show(n=5)
        return df


def run():
    table_meta = TableMeta(from_files="metas/bronze/colombia/company_person.yaml")
    executer = Executer(
        app_name="colombia_raw_to_bronze",
        meta_table_model=table_meta.model,
        meta_input_resource=table_meta.input_resources,
    )
    executer.execute()


if __name__ == "__main__":
    run()
