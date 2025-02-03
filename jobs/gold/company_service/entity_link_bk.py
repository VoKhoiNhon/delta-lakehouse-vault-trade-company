from pyspark.sql import functions as F
import pandas as pd

from libs.executers.gold_executer import GoldExecuter
from libs.meta import TableMeta
from libs.utils.base_transforms import update_hashkey_company
from libs.utils.delta_utils import delta_insert

struct = "'struct<to_entity:struct<id:string,is_person:boolean,is_sanctioned:boolean,is_trade:boolean,name:string,description:string,lei_code:string,country_code:string,country_name:string,registration_number:string,date_incorporated:date,date_struck_off:date,jurisdiction:string,legal_form:string,category:array<string>,phone_numbers:array<string>,emails:array<string>,websites:array<string>,linkedin_url:string,twitter_url:string,facebook_url:string,fax_numbers:array<string>,other_names:array<string>,addresses:array<struct<type:int,start_date:timestamp,end_date:timestamp,full_address:string,street:string,city:string,region:string,state:string,province:string,country_code:string,postal_code:string,latitude:float,longitude:float>>,statuses:array<struct<status:string,status_code:int,status_desc:string>>,industries:array<struct<type:boolean,industry_code:string,desc:string,country_code:string,standard_type:string>>,company:struct<image_url:string,no_of_employees:int,authorised_capital:float,paid_up_capital:float,currency_code:string,is_branch:boolean>,person:struct<first_name:string,last_name:string,middle_name:string,full_name:string,image_url:string,dob:string,birthplace:string,nationality:string,country_of_residence:string,accuracy_level:float,gender:int,skills:array<string>,job_summary:string,salary:string,yoe:int,industry:string>>>'"


class EntityLinkExecuter(GoldExecuter):
    def process_jurisdiction_batch(
        self, batch_entity_link, to_entity_with_struct, from_entity_with_struct
    ):

        batch_entity_link_df = batch_entity_link.join(
            to_entity_with_struct,
            on=(batch_entity_link["to_entity_id"] == to_entity_with_struct["to_id"])
            & (
                batch_entity_link["to_jurisdiction"]
                == to_entity_with_struct["jurisdiction"]
            ),
            how="left",
        ).drop(to_entity_with_struct["to_id"], "jurisdiction")

        batch_entity_link_df = batch_entity_link_df.join(
            from_entity_with_struct,
            on=(
                batch_entity_link_df["from_entity_id"]
                == from_entity_with_struct["from_id"]
            )
            & (
                batch_entity_link_df["from_jurisdiction"]
                == from_entity_with_struct["jurisdiction"]
            ),
            how="left",
        ).drop(from_entity_with_struct["from_id"], "jurisdiction")

        batch_entity_link_df = batch_entity_link_df.withColumn(
            "continent",
            F.when(
                F.col("to_entity.continent") == F.col("from_entity.continent"),
                F.col("from_entity.continent"),
            ).otherwise(F.lit("diff_jurisdiction")),
        ).drop("to_entity.continent", "from_entity.continent")

        return batch_entity_link_df

    def transform(self):
        entity = self.input_dataframe_dict["gold.entity"].dataframe.drop(
            "dv_recsrc", "dv_loaddts", "dv_source_version"
        )
        l_company_company_relationship = self.input_dataframe_dict[
            "silver.l_company_company_relationship"
        ].dataframe
        l_person_company_relationship = self.input_dataframe_dict[
            "silver.l_person_company_relationship"
        ].dataframe
        l_person_person_relationship = self.input_dataframe_dict[
            "silver.l_person_person_relationship"
        ].dataframe

        mapping_df = (
            self.input_dataframe_dict["silver.bv_bridge_company_key"]
            .dataframe.filter(F.col("from_key").isNotNull())
            .select("from_key", "to_key")
        )

        l_company_company_relationship = update_hashkey_company(
            l_company_company_relationship, mapping_df, id_col="dv_hashkey_from_company"
        )
        l_company_company_relationship = update_hashkey_company(
            l_company_company_relationship, mapping_df, id_col="dv_hashkey_to_company"
        )
        l_person_company_relationship = update_hashkey_company(
            l_person_company_relationship, mapping_df, id_col="dv_hashkey_company"
        )

        entity_link = (
            l_company_company_relationship.withColumnRenamed(
                "dv_hashkey_l_company_company_relationship", "id"
            )
            .withColumnRenamed("dv_hashkey_from_company", "from_entity_id")
            .withColumnRenamed("dv_hashkey_to_company", "to_entity_id")
            .unionByName(
                l_person_person_relationship.withColumnRenamed(
                    "dv_hashkey_l_person_person_relationship", "id"
                )
                .withColumnRenamed("dv_hashkey_from_person", "from_entity_id")
                .withColumnRenamed("dv_hashkey_to_person", "to_entity_id"),
                allowMissingColumns=True,
            )
            .unionByName(
                l_person_company_relationship.withColumnRenamed(
                    "dv_hashkey_l_person_company_relationship", "id"
                )
                .withColumnRenamed("dv_hashkey_person", "from_entity_id")
                .withColumnRenamed("dv_hashkey_company", "to_entity_id"),
                allowMissingColumns=True,
            )
        )

        to_entity_with_struct = entity.select(
            F.col("id").alias("to_id"),
            "jurisdiction",
            F.struct(*[F.col(c) for c in entity.columns if c != "to_id"]).alias(
                "to_entity"
            ),
        )

        from_entity_with_struct = entity.select(
            F.col("id").alias("from_id"),
            "jurisdiction",
            F.struct(*[F.col(c) for c in entity.columns if c != "from_id"]).alias(
                "from_entity"
            ),
        )

        jurisdictions = entity_link.select("to_jurisdiction").distinct().collect()
        jurisdiction_list = [row.to_jurisdiction for row in jurisdictions]

        batch_size = 15

        for i in range(0, len(jurisdiction_list), batch_size):
            current_batch = jurisdiction_list[i : i + batch_size]
            print(f"Processing jurisdictions: {current_batch}")
            batch_entity_link = entity_link.filter(
                F.col("to_jurisdiction").isin(jurisdiction_list)
            )

            batch_entity_link = self.process_jurisdiction_batch(
                batch_entity_link, from_entity_with_struct, to_entity_with_struct
            )

            delta_insert(
                spark=self.spark,
                df=batch_entity_link,
                data_location=self.meta_table_model.data_location,
                partition_by=self.meta_table_model.partition_by,
                options=self.meta_table_model.options,
            )
            self.spark.catalog.clearCache()

        print("Done")


def run(env="pro", params={}, spark=None):
    import sys

    table_meta = TableMeta(
        from_files=["metas/gold/company_service/entity_link.yaml"], env=env
    )
    executer = EntityLinkExecuter(
        sys.argv[0],
        table_meta.model,
        table_meta.input_resources,
        params,
        spark,
    )
    executer.execute()


if __name__ == "__main__":
    run()
