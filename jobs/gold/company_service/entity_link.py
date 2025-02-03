from pyspark.sql import functions as F
from libs.executers.gold_executer import GoldCompanyExecuter
from libs.meta import TableMeta
from libs.utils.base_transforms import update_hashkey_company
from libs.utils.delta_utils import (
    delta_insert,
    print_last_delta_last_operation,
    delta_insert_for_hubnlink,
)
from libs.utils.commons import run_with_juris_splited


def process_join(entity_link, to_entity, from_entity):
    to_entity_with_struct = to_entity.select(
        F.col("id").alias("to_id"),
        "continent",
        "jurisdiction",
        F.struct(
            *[F.col(c) for c in to_entity.columns if c not in ("continent", "to_id")]
        ).alias("to_entity"),
    )

    from_entity_with_struct = from_entity.select(
        F.col("id").alias("from_id"),
        "continent",
        "jurisdiction",
        F.struct(
            *[
                F.col(c)
                for c in from_entity.columns
                if c not in ("continent", "from_id")
            ]
        ).alias("from_entity"),
    )

    df = (
        entity_link.alias("el")
        .join(
            to_entity_with_struct.alias("to"),
            on=(F.col("el.to_entity_id") == F.col("to.to_id"))
            & (F.col("el.to_jurisdiction") == F.col("to.jurisdiction")),
            how="left",
        )
        .join(
            from_entity_with_struct.alias("from"),
            on=(F.col("el.from_entity_id") == F.col("from.from_id"))
            & (F.col("el.from_jurisdiction") == F.col("from.jurisdiction")),
            how="left",
        )
        .withColumn(
            "final_continent",
            F.when(
                F.col("to.continent") == F.col("from.continent"),
                F.col("from.continent"),
            ).otherwise(F.lit("diff_jurisdiction")),
        )
        .where("to_entity.name is not null and from_entity.name is not null")
        .drop("continent", "to_id", "from_id", "jurisdiction")
        .withColumnRenamed("final_continent", "continent")
    )

    return df


class EntityLinkCompCompExecuter(GoldCompanyExecuter):
    def transform(self):
        entity = self.input_dataframe_dict["gold.entity_company"].dataframe.drop(
            "dv_recsrc", "dv_loaddts", "dv_source_version"
        )
        l_company_company_relationship = self.input_dataframe_dict[
            "silver.l_company_company_relationship"
        ].dataframe

        mapping_df = (
            self.input_dataframe_dict["silver.bv_bridge_company_key"]
            .dataframe.filter(F.col("from_key").isNotNull())
            .select("from_key", "to_key")
        )

        # l_company_company_relationship.alias('l').join(
        #     mapping_df.alias('m'),
        #     F.col('l.dv_hashkey_from_company') == F.col("m.from_key"),
        #     "left"
        # ).selectExpr('l.*', "coalesce(m.from_key, l.dv_hashkey_from_company) as new_hk")

        l_company_company_relationship = update_hashkey_company(
            l_company_company_relationship, mapping_df, id_col="dv_hashkey_from_company"
        ).drop("from_key", "to_key")
        l_company_company_relationship = update_hashkey_company(
            l_company_company_relationship, mapping_df, id_col="dv_hashkey_to_company"
        ).drop("from_key", "to_key")

        entity_link = (
            l_company_company_relationship.withColumnRenamed(
                "dv_hashkey_l_company_company_relationship", "id"
            )
            .withColumnRenamed("dv_hashkey_from_company", "from_entity_id")
            .withColumnRenamed("dv_hashkey_to_company", "to_entity_id")
        )
        return process_join(entity_link, entity, entity)


class EntityLinkPersonPersonExecuter(GoldCompanyExecuter):
    def transform(self):
        entity = self.input_dataframe_dict["gold.entity_person"].dataframe.drop(
            "dv_recsrc", "dv_loaddts", "dv_source_version"
        )
        l_person_person_relationship = self.input_dataframe_dict[
            "silver.l_person_person_relationship"
        ].dataframe

        entity_link = (
            l_person_person_relationship.withColumnRenamed(
                "dv_hashkey_l_person_person_relationship", "id"
            )
            .withColumnRenamed("dv_hashkey_from_person", "from_entity_id")
            .withColumnRenamed("dv_hashkey_to_person", "to_entity_id")
        )
        return process_join(entity_link, entity, entity)


class EntityLinkPersonCompExecuter(GoldCompanyExecuter):
    def execute(self, n_group):
        l_person_company_relationship = self.input_dataframe_dict[
            "silver.l_person_company_relationship"
        ].dataframe

        if self.query:
            l_person_company_relationship = l_person_company_relationship.where(
                self.query
            )
            print(
                f"filter {self.query} \n"
                f"l_person_company_relationship.count(): {l_person_company_relationship.count():,}"
            )

        run_with_juris_splited(
            l_person_company_relationship.withColumn(
                "jurisdiction", F.col("to_jurisdiction")
            ),
            n_group,
            self.execute_each_batch,
        )

    def execute_each_batch(self, l_person_company_relationship):

        l_person_company_relationship = l_person_company_relationship.drop(
            "jurisdiction"
        )
        entity_company = self.input_dataframe_dict[
            "gold.entity_company"
        ].dataframe.drop("dv_recsrc", "dv_loaddts", "dv_source_version")

        entity_person = self.input_dataframe_dict["gold.entity_person"].dataframe.drop(
            "dv_recsrc", "dv_loaddts", "dv_source_version"
        )

        mapping_df = (
            self.input_dataframe_dict["silver.bv_bridge_company_key"]
            .dataframe.filter(F.col("from_key").isNotNull())
            .select("from_key", "to_key")
        )

        l_person_company_relationship = update_hashkey_company(
            l_person_company_relationship, mapping_df, id_col="dv_hashkey_company"
        )

        entity_link = (
            l_person_company_relationship.withColumnRenamed(
                "dv_hashkey_l_person_company_relationship", "id"
            )
            .withColumnRenamed("dv_hashkey_person", "from_entity_id")
            .withColumnRenamed("dv_hashkey_company", "to_entity_id")
        )

        df = process_join(entity_link, entity_company, entity_person)

        delta_insert_for_hubnlink(
            spark=self.spark,
            df=df,
            data_location=self.meta_table_model.data_location,
            base_cols=self.meta_table_model.unique_key,
            struct_type=self.meta_table_model.struct_type,
            partition_by=self.meta_table_model.partition_by,
        )
        # delta_insert(
        #     spark=self.spark,
        #     df=df,
        #     data_location=self.meta_table_model.data_location,
        #     partition_by=self.meta_table_model.partition_by,
        #     options=self.meta_table_model.options,
        # )
        # from delta.tables import DeltaTable

        # delta_table = DeltaTable.forPath(
        #     self.spark, self.meta_table_model.data_location
        # )
        # print_last_delta_last_operation(delta_table)


def run_entity_link_company_company(env="pro", params={}, spark=None):
    import sys

    table_meta = TableMeta(
        from_files=["metas/gold/company_service/entity_link.yaml"], env=env
    )
    model = table_meta.model
    model.data_location = (
        "s3a://lakehouse-gold/company_service/entity_link_company_company"
    )
    executer = EntityLinkCompCompExecuter(
        sys.argv[0],
        model,
        table_meta.input_resources,
        params,
        spark,
    )
    executer.execute()


def run_entity_link_person_person(env="pro", params={}, spark=None):
    import sys

    table_meta = TableMeta(
        from_files=["metas/gold/company_service/entity_link.yaml"], env=env
    )
    model = table_meta.model
    model.data_location = (
        "s3a://lakehouse-gold/company_service/entity_link_person_person"
    )
    executer = EntityLinkPersonPersonExecuter(
        sys.argv[0],
        table_meta.model,
        table_meta.input_resources,
        params,
        spark,
    )
    executer.execute()


def run_entity_link_person_company(env="pro", params={}, spark=None, n_group=10):
    import sys

    table_meta = TableMeta(
        from_files=["metas/gold/company_service/entity_link.yaml"], env=env
    )
    model = table_meta.model
    model.data_location = (
        "s3a://lakehouse-gold/company_service/entity_link_person_company"
    )

    executer = EntityLinkPersonCompExecuter(
        sys.argv[0],
        table_meta.model,
        table_meta.input_resources,
        params,
        spark,
    )
    executer.execute(n_group)


if __name__ == "__main__":
    run()
