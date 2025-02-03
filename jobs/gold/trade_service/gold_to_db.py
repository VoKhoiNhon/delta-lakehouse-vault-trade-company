from delta.tables import DeltaTable

from libs.executers.gold_executer import DBTradeServiceExecuter
from libs.utils.db_utils import write_to_db, merge_to_db, delete_to_db


class Executer(DBTradeServiceExecuter):
    def transform(self):
        delta_table_source = self.params.get("delta_source", "")
        delta_log_version_source = self.params.get("delta_log_version", "")

        delta_table = DeltaTable.forPath(self.spark, delta_table_source)
        history_df = delta_table.history()
        lastest_version_row = (
            history_df.select("version", "timestamp")
            .orderBy("version", ascending=False)
            .first()
        )

        if DeltaTable.isDeltaTable(self.spark, delta_log_version_source):
            lastest_version_log = (
                self.spark.read.format("delta")
                .load(delta_log_version_source)
                .orderBy("version", ascending=False)
                .first()["version"]
            )
            if not lastest_version_row["version"] > lastest_version_log:
                print("Data has not changed. No processing required.")
                return
            lastest_version = lastest_version_log + 1
        else:
            self.spark.createDataFrame([lastest_version_row]).write.format(
                "delta"
            ).save(delta_log_version_source)
        changes = (
            self.spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", lastest_version)
            .table(f"delta.`{delta_table_source}`")
        )
        return changes.where("_change_type != 'update_preimage'")

    def execute(self):
        temp_table_name = self.params.get("delta_source", "")
        table_name = self.params.get("delta_source", "")
        df = self.transform()
        insert_update_df = df.where("_change_type != 'delete'").drop(
            "_change_type", "_commit_version", "_commit_timestamp"
        )
        delete_df = df.where("_change_type == 'delete'").drop(
            "_change_type", "_commit_version", "_commit_timestamp"
        )
        if insert_update_df.count() > 0:
            write_to_db(insert_update_df, temp_table_name, "append")
            print("write done")
            merge_to_db(table_name, temp_table_name, table_name.columns)
            print(f"Merge from {temp_table_name} into {table_name}")
        if delete_df.count() > 0:
            delete_broadcast = self.spark.sparkContext.broadcast(delete_to_db)

            def delete_partition(partition):
                delete_row_to_db = delete_broadcast.value
                return delete_row_to_db(partition, table_name, ["id"])

            delete_df.foreachPartition(delete_partition)
            print("delete success")


def run(env="pro", params={}, spark=None):
    executer_hub = Executer(
        "write_db",
        None,
        None,
        params,
        spark,
    )
    executer_hub.execute()


if __name__ == "__main__":
    run()
