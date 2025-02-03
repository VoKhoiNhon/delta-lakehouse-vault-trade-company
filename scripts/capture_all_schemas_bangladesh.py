from libs.utils.capture_all_schemas import capture_all_schemas


def main_bangladesh():
    list_table_meta_files = [
        "metas/bronze/bangladesh/bangladesh_company.yaml",
        "metas/bronze/bangladesh/bangladesh_trade_export.yaml",
        "metas/bronze/bangladesh/bangladesh_trade_import.yaml",
        "metas/silver/s_bol.yaml",
        "metas/silver/h_company.yaml",
        "metas/silver/s_company_demographic.yaml",
        "metas/silver/s_company_address.yaml",
        "metas/silver/l_bol.yaml",
    ]
    list_payloads = [
        {
            "env": "/test_on_s3",
            "load_date": "20241217",
            "spark_executor_memory": "20g",
            "spark_executor_cores": "10",
            "filter_input_resources": ["raw_bangladesh_company"],
        },
        {
            "env": "/test_on_s3",
            "load_date": "20241217",
            "spark_executor_memory": "20g",
            "spark_executor_cores": "10",
            "filter_input_resources": ["raw_bangladesh_trade_export"],
        },
        {
            "env": "/test_on_s3",
            "load_date": "20241217",
            "spark_executor_memory": "20g",
            "spark_executor_cores": "10",
            "filter_input_resources": ["raw_bangladesh_trade_import"],
        },
        {
            "env": "/test_on_s3",
            "load_date": "20241217",
            "spark_executor_memory": "20g",
            "spark_executor_cores": "10",
            "filter_input_resources": [
                "bronze.bangladesh.export",
                "bronze.bangladesh.import",
                "bronze.port",
            ],
        },
        {
            "env": "/test_on_s3",
            "load_date": "20241217",
            "spark_executor_memory": "20g",
            "spark_executor_cores": "10",
            "filter_input_resources": ["bronze.bangladesh"],
        },
        {
            "env": "/test_on_s3",
            "load_date": "20241217",
            "spark_executor_memory": "20g",
            "spark_executor_cores": "10",
            "filter_input_resources": [],
        },
        {
            "env": "/test_on_s3",
            "load_date": "20241217",
            "spark_executor_memory": "20g",
            "spark_executor_cores": "10",
            "filter_input_resources": [],
        },
        {
            "env": "/test_on_s3",
            "load_date": "20241217",
            "spark_executor_memory": "20g",
            "spark_executor_cores": "10",
            "filter_input_resources": [],
        },
    ]
    mapping = dict(zip(list_table_meta_files, list_payloads))
    capture_all_schemas(mapping=mapping)


if __name__ == "__main__":
    main_bangladesh()
