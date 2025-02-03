from libs.utils.capture_all_schemas import capture_all_schemas


def main_sanction():
    list_table_meta_files = [
        "metas/bronze/sanction/sanction_address.yaml",
        "metas/bronze/sanction/sanction_person.yaml",
        "metas/bronze/sanction/sanction_sanction.yaml",
        "metas/bronze/sanction/sanction_company.yaml",
        "metas/bronze/sanction/sanction_entitylink.yaml",
        "metas/silver/h_company.yaml",
        "metas/silver/s_company_demographic.yaml",
        "metas/silver/s_company_address.yaml",
        "metas/silver/h_person.yaml",
        "metas/silver/s_person_demographic.yaml",
        "metas/silver/s_person_address.yaml",
    ]
    list_payloads = [
        {
            "env": "/test_on_s3",
            "load_date": "20250124",
            "spark_executor_memory": "20g",
            "spark_executor_cores": "10",
            "filter_input_resources": ["raw.sanction.address"],
        },
        {
            "env": "/test_on_s3",
            "load_date": "20250124",
            "spark_executor_memory": "20g",
            "spark_executor_cores": "10",
            "filter_input_resources": ["raw.sanction.person"],
        },
        {
            "env": "/test_on_s3",
            "load_date": "20250124",
            "spark_executor_memory": "20g",
            "spark_executor_cores": "10",
            "filter_input_resources": ["raw.sanction.sanction"],
        },
        {
            "env": "/test_on_s3",
            "load_date": "20250124",
            "spark_executor_memory": "20g",
            "spark_executor_cores": "10",
            "filter_input_resources": [
                "raw.sanction.company.company",
                "raw.sanction.company.organization",
            ],
        },
        {
            "env": "/test_on_s3",
            "load_date": "20250124",
            "spark_executor_memory": "20g",
            "spark_executor_cores": "10",
            "filter_input_resources": [
                "raw.sanction.entity_link.family",
                "raw.sanction.entity_link.membership",
                "raw.sanction.entity_link.ownership",
                "raw.sanction.entity_link.directorship",
                "raw.sanction.entity_link.employment",
                "raw.sanction.entity_link.unknownlink",
            ],
        },
        {
            "env": "/test_on_s3",
            "load_date": "20250124",
            "spark_executor_memory": "20g",
            "spark_executor_cores": "10",
            "filter_input_resources": [
                "bronze.sanction.company",
            ],
        },
        {
            "env": "/test_on_s3",
            "load_date": "20250124",
            "spark_executor_memory": "20g",
            "spark_executor_cores": "10",
            "filter_input_resources": [
                "bronze.sanction.company",
            ],
        },
        {
            "env": "/test_on_s3",
            "load_date": "20250124",
            "spark_executor_memory": "20g",
            "spark_executor_cores": "10",
            "filter_input_resources": [
                "bronze.sanction.company",
            ],
        },
        {
            "env": "/test_on_s3",
            "load_date": "20250124",
            "spark_executor_memory": "20g",
            "spark_executor_cores": "10",
            "filter_input_resources": [
                "bronze.sanction.person",
            ],
        },
        {
            "env": "/test_on_s3",
            "load_date": "20250124",
            "spark_executor_memory": "20g",
            "spark_executor_cores": "10",
            "filter_input_resources": [
                "bronze.sanction.person",
            ],
        },
        {
            "env": "/test_on_s3",
            "load_date": "20250124",
            "spark_executor_memory": "20g",
            "spark_executor_cores": "10",
            "filter_input_resources": [
                "bronze.sanction.person",
            ],
        },
    ]
    mapping = dict(zip(list_table_meta_files, list_payloads))
    capture_all_schemas(mapping=mapping)


if __name__ == "__main__":
    main_sanction()
