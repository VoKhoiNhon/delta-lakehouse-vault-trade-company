from pyspark.sql.types import *


def compare_schema(schema1, schema2):

    if len(schema1.fields) != len(schema2.fields):
        print(len(schema1.fields), len(schema2.fields), " length not match")

    cols1 = [field.name for field in schema1.fields]
    cols2 = [field.name for field in schema2.fields]

    list1_as_set = set(cols1)
    intersection = list1_as_set.intersection(cols2)
    print("intersection: ", intersection)
    print(len(intersection))

    in_1_notin_2 = list(set(cols1) - set(cols2))
    in_2_notin_1 = list(set(cols2) - set(cols1))
    print(in_1_notin_2)
    print(in_2_notin_1)

    for (field1, field2) in zip(schema1.fields, schema2.fields):
        if field1.name != field2.name:
            print(field1.name, field2.name, " name not match")

        if str(field1.dataType) != str(field2.dataType):
            print(field1.name, field1.dataType, field2.dataType, " type not match")

    # print('all done')


from libs.meta import TableMeta


def notest_compare():

    table_s_person = TableMeta(from_files="metas/silver/s_person_demographic.yaml")
    table_s_company = TableMeta(from_files="metas/silver/s_company_demographic.yaml")

    person_schema = table_s_person.model.struct_type
    company_schema = table_s_company.model.struct_type

    compare_schema(person_schema, company_schema)
    compare_schema(company_schema, person_schema)


def notest_compare():

    table_s_person = TableMeta(from_files="metas/silver/s_person_demographic.yaml")
    table_s_company = TableMeta(from_files="metas/silver/s_company_demographic.yaml")

    person_schema = table_s_person.model.struct_type
    company_schema = table_s_company.model.struct_type

    compare_schema(person_schema, company_schema)
    compare_schema(company_schema, person_schema)


def test_compare2():
    struct = {
        "fields": [
            {"metadata": {}, "name": "id", "nullable": True, "type": "string"},
            {"metadata": {}, "name": "is_person", "nullable": True, "type": "boolean"},
            {
                "metadata": {},
                "name": "is_sanctioned",
                "nullable": True,
                "type": "boolean",
            },
            {"metadata": {}, "name": "name", "nullable": True, "type": "string"},
            {"metadata": {}, "name": "description", "nullable": True, "type": "string"},
            {"metadata": {}, "name": "lei_code", "nullable": True, "type": "string"},
            {
                "metadata": {},
                "name": "country_code",
                "nullable": True,
                "type": "string",
            },
            {
                "metadata": {},
                "name": "country_name",
                "nullable": True,
                "type": "string",
            },
            {
                "metadata": {},
                "name": "registration_number",
                "nullable": True,
                "type": "string",
            },
            {
                "metadata": {},
                "name": "date_incorporated",
                "nullable": True,
                "type": "date",
            },
            {
                "metadata": {},
                "name": "date_struck_off",
                "nullable": True,
                "type": "date",
            },
            {
                "metadata": {},
                "name": "jurisdiction",
                "nullable": True,
                "type": "string",
            },
            {"metadata": {}, "name": "legal_form", "nullable": True, "type": "string"},
            {
                "metadata": {},
                "name": "category",
                "nullable": True,
                "type": {
                    "containsNull": True,
                    "elementType": "string",
                    "type": "array",
                },
            },
            {
                "metadata": {},
                "name": "phone_numbers",
                "nullable": True,
                "type": {
                    "containsNull": True,
                    "elementType": "string",
                    "type": "array",
                },
            },
            {
                "metadata": {},
                "name": "emails",
                "nullable": True,
                "type": {
                    "containsNull": True,
                    "elementType": "string",
                    "type": "array",
                },
            },
            {
                "metadata": {},
                "name": "websites",
                "nullable": True,
                "type": {
                    "containsNull": True,
                    "elementType": "string",
                    "type": "array",
                },
            },
            {
                "metadata": {},
                "name": "linkedin_url",
                "nullable": True,
                "type": "string",
            },
            {"metadata": {}, "name": "twitter_url", "nullable": True, "type": "string"},
            {
                "metadata": {},
                "name": "facebook_url",
                "nullable": True,
                "type": "string",
            },
            {
                "metadata": {},
                "name": "fax_numbers",
                "nullable": True,
                "type": {
                    "containsNull": True,
                    "elementType": "string",
                    "type": "array",
                },
            },
            {
                "metadata": {},
                "name": "other_names",
                "nullable": True,
                "type": {
                    "containsNull": True,
                    "elementType": "string",
                    "type": "array",
                },
            },
            {
                "metadata": {},
                "name": "created_at",
                "nullable": True,
                "type": "timestamp",
            },
            {
                "metadata": {},
                "name": "updated_at",
                "nullable": True,
                "type": "timestamp",
            },
            {"metadata": {}, "name": "is_trade", "nullable": True, "type": "boolean"},
            {"metadata": {}, "name": "image_url", "nullable": True, "type": "string"},
            {
                "metadata": {},
                "name": "no_of_employees",
                "nullable": True,
                "type": "integer",
            },
            {
                "metadata": {},
                "name": "authorised_capital",
                "nullable": True,
                "type": "decimal(10,0)",
            },
            {
                "metadata": {},
                "name": "paid_up_capital",
                "nullable": True,
                "type": "decimal(10,0)",
            },
            {
                "metadata": {},
                "name": "currency_code",
                "nullable": True,
                "type": "string",
            },
            {"metadata": {}, "name": "is_branch", "nullable": True, "type": "boolean"},
            {
                "metadata": {},
                "name": "created_at",
                "nullable": True,
                "type": "timestamp",
            },
            {
                "metadata": {},
                "name": "updated_at",
                "nullable": True,
                "type": "timestamp",
            },
            {"metadata": {}, "name": "id", "nullable": True, "type": "string"},
            {"metadata": {}, "name": "entity_id", "nullable": True, "type": "string"},
            {"metadata": {}, "name": "address_id", "nullable": True, "type": "string"},
            {"metadata": {}, "name": "type", "nullable": True, "type": "integer"},
            {"metadata": {}, "name": "start_date", "nullable": True, "type": "date"},
            {"metadata": {}, "name": "end_date", "nullable": True, "type": "date"},
            {
                "metadata": {},
                "name": "created_at",
                "nullable": True,
                "type": "timestamp",
            },
            {
                "metadata": {},
                "name": "updated_at",
                "nullable": True,
                "type": "timestamp",
            },
            {"metadata": {}, "name": "id", "nullable": True, "type": "string"},
            {"metadata": {}, "name": "company_id", "nullable": True, "type": "string"},
            {"metadata": {}, "name": "industry_id", "nullable": True, "type": "string"},
            {"metadata": {}, "name": "type", "nullable": True, "type": "boolean"},
            {
                "metadata": {},
                "name": "created_at",
                "nullable": True,
                "type": "timestamp",
            },
            {
                "metadata": {},
                "name": "updated_at",
                "nullable": True,
                "type": "timestamp",
            },
            {"metadata": {}, "name": "id", "nullable": True, "type": "string"},
            {"metadata": {}, "name": "entity_id", "nullable": True, "type": "string"},
            {"metadata": {}, "name": "status_id", "nullable": True, "type": "string"},
            {
                "metadata": {},
                "name": "status_code",
                "nullable": True,
                "type": "integer",
            },
            {
                "metadata": {},
                "name": "created_at",
                "nullable": True,
                "type": "timestamp",
            },
            {
                "metadata": {},
                "name": "updated_at",
                "nullable": True,
                "type": "timestamp",
            },
        ],
        "type": "struct",
    }

    all_schema = StructType.fromJson(struct)

    company_address = TableMeta(from_files="metas/silver/s_company_address.yaml")
    company_demographic = TableMeta(
        from_files="metas/silver/s_company_demographic.yaml"
    )

    address_schema = company_address.model.struct_type
    demographic_schema = company_demographic.model.struct_type

    cols1 = [field.name for field in address_schema.fields]
    cols2 = [field.name for field in demographic_schema.fields]

    colsall = [field.name for field in all_schema.fields]

    print("address", cols1)
    print("demographic", cols2)

    # intersection = list1_as_set.intersection(cols2)
