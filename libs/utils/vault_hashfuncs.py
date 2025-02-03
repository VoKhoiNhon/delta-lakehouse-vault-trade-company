DV_HASHKEY_COMPANY = "md5(concat_ws(';', jurisdiction, registration_number, pure_name))"
DV_HASHKEY_PERSON = "md5(concat_ws(';', jurisdiction, name, full_address))"
DV_HASHKEY_SANCTION = "md5(id)"
DV_HASHKEY_L_SANCTION_PERSON = (
    "md5(concat_ws(';', dv_hashkey_sanction, dv_hashkey_person))"
)
DV_HASHKEY_L_SANCTION_COMPANY = (
    "md5(concat_ws(';', dv_hashkey_sanction, dv_hashkey_company))"
)

# position can't null
DV_HASHKEY_L_PERSON_COMPANY_POSITION = (
    "md5(concat_ws(';', dv_hashkey_company, dv_hashkey_person, position))"
)

DV_HASHKEY_PORT = "md5(concat_ws(';',port_code,port_name))"

DV_HASHKEY_JURISDICTION = "md5(concat_ws(';', country_code, jurisdiction))"

DV_HASHKEY_INDUSTRY = "md5(concat_ws(';', country_code, industry_code))"

DV_HASHKEY_L_BOL = "md5(concat_ws(';',dv_hashkey_bol,buyer_dv_hashkey_company,\
    supplier_dv_hashkey_company,export_port,import_port,actual_arrival_date))"

DV_HASHKEY_BOL = "md5(concat_ws(';', bol, hs_code, teu_number, invoice_value,\
    value_usd,exchange_rate,description, actual_arrival_date,\
    estimated_arrival_date,vessel_name,quantity,quantity_unit,weight,weight_unit))"

DV_HASHKEY_EXPORT_PORT_ID = "md5(concat_ws(';',port_code,port_name))"

DV_HASHKEY_IMPORT_PORT_ID = "md5(concat_ws(';',port_code,port_name))"


# Link person - person
DV_HASHKEY_L_PERSON_PERSON_RELATED = (
    "md5(concat_ws(';', dv_hashkey_from_person, dv_hashkey_to_person, related_type))"
)

# Link company - company
DV_HASHKEY_L_COMPANY_COMPANY_POSITION = (
    "md5(concat_ws(';', dv_hashkey_from_company, dv_hashkey_to_company, position))"
)

# Link person - company
DV_HASHKEY_L_PERSON_COMPANY_POSITION = (
    "md5(concat_ws(';', dv_hashkey_person, dv_hashkey_company, position))"
)

DV_HASHKEY_TO_PERSON = "md5(concat_ws(';', to_jurisdiction, to_name, to_full_address))"

DV_HASHKEY_FROM_PERSON = (
    "md5(concat_ws(';', from_jurisdiction, from_name, from_full_address))"
)

DV_HASHKEY_FROM_COMPANY = (
    "md5(concat_ws(';', from_jurisdiction, from_registration_number, from_pure_name))"
)

DV_HASHKEY_TO_COMPANY = (
    "md5(concat_ws(';', to_jurisdiction, to_registration_number, to_pure_name))"
)
