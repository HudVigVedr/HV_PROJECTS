import pyodbc
from email.mime.text import MIMEText


import sys
sys.path.append('C:/Python/HV_PROJECTS')
import _AUTH
import _DEF 

script_name = "BC_wmsItemLedger"
script_cat = "DWH_extract"
script_type = "Copying" 

# SQL Server connection settings
connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}"

sql_table = f"dbo." + script_name
columns_insert = [ "ODataEtag"
      ,"id"
      ,"systemCreatedAt"
      ,"systemCreatedBy"
      ,"systemModifiedAt"
      ,"systemModifiedBy"
      ,"entryNo"
      ,"entryType"
      ,"customerItemNo"
      ,"itemNo"
      ,"carrierNo"
      ,"locationNo"
      ,"sourceType"
      ,"sourceNo"
      ,"postingDate"
      ,"quantity"
      ,"unitOfMeasureCode"
      ,"qtyPerUnitOfMeasure"
      ,"quantityBase"
      ,"sourceEntryType"
      ,"locationPath"
      ,"storageChargeNo"
      ,"description"
      ,"batchNo"
      ,"itemCondition"
      ,"quantityCarrier"
      ,"dossierNo"
      ,"sourceCode"
      ,"buildingCode"
      ,"unitOfMeasureCodeBase"
      ,"length"
      ,"width"
      ,"height"
      ,"cubage"
      ,"globalDimension1Code"
      ,"globalDimension2Code"
      ,"specificationSetID"
      ,"carrierTypeCode"
      ,"externalCustomerItemNo"
      ,"reasonCode"
      ,"cycleCountingTaskEntryNo"
      ,"expirationDate"
      ,"productionDate"
      ,"receiptDate"
      ,"externalCarrierNo"
      ,"productionPeriod"
      ,"freezingDate"
      ,"containerNo"
      ,"vesselNo"
      ,"unitCostLCY"
      ,"totalCostLCY"
      ,"qtyPerCarrier"
      ,"receiptCarrierNo"
      ,"productionTime"
      ,"accompanyingDocSetID"
      ,"locationTypeCode"
      ,"locInvoiceClassCode"
      ,"dgAttributeSetID"
      ,"allocationAttrSetID"
      ,"documentType"
      ,"documentNo"
      ,"documentLineNo"
      ,"documentDate"
      ,"processLineNo"
      ,"processType"
      ,"processTypeLineNo"
      ,"processLineGroup"
      ,"netWeight"
      ,"grossWeight"
      ,"packageSetID"
      ,"carrierStackID"
      ,"weightRegistration"
      ,"featureRecognition"
      ,"zoneCode"
      ,"zoneType"
      ,"externalBatchNo"
      ,"partnerCode"
      ,"taricCode"
      ,"customsCode"
      ,"tariffNo"
      ,"countryOfOriginCode"
      ,"countryPurchasedCode"
      ,"customsValue"
      ,"customsValuePer"
      ,"customsCurrencyCode"
      ,"alcoholByVolume"
      ,"entrepotStorage"
      ,"privateStorage"
      ,"crossDocking"
      ,"productionCode"
      ,"carrierSetID"
      ,"activityGroupCode"
      ,"activityTypeCode"
      ,"actionCode"
      ,"invoiceTypeCode"
      ,"addressNo"
      ,"dimensionSetID"
      ,"userID"
      ,"createdDateTime"
      ,"modifiedDateTime"
      ,"systemCreatedEntry"
      ,"relatedEntryNo"
      ,"undoPosting"
      ,"itemRegisterNo"
      ,"attribute01"
      ,"attribute02"
      ,"attribute03"
      ,"attribute04"
      ,"attribute05"
      ,"attribute06"
      ,"attribute07"
      ,"attribute08"
      ,"attribute09"
      ,"attribute10"
      ,"itemCategoryCode"
      ,"productGroupCode"
      ,"sqlTimestamp"
      ,"apiCrossColumnFilter3PL"
      ,"apiContext3PL"
      ,"Entity"

]


# API endpoint URL (same as before) -> aanvullen
api_url = _AUTH.end_REST_BOLTRICS_BC
api_table = "wmsItemLedgerEntries"
#api_full = api_url + "/" + api_table + "?" + $filter=systemModifiedAt gt " + _DEF.yesterday_date +"T00:00:00Z&company="
api_full = api_url + "/" + api_table + "?company="

if __name__ == "__main__":
    print(f"{script_type} {script_name} to SQL/Staging...")
    connection = pyodbc.connect(connection_string)
    threshold = 0

    start_time = _DEF.datetime.now()
    overall_status = "Success"
    total_inserted_rows = 0

    try:
        _DEF.delete_sql_table(connection, sql_table)
        company_names = _DEF.get_company_names(connection)

        for company_name in company_names:
            api = f"{api_full}{company_name}"
            api_data_generator = _DEF.make_api_request(api, _AUTH.client_id, _AUTH.client_secret, _AUTH.token_url)
    
            data_to_insert = list(api_data_generator)
            row_count = len(data_to_insert)

            if row_count > threshold:
                _DEF.insert_data_into_sql(connection, data_to_insert, sql_table, company_name, columns_insert)
                #_DEF.insert_or_delete_and_insert_data_into_sql(connection, data_to_insert, sql_table, company_name, columns_insert)         
                inserted_rows = _DEF.count_rows(data_to_insert)
                total_inserted_rows += inserted_rows

                if inserted_rows != row_count:
                    overall_status = "Error"
                    error_details = f"Expected to insert {row_count} rows, but only {inserted_rows} were inserted."
                    _DEF.log_status(connection, "Error", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), row_count - inserted_rows, error_details, company_name, api)

                    _DEF.send_email_mfa(f"ErrorLog -> {script_name} / {script_cat}", error_details,  _AUTH.email_sender,  _AUTH.email_recipient, _AUTH.guid_blink, _AUTH.email_client_id, _AUTH.email_client_secret)  

    except Exception as e:
        overall_status = "Error"
        error_details = str(e)
        print(f"An error occurred: {e}")
        _DEF.log_status(connection, "Error", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), 0, error_details, "None", "N/A")

        _DEF.send_email_mfa(f"ErrorLog -> {script_name} / {script_cat}", error_details,  _AUTH.email_sender,  _AUTH.email_recipient, _AUTH.guid_blink, _AUTH.email_client_id, _AUTH.email_client_secret)  

    finally:
        if overall_status == "Success":
            success_message = f"Total rows inserted/updated: {total_inserted_rows}."
            _DEF.log_status(connection, "Success", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), total_inserted_rows, success_message, "All", "N/A")

        elif overall_status == "Error":
            # Additional logging for error scenario can be added here if needed
            pass