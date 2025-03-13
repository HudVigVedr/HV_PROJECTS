## -> Step 1: adjust paths ##
# Local path
path_script = "C:/Python/HV_PROJECTS"
#server_path = "C:/Python/ft_projects"

## -> No changes needed for these imports ##
import pyodbc
from email.mime.text import MIMEText
import sys
sys.path.append(path_script)
import _AUTH
import _DEF

## -> Step 2: Adjust script variables ##
# Variables for logging
script_name = "BC_customers_boltrics_Bulk"
script_cat = "DWH_extract"

# Variables for the destination table and columns
sql_table = "BC_Customers_boltrics"
staging_table = "_staging_" + sql_table 
columns_insert = [
    "[@odata.etag]",
    "id",
    "systemCreatedAt",
    "systemCreatedBy",
    "systemModifiedAt",
    "systemModifiedBy",
    "no",
    "name",
    "searchName",
    "name2",
    "address",
    "address2",
    "city",
    "contact",
    "phoneNo",
    "telexNo",
    "documentSendingProfile",
    "shipToCode",
    "ourAccountNo",
    "territoryCode",
    "globalDimension1Code",
    "globalDimension2Code",
    "chainName",
    "budgetedAmount",
    "creditLimitLCY",
    "customerPostingGroup",
    "currencyCode",
    "customerPriceGroup",
    "languageCode",
    "statisticsGroup",
    "paymentTermsCode",
    "finChargeTermsCode",
    "salespersonCode",
    "shipmentMethodCode",
    "shippingAgentCode",
    "placeOfExport",
    "invoiceDiscCode",
    "customerDiscGroup",
    "countryRegionCode",
    "collectionMethod",
    "amount",
    "comment",
    "blocked",
    "invoiceCopies",
    "lastStatementNo",
    "printStatements",
    "billToCustomerNo",
    "priority",
    "paymentMethodCode",
    "lastModifiedDateTime",
    "lastDateModified",
    "balance",
    "balanceLCY",
    "netChange",
    "netChangeLCY",
    "salesLCY",
    "profitLCY",
    "invDiscountsLCY",
    "pmtDiscountsLCY",
    "balanceDue",
    "balanceDueLCY",
    "payments",
    "invoiceAmounts",
    "crMemoAmounts",
    "financeChargeMemoAmounts",
    "paymentsLCY",
    "invAmountsLCY",
    "crMemoAmountsLCY",
    "finChargeMemoAmountsLCY",
    "outstandingOrders",
    "shippedNotInvoiced",
    "applicationMethod",
    "pricesIncludingVAT",
    "locationCode",
    "faxNo",
    "telexAnswerBack",
    "vatRegistrationNo",
    "combineShipments",
    "genBusPostingGroup",
    "gln",
    "postCode",
    "county",
    "eoriNumber",
    "useGLNInElectronicDocument",
    "debitAmount",
    "creditAmount",
    "debitAmountLCY",
    "creditAmountLCY",
    "eMail",
    "homePage",
    "reminderTermsCode",
    "reminderAmounts",
    "reminderAmountsLCY",
    "noSeries",
    "taxAreaCode",
    "taxLiable",
    "vatBusPostingGroup",
    "outstandingOrdersLCY",
    "shippedNotInvoicedLCY",
    "reserve",
    "blockPaymentTolerance",
    "pmtDiscToleranceLCY",
    "pmtToleranceLCY",
    "icPartnerCode",
    "refunds",
    "refundsLCY",
    "otherAmounts",
    "otherAmountsLCY",
    "prepayment",
    "outstandingInvoicesLCY",
    "outstandingInvoices",
    "billToNoOfArchivedDoc",
    "sellToNoOfArchivedDoc",
    "partnerType",
    "image",
    "privacyBlocked",
    "disableSearchByName",
    "preferredBankAccountCode",
    "coupledToCRM",
    "cashFlowPaymentTermsCode",
    "primaryContactNo",
    "contactType",
    "mobilePhoneNo",
    "responsibilityCenter",
    "shippingAdvice",
    "shippingTime",
    "shippingAgentServiceCode",
    "serviceZoneCode",
    "contractGainLossAmount",
    "outstandingServOrdersLCY",
    "servShippedNotInvoicedLCY",
    "outstandingServInvoicesLCY",
    "priceCalculationMethod",
    "allowLineDisc",
    "noOfQuotes",
    "noOfBlanketOrders",
    "noOfOrders",
    "noOfInvoices",
    "noOfReturnOrders",
    "noOfCreditMemos",
    "noOfPstdShipments",
    "noOfPstdInvoices",
    "noOfPstdReturnReceipts",
    "noOfPstdCreditMemos",
    "noOfShipToAddresses",
    "billToNoOfQuotes",
    "billToNoOfBlanketOrders",
    "billToNoOfOrders",
    "billToNoOfInvoices",
    "billToNoOfReturnOrders",
    "billToNoOfCreditMemos",
    "billToNoOfPstdShipments",
    "billToNoOfPstdInvoices",
    "billToNoOfPstdReturnR",
    "billToNoOfPstdCrMemos",
    "baseCalendarCode",
    "copySellToAddrToQteFrom",
    "validateEUVatRegNo",
    "currencyId",
    "paymentTermsId",
    "shipmentMethodId",
    "paymentMethodId",
    "taxAreaID",
    "contactID",
    "contactGraphId",
    "wmsInvoicePeriodCode",
    "wmsBatchNos",
    "wmsCustomerItemNos",
    "wmsNVESSCC18Nos",
    "eMail2Invoice3PL",
    "wmsExtBatchNoMandPost",
    "wmsLabelCode",
    "wmsDefaultReceiptLocation",
    "wmsExtCarrierNoMandatory",
    "lastInvoiced3PL",
    "billToContactNo3PL",
    "billToContact3PL",
    "wmsExpDateReceiptDate",
    "wmsExpDateShipmentDate",
    "wmsWeightMandatory",
    "skypeName3PL",
    "wmsProductionDateMand",
    "wmsExpirationDateMand",
    "no23PL",
    "emergencyPhoneNo3PL",
    "postalAddress3PL",
    "postalCity3PL",
    "postalCountryRegionCode3PL",
    "postalPostCode3PL",
    "wmsCrossDockingEnabled",
    "wmsBlockPartialPalletShp",
    "wmsDefaultReservationMethod",
    "wmsExtBatchNoMandCreat",
    "wmsDefaultShipmentLocation",
    "wmsFreezingDateMandatory",
    "wmsOrderPickType",
    "wmsConditionSetID",
    "correspondenceType3PL",
    "defaultFunctionSetID3PL",
    "invoiceinfoDescrCode3PL",
    "docAttCategoryCode3PL",
    "registrationNo3PL",
    "wmsCostWarningAtShipment",
    "wmsExtBatchNoEqualBatchNo",
    "wmsCustItemUnitCostMand",
    "wmsEANCode",
    "wmsReuseExtDocNoReceipt",
    "wmsReuseExtDocNoPstRcpt",
    "wmsReuseExtDocNoShipment",
    "wmsReuseExtDocNoPstShpt",
    "wmsReuseExtDocNoCTransf",
    "wmsReuseExtDocNoPstCTr",
    "wmsReuseExtDocNoVAL",
    "postalAddress23PL",
    "wmsReuseExtDocNoPstVAL",
    "statusTemplateCode3PL",
    "statusCode3PL",
    "declarantNo3PL",
    "dgvsCode3PL",
    "includeInGPA3PL",
    "docInfoSetID3PL",
    "combinePackage3PL",
    "combinePDF3PL",
    "attribute013PL",
    "attribute023PL",
    "attribute033PL",
    "attribute043PL",
    "attribute053PL",
    "attribute063PL",
    "attribute073PL",
    "attribute083PL",
    "attribute093PL",
    "attribute103PL",
    "fromAddressNo3PL",
    "toAddressNo3PL",
    "defaultInvoiceGrouping3PL",
    "customvatRegistrationNo3PL",
    "podMandForInvoicing3PL",
    "oneTimeCustomer3PL",
    "createdDateTime3PL",
    "createdUserID3PL",
    "Entity"
]

# Variables for API request
api_table = "customers"
api_full = _AUTH.end_REST_BOLTRICS_BC + "/" + api_table + "?company="

###############################################################################
# Helper functions for staging/bulk operations
###############################################################################
def truncate_table(connection, table_name):
    """Clears out all records from the specified table."""
    cursor = connection.cursor()
    cursor.execute(f"TRUNCATE TABLE {table_name}")
    connection.commit()

def bulk_insert_staging(connection, data, staging_table, company_name, columns):
    """
    Bulk inserts a list of dictionaries (API records) into the staging table.
    It uses the column order defined in 'columns'. The 'Entity' column is set
    using the provided company_name.
    """
    if not data:
        return
    placeholders = ", ".join(["?"] * len(columns))
    columns_sql = ', '.join(columns)
    insert_sql = f"INSERT INTO {staging_table} ({columns_sql}) VALUES ({placeholders})"


    values = []
    for item in data:
        row = []
        for col in columns:
            if col == "Entity":
                row.append(company_name)
            else:
                row.append(item.get(col, None))
        values.append(row)
    
    cursor = connection.cursor()
    cursor.executemany(insert_sql, values)
    connection.commit()

def insert_staging_to_target(connection, staging_table, target_table, columns):
    """
    Performs a set-based INSERT from the staging table into the target table,
    selecting all columns in the defined order.
    """
    columns_sql = ", ".join([f"[{col}]" if "@" not in col else f"\"{col}\"" for col in columns])

    sql_insert = f"INSERT INTO {target_table} ({columns_sql}) SELECT {columns_sql} FROM {staging_table}"
    cursor = connection.cursor()
    cursor.execute(sql_insert)
    connection.commit()

###############################################################################
# Main Script
###############################################################################
if __name__ == "__main__":
    print(f"Bulk copying {script_name} to SQL/Staging ...")
    connection = pyodbc.connect(_AUTH.connection_string)
    threshold = 0  # Minimum row count threshold
    start_time = _DEF.datetime.now()
    overall_status = "Success"
    total_inserted_rows = 0

    try:
        # Truncate staging and target tables for a full refresh.
        truncate_table(connection, staging_table)
        truncate_table(connection, sql_table)
        
        # Get the list of companies from your helper function.
        company_names = _DEF.get_company_names(connection)
        for company_name in company_names:
            api = f"{api_full}{company_name}"
            api_data_generator = _DEF.make_api_request(api, _AUTH.client_id, _AUTH.client_secret, _AUTH.token_url)
            data_to_insert = list(api_data_generator)
            row_count = len(data_to_insert)
            
            if row_count > threshold:

                bulk_insert_staging(connection, data_to_insert, staging_table, company_name, columns_insert)
                total_inserted_rows += row_count
                print(f"Inserted {row_count} rows for company {company_name} into staging.")
        
        # Move all data from staging into the target table in one set-based operation.
        insert_staging_to_target(connection, staging_table, sql_table, columns_insert)
    
    except Exception as e:
        overall_status = "Error"
        error_details = str(e)
        print(f"An error occurred: {e}")
        _DEF.log_status(connection, "Error", script_cat, script_name, start_time, _DEF.datetime.now(),
                        int((_DEF.datetime.now() - start_time).total_seconds() / 60), 0, error_details, "None", "N/A")
        _DEF.send_email_mfa(f"ErrorLog -> {script_name} / {script_cat}",
                            error_details, _AUTH.email_sender, _AUTH.email_recipient,
                            _AUTH.guid_blink, _AUTH.email_client_id, _AUTH.email_client_secret)
    
    finally:
        if overall_status == "Success":
            success_message = f"Total rows inserted: {total_inserted_rows}."
            _DEF.log_status(connection, "Success", script_cat, script_name, start_time, _DEF.datetime.now(),
                            int((_DEF.datetime.now() - start_time).total_seconds() / 60), total_inserted_rows, success_message, "All", "N/A")
        connection.close()
