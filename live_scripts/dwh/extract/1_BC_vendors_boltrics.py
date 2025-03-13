## -> Step 1: adjust paths ##
# Local path
path_script = "C:/Python/HV_PROJECTS"
#server_path = "C:/Python/ft_projects"

import pyodbc
from email.mime.text import MIMEText
import sys
sys.path.append(path_script)
import _AUTH
import _DEF

## -> Step 2: Adjust script variables ##
# Variables for logging
script_name = "BC_vendors_boltrics_Bulk"
script_cat = "DWH_extract"

# Variables for the destination table and columns
sql_table = "BC_Vendors_boltrics"
staging_table =   "_staging_" + sql_table
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
    "ourAccountNo",
    "territoryCode",
    "globalDimension1Code",
    "globalDimension2Code",
    "budgetedAmount",
    "vendorPostingGroup",
    "currencyCode",
    "languageCode",
    "statisticsGroup",
    "paymentTermsCode",
    "finChargeTermsCode",
    "purchaserCode",
    "shipmentMethodCode",
    "shippingAgentCode",
    "invoiceDiscCode",
    "countryRegionCode",
    "comment",
    "blocked",
    "payToVendorNo",
    "priority",
    "paymentMethodCode",
    "lastModifiedDateTime",
    "lastDateModified",
    "balance",
    "balanceLCY",
    "netChange",
    "netChangeLCY",
    "purchasesLCY",
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
    "amtRcdNotInvoiced",
    "applicationMethod",
    "pricesIncludingVAT",
    "faxNo",
    "telexAnswerBack",
    "vatRegistrationNo",
    "genBusPostingGroup",
    "gln",
    "postCode",
    "county",
    "eoriNumber",
    "debitAmount",
    "creditAmount",
    "debitAmountLCY",
    "creditAmountLCY",
    "eMail",
    "homePage",
    "reminderAmounts",
    "reminderAmountsLCY",
    "noSeries",
    "taxAreaCode",
    "taxLiable",
    "vatBusPostingGroup",
    "outstandingOrdersLCY",
    "amtRcdNotInvoicedLCY",
    "blockPaymentTolerance",
    "pmtDiscToleranceLCY",
    "pmtToleranceLCY",
    "icPartnerCode",
    "refunds",
    "refundsLCY",
    "otherAmounts",
    "otherAmountsLCY",
    "prepayment",
    "outstandingInvoices",
    "outstandingInvoicesLCY",
    "payToNoOfArchivedDoc",
    "buyFromNoOfArchivedDoc",
    "partnerType",
    "image",
    "privacyBlocked",
    "disableSearchByName",
    "creditorNo",
    "preferredBankAccountCode",
    "coupledToCRM",
    "cashFlowPaymentTermsCode",
    "primaryContactNo",
    "mobilePhoneNo",
    "responsibilityCenter",
    "locationCode",
    "leadTimeCalculation",
    "priceCalculationMethod",
    "noOfPstdReceipts",
    "noOfPstdInvoices",
    "noOfPstdReturnShipments",
    "noOfPstdCreditMemos",
    "payToNoOfOrders",
    "payToNoOfInvoices",
    "payToNoOfReturnOrders",
    "payToNoOfCreditMemos",
    "payToNoOfPstdReceipts",
    "payToNoOfPstdInvoices",
    "payToNoOfPstdReturnS",
    "payToNoOfPstdCrMemos",
    "noOfQuotes",
    "noOfBlanketOrders",
    "noOfOrders",
    "noOfInvoices",
    "noOfReturnOrders",
    "noOfCreditMemos",
    "noOfOrderAddresses",
    "payToNoOfQuotes",
    "payToNoOfBlanketOrders",
    "noOfIncomingDocuments",
    "baseCalendarCode",
    "documentSendingProfile",
    "validateEUVatRegNo",
    "currencyId",
    "paymentTermsId",
    "paymentMethodId",
    "overReceiptCode",
    "no23PL",
    "postalAddress3PL",
    "postalCity3PL",
    "postalCountryRegionCode3PL",
    "postalPostCode3PL",
    "registrationNo3PL",
    "sortingFunctionCode3PL",
    "objectTypeSorting3PL",
    "objectNoSorting3PL",
    "postalAddress23PL",
    "statusTemplateCode3PL",
    "statusCode3PL",
    "allowNegativeInvoice3PL",
    "mappingMethod3PL",
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
    "vendorPriceGroup3PL",
    "charter3PL",
    "sortingTMSFunctSetID3PL",
    "wmsConditionSetID",
    "Entity"
]

# Variables for API request
api_table = "vendors"
api_full = _AUTH.end_REST_BOLTRICS_BC + "/" + api_table + "?company="

###############################################################################
# Helper functions for staging operations
###############################################################################
def truncate_table(connection, table_name):
    """Clears out all records from the specified table."""
    cursor = connection.cursor()
    cursor.execute(f"TRUNCATE TABLE {table_name}")
    connection.commit()

def bulk_insert_staging(connection, data, staging_table, company_name, columns):
    """
    Bulk inserts a list of dictionaries (API records) into the staging table.
    The 'Entity' column is set using the provided company_name.
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
        # Truncate staging and target tables to start with a clean slate.
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
        
        # Insert all records from staging into the target table in one set-based operation.
        insert_staging_to_target(connection, staging_table, sql_table, columns_insert)
    
    except Exception as e:
        overall_status = "Error"
        error_details = str(e)
        print(f"An error occurred: {e}")
        _DEF.log_status(
            connection,
            "Error",
            script_cat,
            script_name,
            start_time,
            _DEF.datetime.now(),
            int((_DEF.datetime.now() - start_time).total_seconds() / 60),
            0,
            error_details,
            "None",
            "N/A"
        )
        _DEF.send_email_mfa(
            f"ErrorLog -> {script_name} / {script_cat}",
            error_details,
            _AUTH.email_sender,
            _AUTH.email_recipient,
            _AUTH.guid_blink,
            _AUTH.email_client_id,
            _AUTH.email_client_secret
        )
    
    finally:
        if overall_status == "Success":
            success_message = f"Total rows inserted: {total_inserted_rows}."
            _DEF.log_status(
                connection,
                "Success",
                script_cat,
                script_name,
                start_time,
                _DEF.datetime.now(),
                int((_DEF.datetime.now() - start_time).total_seconds() / 60),
                total_inserted_rows,
                success_message,
                "All",
                "N/A"
            )
        connection.close()
