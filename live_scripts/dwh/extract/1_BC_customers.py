## -> Step 1: adjust paths ##

#Local path
path_script = "C:/Python/HV_PROJECTS"

#server_path
#path_script = "C:/Python/ft_projects"

#No changes needed
import pyodbc
from email.mime.text import MIMEText
import sys
sys.path.append(path_script)
import _AUTH
import _DEF 


## -> Step 2: Adjust script variables ##

# Variables for logging
script_name = "BC_customers"
script_cat = "DWH"

# Variables for the destination table
sql_table = "dbo.BC_Customers"
columns_insert = [
    "[@odata.etag]"
      ,"id"
      ,"systemCreatedAt"
      ,"systemCreatedBy"
      ,"systemModifiedAt"
      ,"systemModifiedBy"
      ,"no"
      ,"name"
      ,"searchName"
      ,"name2"
      ,"address"
      ,"address2"
      ,"city"
      ,"contact"
      ,"phoneNo"
      ,"telexNo"
      ,"documentSendingProfile"
      ,"shipToCode"
      ,"ourAccountNo"
      ,"territoryCode"
      ,"globalDimension1Code"
      ,"globalDimension2Code"
      ,"chainName"
      ,"budgetedAmount"
      ,"creditLimitLCY"
      ,"customerPostingGroup"
      ,"currencyCode"
      ,"customerPriceGroup"
      ,"languageCode"
      ,"statisticsGroup"
      ,"paymentTermsCode"
      ,"finChargeTermsCode"
      ,"salespersonCode"
      ,"shipmentMethodCode"
      ,"shippingAgentCode"
      ,"placeOfExport"
      ,"invoiceDiscCode"
      ,"customerDiscGroup"
      ,"countryRegionCode"
      ,"collectionMethod"
      ,"amount"
      ,"comment"
      ,"blocked"
      ,"invoiceCopies"
      ,"lastStatementNo"
      ,"printStatements"
      ,"billToCustomerNo"
      ,"priority"
      ,"paymentMethodCode"
      ,"lastModifiedDateTime"
      ,"lastDateModified"
      ,"balance"
      ,"balanceLCY"
      ,"netChange"
      ,"netChangeLCY"
      ,"salesLCY"
      ,"profitLCY"
      ,"invDiscountsLCY"
      ,"pmtDiscountsLCY"
      ,"balanceDue"
      ,"balanceDueLCY"
      ,"payments"
      ,"invoiceAmounts"
      ,"crMemoAmounts"
      ,"financeChargeMemoAmounts"
      ,"paymentsLCY"
      ,"invAmountsLCY"
      ,"crMemoAmountsLCY"
      ,"finChargeMemoAmountsLCY"
      ,"outstandingOrders"
      ,"shippedNotInvoiced"
      ,"applicationMethod"
      ,"pricesIncludingVAT"
      ,"locationCode"
      ,"faxNo"
      ,"telexAnswerBack"
      ,"vatRegistrationNo"
      ,"combineShipments"
      ,"genBusPostingGroup"
      ,"gln"
      ,"postCode"
      ,"county"
      ,"eoriNumber"
      ,"useGLNInElectronicDocument"
      ,"debitAmount"
      ,"creditAmount"
      ,"debitAmountLCY"
      ,"creditAmountLCY"
      ,"eMail"
      ,"homePage"
      ,"reminderTermsCode"
      ,"reminderAmounts"
      ,"reminderAmountsLCY"
      ,"noSeries"
      ,"taxAreaCode"
      ,"taxLiable"
      ,"vatBusPostingGroup"
      ,"outstandingOrdersLCY"
      ,"shippedNotInvoicedLCY"
      ,"reserve"
      ,"blockPaymentTolerance"
      ,"pmtDiscToleranceLCY"
      ,"pmtToleranceLCY"
      ,"icPartnerCode"
      ,"refunds"
      ,"refundsLCY"
      ,"otherAmounts"
      ,"otherAmountsLCY"
      ,"prepayment"
      ,"outstandingInvoicesLCY"
      ,"outstandingInvoices"
      ,"billToNoOfArchivedDoc"
      ,"sellToNoOfArchivedDoc"
      ,"partnerType"
      ,"image"
      ,"privacyBlocked"
      ,"disableSearchByName"
      ,"preferredBankAccountCode"
      ,"coupledToCRM"
      ,"cashFlowPaymentTermsCode"
      ,"primaryContactNo"
      ,"contactType"
      ,"mobilePhoneNo"
      ,"responsibilityCenter"
      ,"shippingAdvice"
      ,"shippingTime"
      ,"shippingAgentServiceCode"
      ,"serviceZoneCode"
      ,"contractGainLossAmount"
      ,"outstandingServOrdersLCY"
      ,"servShippedNotInvoicedLCY"
      ,"outstandingServInvoicesLCY"
      ,"priceCalculationMethod"
      ,"allowLineDisc"
      ,"noOfQuotes"
      ,"noOfBlanketOrders"
      ,"noOfOrders"
      ,"noOfInvoices"
      ,"noOfReturnOrders"
      ,"noOfCreditMemos"
      ,"noOfPstdShipments"
      ,"noOfPstdInvoices"
      ,"noOfPstdReturnReceipts"
      ,"noOfPstdCreditMemos"
      ,"noOfShipToAddresses"
      ,"billToNoOfQuotes"
      ,"billToNoOfBlanketOrders"
      ,"billToNoOfOrders"
      ,"billToNoOfInvoices"
      ,"billToNoOfReturnOrders"
      ,"billToNoOfCreditMemos"
      ,"billToNoOfPstdShipments"
      ,"billToNoOfPstdInvoices"
      ,"billToNoOfPstdReturnR"
      ,"billToNoOfPstdCrMemos"
      ,"baseCalendarCode"
      ,"copySellToAddrToQteFrom"
      ,"validateEUVatRegNo"
      ,"currencyId"
      ,"paymentTermsId"
      ,"shipmentMethodId"
      ,"paymentMethodId"
      ,"taxAreaID"
      ,"contactID"
      ,"contactGraphId"
      ,"wmsInvoicePeriodCode"
      ,"wmsBatchNos"
      ,"wmsCustomerItemNos"
      ,"wmsNVESSCC18Nos"
      ,"eMail2Invoice3PL"
      ,"wmsExtBatchNoMandPost"
      ,"wmsLabelCode"
      ,"wmsDefaultReceiptLocation"
      ,"wmsExtCarrierNoMandatory"
      ,"lastInvoiced3PL"
      ,"billToContactNo3PL"
      ,"billToContact3PL"
      ,"wmsExpDateReceiptDate"
      ,"wmsExpDateShipmentDate"
      ,"wmsWeightMandatory"
      ,"skypeName3PL"
      ,"wmsProductionDateMand"
      ,"wmsExpirationDateMand"
      ,"no23PL"
      ,"emergencyPhoneNo3PL"
      ,"postalAddress3PL"
      ,"postalCity3PL"
      ,"postalCountryRegionCode3PL"
      ,"postalPostCode3PL"
      ,"wmsCrossDockingEnabled"
      ,"wmsBlockPartialPalletShp"
      ,"wmsDefaultReservationMethod"
      ,"wmsExtBatchNoMandCreat"
      ,"wmsDefaultShipmentLocation"
      ,"wmsFreezingDateMandatory"
      ,"wmsOrderPickType"
      ,"wmsConditionSetID"
      ,"correspondenceType3PL"
      ,"defaultFunctionSetID3PL"
      ,"invoiceinfoDescrCode3PL"
      ,"docAttCategoryCode3PL"
      ,"registrationNo3PL"
      ,"wmsCostWarningAtShipment"
      ,"wmsExtBatchNoEqualBatchNo"
      ,"wmsCustItemUnitCostMand"
      ,"wmsEANCode"
      ,"wmsReuseExtDocNoReceipt"
      ,"wmsReuseExtDocNoPstRcpt"
      ,"wmsReuseExtDocNoShipment"
      ,"wmsReuseExtDocNoPstShpt"
      ,"wmsReuseExtDocNoCTransf"
      ,"wmsReuseExtDocNoPstCTr"
      ,"wmsReuseExtDocNoVAL"
      ,"postalAddress23PL"
      ,"wmsReuseExtDocNoPstVAL"
      ,"statusTemplateCode3PL"
      ,"statusCode3PL"
      ,"declarantNo3PL"
      ,"dgvsCode3PL"
      ,"includeInGPA3PL"
      ,"docInfoSetID3PL"
      ,"combinePackage3PL"
      ,"combinePDF3PL"
      ,"attribute013PL"
      ,"attribute023PL"
      ,"attribute033PL"
      ,"attribute043PL"
      ,"attribute053PL"
      ,"attribute063PL"
      ,"attribute073PL"
      ,"attribute083PL"
      ,"attribute093PL"
      ,"attribute103PL"
      ,"fromAddressNo3PL"
      ,"toAddressNo3PL"
      ,"defaultInvoiceGrouping3PL"
      ,"customvatRegistrationNo3PL"
      ,"podMandForInvoicing3PL"
      ,"oneTimeCustomer3PL"
      ,"createdDateTime3PL"
      ,"createdUserID3PL"
      ,"Entity"
]

# Variables for API request
api_table = "customers"
api_full = _AUTH.end_REST_MS_BC + "/" + api_table + "?company="

 
# No changes needed 
if __name__ == "__main__":
    print(f"Copying {script_name} to SQL/Staging ...")
    connection = pyodbc.connect(_AUTH.connection_string)
    threshold = 0

    start_time = _DEF.datetime.now()
    overall_status = "Success"
    total_inserted_rows = 0

    try:
        company_names = _DEF.get_company_names(connection)

        _DEF.delete_sql_table(connection, sql_table)

        for company_name in company_names:
            api = f"{api_full}{company_name}"
            api_data_generator = _DEF.make_api_request(api, _AUTH.client_id, _AUTH.client_secret, _AUTH.token_url)

            data_to_insert = list(api_data_generator)
            row_count = len(data_to_insert)

            
            if row_count > threshold:
                _DEF.insert_data_into_sql(connection, data_to_insert, sql_table, company_name, columns_insert)
                inserted_rows = _DEF.count_rows(data_to_insert)  # Assuming all rows are successfully inserted
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
            success_message = f"Total rows inserted: {total_inserted_rows}."
            _DEF.log_status(connection, "Success", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), total_inserted_rows, success_message, "All", "N/A")

        elif overall_status == "Error":
            # Additional logging for error scenario can be added here if needed
            pass