import configparser
import csv
import json
import math
import requests
import sys
import time
import os
import urllib.parse
from requests.auth import HTTPBasicAuth


# URL used to obtain tokens from Xero
XERO_TOKEN_URL = "https://identity.xero.com/connect/token"

# Read in the config.ini file
config = configparser.ConfigParser()
config.read('config.ini')
try:
    VOID_TYPE = str(config["DEFAULT"]["VOID_TYPE"])
    DRY_RUN = str(config['DEFAULT']['DRY_RUN'])
except KeyError:
    print("Please check your file is named config.ini - we couldn't find it")
    sys.exit(1)

def check_config():
    """
    Check the config entries are valid
    """
    # Immediately exit if someone hasn't set DRY_RUN properly
    if DRY_RUN not in ("Enabled", "Disabled"):
        print("Dry run needs to be set to Enabled or Disabled. Exiting...")
        sys.exit(1)

    # Check void type is supported, otherwise exit immediately
    if VOID_TYPE not in ("Invoices", "CreditNotes"):
        print("Void type needs to be Invoices or CreditNotes")
        sys.exit(1)


def get_token():
    """
    Obtains a token from Xero, lasts 30 minutes
    """
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    data = {
        'grant_type': "client_credentials",
        'scopes': ['accounting.transactions']
    }
    token_res = post_xero_api_call(XERO_TOKEN_URL, headers, data, auth=True)

    if token_res.status_code == 200:
        print(f"Obtained token, it will expire in 30 minutes")
        return token_res.json()['access_token']
    else:
        print("Couldn't fetch a token, have you set up the App at developer.xero.com?")
        print(f"Status Code: {token_res.status_code}")
        print(f"Response: {token_res.text}")
        sys.exit(1)  # Exit if token retrieval fails


def find_csv_files():
    """
    Finds all .csv files in the current working directory.
    """
    csv_files = [f for f in os.listdir('.') if f.endswith('.csv')]
    if not csv_files:
        print("No .csv files found in the current directory.")
        sys.exit(1)
    print(f"Found the following CSV files: {', '.join(csv_files)}")
    return csv_files


def read_invoice_numbers_from_csv(filename, column_name="InvoiceNumber"):
    """
    Opens a specific .csv file and returns a list of invoice numbers.
    """
    try:
        with open(filename, newline='', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile)
            if column_name not in reader.fieldnames:
                print(f"Error: Column '{column_name}' not found in {filename}.")
                return []
            return [row[column_name] for row in reader]
    except FileNotFoundError:
        print(f"Error: File not found - {filename}")
        return []
    except Exception as e:
        print(f"An error occurred with {filename}: {e}")
        return []


def post_xero_api_call(url, headers, data, auth=False):
    """
    Send a post request to Xero
    1) Auth true will pass client id and secret into BasicAuth
    2) Auth false expects you to have added the Bearer token header
    """
    if auth:
        xero_res = requests.post(
            url, 
            headers=headers, 
            auth=HTTPBasicAuth(config['DEFAULT']['CLIENT_ID'], config['DEFAULT']['CLIENT_SECRET']), 
            data=data
        )
    else:
        xero_res = requests.post(
            url, 
            headers=headers, 
            data=json.dumps(data)
        )
    return xero_res

def put_xero_api_call(url, headers, data):
    """
    Send a put request to Xero for updating existing resources
    """
    xero_res = requests.put(
        url, 
        headers=headers, 
        data=json.dumps(data)
    )
    return xero_res


def process_void_job(token, invoice_ids, all_at_once):
    """
    We either void instantly or wait 1 second inbetween API calls using all_at_once
    """
    total_invoices = len(invoice_ids)
    if total_invoices == 0:
        print("No invoices to process.")
        return

    print(f"Starting to process {total_invoices} invoices.")

    start_time = time.time()
    processed = 0

    for idx, invoice_id in enumerate(invoice_ids, start=1):
        if not all_at_once:
            # Sleep 1.5 seconds to respect rate limit
            # of 60 API calls max per minute
            time.sleep(1.5)
        # Calculate ETA before voiding to ensure accurate timing
        elapsed_time = time.time() - start_time
        if processed > 0:
            average_time_per_call = elapsed_time / processed
            remaining_calls = total_invoices - processed
            eta_seconds = remaining_calls * average_time_per_call
            eta_formatted = time.strftime("%H:%M:%S", time.gmtime(eta_seconds))
        else:
            eta_formatted = "Calculating..."

        void_invoice(token, invoice_id, processed + 1, total_invoices, eta_formatted)
        processed += 1

    total_elapsed = time.time() - start_time
    total_formatted = time.strftime("%H:%M:%S", time.gmtime(total_elapsed))
    print(f"Completed processing {total_invoices} invoices in {total_formatted}.")


def void_invoice(token, invoice_number, processed, total, eta_formatted):
    """
    Voids a given invoice number
    """
    print(f"Asking Xero to void {invoice_number}")

    # First, try to find the invoice by its invoice number using Xero's filtering
    import urllib.parse
    encoded_invoice_number = urllib.parse.quote(invoice_number)
    get_url = f"https://api.xero.com/api.xro/2.0/{VOID_TYPE}?where=InvoiceNumber=\"{encoded_invoice_number}\""
    get_headers = {
        'Accept': 'application/json',
        'Authorization': f"Bearer {token}"
    }
    
    # Get the specific invoice
    get_res = requests.get(get_url, headers=get_headers)
    
    if get_res.status_code != 200:
        print(f"Failed to retrieve invoice {invoice_number} from Xero. Status Code: {get_res.status_code}")
        print(f"Response: {get_res.text}")
        print(f"Skipping this invoice and continuing with others...")
        return
    
    try:
        invoices_data = get_res.json()
        invoice_id = None
        
        # Look for the specific invoice by invoice number
        if 'Invoices' in invoices_data and len(invoices_data['Invoices']) > 0:
            invoice_id = invoices_data['Invoices'][0].get('InvoiceID')
            print(f"Found invoice {invoice_number} with ID {invoice_id}")
        
        if not invoice_id:
            print(f"Invoice {invoice_number} not found in Xero.")
            print(f"Skipping this invoice and continuing with others...")
            return
        
        # Now void the invoice using its ID
        # Get the full invoice details first
        get_invoice_url = f"https://api.xero.com/api.xro/2.0/{VOID_TYPE}/{invoice_id}"
        get_invoice_res = requests.get(get_invoice_url, headers=get_headers)
        
        if get_invoice_res.status_code != 200:
            print(f"Failed to retrieve full invoice details. Status Code: {get_invoice_res.status_code}")
            print(f"Response: {get_invoice_res.text}")
            print(f"Skipping this invoice and continuing with others...")
            return
        
        try:
            full_invoice_data = get_invoice_res.json()
            if 'Invoices' in full_invoice_data and len(full_invoice_data['Invoices']) > 0:
                # Check the current status
                current_status = full_invoice_data['Invoices'][0].get('Status', 'UNKNOWN')
                print(f"Invoice {invoice_number} current status: {current_status}")
                
                # Check if invoice is already voided
                if current_status == 'VOIDED':
                    print(f"Invoice {invoice_number} is already voided. No action needed.")
                    return
                
                # According to Xero API documentation, to void an invoice we need to add a history record
                # with the status change, not update the invoice directly
                url = f"https://api.xero.com/api.xro/2.0/{VOID_TYPE}/{invoice_id}/history"
                headers = {
                    'Accept': 'application/json',
                    'Authorization': f"Bearer {token}",
                    'Content-Type': 'application/json'
                }
                data = {
                    "HistoryRecords": [
                        {
                            "Details": "Voided via API"
                        }
                    ],
                    "Status": "VOIDED"
                }
                
                # Use POST to add a history record that voids the invoice
                void_res = post_xero_api_call(url, headers, data)
                
                # Check if we have a response from the voiding attempt
                if void_res.status_code in (200, 204):  # 204 No Content is also a success
                    print(f"Voided {invoice_number} successfully! ({processed}/{total}) ETA remaining: {eta_formatted}")
                else:
                    print(f"Couldn't void {invoice_number} on first attempt, checking for validation errors...")
                    print(f"Status Code: {void_res.status_code}")
                    has_minor_rounding_error = False
                    try:
                        response_content = void_res.json()
                        print(f"Response: {response_content}")
                        
                        # Check for validation errors
                        has_validation_error = False
                        if 'Elements' in response_content and len(response_content['Elements']) > 0:
                            element = response_content['Elements'][0]
                            if 'ValidationErrors' in element:
                                has_validation_error = True
                                for error in element['ValidationErrors']:
                                    print(f"Validation Error: {error['Message']}")
                                    
                                    # Handle line total mismatch errors specifically
                                    if 'line total' in error['Message'].lower():
                                        # Extract numeric values from error message
                                        import re
                                        numbers = re.findall(r'\d+\.\d+', error['Message'])
                                        if len(numbers) >= 2:
                                            actual = float(numbers[0])
                                            expected = float(numbers[1])
                                            
                                            # Check if difference is within tolerance (penny/cent difference)
                                            if math.isclose(actual, expected, abs_tol=0.01):
                                                print(f"NOTE: This is a minor floating-point precision issue (difference: {abs(actual-expected):.4f}).")
                                                print(f"This invoice is in your CSV and should be voided despite the minor rounding error.")
                                                has_minor_rounding_error = True
                
                        # Handle top-level validation errors
                        elif 'ValidationErrors' in response_content:
                            has_validation_error = True
                            for error in response_content['ValidationErrors']:
                                print(f"Validation Error: {error['Message']}")
                                
                                # Handle line total mismatch errors specifically
                                if 'line total' in error['Message'].lower():
                                    # Extract numeric values from error message
                                    import re
                                    numbers = re.findall(r'\d+\.\d+', error['Message'])
                                    if len(numbers) >= 2:
                                        actual = float(numbers[0])
                                        expected = float(numbers[1])
                                        
                                        # Check if difference is within tolerance (penny/cent difference)
                                        if math.isclose(actual, expected, abs_tol=0.01):
                                            print(f"NOTE: This is a minor floating-point precision issue (difference: {abs(actual-expected):.4f}).")
                                            print(f"This invoice is in your CSV and should be voided despite the minor rounding error.")
                                            has_minor_rounding_error = True
                
                        # If we have a minor rounding error, we should still try to void the invoice
                        # as the error might be preventing the voiding from completing
                        if has_minor_rounding_error:
                            print(f"NOTE: This is a minor floating-point precision issue.")
                            print(f"The invoice may still be in its original state in Xero.")
                            print(f"Please check the invoice manually in Xero and void it if needed.")
                            return
                        
                        # If we have validation errors, the voiding attempt failed
                        # regardless of what the status field says
                        if has_validation_error:
                            print(f"This invoice has validation errors that prevent voiding.")
                            print(f"The invoice may still be in its original state in Xero.")
                            print(f"Please check the invoice manually in Xero.")
                            print(f"Skipping this invoice and continuing with others...")
                            return
                            
                    except json.JSONDecodeError:
                        print(f"Response Text: {void_res.text}")
                    except Exception as e:
                        print(f"Error processing response: {str(e)}")
                    print(f"Failed to void {invoice_number}. ({processed}/{total}) ETA remaining: {eta_formatted}")
            else:
                print(f"Failed to retrieve full invoice details for {invoice_number}.")
                print(f"Skipping this invoice and continuing with others...")
                return
        except Exception as e:
            print(f"Error processing full invoice data for {invoice_number}: {str(e)}")
            print(f"Skipping this invoice and continuing with others...")
            return
    except Exception as e:
        print(f"Error processing invoice {invoice_number}: {str(e)}")
        print(f"Skipping this invoice and continuing with others...")
        return


def main():
    """
    Main execution loop
    """
    try:
        # Request access token from Xero
        print("Asking Xero for an Access Token...")
        token = get_token()

        # Find all CSV files in the folder
        csv_files = find_csv_files()
        all_invoice_ids = set()

        # Read invoice numbers from all found CSV files
        for csv_file in csv_files:
            print(f"\nReading from {csv_file}...")
            invoice_ids_from_file = read_invoice_numbers_from_csv(csv_file)
            if invoice_ids_from_file:
                all_invoice_ids.update(invoice_ids_from_file)

        if not all_invoice_ids:
            print("No invoice numbers found in any of the CSV files. Exiting.")
            sys.exit(0)

        # Safety mechanism for those wanting to check before committing
        if DRY_RUN == "Enabled":
            print("Dry run is enabled, not voiding anything")
            print(f"Without Dry run we will void: \n{all_invoice_ids}")
        else:
            if len(all_invoice_ids) > 60:
                print("Warning: The Xero API limit is 60 calls per minute. We will void one per second.")
                process_void_job(token, all_invoice_ids, all_at_once=False)
            else:
                print("Warning: The Xero API limit is 60 calls per minute. You are voiding less than 60 so we will blast through them.")
                process_void_job(token, all_invoice_ids, all_at_once=True)
    except Exception as err:
        print(f"Encountered an error: {str(err)}")


if __name__ == "__main__":
    print("Running bulk void tool...")
    check_config()
    main()
    print("Exiting...")
