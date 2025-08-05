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
        token_data = token_res.json()
        access_token = token_data['access_token']
        
        # Now get tenant information
        tenants_url = "https://api.xero.com/connections"
        tenants_headers = {
            'Authorization': f"Bearer {access_token}"
        }
        tenants_res = requests.get(tenants_url, headers=tenants_headers)
        
        if tenants_res.status_code == 200:
            tenants_data = tenants_res.json()
            if tenants_data:
                tenant_id = tenants_data[0].get('tenantId')
                print(f"Found tenant ID: {tenant_id}")
                return access_token, tenant_id
            else:
                print("No tenant information found")
                sys.exit(1)
        else:
            print(f"Failed to get tenant information. Status Code: {tenants_res.status_code}")
            print(f"Response: {tenants_res.text}")
            sys.exit(1)
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


def process_void_job(token, tenant_id, invoice_ids, all_at_once):
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

        void_invoice(token, tenant_id, invoice_id, processed + 1, total_invoices, eta_formatted)
        processed += 1

    total_elapsed = time.time() - start_time
    total_formatted = time.strftime("%H:%M:%S", time.gmtime(total_elapsed))
    print(f"Completed processing {total_invoices} invoices in {total_formatted}.")


def void_invoice(token, tenant_id, invoice_number, processed, total, eta_formatted):
    """
    Voids a given invoice number
    """
    try:
        print(f"Asking Xero to void {invoice_number}")
        
        # 1) Find the invoice by number
        import urllib.parse
        encoded_invoice_number = urllib.parse.quote(invoice_number)
        get_list_url = f"https://api.xero.com/api.xro/2.0/{VOID_TYPE}?where=InvoiceNumber==\"{encoded_invoice_number}\""
        get_headers = {
            'Accept': 'application/json',
            'Authorization': f"Bearer {token}",
            'xero-tenant-id': tenant_id
        }
        
        list_res = requests.get(get_list_url, headers=get_headers)
        if list_res.status_code != 200:
            print(f"Failed to retrieve invoice list for {invoice_number}: {list_res.status_code}")
            print(f"Response: {list_res.text}")
            print(f"Skipping this invoice and continuing with others...")
            return

        data = list_res.json()
        if not data.get('Invoices'):
            print(f"No invoice found with number {invoice_number}")
            print(f"Skipping this invoice and continuing with others...")
            return

        invoice_id = data['Invoices'][0]['InvoiceID']
        print(f"Found invoice {invoice_number} with ID {invoice_id}")

        # 2) Fetch full invoice details
        get_full_url = f"https://api.xero.com/api.xro/2.0/{VOID_TYPE}/{invoice_id}"
        full_res = requests.get(get_full_url, headers=get_headers)
        if full_res.status_code != 200:
            print(f"Failed to retrieve full details for {invoice_number}: {full_res.status_code}")
            print(f"Response: {full_res.text}")
            print(f"Skipping this invoice and continuing with others...")
            return

        invoice = full_res.json()['Invoices'][0]
        current_status = invoice.get('Status', 'UNKNOWN')
        print(f"Current status of {invoice_number}: {current_status}")

        if current_status == 'VOIDED':
            print(f"{invoice_number} already voided; skipping.")
            return

        # 3) Void via update endpoint
        # Use the exact invoice data from the API response to avoid rounding errors
        void_url = f"https://api.xero.com/api.xro/2.0/{VOID_TYPE}"
        void_headers = {
            'Accept': 'application/json',
            'Authorization': f"Bearer {token}",
            'xero-tenant-id': tenant_id,
            'Content-Type': 'application/json'
        }
        
        # Generate a unique idempotency key for this request
        import uuid
        idempotency_key = str(uuid.uuid4())
        void_headers['Idempotency-Key'] = idempotency_key
        
        # Create void payload using the exact invoice data to avoid rounding errors
        void_invoice_data = {
            "InvoiceID": invoice_id,
            "Status": "VOIDED"
        }
        
        # Add other required fields from the original invoice to maintain consistency
        if 'Type' in invoice:
            void_invoice_data['Type'] = invoice['Type']
        if 'Contact' in invoice:
            void_invoice_data['Contact'] = invoice['Contact']
        if 'Date' in invoice:
            void_invoice_data['Date'] = invoice['Date']
        if 'DueDate' in invoice:
            void_invoice_data['DueDate'] = invoice['DueDate']
        if 'LineAmountTypes' in invoice:
            void_invoice_data['LineAmountTypes'] = invoice['LineAmountTypes']
        if 'LineItems' in invoice:
            void_invoice_data['LineItems'] = invoice['LineItems']
        
        void_payload = {
            "Invoices": [void_invoice_data]
        }
        
        void_res = post_xero_api_call(void_url, void_headers, void_payload)
        if void_res.status_code in (200, 204):
            print(f"Voided {invoice_number} successfully! ({processed}/{total}) ETA remaining: {eta_formatted}")
        else:
            print(f"Failed to void {invoice_number}: {void_res.status_code}")
            print(f"Response: {void_res.text}")
            
            # Try to parse the response for validation errors
            try:
                error_response = void_res.json()
                has_validation_errors = False
                has_minor_rounding_error = False
                
                # Check for validation errors in the top level
                if 'ValidationErrors' in error_response:
                    has_validation_errors = True
                    for error in error_response['ValidationErrors']:
                        print(f"Validation Error: {error['Message']}")
                        
                        # Handle line total mismatch errors specifically
                        if 'line total' in error['Message'].lower():
                            # Extract numeric values from error message
                            import re
                            numbers = re.findall(r'\d+\.\d+', error['Message'])
                            if len(numbers) >= 2:
                                actual = float(numbers[0])
                                expected = float(numbers[1])
                                difference = abs(actual - expected)
                                
                                # Check if difference is within tolerance (penny/cent difference)
                                if difference <= 0.02:  # Allow up to 2 pence/cents difference
                                    print(f"NOTE: This is a minor floating-point precision issue (difference: {difference:.4f}).")
                                    print(f"This is a known issue with Xero's API and floating-point arithmetic.")
                                    has_minor_rounding_error = True
                
                # Check for validation errors in Elements
                if 'Elements' in error_response and len(error_response['Elements']) > 0:
                    for element in error_response['Elements']:
                        if 'ValidationErrors' in element:
                            has_validation_errors = True
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
                                        difference = abs(actual - expected)
                                        # Check if difference is within tolerance (penny/cent difference)
                                        if difference <= 0.02:  # Allow up to 2 pence/cents difference
                                            print(f"NOTE: This is a minor floating-point precision issue (difference: {difference:.4f}).")
                                            print(f"This is a known issue with Xero's API and floating-point arithmetic.")
                                            has_minor_rounding_error = True

                # Provide appropriate guidance based on error analysis
                if has_validation_errors:
                    if has_minor_rounding_error:
                        print(f"IMPORTANT: This invoice ({invoice_number}) appears to have minor rounding errors that prevent automatic voiding.")
                        print(f"Please void this invoice manually in Xero as it is still in your CSV file.")
                        print(f"The invoice ID is: {invoice_id}")
                    else:
                        # Check if this might be a rounding error we missed
                        is_potential_rounding_error = False
                        if 'Elements' in error_response and len(error_response['Elements']) > 0:
                            for element in error_response['Elements']:
                                if 'ValidationErrors' in element:
                                    for error in element['ValidationErrors']:
                                        if 'line total' in error['Message'].lower():
                                            # Extract numeric values from error message
                                            import re
                                            numbers = re.findall(r'\d+\.\d+', error['Message'])
                                            if len(numbers) >= 2:
                                                actual = float(numbers[0])
                                                expected = float(numbers[1])
                                                difference = abs(actual - expected)
                                                
                                                # Check if difference is within tolerance (accept voids within ±0.05)
                                                if difference <= 0.05:
                                                    is_potential_rounding_error = True
                        if is_potential_rounding_error:
                            print(f"IMPORTANT: This invoice ({invoice_number}) appears to have minor rounding errors that prevent automatic voiding.")
                            print(f"Please void this invoice manually in Xero as it is still in your CSV file.")
                            print(f"The invoice ID is: {invoice_id}")
                        else:
                            print(f"This invoice has validation errors that are not related to rounding issues.")
                            print(f"Please check the invoice manually in Xero.")
                else:
                    print(f"No validation errors found in response, but voiding still failed.")
                    print(f"Please check the invoice manually in Xero.")
            except Exception as e:
                print(f"Error processing response: {str(e)}")
                print(f"Response text: {void_res.text}")
            print(f"Failed to void {invoice_number}. ({processed}/{total}) ETA remaining: {eta_formatted}")
    except requests.exceptions.RequestException as e:
        print(f"Network error processing invoice {invoice_number}: {str(e)}")
        print(f"Skipping {invoice_number} and continuing with the next invoice.")
        return
    except json.JSONDecodeError as e:
        print(f"JSON decode error processing invoice {invoice_number}: {str(e)}")
        print(f"Skipping {invoice_number} and continuing with the next invoice.")
        return
    except Exception as e:
        print(f"Unexpected error processing invoice {invoice_number}: {str(e)}")
        print(f"Skipping {invoice_number} and continuing with the next invoice.")
        return

def main():
    """
    Main execution loop
    """
    try:
        # Get token from Xero
        print("Asking Xero for an Access Token...")
        token, tenant_id = get_token()
        
        # Find all CSV files in the current directory
        csv_files = find_csv_files()
        all_invoice_ids = set()  # Use a set to avoid duplicates
        
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
                process_void_job(token, tenant_id, all_invoice_ids, all_at_once=False)
            else:
                print("Warning: The Xero API limit is 60 calls per minute. You are voiding less than 60 so we will blast through them.")
                process_void_job(token, tenant_id, all_invoice_ids, all_at_once=True)
    except Exception as err:
        print(f"Encountered an error: {str(err)}")


if __name__ == "__main__":
    print("Running bulk void tool...")
    check_config()
    main()
    print("Exiting...")
