from urllib.request import urlopen
from bs4 import BeautifulSoup
import urllib.request
from urllib.error import HTTPError
from lxml.html import parse
import pandas as pd
import multiprocessing
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError, ClientAuthenticationError
import os
import sys


def connecToAzure() -> BlobServiceClient:
    # Retrieve the storage key from an environment variable
    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
    storage_key = os.getenv("AZURE_STORAGE_KEY")

    # Check if the key is not set
    if not storage_account:
        # Prompt the user to input the key if it's not set
         storage_account = input("Please enter your Azure Storage Account Name: ")

    # Check if the key is not set
    if not storage_key:
        # Prompt the user to input the key if it's not set
        storage_key = input("Please enter your Azure Storage Account Access Key: ")

    try:
        # Azure storage account connection string
        # Replace with your actual connection string from Azure portal
        blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName="+storage_account+";AccountKey="+storage_key+";EndpointSuffix=core.windows.net")

        # Attempt to list containers to test the connection
        containers = list(blob_service_client.list_containers())
        
    except ClientAuthenticationError as e:
        print("Authentication failed. Please check your connection string.")
        print(e)
        sys.exit(1)
    except ValueError as e:
        print("The connection string is invalid.")
        print(e)
        sys.exit(1) 
    except ResourceNotFoundError as e:
        print("Resource not found. Check if your storage account exists and you have access.")
        print(e)
        sys.exit(1) 
    except HttpResponseError as e:
        print("HTTP response error. Check if the connection string is correct.")
        print(e)
        sys.exit(1) 
    except Exception as e:
        print("An error occurred while testing the connection string.")
        print(e)
        sys.exit(1) 

    return blob_service_client

def cleanse_data(team):
    # Read data from file
    df = pd.read_csv(team+"_stats_all-time-by-season.csv", skiprows=1)

    print("Cleansing "+team+" data")

    # Data cleanup - column name header appear many times
    filter_condition = df['PLAYERPLAYER'] == 'PLAYERPLAYER'

    # Delete rows based on the filter condition
    df = df[~filter_condition]  # Use ~ to negate the filter condition
    # Rename bad column
    df = df.rename(columns={'caret-upcaret-downHRcaret-upcaret-downHR': 'HRHR'})
    # rename columns
    df.columns = [col[:len(col)//2] for col in df.columns]

    df['G'] = df['G'].astype(int)
    df['R'] = df['R'].astype(int)
    df['H'] = df['H'].astype(int)
    df['HR'] = df['HR'].astype(int)


    sorted_df = df.sort_values(by=['HR'], ascending=False)
    sorted_df.to_csv(team+'_stats_clean.csv', index=False)
    #print(sorted_df.head(10))

def extract_data(team):
    # Open the URL and read its HTML content
    # Loop thruogh all pages
    url = "https://www.mlb.com/"+team+"/stats/all-time-by-season?page="
    df = pd.DataFrame()

    for i in range(1,MAX_PAGES):
        url_p = url+str(i)
        try:
            req = urllib.request.Request(url_p,headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'})
        except HTTPError as e:
            print("urllib.error.HTTPError: ", e.reason)
            break

        print("Getting data from page: "+url_p)
        with urlopen(req) as response:
            html_content = response.read()

        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')

        # Find the table element (assuming it's the first table on the page)
        table = soup.find('table')

        # Extract data from the table
        table_data = []
        if table:
            # Iterate through rows of the table
            rows = table.find_all('tr')
            if len(rows) > 1:
                for row in rows:
                    row_data = []
                    # Iterate through cells of the row
                    for cell in row.find_all(['td', 'th']):
                        row_data.append(cell.get_text(strip=True))
                    table_data.append(row_data)
            else:
                # Empty page
                break
        else:
            # No page
            break

        #df = pd.DataFrame(table_data)
        if df.empty:
            df = pd.DataFrame(table_data)
        else:
            df = df._append(table_data, ignore_index=True)
        
        print("Done")

    #print(df)
    df.to_csv(team+'_stats_all-time-by-season.csv', index=False)
    #print(df.info())

def upload_data(team):

<<<<<<< HEAD
    # Azure storage account connection string
    # Replace with your actual connection string from Azure portal
    # Initialize a BlobServiceClient using the connection string
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

=======
>>>>>>> 65ce153 (uploading to Azure)
    # Define the container name and the blob (file) name
    container_name = "mlb"

    blob_stats = 'data/'+team+'/stats_all-time-by-season.csv'
    blob_clean = 'data/'+team+'/stats_clean.csv'

    # Create a BlobClient object
    blob_client_stats = blob_service_client.get_blob_client(container=container_name, blob=blob_stats)
    blob_client_clean = blob_service_client.get_blob_client(container=container_name, blob=blob_clean)

    # Upload the CSV file to Azure ADLS
    with open(team+'_stats_all-time-by-season.csv', "rb") as data:
        blob_client_stats.upload_blob(data, overwrite=True)  # overwrite=True ensures the file is replaced if it exists
    print(f"CSV file '{blob_stats}' uploaded to Azure ADLS successfully.")
    os.remove(team+'_stats_all-time-by-season.csv')
    
    
    with open(team+'_stats_clean.csv', "rb") as data:
        blob_client_clean.upload_blob(data, overwrite=True)  # overwrite=True ensures the file is replaced if it exists
    print(f"CSV file '{blob_clean}' uploaded to Azure ADLS successfully.")
    os.remove(team+'_stats_clean.csv')

# All data steps
def process_data(team):
    team = team.strip()
    extract_data(team)
    cleanse_data(team)
    upload_data(team)
    return

#main
def main():
    # Data for parallel processing
    #data_list = [data1, data2, ...]
    with open("mlb_teams.txt", "r") as f:
        teams = f.readlines()

    #teams = ["yankees", "orioles", "redsox", "rays", "brewers", "guardians", "royals", "twins" ]
    # Number of processes (adjust based on your system)
    num_processes = 4
    # Create a pool of worker processes
    pool = multiprocessing.Pool(processes=4)
    results = pool.map(process_data, teams)

# URL of the webpage containing the table
# for example: url = 'https://www.mlb.com/phillies/stats/all-time-by-season?page='
blob_service_client = connecToAzure()
if __name__ == "__main__":
    MAX_PAGES = 5000
    main()
