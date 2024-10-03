from gen_ai_hub.proxy.langchain.openai import OpenAIEmbeddings 
from langchain.text_splitter import CharacterTextSplitter 
from langchain_community.vectorstores.hanavector import HanaDB
from langchain.docstore.document import Document
from hdbcli import dbapi
import os
from dotenv import load_dotenv 
import requests
from requests.auth import HTTPBasicAuth 


load_dotenv() 
url = os.getenv("url")
port = os.getenv("port")
user = os.getenv("user")
passwd = os.getenv("passwd")

shi_username = os.getenv("shi_username")
shi_password = os.getenv("shi_password")

EMBEDDING_DEPLOYMENT_ID = os.getenv("EMBEDDING_DEPLOYMENT_ID") 
 
connection = dbapi.connect(
    address=url,
    port=port,
    user=user,
    password=passwd,
    autocommit=True,
    sslValidationCertificate=False
) 

def call_odata(url): 
    # Request headers to get the response in JSON format
    headers = {
        'Accept': 'application/json',  # Ensures response is in JSON format
        'Content-Type': 'application/json'  # Optional, if you're sending data
    }
    response = requests.get(url,headers=headers,auth=HTTPBasicAuth(shi_username, shi_password))
    # print(response)
    # Ensure the request was successful
    if response.status_code == 200: 
        return  response.json()
    else:
        raise Exception(f"Failed to fetch OData: {response.status_code} - {response.text}")
 
 
def remove_metadata(json_data):
    if isinstance(json_data, dict):
        # Remove __metadata if it exists at the current level
        json_data.pop('__metadata', None)
        # Recursively remove __metadata from nested dictionaries
        for key, value in json_data.items():
            remove_metadata(value)
    elif isinstance(json_data, list):
        # If the current level is a list, iterate through each item
        for item in json_data:
            remove_metadata(item)

def json_to_text(json_data, indent=0):
    result = ""
    for key, value in json_data.items():
        if isinstance(value, dict):
            result += " " * indent + f"{key}:\n" + json_to_text(value, indent + 2)
        elif isinstance(value, list):
            result += " " * indent + f"{key} contains:\n"
            for idx, item in enumerate(value):
                result += " " * (indent + 2) + f"Item {idx + 1}:\n" + json_to_text(item, indent + 4)
        else:
            result += " " * indent + f"{key}: {value}\n"
    return result

# Function to parse the OData JSON and extract relevant fields
def extract_data_from_odata_JSON(json_data):
    extracted_data = []
    remove_metadata(json_data)   
    results_json = json_data['d']
    readable_string = json_to_text(results_json)
    # return readable_string
    doc = Document(page_content=readable_string)
    extracted_data.append(doc)
    return extracted_data
 
# Process the extracted odata and store in vector db
def process_odata_documents(json_data, connection, EMBEDDING_DEPLOYMENT_ID):
    try:
        # Extract data from OData JSON
        documents = extract_data_from_odata_JSON(json_data)
        if not documents:
            raise ValueError("No documents extracted from OData JSON.")
        
        # Initialize text splitter with defined parameters
        text_splitter = CharacterTextSplitter(separator="\n", chunk_size=5000, chunk_overlap=50)
        
        # Split documents into chunks
        texts = text_splitter.split_documents(documents)
        if not texts:
            raise ValueError("Text splitting resulted in no chunks.")
        
        # print(f"Number of document chunks: {len(texts)}")

        # Initialize embeddings
        embeddings = OpenAIEmbeddings(deployment_id=EMBEDDING_DEPLOYMENT_ID)
        
        # Initialize the database with connection details and table name
        db = HanaDB(embedding=embeddings, connection=connection, table_name="GMT_SPEC_SEARCH_VECTOR")
        # print(db)

        # Delete existing records if any
        # db.delete(filter={})
        
        # Add the new document chunks
        db.add_documents(texts)
        
        # print("Documents processed and added to the database successfully.")         

    except Exception as e:
        # Raise the exception with the error message to be handled by the calling code
        raise RuntimeError(f"An error occurred during the OData document processing: {str(e)}")
    
    # Process odata
def process_odata(odata_url): 
    try:         
            json_data = call_odata(odata_url)  
            process_odata_documents(json_data, connection, EMBEDDING_DEPLOYMENT_ID)  
    except Exception as e:
        # Return an error message if there was an exception
       raise