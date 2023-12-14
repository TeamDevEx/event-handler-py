import os
import json
import requests
import config

from datetime import datetime
from google.cloud import storage
from google.auth import impersonated_credentials
from dotenv import load_dotenv
from flask import Flask, request, jsonify

app = Flask(__name__)

load_dotenv()
project_id = os.getenv("PROJECT_ID")
sa_path  = os.getenv("SA")
bucket_name = os.getenv("BUCKET_NAME")
folder_name = os.getenv("FOLDER_NAME")

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = sa_path
storage_client = storage.Client()

current_date = datetime.utcnow().isoformat()   
base_event = f"webhook-event/github/event_1_{current_date}.json"


def get_target_file(base_files):
    try:
        max_value = 0
        target = ""

        for i in base_files:
            chars = list(i)
            if len(chars) > 27:
                result = int(chars[27])
                if result > max_value:
                    max_value = result
                    target = i            
        return target
    except Exception as error:        
        print(f"An error occurred in get_target_file: {error}")
        return None  


def create_new_target_file(input_str):
    try:
        chars = list(input_str)
        result = int(chars[27]) + 1
        return result
    except (ValueError, IndexError) as error:        
        print(f"An error occurred in create_new_target_file: {error}")
        return None 

def check_batch_file():
    try:      
        bucket = storage_client.bucket(bucket_name)
        files = list(bucket.list_blobs(prefix=folder_name))
        base_files = [blob.name for blob in files]
        print("files", base_files)
        base_files = [file.name for file in files]
        print("Array of base files", base_files)

        target_exists = get_target_file(base_files)
        print("Does target exist?", target_exists)
   
        if not target_exists:
            print("CREATE A NEW BASE FILE")             
            bucket.blob(base_event).upload_from_string(json.dumps([]),content_type="application/json")      
            return {"file": base_event, "status": "success"}
        else:
            target_exists_download = bucket.blob(target_exists)
            record = target_exists_download.download_as_text()
            record_data = json.loads(record)
            print("Count exist object", len(record_data))

            if len(record_data) == 4:
                print("BASE FILE IS FULL CREATE A NEW A NEW ONE")
                new_target_number = create_new_target_file(target_exists)
                print("New Target number is:", new_target_number)
                new_target_file = f"webhook-event/github/event_{new_target_number}_{current_date}.json"                
                bucket.blob(new_target_file).upload_from_string(json.dumps([]),content_type="application/json")                             
                return {"file": new_target_file, "status": "success"}
            else:
                print("BASE FILE STILL EXISTS")
                return {"file": target_exists, "status": "success"}

    except Exception as error:
        return str(error)


def upload_to_gcs(payload):
    check_bucket = check_batch_file()
    print("checkBucket status", check_bucket)

    if check_bucket["status"] == "success":
        try:
            
            bucket = storage_client.bucket(bucket_name)        
                          
            existing_data = json.loads(bucket.blob(check_bucket['file']).download_as_text() )            
            print("file from bucket", len(existing_data))           
            existing_data.append(json.loads(payload))
            print("Count additional object", len(existing_data))               
              
            bucket.blob(check_bucket['file']).upload_from_string(json.dumps(existing_data), content_type="application/json")     
            print("successfully save to bucket")
            
        except requests.exceptions.RequestException as error:
            print(f"Error uploading JSON data to GCS: {error}")

@app.route('/upload_to_gcs', methods=['POST'])
def upload_to_gcs_route():
    try:
        payload = request.get_data().decode('utf-8')
        upload_to_gcs(payload)
        return jsonify({"status": "success"}), 200
    except Exception as error:
        print(f"Error in upload_to_gcs_route: {error}")
        return jsonify({"status": "error", "message": str(error)}), 500
    
if __name__ == "__main__":
    # app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", config.PORT)))
    app.run(host="127.0.0.1", port=config.PORT, debug=True)    