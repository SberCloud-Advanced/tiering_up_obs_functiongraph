import os

from com.obs.client.obs_client import ObsClient
from obs import PutObjectHeader
from obs import RestoreTier
import time

import json
import gzip

def handler (event, context):
    # Obtain and print an event
    print("event:" + json.dumps(event) )

    # Obtain and print the content of an SMN message
    message = event['record'][0]['smn']['message']
    print("message:" + message)

    # Parse content of an SMN message (JSON string) and convert it into a Python Dictionary
    data = json.loads(message)

    # Obtain and print region for event from the content of an SMN message
    event_region = data['Records'][0]['eventRegion']
    print("event_region:" + event_region)

    # Obtain and print name of bucket with trace files (according to tracking bucket, from CTS service) from the content of an SMN message
    bucket_name_tracker = data['Records'][0]['obs']['bucket']['name']
    print("bucket_name_tracker:" + bucket_name_tracker)

    # Obtain and print name of created .json.gz trace file with operation logs (according to tracking bucket, from CTS service) from the content of an SMN message
    object_key_tracker = data['Records'][0]['obs']['object']['key']
    print("object_key_tracker:" + object_key_tracker)

    # Obtain an Access Key
    ak = context.getAccessKey()
    # Obtain an Secret Key
    sk = context.getSecretKey()

    # Set up endpoint for OBS
    endpoint = "obs.ru-moscow-1.hc.sbercloud.ru"

    # Open connection to OBS
    conn = ObsClient(access_key_id=ak, secret_access_key=sk, server=endpoint, path_style=True, region="ru-moscow-1")

    # Construct the local path of the incoming file
    local_incoming_file_name = os.path.join(os.sep, "tmp", object_key_tracker)

    # Download the trace file with operation logs (according to tracking bucket, from CTS service) from the bucket with trace files
    resp = conn.getObject(bucket_name_tracker, object_key_tracker, local_incoming_file_name)

    # Decompress .json.gz trace file, read, parse, convert it into a Python Dictionary and print the operation logs from it
    with gzip.open(local_incoming_file_name, "rb") as f:
      logs_data = json.loads(f.read())
    print(json.dumps(logs_data))
    print (len(logs_data))

    # Obtain operation logs details for traces with GET.OBJECT tracker name (to make following operations ONLY for objects to which GET request were made)
    for i in logs_data:
        if "GET.OBJECT" in i['trace_name']:
            resource_name = i['resource_name']
            print("resource_name:" + resource_name)
            # Obtain and print name of bucket with objects (tracking bucket) from operation logs details
            backet_name_logs = resource_name.split(':')[0]
            print("backet_name_logs:" + backet_name_logs)
            # Obtain and print object name with objects (tracking bucket) from operation logs details
            object_key_logs = resource_name.split(':')[1]
            print("object_key_logs:" + object_key_logs)

            # Obtain and print storage class of the object (Standard storage class value is None, Warm storage class value is STANDARD_IA, Cold storage class value is GLACIER)
            resp = conn.getObjectMetadata(bucketName = backet_name_logs, objectKey = object_key_logs)
            object_storageClass = resp.body.storageClass
            print(object_storageClass)

            # Change storage class to STANDARD if storage class of the tracking object is WARM (value is STANDARD_IA)
            if object_storageClass == 'STANDARD_IA':
                headers = PutObjectHeader()
                headers.storageClass = 'STANDARD'
                # Change storage class in object header to STANDARD and print the message after this operation is completed
                resp = conn.putContent(bucketName = backet_name_logs, objectKey = object_key_logs, headers = headers)
                print("object " + object_key_logs + " has STANDARD storage class now!")

            # Change storage class to STANDARD if storage class of the tracking object is COLD (value is GLACIER)
            if object_storageClass == 'GLACIER':
                # Obtain and print restore status of the tracking object (Unrestored (when object is in archive and can not be downloaded) or Restored). If the tracking object is Unrestored, the value is None.
                object_restore = resp.body.restore
                print(object_restore)

                # If the tracking object is Unrestored (in archive)
                if object_restore is None:
                    # Restore the tracking object with the fastest restoration option - Expedited. Expedited restoration restores an object in 1 to 5 minutes.
                    resp = conn.restoreObject(bucketName = backet_name_logs, objectKey = object_key_logs, days = 1, tier = RestoreTier.EXPEDITED)
                    # Wait for 6 minutes
                    time.sleep(6 * 60)
                    headers = PutObjectHeader()
                    headers.storageClass = 'STANDARD'
                    # Change storage class in object header to STANDARD and print the message after this operation is completed
                    resp = conn.putContent(bucketName = backet_name_logs, objectKey = object_key_logs, headers = headers)
                    print("object " + object_key_logs + " has STANDARD storage class now!")

                # If the tracking object is Restored
                else:
                    headers = PutObjectHeader()
                    headers.storageClass = 'STANDARD'
                    # Change storage class in object header to STANDARD and print the message after this operation is completed
                    resp = conn.putContent(bucketName = backet_name_logs, objectKey = object_key_logs, headers = headers)
                    print("object " + object_key_logs + " has STANDARD storage class now!")

            # If storage class of the tracking object is already STANDARD (value is None) print the message about it
            if object_storageClass is None:
                print("object " + object_key_logs + " already has STANDARD storage class")

    return "ok"
