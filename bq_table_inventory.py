"""
Author:Chetan Dixit
This code reads Bigquery Table metadata and stores in a Bigquery Table.
Purpose of this code is to build an inventory of Bigquery Tables where there are
too many tables being created and Bigquery Storage costs are high.
This code could help in getting metadata on a periodic basis and take correctives actions as required.
Table has an attribute called insertDatetime which represents the datetime when this metadata is captured.
You can get snapshot of that day based insertDatetime.

"""
from google.cloud.bigquery import client
from google.cloud.bigquery import dataset as bq_dataset
# from google.cloud.bigquery import table as bq_table
# from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery import Table
from google.oauth2 import service_account
# import json
from datetime import datetime
from datetime import timezone

# Details of the GCP Project for which you would like to see all details (metadata) of tables
PROJECT_ID = "YOUR-PROJECT-ID"
SERVICE_ACCOUNT_KEY_FILE_PATH = "PATH-TO-SERVICE-ACCOUNT-KEY-FILE"

# for storing all bigquery inventory data i.e. metadata of these tables
PROJECT_ID2 = "YOUR-PROJECT-ID"
SERVICE_ACCOUNT_KEY_FILE_PATH2 = "PATH-TO-SERVICE-ACCOUNT-KEY-FILE"
DATASET_ID = "YOUR-DATASET-ID"
TABLE_ID = "bq_table_inventory"


def create_table(schema_file_name):
    # Creates table named bq_table_inventory to store metadata
    credentials2 = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_KEY_FILE_PATH2)
    ct_bigquery_client = client.Client(project=PROJECT_ID2, credentials=credentials2)
    table1 = Table.from_string(PROJECT_ID2 + "." + DATASET_ID + "." + TABLE_ID)
    table1.schema = prepare_schema(schema_file_name)
    table1.partitioning_type = 'DAY'
    ct_bigquery_client.create_table(table1, exists_ok=True)


def prepare_schema(schema_file_name):
    # name with full path or relative path from current directory if schema file not in same directory
    # It reads table.schema file in this code, file expected to be in same directory
    table_schema = []
    try:
        f = open(schema_file_name)
        for line in iter(f):
            li = line.strip()
            field_parts = []
            if not li.startswith("#"):
                field_parts = li.split(",")
                if len(field_parts) == 2:
                    table_field = client.SchemaField(field_parts[0].strip(),
                                                     field_parts[1].strip())
                elif len(field_parts) == 3:  # with mode
                    table_field = client.SchemaField(field_parts[0].strip(),
                                                     field_parts[1].strip(),
                                                     field_parts[2].strip())
                elif len(field_parts) == 4:  # with mode and description
                    table_field = client.SchemaField(field_parts[0].strip(),
                                                     field_parts[1].strip(),
                                                     field_parts[2].strip(),
                                                     field_parts[3].strip())
                table_schema.append(table_field)
        f.close()
    except IOError:
        print('Unable to open/find the {} schema file'.format(schema_file_name))
    return table_schema


# Starts here
create_table("table.schema")
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_KEY_FILE_PATH)
bq_client = client.Client(project=PROJECT_ID, credentials=credentials)
# get the list of all datasets in the intended project
dataset_list = bq_client.list_datasets(project=PROJECT_ID)

# Iterate over each dataset in the project
for i_dataset in dataset_list:
    v_dataset = bq_dataset.DatasetReference(project=PROJECT_ID, dataset_id=i_dataset.dataset_id)
    print("Processing Dataset : {}".format(i_dataset.dataset_id))
    table_list = bq_client.list_tables(dataset=v_dataset)
    # if table_list.page_number > 0:
    print("Processing Tables metadata")
    # table_metadata holds dictionary of table attributes one dict per table
    table_metadata = []
    for i_table in table_list:
        table = bq_client.get_table(i_table.reference)
        created = None
        expires = None
        # Datetime is converted to String to avoid serialization errors.
        if table.created:
            created = (table.created).strftime("%Y-%m-%d %H:%M:%S")
            modified = (table.modified).strftime("%Y-%m-%d %H:%M:%S")
        if table.expires:
            expires = (table.expires).strftime("%Y-%m-%d %H:%M:%S")
        table_attributes = {"insertDatetime": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                            "tableID": table.table_id,
                            "projectId": table.project,
                            "datasetId": table.dataset_id,
                            "numBytes": table.num_bytes,
                            "timePartitioning": str(table.time_partitioning),
                            "rangePartitioning": str(table.range_partitioning),
                            "clustering": table.clustering_fields,
                            "created": created,
                            "expires": expires,
                            "tableType": table.table_type,
                            "numRows": table.num_rows,
                            "lastModifiedTime": modified,
                            "location": table.location
                            # "numLongTermBytes": table.numLongTermBytes
                            }
        table_metadata.append(table_attributes)
    # Write data for dataset to bigquery tables
    # Bigquery Streaming Inserts
    print("Writing data for tables in dataset {}".format(i_dataset.dataset_id))
    if len(table_metadata) > 0:
        bq_client.insert_rows_json(
            PROJECT_ID + "." + DATASET_ID + "." + TABLE_ID + "$" + datetime.now(timezone.utc).strftime("%Y%m%d"),
            table_metadata,
            # row_ids=[None] * len(table_metadata)
        )  # Make an API request.
        print("Inserted data for tables in dataset {} total rows inserted {}".format(i_dataset.dataset_id,
                                                                                     len(table_metadata)))
