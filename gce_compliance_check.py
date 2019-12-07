"""
Author: Chetan Dixit
This is a sample code to get the list of compute engines in all zones.
You can get all the attributes of GCE instances,
important ones for compliance check
1. creation date
2. zone of GCE
3. status
5. service accounts

This code uses application default credentials.
You need to setup environment variable with correct service account
export GOOGLE_APPLICATION_CREDENTIALS=FULL_PATH_OF_SRVCE_ACCNT_KEY_JSON_FILE
"""
# from pprint import pprint
import json
import argparse
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials


def get_service():
    credentials = GoogleCredentials.get_application_default()
    service = discovery.build('compute', 'v1', credentials=credentials)
    return service


def get_zones(service, project):
    zone_list = []
    # Collect all zone names
    request = service.zones().list(project=project)
    while request is not None:
        response = request.execute()

        for zone in response['items']:
            # Get the list of all zones
            zone_list.append(zone['name'])

        request = service.zones().list_next(
                previous_request=request, previous_response=response)

    return zone_list


def get_instance_list(service, project, zone_list):
    instance_list = {}
    # List all instances in each zone
    for each_zone in zone_list:
        request = service.instances().list(project=project, zone=each_zone)
        # print("instance zone is " + each_zone)
        while request is not None:
            response = request.execute()
            # print("Response is: " + str(response))
            if response.get('items'):

                for instance in response['items']:
                    # TODO: Change code below toprocesseach`instance` resource:
                    # 'instance' has all attributes, select only of interest
                    # pprint(instance['name'])
                    temp_dict = {"creationTimestamp":
                                    instance['creationTimestamp'],
                                    "status":
                                    instance['status'],
                                    "zone":
                                    each_zone}
                    instance_list[instance['name']] = temp_dict
                    # instance_list.append(temp_dict)

            request = service.instances().list_next(
                previous_request=request, previous_response=response)
    return instance_list


def run(project):
    service = get_service()
    zones = get_zones(service, project)
    instances = get_instance_list(service, project, zones)

    # write results to a temp file
    f = open(project+'_instance_compliance_check.json', 'w')
    f.write(json.dumps(instances))
    f.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
                        description=__doc__,
                        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        '--projectid',
        required=True,
        help=('Provide Project Id')
        )
    args = parser.parse_args()
    run(args.projectid)
