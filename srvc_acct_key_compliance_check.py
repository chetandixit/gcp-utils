import json
import argparse
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

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

# Required. The resource name of the project associated with the service
# accounts, such as `projects/my-project-123`.
project_name = 'projects/' + args.projectid

credentials = GoogleCredentials.get_application_default()
service = discovery.build('iam', 'v1', credentials=credentials)

service_account_list = []
request = service.projects().serviceAccounts().list(name=project_name)
while True:
    response = request.execute()

    for service_account in response.get('accounts', []):
        service_account_list.append(service_account['name'])

    request = service.projects().serviceAccounts().list_next(
                                    previous_request=request,
                                    previous_response=response)
    if request is None:
        break
# print(service_account_list)
service_account_keys = {}
cnt = 1
for sa in service_account_list:
    request = service.projects().serviceAccounts().keys().list(name=sa)
    response = request.execute()
    service_account_keys[cnt] = response
    cnt = cnt + 1

f = open(args.projectid + '_service_accnt_key_compliance_check.json', 'w')
f.write(json.dumps(service_account_keys))
f.close()
