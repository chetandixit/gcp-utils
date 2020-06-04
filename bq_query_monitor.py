"""This function reads bigquery job metadata
It takes input as projectId and gets metadata for default last 24 Hours
Produces two output files
bq_monitor_YYYY-MM-DD.json which has details about query performance
bq_monitor_query_text_YYYY-MM-DD.json which has entire SQL Query Text for
corresponding jobids.
Output files contain important paramters such as execution time,
tables scanned, bytes total_bytes_billed etc.
These parameters could be analyzed to improve query performance and
reduce costs.
This can be further enhanced to put results into database
Author: Chetan Dixit
"""
from google.cloud import bigquery
import datetime
import argparse
import json


def get_data(projectid, hours=24, max_results=500):
    # Default parameters Last 24 Hours, return max 500 query stats
    # project = projectid # 'xw-winter-bloom-7'  # replace with your project ID
    client = bigquery.Client(project=projectid)
    mins_ago = (datetime.datetime.utcnow()
                - datetime.timedelta(minutes=hours*60))
    results = {}
    query_text = {}
    # Use all_users to include jobs run by all users in the project.
    for job in client.list_jobs(
                            min_creation_time=mins_ago,
                            max_results=max_results,
                            all_users=True):
        # get JobStatistics from job
        j = client.get_job(job.job_id)
        if j.job_type == 'query':
            # Look out for Select * in query text
            if 'SELECT *' in j.query.upper():
                all_columns_used = True
            else:
                all_columns_used = False
            # Get required attribute from JobStatistics Object and make a dict,
            # Count the TableReference from attribute query
            results[job.job_id] = {
                "job_id": job.job_id,
                "num_of_tables_referred":
                str(j.referenced_tables).count("TableReference"),
                "select_star_used": all_columns_used,
                "start_time": str(j.started),
                "total_bytes_billed": j.total_bytes_billed,
                "cache_hit": j.cache_hit,
                "total_bytes_processed": j.total_bytes_processed,
                "execution_time": str(j.ended-j.started),
                "user_email": j.user_email}
            # Make a separate dict with query text
            # to analyze problematic queries
            query_text[job.job_id] = {
                "job_id": job.job_id,
                "query_text": j.query}

    # print (results)
    # print (query_text)
    # Write results to file
    filename1 = "bq_monitor_"+str(datetime.date.today())+".json"
    f1 = open(filename1, 'w')
    for jobid in results:
        f1.write(str(results[jobid])+"\n")
    f1.close()
    filenametemp = "temp_"+filename1
    temp_json = json.dumps(results)
    ft = open(filenametemp, 'w')
    ft.write(temp_json)
    # Write Query text alongwith jobid to file
    filename2 = "bq_monitor_query_text_"+str(datetime.date.today())+".json"
    f2 = open(filename2, 'w')
    for jobid in query_text:
        f2.write(str(query_text[jobid])+"\n")
    f2.close()
# End get_data


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
                        description=__doc__,
                        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        '--projectid',
        required=True,
        help=('Provide Project Id')
        )
    parser.add_argument(
        '--hours',
        required=False,
        default=24,
        help=('Provide a number, 24 is default')
        )
    parser.add_argument(
        '--max_results',
        required=False,
        default=500,
        help=('Provide a number for max results returned, 500 is default')
        )
    args = parser.parse_args()
    get_data(args.projectid, args.hours, int(args.max_results))
