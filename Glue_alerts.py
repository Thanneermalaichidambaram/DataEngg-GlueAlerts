import boto3
import json
import urllib3
from botocore.exceptions import NoCredentialsError, TokenRetrievalError
from datetime import datetime

# Replace with your Slack webhook URL
SLACK_WEBHOOK_URL = '<SLACK_WEBHOOK_URL>'  # Example: https://hooks.slack.com/services/TXXXXXX/BXXXXXX/XXXXXXXX

def get_glue_jobs_exceeding_2dpu(aws_profile):
    """
    Retrieves Glue jobs with MaxCapacity greater than or equal to 2 DPUs.
    
    Args:
    - aws_profile (str): AWS SSO or named profile configured in AWS CLI.
    
    Returns:
    - Tuple of lists containing running, idle, and failed jobs.
    """
    try:
        # Replace with your AWS profile name if using a named profile
        session = boto3.Session(profile_name=aws_profile)
        glue_client = session.client('glue')

        # List all Glue jobs
        jobs = glue_client.get_jobs()
        running_jobs = []
        idle_jobs = []
        failed_jobs = []

        for job in jobs['Jobs']:
            # Get job runs to check the current status
            job_runs = glue_client.get_job_runs(JobName=job['Name'], MaxResults=1)
            if job_runs['JobRuns']:
                job_run = job_runs['JobRuns'][0]
                job_status = job_run['JobRunState']
                last_run_time = job_run.get('StartedOn', None)

                # Check if MaxCapacity (DPUs) is greater than or equal to 2
                if job.get('MaxCapacity', 0) > 2 or (job.get('MaxCapacity', 0) == 2 and job_status == 'FAILED'):
                    job_info = {
                        'JobName': job['Name'],
                        'MaxCapacity': job['MaxCapacity'],
                        'Status': job_status,
                        'LastRunTime': last_run_time.strftime('%Y-%m-%d %H:%M:%S') if last_run_time else 'N/A'
                    }
                    if job_status in ['RUNNING', 'STARTING']:
                        running_jobs.append(job_info)
                    elif job_status == 'FAILED':
                        failed_jobs.append(job_info)
                    else:
                        idle_jobs.append(job_info)

        return running_jobs, idle_jobs, failed_jobs

    except NoCredentialsError:
        print("Credentials not available. Please configure your AWS credentials or IAM role.")
        return [], [], []
    except TokenRetrievalError:
        print("SSO token has expired. Please refresh your SSO session.")
        return [], [], []

def send_slack_alert(running_jobs, idle_jobs, failed_jobs):
    """
    Sends a Slack alert with the details of the Glue jobs exceeding 2 DPUs.
    
    Args:
    - running_jobs (list): List of running jobs.
    - idle_jobs (list): List of idle jobs.
    - failed_jobs (list): List of failed jobs.
    """
    http = urllib3.PoolManager()

    def format_job_list(jobs):
        if jobs:
            attachments = []
            for job in jobs:
                # Logic for color coding
                if job['Status'] == 'FAILED':
                    color = "#ff0000"  # Red for failed jobs
                elif job['MaxCapacity'] <= 10:
                    color = "#36a64f"  # Green
                elif 10 < job['MaxCapacity'] < 50:
                    color = "#ffcc00"  # Yellow
                elif job['MaxCapacity'] >= 50:
                    color = "#ff0000"  # Red for high capacity jobs

                attachment = {
                    "color": color,
                    "title": f"Job Name: {job['JobName']}",
                    "fields": [
                        {
                            "title": "DPUs",
                            "value": job['MaxCapacity'],
                            "short": True
                        },
                        {
                            "title": "Status",
                            "value": job['Status'],
                            "short": True
                        },
                        {
                            "title": "Last Run Time",
                            "value": job['LastRunTime'],
                            "short": True
                        }
                    ]
                }
                attachments.append(attachment)
            return attachments
        else:
            return []

    running_attachments = format_job_list(running_jobs)
    idle_attachments = format_job_list(idle_jobs)
    failed_attachments = format_job_list(failed_jobs)
    
    attachments = running_attachments + idle_attachments + failed_attachments
    
    message = {
        'text': "AWS Glue Jobs Alert For Above 2 DPUs",
        'attachments': attachments
    }

    encoded_message = json.dumps(message).encode('utf-8')
    try:
        response = http.request('POST', SLACK_WEBHOOK_URL, body=encoded_message, headers={'Content-Type': 'application/json'})
        if response.status == 200:
            print(f"Message posted to Slack. Status code: {response.status}")
        else:
            print(f"Failed to post message to Slack. Status code: {response.status}. Response: {response.data.decode('utf-8')}")
    except Exception as e:
        print(f"An error occurred while sending the message to Slack: {e}")

def main():
    # Replace 'your-aws-profile-name' with the actual AWS CLI profile configured in your system
    aws_profile = '<AWS_PROFILE_NAME>'  # Example: 'default' or 'prod-profile'
    
    running_jobs, idle_jobs, failed_jobs = get_glue_jobs_exceeding_2dpu(aws_profile)

    if running_jobs or idle_jobs or failed_jobs:
        print("Jobs with more than 2 DPUs:")
        if running_jobs:
            print("Running Jobs:")
            for job in running_jobs:
                print(f"Job Name: {job['JobName']}, DPUs: {job['MaxCapacity']}, Status: {job['Status']}, Last Run Time: {job['LastRunTime']}")
        else:
            print("Running Jobs: No jobs with more than 2 DPUs.")
        if idle_jobs:
            print("Idle Jobs:")
            for job in idle_jobs:
                print(f"Job Name: {job['JobName']}, DPUs: {job['MaxCapacity']}, Status: {job['Status']}, Last Run Time: {job['LastRunTime']}")
        else:
            print("Idle Jobs: No jobs with more than 2 DPUs.")
        if failed_jobs:
            print("Failed Jobs:")
            for job in failed_jobs:
                print(f"Job Name: {job['JobName']}, DPUs: {job['MaxCapacity']}, Status: {job['Status']}, Last Run Time: {job['LastRunTime']}")
        else:
            print("Failed Jobs: No jobs with more than 2 DPUs.")
        send_slack_alert(running_jobs, idle_jobs, failed_jobs)
    else:
        print("No jobs with more than 2 DPUs.")

if __name__ == "__main__":
    main()
