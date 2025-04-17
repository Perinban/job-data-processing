# supabase_insertion.py
from supabase import create_client
from tqdm import tqdm
import pandas as pd

# Supabase configuration
def create_supabase_client():
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    return create_client(url, key)

RECRUITER_ID = os.environ.get("RECRUITER_ID")

supabase = create_supabase_client()

def insert_companies_data(companies_data):
    companies_list = []
    for index, row in companies_data.iterrows():
        company = {
            'name': row['Company_Name'],
            'logo_url': row['Company_Logo_Url']
        }
        companies_list.append(company)

    response = supabase.table('companies').insert(companies_list).execute()

    if response.data:
        print(f"Successfully inserted {len(response.data)} companies.")
    else:
        print("Failed to insert companies.")

def insert_jobs_data(data, company_map):
    jobs_to_insert = []

    for index, row in tqdm(data.iterrows(), total=len(data)):
        if not pd.isnull(row['reject_reason']) or pd.isnull(row['Company_Name']):
            continue

        company_name = row['Company_Name']
        company_id = company_map.get(company_name)

        job_details = row['Job_Details']
        about_company = next((item['content'] for item in job_details if item['header'] == 'About the Company'), '')
        requirements = ' '.join([item['content'].strip() for item in job_details if item['header'] != 'About the Company'])

        job = {
            'recruiter_id': RECRUITER_ID,
            'title': row['Job_Title'],
            'company_id': company_id,
            'description': about_company,
            'state': row['Job_State'],
            'country': row['Job_Country'],
            'mode': row['Job_Mode'],
            'requirements': requirements,
            'domain': row['Job_Domain'],
            'salary': row['Job_Salary'],
            'posted_date': row['Last_Updated'],
            'job_url': row['Job_URL']
        }

        jobs_to_insert.append(job)

    BATCH_SIZE = 500
    for i in range(0, len(jobs_to_insert), BATCH_SIZE):
        batch = jobs_to_insert[i:i + BATCH_SIZE]
        response = supabase.table('jobs').insert(batch).execute()

        if response.data:
            print(f"Inserted {len(response.data)} jobs in batch {i // BATCH_SIZE + 1}")
        else:
            print(f"Failed to insert batch {i // BATCH_SIZE + 1}: {response.error}")

    print("Job data insertion complete!")