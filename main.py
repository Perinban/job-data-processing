# main.py
from filedownload import download_and_load_job_data
from data_processing import clean_data, extract_companies_data, process_job_locations, process_timestamps
from supabase_insertion import insert_companies_data, insert_jobs_data, create_supabase_client
from supabase import create_client

# Initialize Supabase
supabase = create_supabase_client()

# Function to truncate all tables before inserting new data
def truncate_all_tables():
    try:
        # Call the truncate_all_tables function
        response = supabase.rpc("truncate_all_tables").execute()
        print("Function executed successfully.")
        print("Response:", response.data)
    except Exception as e:
        print("Error running function:", str(e))

# Main execution
if __name__ == "__main__":
    # Truncate all tables before inserting new data
    truncate_all_tables()

    # Download and load raw job data
    raw_data = download_and_load_job_data()

    if raw_data:
        # Clean and process the data
        data = clean_data(raw_data)
        companies_data = extract_companies_data(data)
        data = process_job_locations(data)
        data = process_timestamps(data)

        # Insert data into Supabase
        insert_companies_data(companies_data)

        # Fetch company IDs for jobs
        company_rows = supabase.table('companies').select('id', 'name').execute().data
        company_map = {c['name']: c['id'] for c in company_rows}

        insert_jobs_data(data, company_map)