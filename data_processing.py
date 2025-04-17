# data_processing.py
import pandas as pd
import re
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# Step 1: Clean the data
def clean_data(raw_data):
    data = pd.json_normalize(raw_data)
    data = data.apply(lambda x: x.str.strip() if isinstance(x, str) else x, axis=1)
    data = data[data['reject_reason'].isnull()].reset_index(drop=True)
    return data

# Step 2: Extract relevant columns for companies table
def extract_companies_data(data):
    companies_data = data[['Company_Name', 'Company_Logo_Url']]
    companies_data = companies_data.drop_duplicates(subset=['Company_Name', 'Company_Logo_Url'], keep='first')
    companies_data['Company_Logo_Url'] = companies_data['Company_Logo_Url'].fillna('')
    companies_data = companies_data.dropna(subset=['Company_Name'])
    return companies_data

# Step 3: Split Job Location into State, Country, and Mode
def split_location(row):
    job_location = row['Job_Location']

    if pd.isna(job_location):
        return pd.Series(['', '', ''])

    job_location = str(job_location)  # Ensure job_location is a string

    if job_location.lower().startswith("remote"):
        work_type = 'Remote'
        if '(' in job_location and ')' in job_location:
            country = job_location[job_location.find('(') + 1:job_location.find(')')].strip()
            if ',' in country:
                state, country = country.split(',', 1)
            else:
                state = ''
        else:
            state = ''
            country = ''
        return pd.Series([state, country, work_type])

    else:
        if '(' in job_location:
            work_type = job_location[job_location.find('(') + 1:job_location.find(')')].strip()
            job_location = job_location.split('(')[0].strip()
        else:
            work_type = ''

        if ',' in job_location:
            state, country = job_location.split(',', 1)
        else:
            state = job_location
            country = ''

        return pd.Series([state, country, work_type])


def process_job_locations(data):
    data[['Job_State', 'Job_Country', 'Job_Mode']] = data.apply(split_location, axis=1)
    data['Job_Country'] = data['Job_Country'].replace('', 'Other')
    data['Job_Mode'] = data['Job_Mode'].replace('', 'Other')
    data.loc[data['Job_Mode'] != 'Remote', 'Job_State'] = data.loc[data['Job_Mode'] != 'Remote', 'Job_State'].replace('',
                                                                                                                  'Other')
    return data

# Step 4: Convert Relative Time to Absolute Timestamps
def extract_and_convert_to_timestamp(last_updated):
    match = re.search(
        r'(?:vor\s+|il\s+y\s+a\s+|)(\d+)\s+(segundo?s?|minute[ns]?|minut[oi]?s?|uur|uren|or[ae]?|giorn[oi]?|settiman[ae]?|hora?s?|día?s?|semana?s?|jour?s?|heure?s?|semaine?s?|mois?|monat?e?|month?s?|days?|tag?s?|tagen|hour?s?|stunde?n?|second?[eois]?|sekunde?n?|weken|maand?e?n?|minuut|mes?e?s?i?|dage?n?)\s*(?:ago|vor|il\s+y\s+a\s+)?',
        last_updated)

    if match:
        time_ago = int(match.group(1))
        unit = match.group(2)

        if "second" in unit or "sekunde" in unit or "segundo" in unit:
            return datetime.now() - timedelta(seconds=time_ago)
        elif "minute" in unit or "minut" in unit or "minuut" in unit:
            return datetime.now() - timedelta(minutes=time_ago)
        elif "hour" in unit or "stunde" in unit or "heure" in unit or "hora" in unit or "or" in unit or "uur" in unit or "uren" in unit:
            return datetime.now() - timedelta(hours=time_ago)
        elif "tag" in unit or "day" in unit or "jour" in unit or "día" in unit or "giorn" in unit or "dag" in unit:
            return datetime.now() - timedelta(days=time_ago)
        elif "week" in unit or "woche" in unit or "semaine" in unit or "semana" in unit or "settiman" in unit or "weken" in unit:
            return datetime.now() - timedelta(weeks=time_ago)
        elif "month" in unit or "monat" in unit or "mois" in unit or "mes" in unit or "maand" in unit:
            return datetime.now() - relativedelta(months=time_ago)

    return None


def process_timestamps(data):
    data['Last_Updated'] = data['Last_Updated'].apply(extract_and_convert_to_timestamp)
    data['Last_Updated'] = data['Last_Updated'].apply(
        lambda x: x.strftime('%Y-%m-%dT%H:%M:%S') if isinstance(x, pd.Timestamp) and not pd.isna(
            x) else '1900-01-01T00:00:00'
    )
    return data