import aiohttp
import os
import asyncio
import json
from datetime import datetime
from dotenv import load_dotenv
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


# Load environment variables from .env file
load_dotenv()

# Constants for CDP queries
EXPOSED_FILTER = 'attributes.data.internal.cdpABExposed = true'
COUNTRY_FILTER = '(attributes.country = "GB" OR attributes.country = "IE")'
ALL_PROFESSIONS_FILTER = '(attributes.data.crmExtended.profession = "01" OR attributes.data.crmExtended.profession = "05" OR attributes.data.crmExtended.profession = "06")'
IN_JOURNEY_FILTER = '(HAVING(segments, (segments.name = "UC1 - Pre-Journey")) OR attributes.uc1.BioMilestone = "LOYALS")'

TEST_USERS_FILTER = '(attributes.crmId != "{}")'.format('" AND attributes.crmId != "'.join([
    '1005895385',
    '1005895387',
    '1005911273',
    '1005911400',
]))

# Returns the first 5 crmIds from a list of profiles.
def get_crm_id_list(profiles):
    crm_ids = [profile.get('attributes', {}).get('crmId', [None])[0] for profile in profiles[:5]]
    if len(profiles) > 5:
        crm_ids.append('...')
    return '\t({})'.format(', '.join(filter(None, crm_ids)))

# Function to send email using SMTP
def send_email(sender_email, receiver_email, subject, body, smtp_server, smtp_port, smtp_username, smtp_password):
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = receiver_email
    message['Subject'] = subject
    message.attach(MIMEText(body, 'plain'))
    
    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_username, smtp_password)
            server.sendmail(sender_email, receiver_email, message.as_string())
            print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email. Error: {str(e)}")

# Async function to query CDP
async def cdp_query(query):
    user_key = os.getenv('CDP_USER_KEY', '')
    secret = os.getenv('CDP_USER_SECRET', '')
    business_unit = os.getenv('CDP_BUSINESS_UNIT', '')
    view = os.getenv('CDP_VIEW', '')
    
    queryParams = {
        'userKey': user_key,
        'secret': secret,
        'query': query
    }
    
    url = f"https://cdp.eu5.gigya.com/api/businessunits/{business_unit}/views/{view}/customers"
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=queryParams) as response:
            response_text = await response.text()
            
            if response.status != 200:
                raise Exception(f"Failed to fetch data from CDP. Status: {response.status}, Response: {response_text}")

            try:
                responseJSON = json.loads(response_text)
            except json.JSONDecodeError:
                raise Exception(f"Failed to decode JSON. Content: {response_text}")
                
            if 'profiles' not in responseJSON or not isinstance(responseJSON['profiles'], list):
                raise Exception('Invalid response from CDP. Profiles array not found in response.')

            return responseJSON

# Async function to run the report and send email
async def run_report_and_send_email():
    report_data = 'UC1 Report (Only GB and IE users considered):\n\n'
    
    # Fetch and process data from CDP
    users_from_uk = await cdp_query(f'SELECT attributes.crmId FROM profiles WHERE {COUNTRY_FILTER} AND {TEST_USERS_FILTER}')
    report_data += f'Total users: {users_from_uk["totalCount"]} {get_crm_id_list(users_from_uk["profiles"])}\n'
    
    exposed_users_with_biomat_ratio = await cdp_query(f'SELECT attributes.crmId FROM profiles WHERE {COUNTRY_FILTER} AND attributes.uc1.currentBioRatio IS NOT NULL AND {TEST_USERS_FILTER}')
    report_data += f'Users with Biomaterial Ratio: {exposed_users_with_biomat_ratio["totalCount"]} {get_crm_id_list(exposed_users_with_biomat_ratio["profiles"])}\n'
    
    exposed_users_from_uk = await cdp_query(f'SELECT attributes.crmId FROM profiles WHERE {EXPOSED_FILTER} AND {COUNTRY_FILTER} AND {TEST_USERS_FILTER}')
    report_data += f'Exposed users: {exposed_users_from_uk["totalCount"]} {get_crm_id_list(exposed_users_from_uk["profiles"])}\n'
    
    exposed_users_with_profession_from_uk = await cdp_query(f'SELECT attributes.crmId FROM profiles WHERE {EXPOSED_FILTER} AND {COUNTRY_FILTER} AND {ALL_PROFESSIONS_FILTER} AND {TEST_USERS_FILTER}')
    report_data += f'Exposed users with relevant professions from UK: {exposed_users_with_profession_from_uk["totalCount"]} {get_crm_id_list(exposed_users_with_profession_from_uk["profiles"])}\n'
    
    exposed_relevant_users_with_low_biomat_ratio = await cdp_query(f'SELECT attributes.crmId FROM profiles WHERE {EXPOSED_FILTER} AND {COUNTRY_FILTER} AND {ALL_PROFESSIONS_FILTER} AND attributes.uc1.currentBioRatio < 0.5333 AND {TEST_USERS_FILTER}')
    report_data += f'Exposed relevant users with biomaterial ratio less than 0.5333: {exposed_relevant_users_with_low_biomat_ratio["totalCount"]} {get_crm_id_list(exposed_relevant_users_with_low_biomat_ratio["profiles"])}\n'
    
    exposed_relevant_users_with_high_biomat_ratio = await cdp_query(f'SELECT attributes.crmId FROM profiles WHERE {EXPOSED_FILTER} AND {COUNTRY_FILTER} AND {ALL_PROFESSIONS_FILTER} AND attributes.uc1.currentBioRatio >= 0.5333 AND {TEST_USERS_FILTER}')
    report_data += f'Exposed relevant users with biomaterial ratio 0.5333 or more: {exposed_relevant_users_with_high_biomat_ratio["totalCount"]} {get_crm_id_list(exposed_relevant_users_with_high_biomat_ratio["profiles"])}\n'
    
    exposed_relevant_users_with_no_biomat_ratio = await cdp_query(f'SELECT attributes.crmId FROM profiles WHERE {EXPOSED_FILTER} AND {COUNTRY_FILTER} AND {ALL_PROFESSIONS_FILTER} AND attributes.uc1.currentBioRatio IS NULL AND {TEST_USERS_FILTER}')
    report_data += f'Exposed relevant users with no biomaterial ratio: {exposed_relevant_users_with_no_biomat_ratio["totalCount"]} {get_crm_id_list(exposed_relevant_users_with_no_biomat_ratio["profiles"])}\n'
    
    users_in_journey = await cdp_query(f'SELECT attributes.crmId FROM profiles WHERE {COUNTRY_FILTER} AND {IN_JOURNEY_FILTER} AND {TEST_USERS_FILTER}')
    report_data += f'Users in journey: {users_in_journey["totalCount"]} {get_crm_id_list(users_in_journey["profiles"])}\n'
    
    report_data += '\n### Users per Biomaterial State: ###\n'
    biomaterial_states = [
        'BIOMAT_AWARE_LANDING_PAGE',
        'BIOMAT_AWARE_VIDEO',
        'BIOMAT_CONSIDER_WEBINAR',
        'BIOMAT_CONSIDER_ELEARNING',
        'BIOMAT_ENGAGE_COURSE',
        'BIOMAT_ENGAGE_PROMO',
        'BIOMAT_NONE'
    ]
    for state in biomaterial_states:
        state_count = await cdp_query(f'SELECT attributes.crmId FROM profiles WHERE attributes.uc1.BioState = "{state}" AND {COUNTRY_FILTER} AND {TEST_USERS_FILTER}')
        report_data += f'Users in state {state}: {state_count["totalCount"]} {get_crm_id_list(state_count["profiles"])}\n'
    
    report_data += '\n### Users per Biomaterial Interest: ###\n'
    biomaterial_interests = [
        'a',  # XenoGrapht
        'b',  # MaxGraft
        'c',  # XenoFlex
        'd',  # BoneCeramic
        'e',  # Cerabone Plus
        'f',  # Jason Membrane
        'g',  # Permamem
        'h',  # Mucoderm
        'j',  # Emdogain (Default Product Interest)
        'k',  # Emdogain FL
        'l',  # Labrida Brush
    ]
    for interest in biomaterial_interests:
        interest_count = await cdp_query(f'SELECT attributes.crmId FROM profiles WHERE attributes.uc1.productInterest = "{interest}" AND {COUNTRY_FILTER} AND {TEST_USERS_FILTER}')
        report_data += f'Product {interest}: {interest_count["totalCount"]} {get_crm_id_list(interest_count["profiles"])}\n'
    
    report_data += '\n### Users professions: ###\n'
    total_dentists = await cdp_query(f'SELECT attributes.crmId FROM profiles WHERE {COUNTRY_FILTER} AND attributes.data.crmExtended.profession = "01" AND {TEST_USERS_FILTER}')
    report_data += f'Dentists: {total_dentists["totalCount"]} {get_crm_id_list(total_dentists["profiles"])}\n'
    
    total_doctors = await cdp_query(f'SELECT attributes.crmId FROM profiles WHERE {COUNTRY_FILTER} AND attributes.data.crmExtended.profession = "05" AND {TEST_USERS_FILTER}')
    report_data += f'Doctors: {total_doctors["totalCount"]} {get_crm_id_list(total_doctors["profiles"])}\n'
    
    total_orthodontists = await cdp_query(f'SELECT attributes.crmId FROM profiles WHERE {COUNTRY_FILTER} AND attributes.data.crmExtended.profession = "06" AND {TEST_USERS_FILTER}')
    report_data += f'Orthodontists: {total_orthodontists["totalCount"]} {get_crm_id_list(total_orthodontists["profiles"])}\n'
    
    report_data += '\n### Distribution of users in UK: ###\n'
    crm_distribution = await cdp_query(f'SELECT attributes.crmId FROM profiles WHERE {COUNTRY_FILTER} AND {TEST_USERS_FILTER}')
    report_data += f'Distribution: {crm_distribution["totalCount"]} {get_crm_id_list(crm_distribution["profiles"])}\n'

    # Send email with the report data
    sender_email = os.getenv('SENDER_EMAIL')
    receiver_email = os.getenv('RECIPIENT_EMAIL')
    current_date = datetime.now().strftime('%Y-%m-%d')
    subject = f'Daily UC1 Report - {current_date}'
    smtp_server = 'smtp.office365.com'
    smtp_port = 587
    smtp_username = os.getenv('SMTP_USERNAME')
    smtp_password = os.getenv('SMTP_PASSWORD')
   
    send_email(sender_email, receiver_email, subject, report_data, smtp_server, smtp_port, smtp_username, smtp_password)

if __name__ == '__main__':
    asyncio.run(run_report_and_send_email())
