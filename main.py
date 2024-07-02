import aiohttp
import os
import asyncio
import json
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

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

# Returns the first 5 crmIds from a list of profiles. Each profile has the crmId in an array, there will only ever be one crmId in the array.
def get_crm_id_list(profiles):
    crm_ids = [profile.get('attributes', {}).get('crmId', [None])[0] for profile in profiles[:5]]
    if len(profiles) > 5:
        crm_ids.append('...')
    return '\t({})'.format(', '.join(filter(None, crm_ids)))

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
                responseJSON = json.loads(response_text)  # Manually parse the JSON
            except json.JSONDecodeError:
                raise Exception(f"Failed to decode JSON. Content: {response_text}")
                
            if 'profiles' not in responseJSON or not isinstance(responseJSON['profiles'], list):
                raise Exception('Invalid response from CDP. Profiles array not found in response.')

            return responseJSON

async def run_report():
    print('UC1 Report (Only GB and IE users considered):\n')

    print('### General User Stats: ###')
    users_from_uk = await cdp_query(f'SELECT attributes.crmId FROM profiles WHERE {COUNTRY_FILTER} AND {TEST_USERS_FILTER}')
    print(f'Total users: {users_from_uk["totalCount"]} {get_crm_id_list(users_from_uk["profiles"])}')

    exposed_users_with_biomat_ratio = await cdp_query(f'SELECT attributes.crmId FROM profiles WHERE {COUNTRY_FILTER} AND attributes.uc1.currentBioRatio IS NOT NULL AND {TEST_USERS_FILTER}')
    print(f'Users with Biomaterial Ratio: {exposed_users_with_biomat_ratio["totalCount"]} {get_crm_id_list(exposed_users_with_biomat_ratio["profiles"])}')

    exposed_users_from_uk = await cdp_query(f'SELECT attributes.crmId FROM profiles WHERE {EXPOSED_FILTER} AND {COUNTRY_FILTER} AND {TEST_USERS_FILTER}')
    print(f'Exposed users: {exposed_users_from_uk["totalCount"]} {get_crm_id_list(exposed_users_from_uk["profiles"])}')

    exposed_users_with_profession_from_uk = await cdp_query(f'SELECT attributes.crmId FROM profiles WHERE {EXPOSED_FILTER} AND {COUNTRY_FILTER} AND {ALL_PROFESSIONS_FILTER} AND {TEST_USERS_FILTER}')
    print(f'Exposed users with relevant professions from UK: {exposed_users_with_profession_from_uk["totalCount"]} {get_crm_id_list(exposed_users_with_profession_from_uk["profiles"])}')

    exposed_relevant_users_with_low_biomat_ratio = await cdp_query(f'SELECT attributes.crmId FROM profiles WHERE {EXPOSED_FILTER} AND {COUNTRY_FILTER} AND {ALL_PROFESSIONS_FILTER} AND attributes.uc1.currentBioRatio < 0.5333 AND {TEST_USERS_FILTER}')
    print(f'Exposed relevant users with biomaterial ratio less than 0.5333: {exposed_relevant_users_with_low_biomat_ratio["totalCount"]} {get_crm_id_list(exposed_relevant_users_with_low_biomat_ratio["profiles"])}')

    exposed_relevant_users_with_high_biomat_ratio = await cdp_query(f'SELECT attributes.crmId FROM profiles WHERE {EXPOSED_FILTER} AND {COUNTRY_FILTER} AND {ALL_PROFESSIONS_FILTER} AND attributes.uc1.currentBioRatio >= 0.5333 AND {TEST_USERS_FILTER}')
    print(f'Exposed relevant users with biomaterial ratio 0.5333 or more: {exposed_relevant_users_with_high_biomat_ratio["totalCount"]} {get_crm_id_list(exposed_relevant_users_with_high_biomat_ratio["profiles"])}')

    exposed_relevant_users_with_no_biomat_ratio = await cdp_query(f'SELECT attributes.crmId FROM profiles WHERE {EXPOSED_FILTER} AND {COUNTRY_FILTER} AND {ALL_PROFESSIONS_FILTER} AND attributes.uc1.currentBioRatio IS NULL AND {TEST_USERS_FILTER}')
    print(f'Exposed relevant users with no biomaterial ratio: {exposed_relevant_users_with_no_biomat_ratio["totalCount"]} {get_crm_id_list(exposed_relevant_users_with_no_biomat_ratio["profiles"])}')

    users_in_journey = await cdp_query(f'SELECT attributes.crmId FROM profiles WHERE {COUNTRY_FILTER} AND {IN_JOURNEY_FILTER} AND {TEST_USERS_FILTER}')
    print(f'Users in journey: {users_in_journey["totalCount"]} {get_crm_id_list(users_in_journey["profiles"])}')

    print('\n### Users per Biomaterial State: ###')
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
        print(f'Users in state {state}: {state_count["totalCount"]} {get_crm_id_list(state_count["profiles"])}')

    print('\n### Users per Biomaterial Interest: ###')
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
        print(f'Product {interest}: {interest_count["totalCount"]} {get_crm_id_list(interest_count["profiles"])}')

    print('\n### Users professions: ###')
    total_dentists = await cdp_query(f'SELECT attributes.crmId FROM profiles WHERE {COUNTRY_FILTER} AND attributes.data.crmExtended.profession = "01" AND {TEST_USERS_FILTER}')
    print(f'Dentists: {total_dentists["totalCount"]} {get_crm_id_list(total_dentists["profiles"])}')
    in_journey_dentists = await cdp_query(f'SELECT attributes.crmId FROM profiles WHERE {COUNTRY_FILTER} AND attributes.data.crmExtended.profession = "01" AND {IN_JOURNEY_FILTER} AND {TEST_USERS_FILTER}')
    print(f'Dentists in journey: {in_journey_dentists["totalCount"]} {get_crm_id_list(in_journey_dentists["profiles"])}')

    total_general_practitioners = await cdp_query(f'SELECT attributes.crmId FROM profiles WHERE {COUNTRY_FILTER} AND attributes.data.crmExtended.profession = "05" AND {TEST_USERS_FILTER}')
    print(f'General Practitioners: {total_general_practitioners["totalCount"]} {get_crm_id_list(total_general_practitioners["profiles"])}')
    in_journey_general_practitioners = await cdp_query(f'SELECT attributes.crmId FROM profiles WHERE {COUNTRY_FILTER} AND attributes.data.crmExtended.profession = "05" AND {IN_JOURNEY_FILTER} AND {TEST_USERS_FILTER}')
    print(f'General Practitioners in journey: {in_journey_general_practitioners["totalCount"]} {get_crm_id_list(in_journey_general_practitioners["profiles"])}')

    total_hygienists = await cdp_query(f'SELECT attributes.crmId FROM profiles WHERE {COUNTRY_FILTER} AND attributes.data.crmExtended.profession = "06" AND {TEST_USERS_FILTER}')
    print(f'Hygienists: {total_hygienists["totalCount"]} {get_crm_id_list(total_hygienists["profiles"])}')
    in_journey_hygienists = await cdp_query(f'SELECT attributes.crmId FROM profiles WHERE {COUNTRY_FILTER} AND attributes.data.crmExtended.profession = "06" AND {IN_JOURNEY_FILTER} AND {TEST_USERS_FILTER}')
    print(f'Hygienists in journey: {in_journey_hygienists["totalCount"]} {get_crm_id_list(in_journey_hygienists["profiles"])}')

    print('\n### Users with page visits per site: ###')
    total_eshop_views_customers = await cdp_query(f'SELECT attributes.crmId FROM profiles WHERE {COUNTRY_FILTER} AND HAVING(activityIndicators, (activityIndicators.name = "Total Web Event - EshopPageView")) AND {TEST_USERS_FILTER}')
    print(f'Shop: Total: {total_eshop_views_customers["totalCount"]} {get_crm_id_list(total_eshop_views_customers["profiles"])}')
    in_journey_eshop_views_customers = await cdp_query(f'SELECT attributes.crmId FROM profiles WHERE {COUNTRY_FILTER} AND {IN_JOURNEY_FILTER} AND HAVING(activityIndicators, (activityIndicators.name = "Total Web Event - EshopPageView")) AND {TEST_USERS_FILTER}')
    print(f'Shop: In journey: {in_journey_eshop_views_customers["totalCount"]} {get_crm_id_list(in_journey_eshop_views_customers["profiles"])}')

    total_aem_views_customers = await cdp_query(f'SELECT attributes.crmId FROM profiles WHERE {COUNTRY_FILTER} AND HAVING(activityIndicators, (activityIndicators.name = "Total Web Event - AEMPageView")) AND {TEST_USERS_FILTER}')
    print(f'AEM: Total: {total_aem_views_customers["totalCount"]} {get_crm_id_list(total_aem_views_customers["profiles"])}')
    in_journey_aem_views_customers = await cdp_query(f'SELECT attributes.crmId FROM profiles WHERE {COUNTRY_FILTER} AND {IN_JOURNEY_FILTER} AND HAVING(activityIndicators, (activityIndicators.name = "Total Web Event - AEMPageView")) AND {TEST_USERS_FILTER}')
    print(f'AEM: In journey: {in_journey_aem_views_customers["totalCount"]} {get_crm_id_list(in_journey_aem_views_customers["profiles"])}')

    total_skill_views_customers = await cdp_query(f'SELECT attributes.crmId FROM profiles WHERE {COUNTRY_FILTER} AND HAVING(activityIndicators, (activityIndicators.name = "Total Web Event - SKILLPageView")) AND {TEST_USERS_FILTER}')
    print(f'Skill: Total: {total_skill_views_customers["totalCount"]} {get_crm_id_list(total_skill_views_customers["profiles"])}')
    in_journey_skill_views_customers = await cdp_query(f'SELECT attributes.crmId FROM profiles WHERE {COUNTRY_FILTER} AND {IN_JOURNEY_FILTER} AND HAVING(activityIndicators, (activityIndicators.name = "Total Web Event - SKILLPageView")) AND {TEST_USERS_FILTER}')
    print(f'Skill: In journey: {in_journey_skill_views_customers["totalCount"]} {get_crm_id_list(in_journey_skill_views_customers["profiles"])}')

asyncio.run(run_report())
