"""
This script is for scraping data from ESEA and uploading it to the Google sheet.
"""
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
import pandas as pd
import itertools
import time
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import gspread_dataframe as gd


def get_stats(division_urls: dict, division: str, season: int, country: str = 'United Kingdom', additional_teams: list = [], blacklisted_teams: list = []) -> list[dict]:
    """
    This function gets ESEA statistics (wins, losses) for teams from a given country in a given division and season
    Returns a list of dictionaries - each dictionary is a team and their statistics
    Additional teams can be added if for example they do not have the correct country classification
    Entries in the additional_teams are url suffixes for play.esea.net. Each entry must have the following format: '/teams/xxxxxxx'
    """
    # bar = Bar(f'Getting {division} stats', max=100)
    # bar.next()
    print(f'Getting {division} statistics...')
    try:
        options = webdriver.ChromeOptions()
        options.add_argument('--user-agent=cat')
        options.add_argument("start-maximized")
        options.add_argument("--headless")
        driver = uc.Chrome(options=options)
        driver.get(division_urls[division])
        WebDriverWait(driver, 5).until(EC.visibility_of_element_located((By.TAG_NAME, 'table')))
    except Exception as error:
        print(error)

    soup = BeautifulSoup(driver.page_source, 'html.parser')

    # Get the urls of teams on ESEA which have a UK flag
    team_urls = [team.parent.parent.parent.parent.find_all('a')[-1].get('href') for team in soup.find_all('title', text=country)]

    # Adding the additional urls to main list of urls
    team_urls.extend(additional_teams)

    # Removing duplicate urls
    team_urls = list(set(team_urls))

    # Removing blacklisted teams from url list
    for team in blacklisted_teams:
        if team in team_urls:
            team_urls.remove(team)

    output = []
    for row in soup.find_all('tr')[1:]:
        if row.find_all('a')[-1].get('href') in team_urls:
            team_name = row.find_all('a')[-1].text
            wins = row.find_all('td')[-7].text
            losses = row.find_all('td')[-6].text
            win_streak = row.find_all('td')[-3].text
            page_url = row.find_all('a')[-1].get('href')

            # Checking if the team has a logo
            if row.find_all('a')[0].find('img'):
                logo_url = row.find_all('a')[0].find('img').get('src')
            else:
                logo_url = None
                
            team_stats = {'team_name': team_name, 'logo_url': logo_url, 'wins': wins, 'losses': losses, 'win_streak': win_streak, 'division': division, 'page_url': page_url}
            output.append(team_stats)

    driver.close()
    driver.quit()

    return output


def get_additional_teams(client) -> list:
    """
    Retrieves a table of teams (which are to be added)and their ESEA urls from Google Sheets and returns a list of these urls.
    """
    add_teams_sheet = client.open('UKCS Hub Sheet').worksheet('Additional Teams')
    add_teams_df = pd.DataFrame(add_teams_sheet.get_all_records())
    teams_list = list(add_teams_df['esea_page'].values)

    # Removing https://play.esea.net prefix
    teams_list = ['/' + '/'.join(team.split('/')[-2:]) for team in teams_list]

    return teams_list


def get_remove_teams(client) -> list:
    """
    This function retrieves a table of teams from Google Sheets which are to be removed.
    """
    remove_teams_sheet = client.open('UKCS Hub Sheet').worksheet('Remove Teams')
    remove_teams_df = pd.DataFrame(remove_teams_sheet.get_all_records())

    if not remove_teams_df.empty:
        teams_list = list(remove_teams_df['esea_page'].values)

        # Removing https://play.esea.net prefix
        teams_list = ['/' + '/'.join(team.split('/')[-2:]) for team in teams_list]

        return teams_list
    else:
        return []


def sheet_img_formula(img_url):
    if not isinstance(img_url, str):
        return ''
    elif not img_url.startswith('https://'):
        return f'=IMAGE("https:{img_url}")'
    else:
        return f'=IMAGE("{img_url}")'


def create_stats_df(esea_stats: list) -> pd.DataFrame:
    """
    This function takes the ESEA stats scraped by selenium and places them in a dataframe with additional formatting.
    """
    stats_merged = list(itertools.chain.from_iterable(esea_stats))
    df = pd.DataFrame.from_records(stats_merged)

    def create_record(w, l):
        return f'{w}-{l}'

    df['logo_formula'] = df['logo_url'].apply(lambda x: sheet_img_formula(x))
    df['division'] = df['division'].str.capitalize()
    df['record'] = df.apply(lambda x: create_record(x['wins'], x['losses']), axis=1)
    df['page_url'] = 'https://play.esea.net' + df['page_url']
    df = df.drop(columns=['logo_url', 'win_streak'])

    return df


def add_coaches(df: pd.DataFrame, client) -> pd.DataFrame:
    """
    This function retrieves a table of coaches from Google Sheets and appends them to their respective teams.
    """
    coaches_sheet = client.open('UKCS Hub Sheet').worksheet('Coaches')
    coaches_df = pd.DataFrame(coaches_sheet.get_all_records())

    df = pd.merge(df, coaches_df, left_on='page_url', right_on='esea_page', how='left')
    df = df.drop(columns=['esea_page', 'team'])

    return df


def add_players(df: pd.DataFrame, client) -> pd.DataFrame:
    """
    This function retrieves a table of players from Google Sheets and appends them to their respective teams.
    """
    players_sheet = client.open('UKCS Hub Sheet').worksheet('Players')
    players_df = pd.DataFrame(players_sheet.get_all_records())

    df = pd.merge(df, players_df, left_on='page_url', right_on='esea_page', how='left')
    df = df.drop(columns=['team', 'esea_page'])

    return df


def rename_teams(df: pd.DataFrame, client) -> pd.DataFrame:
    """
    This function renames teams based on a table from Google Sheets
    """
    pass


def reorder_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reordering the dataframe ready for loading into Google Sheets.
    """
    return df[['team_name', 'logo_formula', 'players', 'division', 'record', 'page_url', 'coach']]


def add_pro_teams(df: pd.DataFrame, client) -> pd.DataFrame:
    """
    This function adds Pro/Challenger teams to the dataframe which are not listed on ESEA or are listed differently.
    """
    pro_sheet = client.open('UKCS Hub Sheet').worksheet('Pro/Ch Teams')
    pro_sheet_df = pd.DataFrame(pro_sheet.get_all_records())

    pro_sheet_df['logo_formula'] = pro_sheet_df['logo_url'].apply(lambda x: sheet_img_formula(x))
    pro_sheet_df = pro_sheet_df[['team_name', 'logo_formula', 'players', 'division', 'record', 'page_url', 'coach']]

    df = pd.concat([pro_sheet_df, df])

    return df


def upload_stats(df: pd.DataFrame, client, worksheet):
    """
    Uploading the dataframe to the main worksheet.
    """
    sheet = client.open('UKCS Hub Sheet').worksheet(worksheet)
    gd.set_with_dataframe(sheet, df, row=2, include_column_header=False)


def main():
    season = int(input('Enter ESEA season number: '))
    country = 'United Kingdom'

    division_urls = {'advanced': f'https://play.esea.net/league/standings?filters[game]=25&filters[season]={season+175}&filters[region]=2&filters[round]=regular%20season&filters[level]=advanced',
                 'main': f'https://play.esea.net/league/standings?filters[game]=25&filters[season]={season+175}&filters[region]=2&filters[round]=regular%20season&filters[level]=main',
                 'intermediate': f'https://play.esea.net/league/standings?filters[game]=25&filters[season]={season+175}&filters[region]=2&filters[round]=regular%20season&filters[level]=intermediate',
                 'open': f'https://play.esea.net/league/standings?filters[game]=25&filters[season]={season+175}&filters[region]=2&filters[round]=regular%20season&filters[level]=open'}

    # Setting connection to google sheets
    scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/spreadsheets',
         'https://www.googleapis.com/auth/drive.file',
         'https://www.googleapis.com/auth/drive']

    creds = ServiceAccountCredentials.from_json_keyfile_name('ukcs-spreadsheet-6b9ba266c9cb.json', scope)
    client = gspread.authorize(creds)

    # Opening the main worksheet
    sheet = client.open('UKCS Hub Sheet').worksheet(f'Season {season}')

    # Getting teams to add/remove
    additional_teams = get_additional_teams(client)
    blacklisted_teams = get_remove_teams(client)

    # Looping over each ESEA division and getting stats for that division
    print('Getting team statistics from ESEA...')
    esea_stats = []
    for division in division_urls:
        esea_stats.append(get_stats(division_urls=division_urls, 
                                    division=division, 
                                    season=season, 
                                    country=country, 
                                    additional_teams=additional_teams, 
                                    blacklisted_teams=blacklisted_teams))
        time.sleep(3)

    # Create dataframe from all gathered stats
    df = create_stats_df(esea_stats)

    df = add_coaches(df, client)
    df = add_players(df, client)
    df = reorder_df(df)
    df = add_pro_teams(df, client)
    
    # Uploading the dataframe to google sheets
    print('Uploading statistics to Google Sheets...')
    upload_stats(df, client, f'Season {season}')
    print('Complete.')


if __name__ == "__main__":
    main()