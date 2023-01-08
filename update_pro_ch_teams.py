"""
This script is for updating the main spreadsheet when changes are made to the Pro/Ch Teams worksheet.
"""
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import gspread_dataframe as gd
from main import sheet_img_formula
# get pro team stats -> re-arrange/img formula -> get main sheet -> remove pro teams from main sheet -> concatenate rows -> upload
# count number of pro/challenger teams -> overwrite pro/challenger teams (no changes made to other division teams)

def google_sheet_auth():
    scope = ['https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive.file',
            'https://www.googleapis.com/auth/drive']

    creds = ServiceAccountCredentials.from_json_keyfile_name('ukcs-spreadsheet-6b9ba266c9cb.json', scope)
    client = gspread.authorize(creds)

    return client


def get_pro_ch_players(client):
    pro_sheet = client.open('UKCS Hub Sheet').worksheet('Pro/Ch Teams')
    pro_sheet_df = pd.DataFrame(pro_sheet.get_all_records())
    
    pro_sheet_df['logo_formula'] = pro_sheet_df['logo_url'].apply(lambda x: sheet_img_formula(x))
    pro_sheet_df = pro_sheet_df[['team_name', 'logo_formula', 'players', 'division', 'record', 'page_url', 'coach']]
    pro_sheet_df = pro_sheet_df.rename(columns={'team_name': 'Team', 'logo_formula': '', 'players': 'Players', 'division': 'Division', 'record': 'Record', 'page_url': 'ESEA Page', 'coach': 'Coach'})

    return pro_sheet_df


def get_main_sheet(client, worksheet):
    main_sheet = client.open('UKCS Hub Sheet').worksheet(worksheet)
    main_sheet_df = pd.DataFrame(main_sheet.get_all_records())

    return main_sheet_df


def del_and_ins_rows(pro_df: pd.DataFrame, num_pro_ch_teams: int, client, worksheet):
    sheet = client.open('UKCS Hub Sheet').worksheet(worksheet)

    del_rows = [i+1 for i in range(num_pro_ch_teams)]

    sheet.delete_rows(del_rows[0] + 1, del_rows[-1] + 1)

    sheet.insert_rows(pro_df.values.tolist(), row=2, value_input_option='user_entered')


def main():
    season = int(input('Enter ESEA season number: '))
    print('Authenticating with Google sheets...')
    client = google_sheet_auth()

    df = get_main_sheet(client=client, worksheet=f'Season {season}')

    # Selecting teams in pro or challenger division
    df = df[(df['Division'] == 'Pro') | (df['Division'] == 'Challenger')]

    pro_df = get_pro_ch_players(client)

    del_and_ins_rows(pro_df=pro_df, num_pro_ch_teams=df.shape[0], client=client, worksheet=f'Season {season}')


if __name__ == "__main__":
    main()