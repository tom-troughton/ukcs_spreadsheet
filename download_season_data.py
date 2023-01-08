"""
Script to download the spreadsheet for a given season.
Useful for backup purposes.
"""
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import gspread


def google_sheet_auth(key_file):
    scope = ['https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive.file',
            'https://www.googleapis.com/auth/drive']

    creds = ServiceAccountCredentials.from_json_keyfile_name(key_file, scope)
    client = gspread.authorize(creds)

    return client


def get_main_sheet(client, season):
    main_sheet = client.open('UKCS Hub Sheet').worksheet(f'Season {season}')
    main_sheet_df = pd.DataFrame(main_sheet.get_all_records())

    return main_sheet_df


def main():
    season = int(input('Enter ESEA season number: '))

    client = google_sheet_auth(key_file='ukcs-spreadsheet-6b9ba266c9cb.json')

    df = get_main_sheet(client=client, season=season)

    df.to_csv(f'ukcshub_season{season}.csv')


if __name__ == "__main__":
    main()