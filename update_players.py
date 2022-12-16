import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import gspread_dataframe as gd


def google_sheet_auth():
    scope = ['https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive.file',
            'https://www.googleapis.com/auth/drive']

    creds = ServiceAccountCredentials.from_json_keyfile_name('ukcs-spreadsheet-6b9ba266c9cb.json', scope)
    client = gspread.authorize(creds)

    return client


def get_players(client):
    players_sheet = client.open('UKCS Hub Sheet').worksheet('Players')
    players_df = pd.DataFrame(players_sheet.get_all_records())

    return players_df


def get_coaches(client):
    coaches_sheet = client.open('UKCS Hub Sheet').worksheet('Coaches')
    coaches_df = pd.DataFrame(coaches_sheet.get_all_records())

    return coaches_df


def get_main_sheet(client, season):
    main_sheet = client.open('UKCS Hub Sheet').worksheet(f'Season {season}')
    main_sheet_df = pd.DataFrame(main_sheet.get_all_records())

    return main_sheet_df


def merge_players(players, players_new, division):
    if division not in ['Pro', 'Challenger']:
        players = players_new

    return players


def merge_coaches(coach, coach_new, division):
    if division not in ['Pro', 'Challenger']:
        coach = coach_new

    return coach


def join_and_drop(df, players_df, coaches_df):
    df = pd.merge(df, players_df, left_on='ESEA Page', right_on='esea_page', how='left')
    df['Players'] = df.apply(lambda x: merge_players(x['Players'], x['players'], x['Division']), axis=1)
    df = df.drop(columns=['team', 'esea_page', 'players'])

    df = pd.merge(df, coaches_df, left_on='ESEA Page', right_on='esea_page', how='left')
    df['Coach'] = df.apply(lambda x: merge_players(x['Coach'], x['coach'], x['Division']), axis=1)
    df = df.drop(columns=['coach', 'esea_page', 'team'])

    return df


def reorder_df(df):
    return df[['Team', '', 'Players', 'Division', 'Record', 'ESEA Page', 'Coach']]


def upload_stats(df: pd.DataFrame, client, worksheet):
    sheet = client.open('UKCS Hub Sheet').worksheet(worksheet)
    gd.set_with_dataframe(sheet, df[['Players']], row=2, include_column_header=False, col=3)
    gd.set_with_dataframe(sheet, df[['Coach']], row=2, include_column_header=False, col=7)


def main():
    season = int(input('Enter ESEA season number: '))
    client = google_sheet_auth()

    players_df = get_players(client)
    coaches_df = get_coaches(client)

    df = get_main_sheet(client, season=season)
    df = join_and_drop(df=df, players_df=players_df, coaches_df=coaches_df)

    upload_stats(df, client, f'Season {season}')


if __name__ == "__main__":
    main()