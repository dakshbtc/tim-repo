import schedule
import requests
from config import *
from utils import *
from time import sleep
from datetime import datetime
from schwab import create_header, get_refresh_token
from tastytrade import get_instruments, generate_access_token_for_tastytrade
import json
import gspread as gs


def refresh_access_token():
    try:
        with open(refresh_token_path, "r") as file:
            refresh_token = file.read()
        headers = create_header("Basic")
        data = {"grant_type": "refresh_token", "refresh_token": str(refresh_token)}

        response = requests.post(authtoken_link, headers=headers, data=data)
        response = response.json()

        with open(access_token_path, "w") as file:
            file.write(response["access_token"])

        print(f"Access token refreshed at {datetime.now(tz=timezone(time_zone))}")

    except Exception as e:
        print(f"Error in refreshing access token = {str(e)}")


def get_google_sheet_params(worksheet):
    try:
        worksheet_df = worksheet.get_all_values()
        titles = worksheet_df[3]
        worksheet_df = pd.DataFrame(worksheet_df[5:], columns=titles)
        tickers = {}
        for i in range(len(worksheet_df)):
            tickers[worksheet_df.iloc[i]["Ticker Name"]] = [
                worksheet_df.iloc[i]["Time Frame"].split(" ")[0],
                worksheet_df.iloc[i]["Schwab Qty"],
                worksheet_df.iloc[i]["Trade"],
                worksheet_df.iloc[i]["Period1"],
                worksheet_df.iloc[i]["Trend line1"],
                worksheet_df.iloc[i]["Period2"],
                worksheet_df.iloc[i]["Trend line2"],
                worksheet_df.iloc[i]["Tastytrade Qty"]
            ]
        with open(tickers_path, "w") as file:
            json.dump(tickers, file)
    except Exception as e:
        print(f"Error in getting google sheet params = {str(e)}")
        sleep(10)
        gc = gs.service_account(filename=gs_json_path)
        worksheet = get_google_sheet(gc, Google_sheet_name, parameter_sheet)
        get_google_sheet_params(worksheet)


def get_google_sheet(gc, spreadsheet_name, sheet_name):
    try:
        worksheet = gc.open(spreadsheet_name).worksheet(sheet_name)
        return worksheet
    except Exception as e:
        print(f"Error in getting google sheet = {str(e)}")
        sleep(10)
        worksheet = get_google_sheet(gc, spreadsheet_name, sheet_name)
        return worksheet


def check_link(worksheet):
    try:
        worksheet_df = worksheet.get_all_values()
        titles = worksheet_df[0]
        worksheet_df = pd.DataFrame(worksheet_df[1:], columns=titles)

        with open(refresh_token_link_path, "r") as file:
            refresh_token_link = json.load(file)

        if worksheet_df.iloc[1]["Links"] != refresh_token_link["refresh_link"]:
            print(worksheet_df.iloc[1]["Links"])
            is_valid = get_refresh_token(worksheet_df.iloc[1]["Links"])
            if not is_valid:
                worksheet.update_acell("B4", "Link is Expired! Please try again...")
                with open(refresh_token_link_path, "w") as file:
                    refresh_token_link["refresh_link"] = worksheet_df.iloc[1]["Links"]
                    json.dump(refresh_token_link, file)
            else:
                worksheet.update_acell("B4", "Token Refreshed")
                with open(refresh_token_link_path, "w") as file:
                    refresh_token_link["refresh_link"] = worksheet_df.iloc[1]["Links"]
                    json.dump(refresh_token_link, file)
                
    except Exception as e:
        print(f"Error in checking link = {str(e)}")
        sleep(10)
        gc = gs.service_account(filename=gs_json_path)
        worksheet = get_google_sheet(gc, Google_sheet_name, link_sheet)
        check_link(worksheet)


def main():

    refresh_access_token()

    gc = gs.service_account(filename=gs_json_path)
    worksheet_eq = get_google_sheet(gc, Google_sheet_name, parameter_sheet)
    worksheet_link = get_google_sheet(gc, Google_sheet_name, link_sheet)

    schedule.every(25).minutes.do(refresh_access_token)
    schedule.every(10).seconds.do(get_google_sheet_params, worksheet_eq)
    schedule.every(5).seconds.do(check_link, worksheet_link)
    schedule.every().day.at("09:00").do(generate_access_token_for_tastytrade)
    schedule.every().day.at("09:05").do(get_instruments)

    while True:
        schedule.run_pending()
        sleep(1)

main()
