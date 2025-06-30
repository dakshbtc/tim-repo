from config import *
import json
from pytz import timezone
import pandas as pd
import pandas_market_calendars as mcal
from time import sleep
from datetime import datetime, time, timedelta
import logging
import numpy as np
import os
import pytz

# Function to check if a given day is a weekend
def is_weekend(date):
    return date.weekday() >= 5  # Saturday = 5, Sunday = 6


# Function to check if the market is closed due to a holiday
def is_holiday(date):
    nyse = mcal.get_calendar("NYSE")

    holidays = nyse.holidays().holidays
    holidays = pd.DatetimeIndex(holidays)

    final_holidays = [date.date() for date in holidays.to_pydatetime()]

    if date in final_holidays:
        return True
    return False


# Function to get the market hours for a specific date
def get_market_hours(date):
    if is_weekend(date):
        return None, "Market closed on weekends"

    if is_holiday(date):
        return None, "Market closed due to holiday"

    nyse = mcal.get_calendar("NYSE")
    market_date_time = nyse.schedule(date, date, tz=timezone(time_zone))
    start_time = market_date_time.iloc[0]["market_open"].time()
    end_time = (market_date_time.iloc[0]["market_close"] - timedelta(minutes=1)).time()

    return (start_time, end_time), "Regular trading day"


def get_current_datetime():
    current_dt = datetime.now(tz=timezone(time_zone))
    current_time = current_dt.time()
    current_date = current_dt.date()
    return current_time, current_date


def is_within_time_range():
    current_datetime = datetime.now(tz=timezone(time_zone))
    # Find most recent Sunday 6:00 PM
    days_since_sunday = (current_datetime.weekday() + 1) % 7
    last_sunday = current_datetime - timedelta(days=days_since_sunday)
    start_time = last_sunday.replace(hour=18, minute=0, second=0, microsecond=0)
    if current_datetime < start_time:
        # If before this week's Sunday 6pm, go to previous Sunday
        start_time -= timedelta(days=7)
    end_time = start_time + timedelta(days=4, hours=23)  # Friday 5:00 PM

    return start_time <= current_datetime <= end_time



def time_convert(dt=None, form="8601"):
    """
    Convert time to the correct format, passthrough if a string, preserve None if None for params parser
    :param dt: datetime.pyi object to convert
    :type dt: datetime.pyi | str | None
    :param form: what to convert input to
    :type form: str
    :return: converted time or passthrough
    :rtype: str | None
    """
    if dt is None or not isinstance(dt, datetime):
        return dt
    elif form == "8601":  # assume datetime object from here on
        return f"{dt.isoformat().split('+')[0][:-3]}Z"
    elif form == "epoch":
        return int(dt.timestamp())
    elif form == "epoch_ms":
        return int(dt.timestamp() * 1000)
    elif form == "YYYY-MM-DD":
        return dt.strftime("%Y-%m-%d")
    else:
        return dt


def wilders_smoothing(df, length=14):
    initial_mean = pd.Series(
    data=[df['close'].iloc[:length].mean()],
    index=[df['close'].index[length-1]],
)
    remaining_data = df['close'].iloc[length:]

    smoothed_values = pd.concat([initial_mean, remaining_data]).ewm(
        alpha=1.0 / length,
        adjust=False,
    ).mean()

    return smoothed_values

def params_parser(params: dict):
    """
    Removes None (null) values
    :param params: params to remove None values from
    :type params: dict
    :return: params without None values
    :rtype: dict
    """
    for key in list(params.keys()):
        if params[key] is None:
            del params[key]
    return params


def sleep_until_next_interval(ticker, interval_minutes):
    """
    Sleeps until the next specified interval in minutes or hours.

    Parameters:
    - interval_minutes: int or str
        The interval to sleep for:
        - Acceptable intervals: 1, 2, 5, 15, 30 (in minutes)
        - '1h', '4h' (in hours)
        - '1d' for daily
    """
    now = datetime.now(tz=timezone(time_zone))
    if interval_minutes.isdigit():
        interval_minutes = int(interval_minutes)
    if "/" == ticker[0]:
        if isinstance(interval_minutes, str):
            # Handle special cases for hours and daily
            if interval_minutes == "1h":
                next_interval = now.replace(
                    minute=0, second=0, microsecond=0
                ) + timedelta(hours=1)
            elif interval_minutes == "4h":
                # Calculate the next 4-hour block (e.g., 00:00, 04:00, 08:00, etc.)
                hour_block = ["02:00", "06:00", "10:00", "14:00", "18:00", "22:00"]
                for i in hour_block:
                    if now.time() < time.fromisoformat(i):
                        next_interval = now.replace(
                            hour=int(i.split(":")[0]),
                            minute=int(i.split(":")[1]),
                            second=0,
                            microsecond=0,
                        )
                        break

            elif interval_minutes == "1d":
                # Sleep until the start of the next day
                next_interval = now.replace(
                    hour=0, minute=0, second=0, microsecond=0
                ) + timedelta(days=1)
            else:
                raise ValueError("Invalid interval string. Use '1h', '4h', or '1d'.")
        else:
            # Handle minute intervals
            if interval_minutes not in [1, 2, 5, 15, 30]:
                raise ValueError("Invalid interval in minutes. Use 1, 2, 5, 15, or 30.")

            # Calculate the next interval in minutes
            minutes = (now.minute // interval_minutes + 1) * interval_minutes
            next_interval = now.replace(minute=minutes % 60, second=0, microsecond=0)

            # Handle hour overflow
            if minutes >= 60:
                next_interval = next_interval + timedelta(hours=1)
    else:
        if isinstance(interval_minutes, str):
            # Handle special cases for hours and daily
            if interval_minutes == "1h":
                hourly_interval = [
                    "10:30",
                    "11:30",
                    "12:30",
                    "13:30",
                    "14:30",
                    "15:30",
                    "15:59",
                ]
                # sleep until next hour and minute from the list above
                for i in hourly_interval:
                    if now.time() < time.fromisoformat(i):
                        next_interval = now.replace(
                            hour=int(i.split(":")[0]),
                            minute=int(i.split(":")[1]),
                            second=0,
                            microsecond=0,
                        )
                        break

            elif interval_minutes == "4h":
                # Calculate the next 4-hour block
                four_hour_intervals = ["13:30", "15:59"]
                for i in four_hour_intervals:
                    if now.time() < time.fromisoformat(i):
                        next_interval = now.replace(
                            hour=int(i.split(":")[0]),
                            minute=int(i.split(":")[1]),
                            second=0,
                            microsecond=0,
                        )
                        break

            elif interval_minutes == "1d":
                # Sleep until the start of the next day
                next_interval = now.replace(hour=15, minute=59, second=0, microsecond=0)
            else:
                raise ValueError("Invalid interval string. Use '1h', '4h', or '1d'.")
        else:
            # Handle minute intervals
            if interval_minutes not in [1, 2, 5, 15, 30]:
                raise ValueError("Invalid interval in minutes. Use 1, 2, 5, 15, or 30.")

            # Calculate the next interval in minutes
            minutes = (now.minute // interval_minutes + 1) * interval_minutes
            next_interval = now.replace(minute=minutes % 60, second=0, microsecond=0)

            # Handle hour overflow
            if minutes >= 60:
                next_interval = next_interval + timedelta(hours=1)

    # Calculate the difference in seconds
    seconds_until_next_interval = (next_interval - now).total_seconds()

    sleep(seconds_until_next_interval)


def get_strategy_prarams(ticker, logger):
    try:
        with open(tickers_path, "r") as file:
            strategy_params = json.load(file)

        return strategy_params[ticker]
    except Exception as e:
        logger.error(f"Error in getting strategy params: {str(e)}")
        sleep(10)
        strategy_params = get_strategy_prarams(ticker, logger)
        return strategy_params


def configure_logger(ticker):
    """Configure a logger specific to each thread (ticker)."""
    logger = logging.getLogger(ticker)
    logger.setLevel(logging.DEBUG)

    # Create file handler
    file_handler = logging.FileHandler(f"logs/{ticker}.log")
    file_handler.setLevel(logging.DEBUG)

    # Create formatter
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)

    # Add handler to logger
    logger.addHandler(file_handler)

    return logger


def store_logs(ticker):

    ticker_name = ticker[1:] if "/" == ticker[0] else ticker
    filename = f"previous_logs/{ticker_name}.txt"
    os.system(f"cat logs/{ticker_name}.log > {filename}")
    # clear log file
    with open(f"logs/{ticker_name}.log", "w") as file:
        file.write("")

# Utility functions for tick data
def is_tick_timeframe(timeframe):
    """Check if timeframe is tick-based (ends with 't')."""
    return isinstance(timeframe, str) and timeframe.lower().endswith('t')


def extract_tick_count(timeframe):
    """Extract number of ticks from timeframe string."""
    if is_tick_timeframe(timeframe):
        return int(timeframe[:-1])
    return None


def get_tick_data(ticker, timeframe, tick_buffers, logger):
    """Get tick-based data for the specified ticker."""
    if not is_tick_timeframe(timeframe):
        return None
        
    # Get DataFrame from tick buffer
    df = tick_buffers[ticker].get_dataframe()
    if df is None:
        logger.warning(f"Insufficient tick data for {ticker}. Need more bars.")
        return None
        
    logger.info(f"Retrieved {len(df)} tick bars for {ticker}")
    return df


def get_active_exchange_symbol(symbol):
    """
    Given a symbol like '/ES', returns the correct exchange symbol string for subscription,
    using tastytrade_instruments.csv logic from place_tastytrade_order.
    """
    if symbol[0] != '/':
        return symbol  # Not a futures symbol, return as is

    instrument_df = pd.read_csv("tastytrade_instruments.csv")
    df = instrument_df[
        (instrument_df["product-code"] == symbol[1:]) & (instrument_df["active-month"] == True)
    ][["exchange-symbol", "expires-at"]]
    expiry = pd.to_datetime(df["expires-at"].values[0])

    # Convert expiry to US/Eastern timezone
    eastern = pytz.timezone('US/Eastern')
    expiry_eastern = expiry.astimezone(eastern)

    # Convert datetime.now() to US/Eastern timezone
    now_eastern = datetime.now(pytz.utc).astimezone(eastern)
    if expiry_eastern <= now_eastern:
        # Use next active month
        next_symbol = instrument_df[
            (instrument_df["product-code"] == symbol[1:]) & (instrument_df["next-active-month"] == True)
        ]["exchange-symbol"].tolist()[0]
        return next_symbol
    else:
        return df["exchange-symbol"].values[0]


async def on_tick_received(tick_data, tick_buffers):
    """Callback function when new tick data is received."""
    symbol = tick_data['symbol']
    if symbol in tick_buffers:
        tick_buffers[symbol].add_tick(tick_data)