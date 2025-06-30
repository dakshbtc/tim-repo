import requests
from config import *
from utils import *
import pandas as pd
import base64


def create_header(auth_type, logger=None):
    try:
        if auth_type == "Basic":
            return {
                "Authorization": f'Basic {base64.b64encode(bytes(f"{api_key}:{api_secret}", "utf-8")).decode("utf-8")}',
                "Content-Type": "application/x-www-form-urlencoded",
            }
        elif auth_type == "Bearer":
            with open(access_token_path, "r") as file:
                token = file.read()
            return {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    except Exception as e:
        logger.error(f"Error in creating header: {str(e)}")
        raise


def get_refresh_token(redirect_link):
    try:
        code = (
            f'{redirect_link[redirect_link.index("code=")+5:redirect_link.index("%40")]}@'
        )

        data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": api_callback_url,
        }

        response = requests.post(
            authtoken_link, headers=create_header("Basic"), data=data
        )
        response = response.json()
        refresh_token = response["refresh_token"]
        access_token = response["access_token"]

        with open(refresh_token_path, "w") as file:
            file.write(refresh_token)

        with open(access_token_path, "w") as file:
            file.write(access_token)

        return True

    except Exception as e:
        print(f"Error in getting refresh token = {str(e)} {response}")
        return False


def _time_convert(dt=None, form="8601"):
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


def historical_data(symbol, time_frame, logger):
    try:
        if symbol in ['SPX']:
            symbol = f'${symbol}'
        logger.info(f"Fetching historical data for {symbol}")
        current_datetime = datetime.now(tz=timezone(time_zone))
        endtime = current_datetime + timedelta(seconds=85)
        if isinstance(time_frame, str):
            if time_frame.isdigit():
                time_frame = int(time_frame)
        # Mapping of time frames to configurations
        time_frame_config = {
            "1h": {
                "periodType": "day",
                "period": 10,
                "frequencyType": "minute",
                "frequency": 30,
                "resample": "1H",
            },
            "4h": {
                "periodType": "day",
                "period": 10,
                "frequencyType": "minute",
                "frequency": 30,
                "resample": "4H",
            },
            "1d": {
                "periodType": "month",
                "period": 2,
                "frequencyType": "daily",
                "frequency": 1,
            },
            1: {
                "periodType": "day",
                "period": 2,
                "frequencyType": "minute",
                "frequency": 1,
            },
            2: {
                "periodType": "day",
                "period": 2,
                "frequencyType": "minute",
                "frequency": 1,
                "resample": "2min",
            },
            5: {
                "periodType": "day",
                "period": 2,
                "frequencyType": "minute",
                "frequency": 5,
            },
            15: {
                "periodType": "day",
                "period": 5,
                "frequencyType": "minute",
                "frequency": 15,
            },
            30: {
                "periodType": "day",
                "period": 5,
                "frequencyType": "minute",
                "frequency": 30,
            },
        }

        if time_frame not in time_frame_config:
            raise ValueError(f"Unsupported time frame: {time_frame}")

        # Extract configuration for the given time_frame
        config = time_frame_config[time_frame]
        periodType, period, frequencyType, frequency = (
            config["periodType"],
            config["period"],
            config["frequencyType"],
            config["frequency"],
        )

        # Prepare request parameters
        params = {
            "symbol": symbol,
            "periodType": periodType,
            "endDate": _time_convert(endtime, "epoch_ms"),
            "period": period,
            "frequencyType": frequencyType,
            "frequency": frequency,
            "needExtendedHoursData": True if symbol[0] == "/" else False,
        }

        # API Request
        data = requests.get(
            f"{base_api_url}/marketdata/v1/pricehistory",
            headers=create_header("Bearer", logger),
            params=params_parser(params),
        )
        data = data.json().get("candles", [])
        if not data:
            raise ValueError("No data returned from API.")

        # Data transformation
        df = pd.DataFrame(data)
        df["symbol"] = symbol
        df["datetime"] = (
            pd.to_datetime(df["datetime"], unit="ms")
            .dt.tz_localize("UTC")
            .dt.tz_convert(time_zone)
        )
        df = df[["datetime", "symbol", "open", "high", "low", "close"]]
        # Resampling if applicable
        if "resample" in config:
            resample_freq = config["resample"]
            if time_frame == "1h":
                # Adjust resampling for 30-minute offset
                df = (
                    df.set_index("datetime")
                    .resample(resample_freq, offset="30min")  # 30-minute offset
                    .agg(
                        {"open": "first", "high": "max", "low": "min", "close": "last"}
                    )
                    .dropna()
                    .reset_index()
                )
            elif time_frame == "4h":
                # Adjust resampling for 90-minute offset
                df = (
                    df.set_index("datetime")
                    .resample(resample_freq, offset="90min")  # 90-minute offset
                    .agg(
                        {"open": "first", "high": "max", "low": "min", "close": "last"}
                    )
                    .dropna()
                    .reset_index()
                )
            else:
                # Standard resampling for other time frames
                df = (
                    df.set_index("datetime")
                    .resample(resample_freq)
                    .agg(
                        {"open": "first", "high": "max", "low": "min", "close": "last"}
                    )
                    .dropna()
                    .reset_index()
                )
        if df.iloc[-1]["datetime"].strftime("%H:%M") == datetime.now(
            tz=timezone(time_zone)
        ).strftime("%H:%M"):
            df = df[:-1]
        logger.info(f"Historical data for {symbol} fetched successfully")
        return df
    except Exception as e:
        logger.error(f"Error in getting historical data for {symbol}: {str(e)}")
        sleep(10)
        df = historical_data(symbol, time_frame, logger)
        return df


def place_order(symbol, quantity, action, account_id, logger, position_effect):
    try:
        logger.info(
            f"Placing order for {symbol}, Action: {action}, Quantity: {quantity}"
        )
        asset_type = "FUTURE" if symbol.startswith("/") else "EQUITY"
        order_payload = {
            "orderType": "MARKET",
            "session": "NORMAL",
            "duration": "DAY",
            "orderStrategyType": "SINGLE",
            "orderLegCollection": [
                {
                    "instruction": action,
                    "quantity": quantity,
                    "instrument": {"symbol": symbol, "assetType": asset_type},
                }
            ],
        }

        encrypted_account_id = get_encrypted_account_id(account_id, logger)
        place_order_url = f"{schwab_trader_link}/accounts/{encrypted_account_id}/orders"
        response = requests.post(
            url=place_order_url,
            json=order_payload,
            headers=create_header("Bearer", logger),
        )

        order_id = dict(response.headers)["Location"].split("/")[-1]
        is_filled, traded_qty = check_order_status(order_id, logger)
        if is_filled:
            logger.info(f"Order placed successfully for {symbol}. Order ID: {order_id}")
            return order_id
        else:
            logger.warning(f"Order not filled for {symbol}. Order ID: {order_id}")
            logger.warning(f"Placing order again for {symbol}. Order ID: {order_id}")
            order_id = place_order(symbol, quantity, action, account_id, logger, position_effect)
            return order_id

    except Exception as e:
        logger.error(f"Error in placing order for {symbol}: {str(e)}")
        return None


def get_encrypted_account_id(schwab_account_id, logger):
    try:
        get_encrypted_account_id_url = f"{schwab_trader_link}/accounts/accountNumbers"
        response = requests.get(
            url=get_encrypted_account_id_url, headers=create_header("Bearer", logger)
        )
        encrypted_account_id = response.json()[0]["hashValue"]
        return encrypted_account_id
    except Exception as e:
        logger.error(
            f"Error in getting encrypted account ID for {schwab_account_id}: {str(e)}"
        )
        sleep(10)
        encrypted_account_id = get_encrypted_account_id(schwab_account_id, logger)
        return encrypted_account_id


def check_position_status(symbol, account_id, logger):
    try:
        logger.info(f"Checking position status for {symbol}")
        position_url = f"{schwab_trader_link}/accounts"
        response = requests.get(
            url=position_url,
            params={"fields": "positions"},
            headers=create_header("Bearer", logger),
        )
        positions = response.json()[0]["securitiesAccount"]["positions"]
        for position in positions:
            if position["instrument"]["symbol"] == symbol:
                logger.info(f"Position found for {symbol}")
                return True

        logger.info(f"No position found for {symbol}")
        return False
    except Exception as e:
        logger.error(f"Error in checking position status for {symbol}: {str(e)}")
        return False


def check_order_status(order_id, logger):
    try:
        logger.info(f"Checking order status for Order ID: {order_id}")
        encrypted_account_id = get_encrypted_account_id(account_id, logger)
        check_order_status_url = (
            f"{schwab_trader_link}/accounts/{encrypted_account_id}/orders/{order_id}"
        )
        response = requests.get(
            url=check_order_status_url, headers=create_header("Bearer", logger)
        )
        order_history = response.json()
        order_status = order_history["status"]
        traded_qty = int(order_history["quantity"])

        if order_status == "FILLED":
            logger.info(f"Order ID {order_id} is filled")
            return True, traded_qty
        else:
            if order_status == "REJECTED":
                logger.warning(f"Order ID {order_id} is rejected")
                return False, 0
            else:
                logger.warning(f"Order ID {order_id} is not filled")
                sleep(1)
                is_filled, traded_qty = check_order_status(order_id, logger)
                return is_filled, traded_qty

    except Exception as e:
        logger.error(
            f"Error in checking order status for Order ID {order_id}: {str(e)}"
        )
        is_filled, traded_qty = check_order_status(order_id, logger)
        return is_filled, traded_qty


def cancel_order(order_id, account_id, schwab_account_id, logger):
    try:
        logger.info(f"Cancelling order with Order ID: {order_id}")
        cancel_order_url = (
            f"{schwab_trader_link}/accounts/{account_id}/orders/{order_id}"
        )
        response = requests.delete(
            url=cancel_order_url, headers=create_header("Bearer", logger)
        )
        logger.info(f"Order ID {order_id} cancelled successfully")
    except Exception as e:
        logger.error(f"Error in cancelling order with Order ID {order_id}: {str(e)}")
