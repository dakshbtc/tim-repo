import threading
import json
from time import sleep
from datetime import datetime
from config import *
from utils import *
from schwab import historical_data, place_order
import pandas_ta as ta
import schedule
import os


def strategy(ticker, logger):
    """Runs the trading strategy for the specified ticker."""
    try:
        [time_frame, qty, trade_flag, period1, trend_line1, period2, trend_line2] = (
            get_strategy_prarams(ticker, logger)
        )
        if trade_flag != "TRUE":
            logger.info(f"Skipping strategy for {ticker}, trade flag is FALSE.")
            with open(
                f"trades/{ticker[1:] if '/' == ticker[0] else ticker}.json", "r"
            ) as file:
                trades = json.load(file)
            if ticker in trades:
                trades = {}
                with open(
                    f"trades/{ticker[1:] if '/' == ticker[0] else ticker}.json",
                    "w",
                ) as file:
                    json.dump(trades, file)
            return

        logger.info(
            f"Running strategy for {ticker} at {datetime.now(tz=timezone(time_zone))} with params: QTY={qty}, TRENDS=({period1}, {trend_line1}), ({period2}, {trend_line2})"
        )

        qty = int(qty)
        with open(
            f"trades/{ticker[1:] if '/' == ticker[0] else ticker}.json", "r"
        ) as file:
            trades = json.load(file)

        df = historical_data(
            ticker,
            time_frame,
            logger=logger,
        )

        if trend_line1 == "EMA":
            df["trend1"] = ta.ema(df["close"], length=int(period1))
        elif trend_line1 == "SMA":
            df["trend1"] = ta.sma(df["close"], length=int(period1))

        if trend_line2 == "EMA":
            df["trend2"] = ta.ema(df["close"], length=int(period2))
        elif trend_line2 == "SMA":
            df["trend2"] = ta.sma(df["close"], length=int(period2))

        Long_condition = (
            df.iloc[-1]["trend1"] > df.iloc[-1]["trend2"]
            and df.iloc[-2]["trend1"] < df.iloc[-2]["trend2"]
        )
        Short_condition = (
            df.iloc[-1]["trend1"] < df.iloc[-1]["trend2"]
            and df.iloc[-2]["trend1"] > df.iloc[-2]["trend2"]
        )

        if ticker not in trades.copy():
            if Long_condition:
                logger.info(f"Long condition triggered for {ticker}")
                order_id = place_order(ticker, qty, "BUY", account_id, logger)
                trades[ticker] = {"action": "LONG", "order_id": order_id}
            elif Short_condition:
                logger.info(f"Short condition triggered for {ticker}")
                order_id = place_order(ticker, qty, "SELL_SHORT", account_id, logger)
                trades[ticker] = {"action": "SHORT", "order_id": order_id}
        else:
            if trades[ticker]["action"] == "LONG" and Short_condition:
                logger.info(
                    f"Reversing position for {ticker}: Closing LONG, opening SHORT"
                )
                long_order_id = place_order(ticker, qty, "SELL", account_id, logger)
                short_order_id = place_order(ticker, qty, "SELL_SHORT", account_id, logger)
                trades[ticker] = {"action": "SHORT", "order_id": short_order_id}

            elif trades[ticker]["action"] == "SHORT" and Long_condition:
                logger.info(
                    f"Reversing position for {ticker}: Closing SHORT, opening LONG"
                )
                short_order_id = place_order(ticker, qty, "BUY_TO_COVER", account_id, logger)
                long_order_id = place_order(ticker, qty, "BUY", account_id, logger)
                trades[ticker] = {"action": "LONG", "order_id": long_order_id}

        with open(
            f"trades/{ticker[1:] if '/' == ticker[0] else ticker}.json", "w"
        ) as file:
            json.dump(trades.copy(), file)

        logger.info(f"Strategy for {ticker} completed.")

    except Exception as e:
        logger.error(f"Error in strategy for {ticker}: {e}", exc_info=True)


def main_strategy_loop(ticker):
    """Main loop for running the strategy for a specific ticker."""
    logger = configure_logger(ticker)

    try:
        while is_within_time_range():
            _, today_date = get_current_datetime()
            if "/" == ticker[0]:
                if is_holiday(today_date):
                    logger.info("Market closed due to holiday")
                    sleep(60)
                else:
                    [time_frame, *_] = get_strategy_prarams(ticker, logger)
                    sleep_until_next_interval(ticker, time_frame)
                    strategy(ticker, logger)
            else:
                market_hours, status = get_market_hours(today_date)
                if not market_hours:
                    logger.info(status)
                    sleep(60)
                else:
                    while True:
                        current_time, _ = get_current_datetime()

                        if market_hours[0] <= current_time <= market_hours[1]:
                            [time_frame, *_] = get_strategy_prarams(ticker, logger)
                            sleep_until_next_interval(ticker, time_frame)
                            strategy(ticker, logger)
                        else:
                            if current_time >= market_hours[1]:
                                logger.info("Market closed")
                                _, today_date = get_current_datetime()
                                today_date = today_date.strftime('%Y-%m-%d')
                                try:
                                    os.system(f"mkdir -p history/date-{today_date}")
                                except Exception as e:
                                    pass
                                ticker_name = ticker[1:] if '/' == ticker[0] else ticker
                                filename = f"history/date-{today_date}/{ticker_name}.txt"
                                os.system(
                                    f"cat logs/{ticker_name}.log > {filename}"
                                )
                                # clear log file
                                with open(f"logs/{ticker_name}.log", "w") as file:
                                    file.write("")
                                break
                            elif current_time < market_hours[0]:
                                sleep(60)
                            else:
                                break

    except Exception as e:
        logger.error(f"Error in main loop for {ticker}: {e}", exc_info=True)


def run_every_week():
    """Starts threads for each ticker."""
    with open(tickers_path, "r") as file:
        ticker_n_tf = json.load(file)

    threads = []
    for ticker in ticker_n_tf.keys():
        thread = threading.Thread(target=main_strategy_loop, args=(ticker,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


def main():
    """Main scheduling function."""
    schedule.every().sunday.at("18:00").do(run_every_week)

    while True:
        schedule.run_pending()
        sleep(1)


# Start the process
if __name__ == "__main__":
    # main()
    run_every_week()
