import threading
import json
from time import sleep
from datetime import datetime
from config import *
from utils import *
from schwab import historical_data, place_order
import pandas_ta as ta
import schedule
from tastytrade import place_tastytrade_order


def strategy(ticker, logger):
    """Runs the trading strategy for the specified ticker."""
    try:
        [time_frame, schwab_qty, trade_flag, period1, trend_line1, period2, trend_line2, tasty_qty] = (
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
            f"Running strategy for {ticker} at {datetime.now(tz=timezone(time_zone))} with params: QTY={schwab_qty}, TRENDS=({period1}, {trend_line1}), ({period2}, {trend_line2})"
        )

        schwab_qty = int(schwab_qty)
        tasty_qty = int(tasty_qty)
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
        elif trend_line1 == "WilderSmoother":
            df["trend1"] = wilders_smoothing(df, length=int(period1))

        if trend_line2 == "EMA":
            df["trend2"] = ta.ema(df["close"], length=int(period2))
        elif trend_line2 == "SMA":
            df["trend2"] = ta.sma(df["close"], length=int(period2))
        elif trend_line2 == "WilderSmoother":
            df["trend2"] = wilders_smoothing(df, length=int(period2))

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
                order_id_schwab = place_order(ticker, schwab_qty, "BUY", account_id, logger, "OPENING") if schwab_qty > 0 else 0
                order_id_tastytrade = place_tastytrade_order(ticker, tasty_qty, "Buy to Open", account_id, logger) if tasty_qty > 0 else 0
                trades[ticker] = {"action": "LONG", "order_id_schwab": order_id_schwab, "order_id_tastytrade": order_id_tastytrade}
            elif Short_condition:
                logger.info(f"Short condition triggered for {ticker}")
                order_id_schwab = place_order(ticker, schwab_qty, "SELL_SHORT", account_id, logger, "OPENING") if schwab_qty > 0 else 0
                order_id_tastytrade = place_tastytrade_order(ticker, tasty_qty, "Sell to Open", account_id, logger) if tasty_qty > 0 else 0
                trades[ticker] = {"action": "SHORT", "order_id_schwab": order_id_schwab, "order_id_tastytrade": order_id_tastytrade}
        else:
            if trades[ticker]["action"] == "LONG" and Short_condition:
                logger.info(
                    f"Reversing position for {ticker}: Closing LONG, opening SHORT"
                )
                long_order_id_schwab = place_order(ticker, schwab_qty, "SELL", account_id, logger, "CLOSING") if schwab_qty > 0 else 0
                long_order_id_tastytrade = place_tastytrade_order(ticker, tasty_qty, "Sell to Close", account_id, logger) if tasty_qty > 0 else 0
                short_order_id_schwab = place_order(ticker, schwab_qty, "SELL_SHORT", account_id, logger, "OPENING") if schwab_qty > 0 else 0
                short_order_id_tastytrade = place_tastytrade_order(ticker, tasty_qty, "Sell to Open", account_id, logger) if tasty_qty > 0 else 0
                trades[ticker] = {"action": "SHORT", "order_id_schwab": short_order_id_schwab, "order_id_tastytrade": short_order_id_tastytrade}

            elif trades[ticker]["action"] == "SHORT" and Long_condition:
                logger.info(
                    f"Reversing position for {ticker}: Closing SHORT, opening LONG"
                )
                short_order_id_schwab = place_order(ticker, schwab_qty, "BUY_TO_COVER", account_id, logger, "CLOSING") if schwab_qty > 0 else 0
                short_order_id_tastytrade = place_tastytrade_order(ticker, tasty_qty, "Buy to Close", account_id, logger) if tasty_qty > 0 else 0
                long_order_id_schwab = place_order(ticker, schwab_qty, "BUY", account_id, logger, "OPENING") if schwab_qty > 0 else 0
                long_order_id_tastytrade = place_tastytrade_order(ticker, tasty_qty, "Buy to Open", account_id, logger) if tasty_qty > 0 else 0
                trades[ticker] = {"action": "LONG", "order_id_schwab": long_order_id_schwab, "order_id_tastytrade": long_order_id_tastytrade}

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
                                store_logs(ticker)
                                logger = None
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
    main()
    # run_every_week()
