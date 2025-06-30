import threading
import json
import redis
import pandas as pd
from time import sleep
import logging
from collections import defaultdict
from config import *
from utils import *
from schwab import historical_data, place_order
import pandas_ta as ta
from tastytrade import place_tastytrade_order
from utils import is_tick_timeframe, get_active_exchange_symbol
from datetime import datetime,timedelta,timezone
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(name)s:%(message)s',
    handlers=[
        logging.FileHandler("strategy_consumer.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
class StrategyConsumer:
    def __init__(self, redis_host='localhost', redis_port=6379, redis_db=0):
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
        self.pubsub = self.redis_client.pubsub()
        self.logger = logging.getLogger('StrategyConsumer')
        # Remove self.tick_dataframes - use Redis only for tick data
        self.pending_strategies = defaultdict(threading.Event)  # For triggering strategy on new bars
        

    def get_tick_dataframe(self, symbol, period1: int = 7, period2: int = 30):
        zset_key = f"bars_history:{symbol}"
        max_bars = max(period1, period2)

        # Fetch latest bars by rank (newest to oldest), then reverse for oldest → newest
        latest_bars = self.redis_client.zrevrange(zset_key, 0, max_bars)
        bars = [json.loads(bar.decode('utf-8')) for bar in reversed(latest_bars)]

        if not bars:
            return pd.DataFrame()

        df = pd.DataFrame(bars)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        df.sort_index(inplace=True)

        return df
    
    def strategy(self, ticker, logger, triggered_by_new_bar=False):
        """Modified strategy function with new bar trigger logic"""
        try:
            [time_frame, schwab_qty, trade_flag, period1, trend_line1, period2, trend_line2, tasty_qty] = (
                get_strategy_prarams(ticker, logger)
            )
            if trade_flag != "TRUE":
                logger.info(f"Skipping strategy for {ticker}, trade flag is FALSE.")
                return

            # For tick-based strategies, only run when triggered by new bar
            if is_tick_timeframe(time_frame) and not triggered_by_new_bar:
                logger.debug(f"Skipping tick strategy for {ticker} - no new bar")
                return

            logger.info(f"Running strategy for {ticker} with params: QTY={schwab_qty}, timeframe={time_frame}")

            schwab_qty = int(schwab_qty)
            tasty_qty = int(tasty_qty)
            trade_file_path = f"trades/{ticker.replace('/', '_')}.json"
            
            if not os.path.exists(trade_file_path):
                with open(trade_file_path, "w") as f:
                    json.dump({}, f)

            with open(trade_file_path, "r") as file:
                trades = json.load(file)
            # Get data based on timeframe type
            ticker = get_active_exchange_symbol(ticker) if ticker.startswith("/") else ticker
            if is_tick_timeframe(time_frame):

                logger.info(f"Using tick data for {ticker}")
                
                df = self.get_tick_dataframe(ticker, int(period1), int(period2))  # This returns DataFrame and updated number of bars needed for each period
                
                if df is None:
                    logger.warning(f"No tick data available for {ticker}")
                    return
            else:
                logger.info(f"Using historical data for {ticker}")
                df = historical_data(ticker, time_frame, logger=logger)

            if df is None or len(df) < max(int(period1), int(period2)):
                logger.warning(f"Insufficient data for {ticker}")
                return
            
            # Calculate trend indicators
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

            # Check for NaN values
            if (df["trend1"].isna().iloc[-1] or df["trend2"].isna().iloc[-1] or 
                df["trend1"].isna().iloc[-2] or df["trend2"].isna().iloc[-2]):
                logger.warning(f"NaN values in trend indicators for {ticker}")
                return

            # Trading logic
            Long_condition = (
                df.iloc[-1]["trend1"] > df.iloc[-1]["trend2"]
                and df.iloc[-2]["trend1"] < df.iloc[-2]["trend2"]
            )
            Short_condition = (
                df.iloc[-1]["trend1"] < df.iloc[-1]["trend2"]
                and df.iloc[-2]["trend1"] > df.iloc[-2]["trend2"]
            )
            # Execute trades
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
                # Position reversal logic
                if trades[ticker]["action"] == "LONG" and Short_condition:
                    logger.info(f"Reversing position for {ticker}: Closing LONG, opening SHORT")
                    long_order_id_schwab = place_order(ticker, schwab_qty, "SELL", account_id, logger, "CLOSING") if schwab_qty > 0 else 0
                    long_order_id_tastytrade = place_tastytrade_order(ticker, tasty_qty, "Sell to Close", account_id, logger) if tasty_qty > 0 else 0
                    short_order_id_schwab = place_order(ticker, schwab_qty, "SELL_SHORT", account_id, logger, "OPENING") if schwab_qty > 0 else 0
                    short_order_id_tastytrade = place_tastytrade_order(ticker, tasty_qty, "Sell to Open", account_id, logger) if tasty_qty > 0 else 0
                    trades[ticker] = {"action": "SHORT", "order_id_schwab": short_order_id_schwab, "order_id_tastytrade": short_order_id_tastytrade}

                elif trades[ticker]["action"] == "SHORT" and Long_condition:
                    logger.info(f"Reversing position for {ticker}: Closing SHORT, opening LONG")
                    short_order_id_schwab = place_order(ticker, schwab_qty, "BUY_TO_COVER", account_id, logger, "CLOSING") if schwab_qty > 0 else 0
                    short_order_id_tastytrade = place_tastytrade_order(ticker, tasty_qty, "Buy to Close", account_id, logger) if tasty_qty > 0 else 0
                    long_order_id_schwab = place_order(ticker, schwab_qty, "BUY", account_id, logger, "OPENING") if schwab_qty > 0 else 0
                    long_order_id_tastytrade = place_tastytrade_order(ticker, tasty_qty, "Buy to Open", account_id, logger) if tasty_qty > 0 else 0
                    trades[ticker] = {"action": "LONG", "order_id_schwab": long_order_id_schwab, "order_id_tastytrade": long_order_id_tastytrade}

            with open(trade_file_path, "w") as file:
                json.dump(trades.copy(), file)

            logger.info(f"Strategy for {ticker} completed.")

        except Exception as e:
            logger.error(f"Error in strategy for {ticker}: {e}", exc_info=True)

    def subscribe_to_tick_bars(self, symbols):
        """Subscribe to tick bar updates for given symbols"""
        for symbol in symbols:
            channel = f"tick_bars:{symbol}" 
            self.pubsub.subscribe(channel)
        
        self.logger.info(f"Subscribed to tick bars for symbols: {symbols}")

    def listen_for_tick_bars(self, tick_symbols_to_tickers):
        """Listen for new tick bars and trigger strategies"""
        for message in self.pubsub.listen():
            if message['type'] == 'message':
                try:
                    # Parse the channel to get symbol
                    channel = message['channel'].decode('utf-8')
                    symbol = channel.split(':')[1]
                    
                    # Parse bar data
                    bar_data = json.loads(message['data'].decode('utf-8'))
                    self.logger.info(f"Received new bar for {symbol}: close={bar_data['close']}")
                    
                    # Trigger strategy for all tickers using this symbol
                    if symbol in tick_symbols_to_tickers:
                        for ticker in tick_symbols_to_tickers[symbol]:
                            self.pending_strategies[ticker].set()
                            self.logger.debug(f"Triggered strategy event for {ticker}")
                    
                except Exception as e:
                    self.logger.error(f"Error processing tick bar message: {e}")

    def main_strategy_loop(self, ticker):
        """Main strategy loop for a specific ticker"""
        logger = configure_logger(ticker)
        logger.info(f"MAIN STRATEGY STARTED for {ticker}")

        try:
            while True:
                while is_within_time_range():
                    _, today_date = get_current_datetime()
                    time_frame, *_ = get_strategy_prarams(ticker, logger)

                    if ticker.startswith("/"):  # Futures
                        if is_holiday(today_date):
                            logger.info("Market closed due to holiday")
                            sleep(60)
                            continue
                        run_strategy = True

                    else:  # Stocks
                        market_hours, status = get_market_hours(today_date)
                        if not market_hours:
                            logger.info(status)
                            sleep(60)
                            continue
                        current_time, _ = get_current_datetime()
                        if not (market_hours[0] <= current_time <= market_hours[1]):
                            if current_time >= market_hours[1]:
                                logger.info("Market closed")
                                break
                            else:
                                sleep(60)
                                continue
                        run_strategy = True

                    if run_strategy:
                        if is_tick_timeframe(time_frame):
                            if self.pending_strategies[ticker].wait(timeout=300):
                                self.pending_strategies[ticker].clear()
                                self.strategy(ticker, logger, triggered_by_new_bar=True)
                            else:
                                logger.debug(f"No new bar received for {ticker} in 5 minutes")
                        else:
                            sleep_until_next_interval(ticker, time_frame)
                            self.strategy(ticker, logger)

                sleep(10)  # Short pause before rechecking time range

        except Exception as e:
            logger.error(f"Error in main loop for {ticker}: {e}", exc_info=True)


    def run(self, tickers_config):
        """Main run method for strategy consumer"""
        # Map symbols to tickers for reverse lookup
        self.logger.info("Startingg Strategy Consumer...")
        tick_symbols_to_tickers = defaultdict(list)
        tick_symbols = []
        
        for ticker, config in tickers_config.items():
            time_frame = config[0]
            if is_tick_timeframe(time_frame):
                ticker_for_data = get_active_exchange_symbol(ticker) if ticker.startswith("/") else ticker
                tick_symbols.append(ticker_for_data)
                tick_symbols_to_tickers[ticker_for_data].append(ticker)
        
        # Subscribe to tick bars if needed
        if tick_symbols:
            self.subscribe_to_tick_bars(tick_symbols)
            
            # Start tick bar listener in separate thread
            listener_thread = threading.Thread(
                target=self.listen_for_tick_bars, 
                args=(tick_symbols_to_tickers,), 
                daemon=True
            )
            listener_thread.start()
        
        # Start strategy threads for each ticker
        threads = []
        for ticker in tickers_config.keys():
            ticker_for_data = get_active_exchange_symbol(ticker) if ticker.startswith("/") else ticker

            thread = threading.Thread(target=self.main_strategy_loop, args=(ticker,), daemon=True)
            threads.append(thread)
            thread.start()
        
        # Keep main thread alive
        try:
            for thread in threads:
                thread.join()
        except KeyboardInterrupt:
            print("Shutting down strategy consumer...")


if __name__ == "__main__":
    # Load ticker configuration
    with open("jsons/tickers.json", "r") as file:
        tickers_config = json.load(file)
    print(f"Loaded {len(tickers_config)} tickers from configuration.")
    # Start strategy consumer
    consumer = StrategyConsumer()
    consumer.run(tickers_config)