import logging
from binance.client import Client
import pandas as pd
import numpy as np
from datetime import datetime
import time
import concurrent.futures
import threading

# Logging 설정
logging.basicConfig(
    filename='binance_screener.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# API 키 설정
api_key = "shaRfojL5PH7hD4os8ctX7MJf47uVBvkAf3SZ0koa92zNq72bFlZdRd4HUZBhmCt"
api_secret = "tiv40Xp1YXsHdtHVQhgRkjwTAVYa72RP1UwXgNc4r3tHmLI0qfjRXSxh02D503Xu"

try:
    client = Client(api_key, api_secret)
except Exception as e:
    logging.error(f"Failed to initialize Binance client: {e}")
    print(f"Error: Failed to connect to Binance. Check API keys and network. {e}")
    exit(1)

# SMA 크로스 체크 함수
def check_sma_cross(series, short_window, long_window):
    short_sma = series.rolling(window=short_window).mean()
    long_sma = series.rolling(window=long_window).mean()
    cross = short_sma > long_sma
    signal = cross.diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    return signal

# RSI 계산 함수 (RMA 방식)
def get_rsi(data, period=14):
    delta = data.diff()
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    
    avg_gain = pd.Series(gain, index=data.index).ewm(alpha=1/period, adjust=False).mean()
    avg_loss = pd.Series(loss, index=data.index).ewm(alpha=1/period, adjust=False).mean()
    
    rs = avg_gain / avg_loss
    rsi = pd.Series(100 - (100 / (1 + rs)), index=data.index)
    
    rsi = rsi.where(avg_loss != 0, 100)
    rsi = rsi.where(avg_gain != 0, 0)
    
    return rsi

# 종목 데이터 처리 함수
def process_symbol(symbol, semaphore):
    with semaphore:
        try:
            time.sleep(0.2)
            klines = client.futures_klines(symbol=symbol, interval='15m', limit=1300)
            df = pd.DataFrame(klines, columns=["time", "open", "high", "low", "close", "volume", "_1", "_2", "_3", "_4", "_5", "_6"])
            df['close'] = pd.to_numeric(df['close'])
            df['time'] = pd.to_datetime(df['time'], unit='ms')

            if len(df) < 1200 + 14:
                logging.warning(f"Insufficient data for {symbol}: {len(df)} rows")
                return []

            df['rsi'] = get_rsi(df['close'])
            current_rsi = df['rsi'].iloc[-1]
            past_rsi = df['rsi'].iloc[-97:-1]  # 24시간 (96개 캔들)

            # RSI 조건: 과매수→과매도, 과매도→과매수
            rsi_overbought_to_oversold = (past_rsi.max() > 70) and (current_rsi <= 30)
            rsi_oversold_to_overbought = (past_rsi.min() < 30) and (current_rsi >= 70)

            cross_400 = check_sma_cross(df['close'], 200, 400)
            cross_1200 = check_sma_cross(df['close'], 200, 1200)

            # 최근 3일(288 캔들) 내 크로스 확인
            recent_cross_400 = cross_400.iloc[-288:]
            recent_cross_1200 = cross_1200.iloc[-288:]

            result = []
            if rsi_overbought_to_oversold and (-1 in recent_cross_400.values):
                result.append((symbol, "Dead Cross 200/400 (within 3 days)", current_rsi))
            if rsi_overbought_to_oversold and (-1 in recent_cross_1200.values):
                result.append((symbol, "Dead Cross 200/1200 (within 3 days)", current_rsi))
            if rsi_oversold_to_overbought and (1 in recent_cross_400.values):
                result.append((symbol, "Golden Cross 200/400 (within 3 days)", current_rsi))
            if rsi_oversold_to_overbought and (1 in recent_cross_1200.values):
                result.append((symbol, "Golden Cross 200/1200 (within 3 days)", current_rsi))

            return result

        except Exception as e:
            logging.error(f"Error processing {symbol}: {e}")
            return []

# 선물 심볼 500개 추출
try:
    futures_info = client.futures_exchange_info()
    valid_symbols = {symbol['symbol'] for symbol in futures_info['symbols']}
    tickers = client.futures_ticker()
    df_tickers = pd.DataFrame(tickers)
    df_tickers = df_tickers[df_tickers['symbol'].isin(valid_symbols)]
    df_tickers = df_tickers.sort_values('quoteVolume', ascending=False).head(500)
    symbols = df_tickers['symbol'].tolist()
except Exception as e:
    logging.error(f"Error fetching symbols: {e}")
    print(f"Error: {e}")
    exit(1)

# 병렬 처리 실행
results = []
semaphore = threading.Semaphore(5)
with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    future_to_symbol = {executor.submit(process_symbol, symbol, semaphore): symbol for symbol in symbols}
    for future in concurrent.futures.as_completed(future_to_symbol):
        symbol_results = future.result()
        if symbol_results:
            results.extend(symbol_results)

# 결과 정리 및 출력
results_df = pd.DataFrame(results, columns=["Symbol", "Signal", "Current RSI"])
print("\n=== Screener Results ===")
if not results_df.empty:
    results_df = results_df.sort_values("Current RSI", ascending=False)
    print(results_df)
    logging.info("Final Results:\n" + results_df.to_string(index=False))
else:
    print("No symbols meet the criteria.")
    logging.info("No symbols meet the criteria.")