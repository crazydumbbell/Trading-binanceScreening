import logging
from binance.client import Client
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time

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

# 1️⃣ RSI 계산 함수
def get_rsi(data, period=14):
    try:
        delta = data.diff()
        gain = np.where(delta > 0, delta, 0)
        loss = np.where(delta < 0, -delta, 0)
        avg_gain = pd.Series(gain).ewm(com=period-1, adjust=False).mean()
        avg_loss = pd.Series(loss).ewm(com=period-1, adjust=False).mean()
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    except Exception as e:
        logging.error(f"Error in RSI calculation: {e}")
        return None

# 2️⃣ EMA 계산 및 크로스 체크 함수
def check_ema_cross(df, short_ema=200, long_ema=400):
    try:
        df['ema_short'] = df['close'].ewm(span=short_ema, adjust=False).mean()
        df['ema_long'] = df['close'].ewm(span=long_ema, adjust=False).mean()
        df['ema_diff'] = df['ema_short'] - df['ema_long']
        df['cross'] = df['ema_diff'].diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
        return df
    except Exception as e:
        logging.error(f"Error in EMA cross calculation: {e}")
        return None

# 3️⃣ 선물 시장 심볼 리스트 가져오기
try:
    futures_info = client.futures_exchange_info()
    valid_symbols = {symbol['symbol'] for symbol in futures_info['symbols']}
    logging.info(f"Retrieved {len(valid_symbols)} valid futures symbols")
except Exception as e:
    logging.error(f"Error fetching futures exchange info: {e}")
    print(f"Error: Failed to fetch futures symbols. {e}")
    exit(1)

# 4️⃣ 시가총액 기준 상위 500개 코인 가져오기
try:
    tickers = client.futures_ticker()
    df_tickers = pd.DataFrame(tickers)
    df_tickers = df_tickers[df_tickers['symbol'].isin(valid_symbols)]
    df_tickers = df_tickers.sort_values('quoteVolume', ascending=False).head(500)
    logging.info(f"Selected top {len(df_tickers)} symbols by quote volume")
except Exception as e:
    logging.error(f"Error fetching tickers: {e}")
    print(f"Error: Failed to fetch tickers. {e}")
    exit(1)

# 5️⃣ 스크리닝
golden_cross_results = []
dead_cross_results = []
lookback_period = 96  # 24시간 (15분봉 기준 96 캔들)

for symbol in df_tickers['symbol']:
    try:
        # API 요청 간 딜레이 추가
        time.sleep(0.1)
        klines = client.futures_klines(symbol=symbol, interval='15m', limit=lookback_period + 1200)
        df = pd.DataFrame(klines, columns=["time", "open", "high", "low", "close", "volume", "_1", "_2", "_3", "_4", "_5", "_6"])
        df['close'] = pd.to_numeric(df['close'])
        df['time'] = pd.to_datetime(df['time'], unit='ms')

        # RSI 계산
        df['rsi'] = get_rsi(df['close'], period=14)
        if df['rsi'] is None:
            continue

        # 현재 RSI
        current_rsi = df['rsi'].iloc[-1]

        # 과거 24시간 RSI
        past_rsi = df['rsi'].iloc[-lookback_period:-1]

        # EMA 계산 및 크로스 체크
        df_400 = check_ema_cross(df, short_ema=200, long_ema=400)
        df_1200 = check_ema_cross(df, short_ema=200, long_ema=1200)
        if df_400 is None or df_1200 is None:
            continue

        # 골드 크로스 확인
        golden_cross_400 = (df_400['cross'].iloc[-1] == 1) or (df_400['cross'].iloc[-2] == 1)
        golden_cross_1200 = (df_1200['cross'].iloc[-1] == 1) or (df_1200['cross'].iloc[-2] == 1)
        is_golden_cross = golden_cross_400 or golden_cross_1200

        # 데드 크로스 확인
        dead_cross_400 = (df_400['cross'].iloc[-1] == -1) or (df_400['cross'].iloc[-2] == -1)
        dead_cross_1200 = (df_1200['cross'].iloc[-1] == -1) or (df_1200['cross'].iloc[-2] == -1)
        is_dead_cross = dead_cross_400 or dead_cross_1200

        # 골드 크로스 조건
        if (past_rsi.max() > 75) and (current_rsi <= 25) and is_golden_cross:
            golden_cross_results.append({
                'symbol': symbol,
                'current_rsi': current_rsi,
                'past_rsi_max': past_rsi.max(),
                'ema_cross': '400' if golden_cross_400 else '1200'
            })
            logging.info(f"[Golden Cross] {symbol}: RSI {current_rsi:.2f}, Past RSI Max {past_rsi.max():.2f}, Cross EMA {golden_cross_400 and '400' or '1200'}")
            print(f"[Golden Cross] {symbol}: RSI {current_rsi:.2f}, Past RSI Max {past_rsi.max():.2f}, Cross EMA {golden_cross_400 and '400' or '1200'}")

        # 데드 크로스 조건
        if (past_rsi.min() < 25) and (current_rsi >= 75) and is_dead_cross:
            dead_cross_results.append({
                'symbol': symbol,
                'current_rsi': current_rsi,
                'past_rsi_min': past_rsi.min(),
                'ema_cross': '400' if dead_cross_400 else '1200'
            })
            logging.info(f"[Dead Cross] {symbol}: RSI {current_rsi:.2f}, Past RSI Min {past_rsi.min():.2f}, Cross EMA {dead_cross_400 and '400' or '1200'}")
            print(f"[Dead Cross] {symbol}: RSI {current_rsi:.2f}, Past RSI Min {past_rsi.min():.2f}, Cross EMA {dead_cross_400 and '400' or '1200'}")

    except Exception as e:
        logging.error(f"Error processing {symbol}: {e}")
        print(f"Error processing {symbol}: {e}")

# 6️⃣ 결과 출력
golden_df = pd.DataFrame(golden_cross_results)
dead_df = pd.DataFrame(dead_cross_results)

print("\n=== Golden Cross Results ===")
print(golden_df)
logging.info("Golden Cross Results:\n" + golden_df.to_string())

print("\n=== Dead Cross Results ===")
print(dead_df)
logging.info("Dead Cross Results:\n" + dead_df.to_string())