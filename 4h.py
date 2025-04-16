from binance.client import Client
import pandas as pd
import numpy as np
import time

# API 키 설정
api_key = "shaRfojL5PH7hD4os8ctX7MJf47uVBvkAf3SZ0koa92zNq72bFlZdRd4HUZBhmCt"
api_secret = "tiv40Xp1YXsHdtHVQhgRkjwTAVYa72RP1UwXgNc4r3tHmLI0qfjRXSxh02D503Xu"

client = Client(api_key, api_secret)

# RSI 계산 함수 (Wilder의 RMA 기반)
def get_rsi(data, period=14):
    delta = data.diff()
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    
    avg_gain = pd.Series(gain, index=data.index).ewm(alpha=1/period, adjust=False).mean()
    avg_loss = pd.Series(loss, index=data.index).ewm(alpha=1/period, adjust=False).mean()
    
    rs = avg_gain / avg_loss
    rsi = pd.Series(100 - (100 / (1 + rs)), index=data.index)
    
    # 예외 처리: avg_loss == 0 또는 avg_gain == 0
    rsi = rsi.where(avg_loss != 0, 100)
    rsi = rsi.where(avg_gain != 0, 0)
    
    return rsi

# 선물 시장 심볼 리스트 가져오기
futures_info = client.futures_exchange_info()
valid_symbols = {symbol['symbol'] for symbol in futures_info['symbols']}

# 시가총액 기준 상위 200개 코인 가져오기
tickers = client.futures_ticker()
df_tickers = pd.DataFrame(tickers)
df_tickers = df_tickers[df_tickers['symbol'].isin(valid_symbols)]
# 대략적인 시가총액 추정 (volume * lastPrice)
df_tickers['approx_market_cap'] = df_tickers['volume'].astype(float) * df_tickers['lastPrice'].astype(float)
df_tickers = df_tickers.sort_values('approx_market_cap', ascending=False).head(200)

# RSI 필터링
rsi_result = []
failed_symbols = []

for symbol in df_tickers['symbol']:
    try:
        klines = client.futures_klines(symbol=symbol, interval='4h', limit=100)
        df = pd.DataFrame(klines, columns=["time", "open", "high", "low", "close", "volume", "_1", "_2", "_3", "_4", "_5", "_6"])
        df["close"] = pd.to_numeric(df["close"])
        
        rsi = get_rsi(df["close"]).iloc[-1]

        if not np.isnan(rsi) and (rsi > 75 or rsi < 25):
            rsi_result.append({"symbol": symbol, "rsi": rsi, "lastPrice": df_tickers[df_tickers['symbol'] == symbol]['lastPrice'].iloc[0]})
            print(f"{symbol}: RSI {rsi:.2f}")

        # API 호출 제한 방지를 위해 잠시 대기
        time.sleep(0.1)

    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        failed_symbols.append(symbol)
        time.sleep(1)  # 에러 발생 시 1초 대기 후 재시도

# 최종 데이터프레임 출력
rsi_df = pd.DataFrame(rsi_result)
if not rsi_df.empty:
    rsi_df = rsi_df.sort_values('rsi', ascending=False)
    print("\nFiltered Symbols (RSI > 75 or RSI < 25):")
    print(rsi_df)
else:
    print("\nNo symbols meet the RSI criteria (RSI > 75 or RSI < 25).")

if failed_symbols:
    print(f"\nFailed to fetch data for {len(failed_symbols)} symbols: {failed_symbols}")