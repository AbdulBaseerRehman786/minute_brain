# worker_short.py
import time, datetime as dt
from core import config, data, indicators, scoring, storage

def run_once():
    # Load tickers
    with open(config.TICKERS_FILE) as f:
        tickers = [t.strip() for t in f if t.strip()]

    ref = {"rsi": (30,70), "macd_hist": (-0.5,0.5),
           "stoch_k": (20,80), "atrp": (0.2,3.0), "bbp": (0.0,1.0)}

    now = dt.datetime.utcnow()
    results = []

    for sym in tickers:
        try:
            df = data.fetch_minute_data(sym)
            if df is None or df.empty:
                continue

            feats = indicators.compute_indicators(df)
            score = scoring.rocket_score(feats, ref)
            results.append((sym, score))
        except Exception:
            continue

    # Sort and pick Top-15
    results.sort(key=lambda x: x[1], reverse=True)
    top = results[:config.TOP_N_SHORT]

    # Update recurring list in Mongo
    for sym, score in top:
        storage.update_recurring(sym)

    print(f"[{now.strftime('%H:%M:%S')}] Updated recurring rockets: {[s for s,_ in top]}")


def sleep_to_next_minute():
    t = time.time()
    time.sleep(60 - (t % 60) + 0.05)


if __name__ == "__main__":
    print("🚀 Short-Term Rocket Catcher Engine Started (Lightweight Mode)")
    while True:
        run_once()
        sleep_to_next_minute()
