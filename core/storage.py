# core/storage.py
import datetime as dt
from pymongo import MongoClient, ASCENDING, UpdateOne
from core import config

client = MongoClient(config.MONGO_URI)
db = client[config.DB_NAME]

# --- COLLECTION REFERENCES ---
recurring_col = db["recurring_rockets"]
hits_col      = db["engine_short_hits"]
metrics_col   = db["engine_metrics"]

# TTL index for hits (auto delete after 30 days)
try:
    hits_col.create_index("ts", expireAfterSeconds=60*60*24*30)
except Exception:
    pass


def update_recurring(symbol: str):
    """
    Update or insert a recurring rocket entry.
    If symbol exists -> increment streak_count and update last_seen.
    If new -> create entry with streak_count = 1.
    """
    now = dt.datetime.utcnow()
    recurring_col.update_one(
        {"symbol": symbol},
        {"$set": {"last_seen": now}, "$inc": {"streak_count": 1}},
        upsert=True
    )


def log_hit(symbol: str, gain_pct: float):
    """
    Log a completed short-term trade result.
    gain_pct: % profit or loss
    """
    doc = {
        "symbol": symbol,
        "ts": dt.datetime.utcnow(),
        "gain_pct": gain_pct
    }
    hits_col.insert_one(doc)


def update_metrics(symbol: str):
    """
    Aggregate hit-rate metrics (7d & 30d) from engine_short_hits.
    Keep only small data footprint.
    """
    now = dt.datetime.utcnow()
    t7  = now - dt.timedelta(days=7)
    t30 = now - dt.timedelta(days=30)

    # Collect recent gains
    last7  = list(hits_col.find({"symbol": symbol, "ts": {"$gte": t7}}))
    last30 = list(hits_col.find({"symbol": symbol, "ts": {"$gte": t30}}))
    total_signals = len(last30)
    if total_signals == 0:
        hit7 = hit30 = 0
    else:
        hit7 = sum(1 for x in last7 if x.get("gain_pct", 0) > 0)
        hit30 = sum(1 for x in last30 if x.get("gain_pct", 0) > 0)

    hr7  = (hit7 / max(1, len(last7))) * 100 if last7 else 0
    hr30 = (hit30 / max(1, len(last30))) * 100 if last30 else 0

    metrics_col.update_one(
        {"symbol": symbol},
        {"$set": {
            "hit_rate_7d": round(hr7, 2),
            "hit_rate_30d": round(hr30, 2),
            "total_signals": total_signals,
            "last_updated": now
        }},
        upsert=True
    )


def load_recurring(limit: int = 20):
    """Return top recurring stocks by streak."""
    return list(recurring_col.find({}, {"_id": 0})
                .sort("streak_count", -1)
                .limit(limit))
