import asyncio
import json
import os
from datetime import datetime

import aiohttp
import clickhouse_connect

# إعداد الاتصال بقاعدة البيانات
print("🔌 محاولة الاتصال بـ ClickHouse...")
try:
    client = clickhouse_connect.get_client(
        host="b2ldg6nk61.europe-west4.gcp.clickhouse.cloud",
        username="default",
        password="8SLA3MyJ_12r0",
        secure=True,
        verify=False,
        database="default"
    )
    client.command("SELECT 1")
    print("✅ تم الاتصال بقاعدة البيانات.")
except Exception as e:
    print(f"❌ فشل الاتصال: {e}")
    exit()

# ========== إعداد الجداول ==========
def create_tables(symbol):
    s = symbol.lower()
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {s}_trade (
            symbol String,
            timestamp DateTime,
            price Float64,
            qty Float64,
            side UInt8
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMMDD(timestamp)
        ORDER BY (symbol, timestamp)
    """)
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {s}_kline_1m (
            symbol String,
            start_time DateTime,
            close_time DateTime,
            interval String,
            open Float64,
            high Float64,
            low Float64,
            close Float64,
            volume Float64,
            trades UInt64,
            is_closed UInt8
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMMDD(start_time)
        ORDER BY (symbol, interval, start_time)
    """)
    print("✅ تم تجهيز الجداول.")

# ========== إدخال بيانات ==========
def insert_trade(symbol, data):
    row = [
        symbol,
        datetime.utcfromtimestamp(data["T"] / 1000.0),
        float(data["p"]),
        float(data["q"]),
        int(data["m"])
    ]
    print("📩 TRADE:", row)
    try:
        client.insert(f"{symbol.lower()}_trade", [row],
                      column_names=['symbol', 'timestamp', 'price', 'qty', 'side'])
    except Exception as e:
        print("❌ Trade insert error:", e)

def insert_kline(symbol, data):
    k = data["k"]
    row = [
        symbol,
        datetime.utcfromtimestamp(k["t"] / 1000.0),
        datetime.utcfromtimestamp(k["T"] / 1000.0),
        k["i"],
        float(k["o"]),
        float(k["h"]),
        float(k["l"]),
        float(k["c"]),
        float(k["v"]),
        int(k["n"]),
        int(k["x"])
    ]
    print("🕯️ KLINE:", row)
    try:
        client.insert(f"{symbol.lower()}_kline_1m", [row],
                      column_names=[
                          'symbol', 'start_time', 'close_time', 'interval', 'open',
                          'high', 'low', 'close', 'volume', 'trades', 'is_closed'
                      ])
    except Exception as e:
        print("❌ Kline insert error:", e)

# ========== WebSocket Async ==========
async def stream(symbol):
    streams = f"{symbol.lower()}@trade/{symbol.lower()}@kline_1m"
    url = f"wss://stream.binance.com:9443/stream?streams={streams}"

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(url, heartbeat=20) as ws:
                    print(f"✅ WebSocket مفتوح لـ {symbol} ...")
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            payload = json.loads(msg.data)
                            stream_type = payload["stream"]
                            if "trade" in stream_type:
                                insert_trade(symbol, payload["data"])
                            elif "kline" in stream_type:
                                insert_kline(symbol, payload["data"])
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            print("❌ WebSocket error, will retry...")
                            break
        except Exception as e:
            print("💥 خطأ في الاتصال:", e)
            await asyncio.sleep(5)  # إعادة المحاولة بعد ثوانٍ

# ========== Main ==========
async def main():
    symbol = "BTCFDUSD"  # أو get_target_symbol()
    create_tables(symbol)
    await stream(symbol)

if __name__ == "__main__":
    asyncio.run(main())
