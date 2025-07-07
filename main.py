import threading
import time
import json
from datetime import datetime
import websocket
import clickhouse_connect

print("🔌 محاولة الاتصال بقاعدة البيانات ClickHouse...")
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
    print("✅ تم الاتصال بقاعدة البيانات بنجاح.")
except Exception as e:
    print("❌ فشل الاتصال بقاعدة البيانات:")
    print(f"  - Exception: {repr(e)}")
    exit()


# === دالة جديدة لإنشاء الجداول ===
def create_tables_if_not_exist(symbol):
    """
    تتحقق من وجود جداول البيانات للعملة المحددة وتقوم بإنشائها إذا لم تكن موجودة.
    """
    symbol_lower = symbol.lower()

    # تعريف بنية جدول التداولات (Trades)
    trade_table_name = f"{symbol_lower}_trade"
    trade_table_query = f"""
    CREATE TABLE IF NOT EXISTS default.{trade_table_name} (
        symbol String,
        timestamp DateTime,
        price Float64,
        qty Float64,
        side UInt8
    ) ENGINE = MergeTree()
    PARTITION BY toYYYYMMDD(timestamp)
    ORDER BY (symbol, timestamp)
    """

    # تعريف بنية جدول الشموع (Klines)
    kline_table_name = f"{symbol_lower}_kline_1m"
    kline_table_query = f"""
    CREATE TABLE IF NOT EXISTS default.{kline_table_name} (
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
    """

    try:
        print(f"Checking/Creating table: {trade_table_name}")
        client.command(trade_table_query)
        print(f"Table '{trade_table_name}' is ready.")

        print(f"Checking/Creating table: {kline_table_name}")
        client.command(kline_table_query)
        print(f"Table '{kline_table_name}' is ready.")
    except Exception as e:
        print(f"[FATAL ERROR] Could not create tables for {symbol}: {e}")
        # إذا فشل إنشاء الجداول، فلا فائدة من المتابعة
        exit()

# 2. تحديد العملة المناسبة
def get_target_symbol():
    print("🔍 جاري البحث عن عملة ذات تصنيف عالي (hot) متاحة...")
    hot_query = """
        SELECT symbol
        FROM symbols
        WHERE guest_status = 0 AND hot_rank > 0 AND hot_rank <= 100
        ORDER BY hot_rank ASC
        LIMIT 1
    """
    result = client.query(hot_query).result_rows
    if result:
        symbol = result[0][0]
        print(f"✅ تم العثور على عملة ذات تصنيف عالي: {symbol}")
        return symbol

    print("⚠️ لم يتم العثور على عملة ذات تصنيف عالي متاحة. جاري البحث عن أي عملة بديلة...")
    fallback_query = """
        SELECT symbol
        FROM symbols
        WHERE guest_status = 0
        LIMIT 1
    """
    fallback_result = client.query(fallback_query).result_rows
    if fallback_result:
        symbol = fallback_result[0][0]
        print(f"✅ تم العثور على عملة بديلة: {symbol}")
        return symbol

    return None

# 3. تحديث حالة العملة المختارة
def mark_symbol_as_in_use(symbol):
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    client.command(f"""
        ALTER TABLE symbols UPDATE guest_status = 1, guest_last_update = toDateTime('{now}')
        WHERE symbol = '{symbol}'
    """)
    def update_loop():
        while True:
            time.sleep(60)
            now_update = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            try:
                client.command(f"""
                    ALTER TABLE symbols UPDATE guest_last_update = toDateTime('{now_update}')
                    WHERE symbol = '{symbol}'
                """)
            except Exception as e:
                print(f"[ERROR] Heartbeat update failed for {symbol}: {e}")
    threading.Thread(target=update_loop, daemon=True).start()

# 4. دوال إدخال البيانات (لا تغيير هنا)
def insert_trade_data(symbol, payload):
    table_name = f"{symbol.lower()}_trade"
    # نحدد أسماء الأعمدة بالترتيب تماماً كما في الجدول
    column_names = ['symbol', 'timestamp', 'price', 'qty', 'side']

    try:
        # نجهز البيانات كقائمة (أو tuple) بنفس الترتيب
        data_row = [
            symbol,
            datetime.utcfromtimestamp(payload["T"] / 1000.0),
            float(payload["p"]),
            float(payload["q"]),
            int(payload["m"])  # True -> 1, False -> 0
        ]

        # نقوم بالإدخال مع تحديد الأعمدة صراحةً
        # نضع الصف داخل قائمة أخرى لأن الدالة تتوقع "قائمة من الصفوف"
        client.insert(table_name, [data_row], column_names=column_names)

    except Exception as e:
        print(f"[ERROR in insert_trade_data]")
        print(f"  - Exception Type: {type(e)}")
        print(f"  - Exception repr: {repr(e)}") # repr() يعطي تفاصيل أفضل من str()
        print(f"  - Payload that caused error: {payload}")


def insert_kline_data(symbol, payload):
    table_name = f"{symbol.lower()}_kline_1m"
    k = payload["k"]
    # نحدد أسماء الأعمدة بالترتيب تماماً كما في الجدول
    column_names = [
        'symbol', 'start_time', 'close_time', 'interval', 'open', 'high',
        'low', 'close', 'volume', 'trades', 'is_closed'
    ]

    try:
        # نجهز البيانات كقائمة (أو tuple) بنفس الترتيب
        data_row = [
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
            int(k["x"]) # True -> 1, False -> 0
        ]

        # نقوم بالإدخال مع تحديد الأعمدة صراحةً
        client.insert(table_name, [data_row], column_names=column_names)

    except Exception as e:
        print(f"[ERROR in insert_kline_data]")
        print(f"  - Exception Type: {type(e)}")
        print(f"  - Exception repr: {repr(e)}") # repr() يعطي تفاصيل أفضل من str()
        print(f"  - Payload that caused error: {payload}")
    except Exception as e:
        print(f"[ERROR] insert_kline_data: {e}")

# 5. جدول توجيه الدوال (لا تغيير هنا)
stream_handlers = {
    "trade": insert_trade_data,
    "kline": insert_kline_data,
}

# 6. دالة WebSocket (لا تغيير هنا)
def stream_handler(symbol, streams):
    stream_path = '/'.join([f"{symbol.lower()}@{s}" for s in streams])
    ws_url = f"wss://stream.binance.com:9443/stream?streams={stream_path}"
    def on_message(ws, message):
        try:
            data = json.loads(message)
            stream_type = data['stream'].split('@')[1]
            stream_key = stream_type.split("_")[0]
            handler = stream_handlers.get(stream_key)
            if handler:
                handler(symbol, data['data'])
        except Exception as e:
            print(f"[ERROR] on_message: {e}")
    def on_error(ws, error):
        print("WebSocket error:", error)
    def on_close(ws, close_status_code, close_msg):
        print("WebSocket closed")
    def on_open(ws):
        print(f"WebSocket opened for: {symbol} streams: {streams}")
    ws = websocket.WebSocketApp(ws_url,
                                 on_message=on_message,
                                 on_error=on_error,
                                 on_close=on_close,
                                 on_open=on_open)
    ws.run_forever()

# 7. Main (تم التعديل هنا)
if __name__ == "__main__":
    symbol = get_target_symbol()
    if not symbol:
        print("❌ مفيش عملة متاحة حالياً.")
        exit()

    # --- الخطوة الجديدة: التأكد من وجود الجداول وإنشائها إذا لزم الأمر ---
    create_tables_if_not_exist(symbol)
    # --------------------------------------------------------------------

    print(f"✅ تم اختيار العملة النهائية: {symbol} وجداولها جاهزة.")
    mark_symbol_as_in_use(symbol)

    streams = [
        "trade",
        "kline_1m"
    ]

    stream_handler(symbol, streams)
