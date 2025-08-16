import os, sys, math
from datetime import timedelta
import pandas as pd
from sqlalchemy import text, select
from common.db import get_engine

# Permite ejecución como módulo (-m) o directa
if __name__ == "__main__" and os.path.basename(os.getcwd()) != "project-1-ltv-attribution":
    ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))
    if ROOT not in sys.path:
        sys.path.append(ROOT)

WINDOW_DAYS = 30
HALF_LIFE_DAYS = 7  # para time-decay

def load_data(eng):
    # Cargamos orders y touches con los IDs ya en DB
    q_orders = """
        SELECT o.order_id, o.order_ts, o.customer_id, o.amount
        FROM fact_orders o
    """
    q_touches = """
        SELECT t.touch_id, t.event_ts, t.customer_id, t.channel_id
        FROM fact_touches t
    """
    orders = pd.read_sql(q_orders, eng, parse_dates=["order_ts"])
    touches = pd.read_sql(q_touches, eng, parse_dates=["event_ts"])
    return orders, touches

def window_touches_for_order(touches_cust, order_ts):
    start = order_ts - timedelta(days=WINDOW_DAYS)
    return touches_cust[(touches_cust["event_ts"] <= order_ts) & (touches_cust["event_ts"] >= start)].copy()

def model_last_click(tws, amount):
    if tws.empty: 
        return []
    row = tws.sort_values("event_ts").iloc[-1]
    return [(row.channel_id, 1.0, amount)]

def model_first_click(tws, amount):
    if tws.empty: 
        return []
    row = tws.sort_values("event_ts").iloc[0]
    return [(row.channel_id, 1.0, amount)]

def model_linear(tws, amount):
    if tws.empty: 
        return []
    tws = tws.sort_values("event_ts")
    w = 1.0 / len(tws)
    return [(int(r.channel_id), w, round(amount * w, 2)) for _, r in tws.iterrows()]

def model_time_decay(tws, order_ts, amount):
    if tws.empty: 
        return []
    # peso = 0.5 ** (delta_days / HALF_LIFE_DAYS)
    tws = tws.copy()
    tws["delta_days"] = (order_ts - tws["event_ts"]).dt.total_seconds() / 86400.0
    tws["raw_w"] = 0.5 ** (tws["delta_days"] / HALF_LIFE_DAYS)
    s = tws["raw_w"].sum()
    tws["w"] = tws["raw_w"] / s if s > 0 else 0
    return [(int(r.channel_id), float(r.w), round(amount * float(r.w), 2)) for _, r in tws.iterrows()]

def compute_attribution(orders, touches):
    # Pre-split touches by customer for rapidez
    touches_by_c = {cid: df for cid, df in touches.groupby("customer_id")}
    rows = []

    for _, o in orders.iterrows():
        cid, ots, amt, oid = int(o.customer_id), o.order_ts, float(o.amount), int(o.order_id)
        tws = touches_by_c.get(cid, pd.DataFrame(columns=["event_ts","channel_id"]))
        tws = window_touches_for_order(tws, ots)

        # last_click
        for ch, w, ar in model_last_click(tws, amt):
            rows.append((oid, "last_click", ch, w, round(amt*w, 2)))
        # first_click
        for ch, w, ar in model_first_click(tws, amt):
            rows.append((oid, "first_click", ch, w, round(amt*w, 2)))
        # linear
        for ch, w, ar in model_linear(tws, amt):
            rows.append((oid, "linear", ch, w, ar))
        # time_decay
        for ch, w, ar in model_time_decay(tws, ots, amt):
            rows.append((oid, "time_decay", ch, w, ar))

    df = pd.DataFrame(rows, columns=["order_id","model","channel_id","weight","attributed_revenue"])
    return df

def write_fact_attribution(eng, df):
    if df.empty:
        print("No hay filas para escribir (fact_attribution).")
        return
    with eng.begin() as conn:
        conn.execute(text("DELETE FROM fact_attribution"))
        # insert masivo por chunks
        rows = df.to_dict("records")
        conn.execute(text("""
            INSERT INTO fact_attribution (order_id, model, channel_id, weight, attributed_revenue)
            VALUES (:order_id, :model, :channel_id, :weight, :attributed_revenue)
        """), rows)

def main():
    eng = get_engine()
    orders, touches = load_data(eng)
    print(f"Pedidos: {len(orders)}, Touches: {len(touches)}")
    df_attr = compute_attribution(orders, touches)
    print(f"Filas de atribución: {len(df_attr)}")
    write_fact_attribution(eng, df_attr)
    print("Atribución calculada y escrita en fact_attribution ✔")

if __name__ == "__main__":
    main()
