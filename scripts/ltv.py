import os, sys
from datetime import timedelta
import pandas as pd
from sqlalchemy import text
from common.db import get_engine

# Permite ejecución como módulo (-m) o directa
if __name__ == "__main__" and os.path.basename(os.getcwd()) != "project-1-ltv-attribution":
    ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))
    if ROOT not in sys.path:
        sys.path.append(ROOT)

HORIZONS = [30, 60, 90, 180]

def load_customers_orders(eng):
    q_cust = "SELECT customer_id, signup_date FROM dim_customer;"
    q_ord  = "SELECT order_id, customer_id, order_ts, amount FROM fact_orders;"

    cust = pd.read_sql(q_cust, eng, parse_dates=["signup_date"])
    orders = pd.read_sql(q_ord, eng, parse_dates=["order_ts"])
    return cust, orders

def compute_ltv(cust, orders):
    if orders.empty or cust.empty:
        return pd.DataFrame(columns=["cohort_month","customer_id","horizon_days","revenue"])

    # Cohorte = primer día del mes de signup
    cust = cust.copy()
    cust["cohort_month"] = cust["signup_date"].dt.to_period("M").dt.to_timestamp()

    # Join para tener signup de cada order
    df = orders.merge(cust[["customer_id","signup_date","cohort_month"]], on="customer_id", how="left")
    # days_since_signup
    df["days_since_signup"] = (df["order_ts"] - df["signup_date"]).dt.total_seconds() / 86400.0

    rows = []
    for h in HORIZONS:
        # revenue dentro del horizonte h
        mask = (df["days_since_signup"] >= 0) & (df["days_since_signup"] <= h)
        agg = (df[mask]
               .groupby(["cohort_month","customer_id"], as_index=False)["amount"]
               .sum()
               .rename(columns={"amount":"revenue"}))
        agg["horizon_days"] = h
        rows.append(agg)

    out = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    out = out[["cohort_month","customer_id","horizon_days","revenue"]].copy()
    # Completar clientes sin compras con revenue 0 en cada horizonte
    # (opcional) – aquí nos quedamos con los que sí tuvieron compras.
    return out

def write_fact_ltv(eng, df):
    if df.empty:
        print("No hay filas para escribir (fact_ltv_cohort).")
        return
    with eng.begin() as conn:
        conn.execute(text("DELETE FROM fact_ltv_cohort"))
        rows = df.to_dict("records")
        conn.execute(text("""
            INSERT INTO fact_ltv_cohort (cohort_month, customer_id, horizon_days, revenue)
            VALUES (:cohort_month, :customer_id, :horizon_days, :revenue)
        """), rows)

def main():
    eng = get_engine()
    cust, orders = load_customers_orders(eng)
    print(f"Clientes: {len(cust)}, Órdenes: {len(orders)}")
    df_ltv = compute_ltv(cust, orders)
    print(f"Filas LTV: {len(df_ltv)}")
    write_fact_ltv(eng, df_ltv)
    print("LTV calculado y escrito en fact_ltv_cohort ✔")

if __name__ == "__main__":
    main()
