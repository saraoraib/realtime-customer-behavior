import time
import pandas as pd
import streamlit as st
from pymongo import MongoClient

st.set_page_config(page_title="Live CMS", layout="wide")
st.title("Live Count-Min Sketch: top-10 products by views")
st.caption("Approximate counts (CMS) vs exact counts, computed on the same Kafka stream.")

db = MongoClient("mongodb://127.0.0.1:27018")["shop"]
auto = st.sidebar.checkbox("Auto-refresh (3 s)", value=True)

cur = db.cms_latest.find_one({"_id": "latest"}) or db.cms_latest.find_one({})
if not cur:
    st.warning("No data in cms_latest yet. Start stream_cms.scala and the replayer.")
else:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total views", f"{cur.get('total_views', 0):,}")
    c2.metric("Recall@10", cur.get("recall_at_10"))
    c3.metric("Max error (top-10)", cur.get("max_abs_error_top10"),
              help=f"Theoretical bound = eps x N = {cur.get('bound', 0):.1f}")
    ratio = cur.get("exact_memory_bytes", 0) / max(cur.get("cms_memory_bytes", 1), 1)
    c4.metric("Memory saving", f"{ratio:.0f}x",
              help=f"CMS {cur.get('cms_memory_bytes',0)/1024:.1f} KB vs exact {cur.get('exact_memory_bytes',0)/1024:.1f} KB")
    st.write(f"Sketch size: width={cur.get('width')} x depth={cur.get('depth')}, "
             f"distinct products seen: {cur.get('distinct_products'):,}")

    top = pd.DataFrame(cur.get("top", []))
    if not top.empty:
        top["product_id"] = top["product_id"].astype(str)
        top["error"] = top["cms_estimate"] - top["exact_count"]
        left, right = st.columns([1, 1])
        with left:
            st.subheader("Top-10: CMS vs exact")
            st.dataframe(top, hide_index=True, width="stretch")
        with right:
            st.subheader("Counts")
            st.bar_chart(top.set_index("product_id")[["cms_estimate", "exact_count"]])

    hist = pd.DataFrame(list(db.cms_history.find({}, {"_id": 0}).sort("_id", -1).limit(300)))
    if not hist.empty:
        hist = hist.iloc[::-1].reset_index(drop=True)
        st.subheader("History (last 300 batches)")
        a, b = st.columns(2)
        err_cols = [c for c in ["max_abs_error_top10", "bound"] if c in hist]
        mem_cols = [c for c in ["cms_memory_bytes", "exact_memory_bytes"] if c in hist]
        with a:
            st.caption("Error vs theoretical bound")
            if err_cols:
                st.line_chart(hist[err_cols])
        with b:
            st.caption("Memory (bytes): CMS vs exact")
            if mem_cols:
                st.line_chart(hist[mem_cols])

if auto:
    time.sleep(3)
    st.rerun()
