import altair as alt
import pandas as pd
import streamlit as st
from pymongo import MongoClient

st.set_page_config(page_title="Customer Behavior Intelligence", layout="wide")


@st.cache_resource
def get_db():
    return MongoClient("mongodb://127.0.0.1:27018", serverSelectionTimeoutMS=3000)["shop"]


db = get_db()
st.title("Real-Time Customer Behavior Intelligence")

tab1, tab2, tab3, tab4 = st.tabs(["Product alerts", "Product timeline", "Sketch accuracy", "Control"])

# ---------- Tab 1: alerts (many views, few add-to-carts) ----------
with tab1:
    starts = [d["_id"] for d in db.product_windows.aggregate(
        [{"$group": {"_id": "$window_start"}}, {"$sort": {"_id": -1}}, {"$limit": 30}])]
    if not starts:
        st.warning("No data in MongoDB yet. Run the producer and the streaming job first.")
    else:
        c1, c2, c3 = st.columns(3)
        ws = c1.selectbox("Window start (5-min window)", starts)
        min_views = c2.slider("Minimum views", 1, 200, 30)
        max_rate = c3.slider("Maximum add-to-cart rate (%)", 0, 100, 5)

        docs = list(db.product_windows.find(
            {"window_start": ws},
            {"_id": 0, "product_id": 1, "views": 1, "carts": 1, "purchases": 1, "unique_users": 1}))
        df = pd.DataFrame(docs)
        df["cart_rate_pct"] = (df["carts"] / df["views"].where(df["views"] > 0) * 100).round(2)
        alerts = df[(df["views"] >= min_views) & (df["cart_rate_pct"] <= max_rate)] \
            .sort_values("views", ascending=False)

        m1, m2 = st.columns(2)
        m1.metric("Products in this window", len(df))
        m2.metric("Alerts fired", len(alerts))
        st.caption("unique_users is a HyperLogLog estimate (rsd = 2%).")
        st.dataframe(alerts, use_container_width=True)

# ---------- Tab 2: one product over time ----------
with tab2:
    pid = st.text_input("Product ID", "1004856")
    if pid.strip().isdigit():
        docs = list(db.product_windows.find({"product_id": int(pid)}, {"_id": 0})
                    .sort("window_start", 1))
        if docs:
            df = pd.DataFrame(docs).set_index("window_start")
            st.line_chart(df[["views", "carts", "purchases"]])
            st.dataframe(df, use_container_width=True)
        else:
            st.info("No windows found for this product.")
    else:
        st.info("Enter a numeric product ID.")

# ---------- Tab 3: sketch accuracy vs memory ----------
with tab3:
    ev = pd.DataFrame(list(db.sketch_eval.find({}, {"_id": 0})))
    if ev.empty:
        st.warning("Run spark/eval_sketches.scala first.")
    else:
        cms = ev[ev["structure"] == "CMS"].sort_values("width")
        hll = ev[ev["structure"] == "HLL"].sort_values("width")

        st.subheader("Count-Min Sketch: product view counts")
        opts = cms["width"].tolist()
        w = st.select_slider("Sketch width (w)", options=opts,
                             value=2719 if 2719 in opts else opts[0])
        r = cms[cms["width"] == w].iloc[0]
        a, b, c, d = st.columns(4)
        a.metric("epsilon", f"{r['eps']}")
        a.caption(f"w = {int(r['width'])}, d = {int(r['depth'])}")
        b.metric("Max absolute error", f"{r['max_abs_error']:,.0f}")
        c.metric("Bound (eps x N)", f"{r['bound']:,.0f}")
        d.metric("Memory", f"{r['memory_bytes'] / 1024:,.1f} KB")
        d.caption(f"exact map: {r['exact_memory_bytes'] / 1024:,.0f} KB")
        plot = cms.melt(id_vars="width", value_vars=["max_abs_error", "bound"],
                        var_name="series", value_name="value")
        chart = alt.Chart(plot).mark_line(point=True).encode(
            x=alt.X("width:Q", scale=alt.Scale(type="log"), title="Sketch width (w)"),
            y=alt.Y("value:Q", scale=alt.Scale(type="log"), title="Absolute error"),
            color="series:N")
        st.altair_chart(chart, width="stretch")

        st.subheader("HyperLogLog: distinct users")
        labels = hll["param"].tolist()
        sel = st.select_slider("Target relative std. error", options=labels,
                               value="rsd=0.020" if "rsd=0.020" in labels else labels[0])
        h = hll[hll["param"] == sel].iloc[0]
        e, f, g = st.columns(3)
        e.metric("Registers (m)", f"{int(h['width']):,}")
        f.metric("Observed error", f"{h['error_pct']:.2f}%")
        g.metric("Memory", f"{h['memory_bytes'] / 1024:,.2f} KB")
        g.caption(f"exact (lower bound): {h['exact_memory_bytes'] / 1024:,.0f} KB")


# ---------- Tab 4: control the replayer ----------
with tab4:
    ctl = db.control.find_one({"_id": "replayer"}) or {"rate": 200, "paused": False}
    st.caption(f"Replayer state: rate = {ctl.get('rate', 200)} events/s, "
               f"paused = {ctl.get('paused', False)}")
    rate = st.slider("Replay rate (events/s)", 10, 3000, int(ctl.get("rate", 200)), step=10)
    b1, b2, b3 = st.columns(3)
    if b1.button("Apply rate"):
        db.control.update_one({"_id": "replayer"}, {"$set": {"rate": rate}}, upsert=True)
        st.success(f"Rate set to {rate} events/s")
    if b2.button("Pause"):
        db.control.update_one({"_id": "replayer"}, {"$set": {"paused": True}}, upsert=True)
        st.info("Paused")
    if b3.button("Resume"):
        db.control.update_one({"_id": "replayer"}, {"$set": {"paused": False}}, upsert=True)
        st.info("Resumed")

    st.subheader("Inject a campaign")
    st.write("Many views but few add-to-carts on one product, to trigger an alert.")
    i1, i2, i3 = st.columns(3)
    inj_pid = i1.text_input("Product ID", "1004856", key="inj_pid")
    inj_views = i2.slider("Views", 50, 2000, 300)
    inj_carts = i3.slider("Add-to-carts", 0, 50, 2)
    if st.button("Inject campaign") and inj_pid.strip().isdigit():
        db.control.update_one({"_id": "replayer"}, {"$set": {"inject": {
            "product_id": int(inj_pid), "views": inj_views, "carts": inj_carts}}}, upsert=True)
        st.success("Campaign queued. Check the Product alerts tab in a few seconds.")
