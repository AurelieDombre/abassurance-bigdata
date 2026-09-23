import streamlit as st
import pandas as pd

st.set_page_config(
    page_title="Big Data Dashboard",
    layout="wide"
)

st.title("Big Data Dashboard")

@st.cache_data
def load_data():
    return pd.read_csv(
    "/data/output/dataClean_fusion/clients",
    sep=";"
)

data = load_data()

st.subheader("Clients")

st.write(f"Nombre de clients : **{len(data)}**")

st.dataframe(
    data,
    use_container_width=True
)