
import streamlit as st
import dask.dataframe as dd
import pandas as pd
import matplotlib.pyplot as plt

st.set_page_config(
    page_title="Big Data CSV Automation Tool",
    layout="wide"
)

st.title("📊 Big Data CSV Automation Tool")
st.write("Upload large CSV files, clean data, generate insights, and download results.")

uploaded_file = st.file_uploader("Upload CSV file", type=["csv"])

if uploaded_file is not None:
    with st.spinner("Reading large CSV using Dask..."):
        df = dd.read_csv(uploaded_file)

    st.subheader("🔍 Data Preview")
    st.write(df.head(10).compute())

    st.subheader("📊 Dataset Info")
    st.write("Total Rows:", df.shape[0].compute())
    st.write("Total Columns:", len(df.columns))

    st.subheader("🧹 Data Cleaning")
    df_cleaned = df.dropna()
    st.write("Rows after cleaning:", df_cleaned.shape[0].compute())

    numeric_cols = df_cleaned.select_dtypes(include='number').columns

    if len(numeric_cols) > 0:
        st.subheader("📈 Visualization")
        selected_col = st.selectbox("Select numeric column", numeric_cols)

        fig, ax = plt.subplots()
        df_cleaned[selected_col].compute().hist(bins=30, ax=ax)
        ax.set_title(f"Distribution of {selected_col}")
        st.pyplot(fig)

    st.subheader("⬇️ Download Cleaned Data")
    final_df = df_cleaned.compute()
    csv = final_df.to_csv(index=False).encode("utf-8")

    st.download_button(
        label="Download CSV",
        data=csv,
        file_name="cleaned_data.csv",
        mime="text/csv"
    )
