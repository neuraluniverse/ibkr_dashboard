import streamlit as st
import pandas as pd
import boto3
from io import StringIO
import altair as alt
from datetime import datetime
import redshift_connector

# Authentication credentials
USERNAME = st.secrets["credentials"]["username"]
PASSWORD = st.secrets["credentials"]["password"]

# AWS credentials
AWS_ACCESS_KEY_ID = st.secrets["aws"]["aws_access_key_id"]
AWS_SECRET_ACCESS_KEY = st.secrets["aws"]["aws_secret_access_key"]
S3_BUCKET_NAME = st.secrets["aws"]["bucket_name"]

TRADE_REPORT_FILE = 'trade_reporting/trade_report.csv'

# Login Function
def login():
    st.title("Login")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    if st.button("Login"):
        if username == USERNAME and password == PASSWORD:
            st.session_state["authenticated"] = True
            st.success("Login successful!")
        else:
            st.error("Invalid username or password.")

if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

if not st.session_state["authenticated"]:
    login()
else:
    if st.button("Logout"):
        st.session_state["authenticated"] = False
        st.experimental_rerun()

# Redshift Connection
def get_redshift_connection():
    conn = redshift_connector.connect(
        host=st.secrets["aws"]["redshift_host"],
        database=st.secrets["aws"]["redshift_dbname"],
        user=st.secrets["aws"]["redshift_user"],
        password=st.secrets["aws"]["redshift_password"],
        port=st.secrets["aws"]["redshift_port"],
    )
    conn.autocommit = True
    return conn

redshift_conn = get_redshift_connection()

# Load Data from S3
def load_data_from_s3(file_name):
    s3_client = boto3.client(
        's3', 
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=file_name)
        return pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None

# Main Function
def main():
    st.title("Trade Reporting Dashboard")

    # Load trade report data
    trade_df = load_data_from_s3(TRADE_REPORT_FILE)
    if trade_df is None:
        return

    # Multiply by 100 for options
    trade_df['Profit'] *= 100
    trade_df['EntrySum'] *= 100

    # Deriving Days to Expiration (DTE)
    today = datetime.today().date()
    trade_df['DTE'] = trade_df['exp_dt'].apply(lambda x: (datetime.strptime(x, '%Y-%m-%d').date() - today).days)

    # Add strike price and expiration date columns
    trade_df['StrikePrice'] = trade_df['strike_price']
    trade_df['ExpirationDate'] = trade_df['exp_dt']

    # Calculate summary metrics for closed positions
    closed_df = trade_df[trade_df['Pos'] == 'close']
    total_profit = closed_df['Profit'].sum()
    total_entry = closed_df['EntrySum'].sum()
    percent_roi = (total_profit / total_entry) * 100 if total_entry != 0 else 0
    total_unrealized = trade_df['FifoPnlUnrealized'].sum()

    # Add Unrealized column for open trades
    trade_df['Unrealized'] = trade_df.apply(
        lambda row: (row['MarkPrice'] - row['OpenPrice']) * row['PositionValue'] 
        if row['Pos'] == 'open' and not pd.isnull(row['MarkPrice']) and not pd.isnull(row['OpenPrice']) else 0,
        axis=1
    )

    # Display Portfolio Summary
    st.subheader("Portfolio Summary")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Profit", f"${total_profit:,.2f}")
    col2.metric("Total Entry", f"${total_entry:,.2f}")
    col3.metric("Percent ROI", f"{percent_roi:.2f}%")
    col4.metric("Total Unrealized", f"${total_unrealized:,.2f}")

    # Filter trades based on status
    status_filter = st.selectbox("Filter by Trade Status", options=["Open", "All",  "Close"], index=0)
    filtered_df = trade_df[trade_df['Pos'] == status_filter.lower()] if status_filter != "All" else trade_df

    # Sort table by Days to Expiration (ascending)
    filtered_df = filtered_df.sort_values(by='DTE', ascending=True)

    # Display Trades
    st.subheader("Trade Entries")
    st.dataframe(
        filtered_df[['Symbol', 'StrikePrice', 'ExpirationDate', 'DTE', 'Unrealized', 'PositionValue', 'EarliestEntryDate', 'EntrySum', 'Profit', 'ProfitPercent', 'MarkPrice']],
        use_container_width=True, hide_index=True
    )

    # Profit Percent Distribution
    st.subheader("Profit Percent Distribution")
    bar_chart = (
        alt.Chart(trade_df.dropna(subset=['Profit']))
        .mark_bar()
        .encode(
            x='ProfitPercent:Q',
            y=alt.Y('Symbol:N', sort='-x'),
            tooltip=['Symbol', 'ProfitPercent', 'Profit', 'EntrySum']
        )
        .properties(height=400, title="Profit Percent by Symbol")
    )
    st.altair_chart(bar_chart, use_container_width=True)

if __name__ == "__main__":
    main()
