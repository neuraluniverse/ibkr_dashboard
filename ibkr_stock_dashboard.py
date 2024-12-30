import streamlit as st
import pandas as pd
import boto3
from io import StringIO
import altair as alt
from datetime import datetime
import redshift_connector
import toml

# Load the secrets from the local secret_keys.toml file.
#secrets = toml.load("secret_keys.toml")

TRADE_REPORT_FILE = 'trade_reporting/trade_report.csv'

# Access AWS credentials from Streamlit secrets
AWS_ACCESS_KEY_ID = st.secrets["aws"]["aws_access_key_id"]
AWS_SECRET_ACCESS_KEY = st.secrets["aws"]["aws_secret_access_key"]
S3_BUCKET_NAME = st.secrets["aws"]["bucket_name"]

def get_redshift_connection(
    redshift_host = st.secrets["aws"]["redshift_host"],
    redshift_dbname = st.secrets["aws"]["redshift_dbname"], 
    redshift_user = st.secrets["aws"]["redshift_user"], 
    redshift_password = st.secrets["aws"]["redshift_password"], 
    redshift_port = st.secrets["aws"]["redshift_port"]
): 
    print('get_redshift_connection')
    conn = redshift_connector.connect(
        host = redshift_host, 
        database=redshift_dbname, 
        port = redshift_port, 
        user = redshift_user, 
        password = redshift_password,
    )
    conn.autocommit = True
    return conn

redshift_conn = get_redshift_connection()

def get_bobby_entries(redshift_conn):
    cursor = redshift_conn.cursor()
    q = '''
        select * from bobby_entries
        order by datadate desc
    '''
    cursor.execute(q)
    result = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(result, columns=colnames)
    return df

def get_high_low_entries(redshift_conn):
    cursor = redshift_conn.cursor()
    q = '''
        select * from high_low_entries order by date desc
    '''
    cursor.execute(q)
    result = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(result, columns=colnames)
    return df

bobby_entries = get_bobby_entries(redshift_conn)
high_low_entries = get_high_low_entries(redshift_conn)

def load_data_from_s3(file_name):
    """Load CSV data from S3."""
    s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                              aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=file_name)
        return pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None

def main():

    # Load trade report data
    trade_df = load_data_from_s3(TRADE_REPORT_FILE)
    if trade_df is None:
        return

    # Multiply by 100 for options
    trade_df['Profit'] *= 100
    trade_df['EntrySum'] *= 100

    # Deriving DTE (Days to Expiration)
    today = datetime.today().date()
    trade_df['DTE'] = trade_df['exp_dt'].apply(lambda x: (datetime.strptime(x, '%Y-%m-%d').date() - today).days)

    # Calculate summary metrics for closed positions
    closed_df = trade_df[trade_df['Pos'] == 'close']  # Filter closed positions only
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

    # Placards for summary metrics
    st.subheader("Portfolio Summary")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Profit", f"${total_profit:,.2f}")
    col2.metric("Total Entry", f"${total_entry:,.2f}")
    col3.metric("Percent ROI", f"{percent_roi:.2f}%")
    col4.metric("Total Unrealized", f"${total_unrealized:,.2f}")

    # Dropdown to filter by trade status
    status_filter = st.selectbox("Filter by Trade Status", options=["Open", "All",  "Close"], index=0)  # Default to "All"
    
    if status_filter == "Open":
        filtered_df = trade_df[trade_df['Pos'] == 'open']
    elif status_filter == "Close":
        filtered_df = trade_df[trade_df['Pos'] == 'close']
    else:
        filtered_df = trade_df  # "All" option shows all trades

    # Sort table by EarliestEntryDate (most recent at the top)
    filtered_df = filtered_df.sort_values(by='DTE', ascending=True)

    # Reorder columns
    display_df = filtered_df[['Symbol', 'DTE', 'Unrealized', 'PositionValue', 'EarliestEntryDate', 
                              'EntrySum', 'Profit', 'ProfitPercent', 'exp_dt', 'MarkPrice']]

    st.subheader("Trade Entries")
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    # Horizontal bar chart sorted by top profit (exclude blank profits)
    st.subheader("Profit Percent Distribution")
    bar_chart_data = trade_df.dropna(subset=['Profit'])
    bar_chart = (
        alt.Chart(bar_chart_data)
        .mark_bar()
        .encode(
            x='ProfitPercent:Q',
            y=alt.Y('Symbol:N', sort='-x'),
            tooltip=['Symbol', 'ProfitPercent', 'Profit', 'EntrySum']
        )
        .properties(height=400, title="Profit Percent by Symbol")
    )
    st.altair_chart(bar_chart, use_container_width=True)

    # Pie chart for closed entries by Symbol and Profit (instead of Profit Percent)
    st.subheader("Closed Entries by Symbol (Proportion of Profit)")
    closed_entries = closed_df[['Profit', 'Symbol']]  # Filter columns for the pie chart

    # Remove rows with negative profit
    closed_entries = closed_entries[closed_entries['Profit'] >= 0]

    # Group by Symbol and calculate the total profit for each symbol
    closed_entries_grouped = closed_entries.groupby('Symbol').agg({'Profit': 'sum'}).reset_index()

    # Calculate the percentage of total profit for each symbol
    total_profit_for_pie = closed_entries_grouped['Profit'].sum()
    closed_entries_grouped['ProfitPercent'] = (closed_entries_grouped['Profit'] / total_profit_for_pie) * 100

    # Add a label for the biggest pies
    closed_entries_grouped['Label'] = closed_entries_grouped.apply(
        lambda row: f"{row['Symbol']} ({row['ProfitPercent']:.2f}%)" if row['ProfitPercent'] >= 10 else '',
        axis=1
    )

    # Pie chart displaying proportions of profit by symbol with labels for large segments
    pie_chart = (
        alt.Chart(closed_entries_grouped)
        .mark_arc()
        .encode(
            theta=alt.Theta(field="Profit", type="quantitative"),
            color=alt.Color(field="Symbol", type="nominal"),
            tooltip=['Symbol', 'Profit', 'ProfitPercent'],
            text=alt.Text(field="Label", type="nominal")
        )
        .properties(height=400, title="Proportion of Closed Entries by Symbol")
    )
    st.altair_chart(pie_chart, use_container_width=True)

    # Display Bobby Entries Table
    st.subheader("Bobby Entries")
    st.dataframe(bobby_entries, use_container_width=True, hide_index=True)

    # Display High Low Entries Table
    st.subheader("High Low Entries")
    st.dataframe(high_low_entries, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
