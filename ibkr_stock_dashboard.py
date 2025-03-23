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
OPEN_POSITIONS_FILE = 'trade_reporting/open_trade_report.csv'
PRIOR_POSITIONS_FILE = 'trade_reporting/prior_positions_report.csv'

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

    # Load open positions data
    open_positions_df = load_data_from_s3(OPEN_POSITIONS_FILE)
    if open_positions_df is None:
        return

    # Load prior positions data
    prior_positions_df = load_data_from_s3(PRIOR_POSITIONS_FILE)
    if prior_positions_df is None:
        return

    # Calculate summary metrics for open positions
    total_profit = prior_positions_df['FifoPnlRealized'].sum()
    total_unrealized = open_positions_df['FifoPnlUnrealized'].sum()

    # Placards for summary metrics
    st.subheader("Portfolio Summary")
    col1, col2 = st.columns(2)
    col1.metric("Total Profit", f"${total_profit:,.2f}")
    col2.metric("Total Unrealized", f"${total_unrealized:,.2f}")

    # Display open trade entries
    st.subheader("Trade Entries")
    display_columns = ['Symbol', 'Expiry', 'FifoPnlUnrealized', 'Strike', 'PositionValue']
    st.dataframe(open_positions_df[display_columns], use_container_width=True, hide_index=True)

    # Horizontal bar chart sorted by top profit (exclude blank profits)
    st.subheader("Profit Percent Distribution")
    bar_chart_data = prior_positions_df.dropna(subset=['FifoPnlRealized'])
    bar_chart = (
        alt.Chart(bar_chart_data)
        .mark_bar()
        .encode(
            x='FifoPnlRealized:Q',
            y=alt.Y('Symbol:N', sort='-x'),
            tooltip=['Symbol', 'FifoPnlRealized']
        )
        .properties(height=400, title="Profit Percent by Symbol")
    )
    st.altair_chart(bar_chart, use_container_width=True)

    # Pie chart for closed entries by Symbol and Profit
    st.subheader("Closed Entries by Symbol (Proportion of Profit)")
    closed_entries = prior_positions_df[['FifoPnlRealized', 'Symbol']]  # Filter columns for the pie chart

    # Remove rows with negative profit
    closed_entries = closed_entries[closed_entries['FifoPnlRealized'] >= 0]

    # Group by Symbol and calculate the total profit for each symbol
    closed_entries_grouped = closed_entries.groupby('Symbol').agg({'FifoPnlRealized': 'sum'}).reset_index()

    # Calculate the percentage of total profit for each symbol
    total_profit_for_pie = closed_entries_grouped['FifoPnlRealized'].sum()
    closed_entries_grouped['ProfitPercent'] = (closed_entries_grouped['FifoPnlRealized'] / total_profit_for_pie) * 100

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
            theta=alt.Theta(field="FifoPnlRealized", type="quantitative"),
            color=alt.Color(field="Symbol", type="nominal"),
            tooltip=['Symbol', 'FifoPnlRealized', 'ProfitPercent'],
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
