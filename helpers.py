import sqlite3
import yfinance as yf
from datetime import datetime, timedelta, date


def validate_date(update_date):
    """Check the date input to ensure it is in the correct format
    Parameters:
        update_date (str): String format of provided trading date to pull trade information (YYYY-MM-DD).
    Returns:
        (bool): True/False depending on whether update_date is the correct str format
    """
    valid_date = None
    valid_format = "%Y-%m-%d"
    try:
        date_check = datetime.strptime(update_date, valid_format)
        return True
    except:
        raise ValueError("Check to ensure the date input is in the correct date format {}".format("YYYY-MM-DD"))
        return False


def create_connection(db_path):
    """Create connection to SQLite Database and return a SQLite3 connection object.
    Parameters:
            db_path (str): local path to SQLite3 Database
    """
    connection = None
    try:
        connection = sqlite3.connect(db_path)
    except Error:
        print(Error)
    return connection


def insert_records(db_connection, table, row):
    """Given a SQLite3 connection object, table name, and row values, insert row values into the given table
    Parameters:
        db_connection (obj): SQLite3 connection object.
        table (str): Database table name.
        row (dict): Dictionary containing table column and row values to be inserted.
    """
    # From given row (dictionary), create columns and values strings representing table fields and row values for insert
    columns = ', '.join('"{}"'.format(column) for column in row.keys())
    values = ', '.join(':{}'.format(column) for column in row.keys())
    # Build sql query from table name and columns, values string variables defined
    sql = 'INSERT INTO "{0}" ({1}) VALUES ({2})'.format(table, columns, values)
    # Define cursor, execute query, and commit to database
    cursor = db_connection.cursor()
    cursor.execute(sql, row)
    db_connection.commit()


def pull_data(update_date, db_connection):
    """Given a specific date, connect to yfinance API and download relevant information for each
    stock found in the stocks SQLite database, excluding cash.
    Parameters:
        update_date (str): String format of provided trading date to pull trade information (YYYY-MM-DD).
        db_connection (obj): SQLite connection object
    Returns:
        stocks (list): List of dictionaries with appended yfinance stock information.
    """
    # Define
    start_dt = datetime.strptime(update_date, "%Y-%m-%d")
    end_dt = start_dt + timedelta(days=1)
    cursor = db_connection.cursor()

    # Exclude stock id of 1, as this represents Cash
    sql = "SELECT DISTINCT * FROM stocks WHERE id NOT IN (1);"
    # Define db cursor, return sql query results into list of dictionaries representing distinct stock records
    results = cursor.execute(sql)
    stocks = [{'stock_id': row[0],
               'symbol': row[1],
               'name': row[2]} for row in results.fetchall()]
    # Define list of stock table field names needed
    quote_vals = ['date', 'open', 'high', 'low', 'close']
    # Define empty list to be used for stocks with valid information from API that will need to be added.
    stocks_update = []
    # Loop through list of stock dictionaries, connect to yfinance API and download respective fields needed for quotes table
    for stock in stocks:
        # Add empty keys as place holders in stock dictionary
        for vals in quote_vals:
            stock[vals] = ''
        # Return response from yfinance for respective stock and timeframe
        current_stock = yf.download(stock["symbol"], start=update_date, end=datetime.strftime(end_dt, "%Y-%m-%d"),
                                    progress=False, show_errors=False)
        # Determine if there is data returned by API, if so updated stock dictionary values and add to stocks_update dictionary
        if len(current_stock) > 0:
            # Only look at values specified in quote_vals list
            for val in quote_vals:
                if val == 'date':
                    # Update date from dataframe index value, format as needed
                    stock[val] = current_stock.index.strftime("%Y-%m-%d")[0]
                else:
                    # Update price information in stock dictionary as float with two decimals
                    stock[val] = '%.2f' % float(current_stock[val.title()][0])
            stocks_update.append(stock)
        else:
            # If there is no data returned by the API, likely not a valid trading day, stock, or the stock has not IPO'd yet
            print("Check to ensure %s is a valid trading day, it's not in the future, and that %s is a valid symbol" % (update_date, stock["symbol"]))
    # return dictionary needed to update quotes records
    return stocks_update


report_summary_sql = (
    """
    SELECT
        date,
        SUM(open_value) as open_value,
        SUM(high_value) as high_value,
        SUM(low_value) as low_value,
        SUM(close_value) as close_value
    FROM report_lines
    WHERE date IN (Date(?))
    GROUP BY 1
    ORDER BY 1
    """)

record_lines_sql = (
    """
            SELECT
                main.date,
                main.name as stock_name,
                main.symbol as stock_symbol,
                main.quantity,
                CASE WHEN main.name IN ('Cash') THEN main.quantity ELSE open * main.quantity END as open_value,
                CASE WHEN main.name IN ('Cash') THEN main.quantity ELSE high * main.quantity END as high_value,
                CASE WHEN main.name IN ('Cash') THEN main.quantity ELSE low * main.quantity END as low_value,
                CASE WHEN main.name IN ('Cash') THEN main.quantity ELSE close * main.quantity END as close_value
            FROM (
              SELECT
                  --insert date here
                  Date(?) as date,
                  s.id,
                  s.symbol,
                  s.name,
                  SUM(p.quantity) as quantity
              FROM portfolio as p
              INNER JOIN stocks as s ON p.stock_id = s.id
              GROUP BY 1, 2, 3, 4
              HAVING SUM(p.quantity) > 0
              ORDER BY 1) as main
            LEFT JOIN quotes as q ON main.date = q.date AND main.id = q.stock_id
            WHERE open_value IS NOT NULL
            """)
