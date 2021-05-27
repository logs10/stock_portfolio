"""
Submission for Storable stocks project
Author: Logan Poe
Last Updated: 2021-05-26
"""
import sqlite3
from datetime import datetime, timedelta, date
import helpers
import sys


def update_quotes(stocks, db_connection, update_date):
    """Given a updated stocks dictionary with trading information, insert the new records into the quotes table
    Parameters:
        stocks (dict): Updated stocks dictionary containing relevant trading information for a particular trading day.
        db_connection (obj): SQLite connection object.
    TODO:
        *Should consider trying to add information for a date/stock that already exists? Overwrite, ignore, throw error?
        *Created a unique index on stock_id, date to ensure duplicate quote entries are avoided for now.
    """
    # Define fields list with only fields needed to update
    fields = ['date', 'stock_id', 'open', 'high', 'low', 'close']
    reports = False
    # Loop through stocks dictionary list, copy the current stock dictionary and remove unecessary keys not need for insert
    for stock in stocks:
        new_insert = stock.copy()
        for keys in stock.keys():
            if keys not in fields:
                new_insert.pop(keys)
        # New_inserts data should be ready to insert, call insert_records function with parameters
        try:
            helpers.insert_records(db_connection, 'quotes', new_insert)
            # If no error arises, records have been inserted
            print("Successfully inserted record into quotes: {}".format(new_insert))
            # Since quotes was updated, report_lines and report_summary can be updated
            reports = True

        # If except block is hit, likely issue and issue with
        except:
            print("Error inserting record into quotes. check to make sure record doesn't already exist for {} on {}.".format(
                "stock id " + str(stock["stock_id"]), stock["date"]))

    # try and update reports
    if reports == True:
        update_report_lines(update_date, db_connection)
        update_report_summary(update_date, db_connection)


def update_report_lines(update_date, db_connection):
    """Update report lines table for the given update date. SQL query populates results based on the most recent portfolio positions,
    and uses the updated quotes table from the supplied update date.
    Parameters:
        pdate_date (str): String format of provided trading date to pull trade information (YYYY-MM-DD).
        db_connection (obj): SQLite connection object.
        TODO: Add better/more specific error handling around SQL insertion
    """
    sql = helpers.record_lines_sql
    cursor = db_connection.cursor()
    results = cursor.execute(sql, [update_date])
    records = [{'date': row[0],
                'stock_name': row[1],
                'stock_symbol': row[2],
                'quantity': row[3],
                'open_value': row[4],
                'high_value': row[5],
                'low_value': row[6],
                'close_value': row[7]} for row in results.fetchall()]
    for record in records:
        try:
            helpers.insert_records(db_connection, 'report_lines', record)
            print("Successfully inserted record into report_lines: {}".format(record))
        except:
            print("Error inserting record into report lines. check to make sure record doesn't already exist for {} on {}.".format(
                "symbol " + str(record["stock_symbol"]), record["date"]))


def update_report_summary(update_date, db_connection):
    """Update report summary table for the given update date. SQL query populates portfolio results based on the most recent portfolio positions
    from information contained in the report_lines table.
    Parameters:
        pdate_date (str): String format of provided trading date to pull trade information (YYYY-MM-DD).
        db_connection (obj): SQLite connection object.
        TODO: Add better/more specific error handling around SQL insertion
    """
    sql = helpers.report_summary_sql
    cursor = db_connection.cursor()
    results = cursor.execute(sql, [update_date])
    records = [{'date': row[0],
                'open_value': row[1],
                'high_value': row[2],
                'low_value': row[3],
                'close_value': row[4]} for row in results.fetchall()]
    for record in records:
        try:
            helpers.insert_records(db_connection, 'report_summary', record)
            print("Successfully inserted record into report_sumary: {}".format(record))
        except:
            print("Error inserting record into report summary. check to make sure record doesn't already exist for {}.".format(record["date"]))


def main(update_date):
    portfolio_db = "stock_portfolio.sqlite"

    # Validate date input, proceed if valid
    if helpers.validate_date(update_date) == True:
        # Define connection to SQLite db
        connection = helpers.create_connection(portfolio_db)
        with connection:
            # Define stage as result of pull_data function, data ready to be inserted
            stage = helpers.pull_data(update_date, connection)
            # Call update_quotes to start the insertion process of quotes, to be followed up report_lines and report_summary
            update_quotes(stage, connection, update_date)


if __name__ == '__main__':
    # Example with one date
    main(sys.argv[1])

    # Example with more dates
    # d1 = date(2021, 5, 1)
    # d2 = date(2021, 5, 31)
    # delta = d2 - d1

    # for i in range(delta.days + 1):
    #     main(str(d1 + timedelta(days=i)))
