# stock_portfolio
Simple stock portfolio program that pulls stock price information from yfinance for a given date and updates a SQLite db.

Author: Logan Poe

# Installation

Python3

Navigate into the folder you clone the respository into (eg. cd /projects/new_project/)

Install all required packages using pip - pip install -r requirements.txt

# Usage

Usage, envoke from the command line:

python3 update_db.py <date as string, YYYY-MM-DD>

Ex: Update quotes, report_lines, and report_summary tables for May 1st, 2021 for active holdings in the portfolio table

python3 update_db.py "2021-03-01"
