import sqlite3
from urllib.request import urlopen
import ssl
import pandas as pd
from datetime import datetime, timedelta

yesterday = str((datetime.today() - timedelta(days=1)).date())
enddate = datetime.strptime(yesterday, '%Y-%m-%d').strftime('%d-%b-%Y')

# while True:
#     inp = input('Enter End Date, or quit: ')
#     if (inp == 'quit'): break
#     if (len(inp) < 1):
#         yesterday = str((datetime.today() - timedelta(days=1)).date())
#         enddate = datetime.strptime(yesterday, '%Y-%m-%d').strftime('%d-%b-%Y')
#     else:
#         enddate = inp
#         print(enddate)
#         try:
#             datetime.strptime(enddate, '%d-%b-%Y')
#         except:
#             print("Incorrect date format, should be DD-MONTH-YYYY, as in 01-January-2017")
#             continue
#
#     print(enddate)

conn = sqlite3.connect('stockdata.sqlite')
cur = conn.cursor()

cur.execute('''
            CREATE TABLE IF NOT EXISTS TradeData
            (ID INTEGER PRIMARY KEY, Stock TEXT, TradeDate DATE, Open FLOAT,
            High FLOAT, Low FLOAT, Close FLOAT, Volume INTEGER, UNIQUE(Stock, TradeDate))
            ''')

cur.execute('''SELECT Name FROM Stocks''') # get stocks to retrieve
stocks = cur.fetchall()

for stock in stocks:
    #print(stock[0])

    cur.execute('''SELECT MAX(TradeDate) FROM TradeData WHERE Stock = ?''',
    (stock[0], )) # get last date retrieved (if any)
    lastdate = cur.fetchone()[0]
    #print(lastdate)
    if lastdate == None:
        startdate = '01-Jan-2000'
    else:
        # need to convert to DD_MMM-YYYY date format otherwise encounter issues with certain dates
        format_lastdate = datetime.strptime(lastdate, '%Y-%m-%d').strftime('%d-%b-%Y') # convert text to date format (DD-MMM-YYYY)
        startdate = format_lastdate
    # **************************************************************************************
    # need to fix: make sure that date is not today (i.e. won't get correct end-of-day data)

    # Ignore SSL certificate errors
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    rooturl = 'https://finance.google.com/finance/historical?q='
    query = stock[0] + '&startdate=' + startdate +'&enddate=' + enddate + '&output=csv'
    url = rooturl + query
    try:
        connection = urlopen(url, context=ctx)
        print('Retrieved', stock[0])
    except:
        print('Error in retrieving', stock[0])
        cur.execute('UPDATE Stocks SET Error=1 WHERE Name = ?', (stock[0], )) # set error indicator in table
        conn.commit()
        continue

    df = pd.read_csv(connection) # read csv into a dataframe
    df = df.iloc[::-1] # reverse order of data (latest dates at the bottom)
    #print(df)
    #import numpy as np
    #df['Volume'].replace(to_replace = '-', value = np.nan, inplace = True, regex = True)

    for index,row in df.iterrows():
        date_formated = datetime.strptime(row[0], '%d-%b-%y').date() #convert text to date format (YYYY-MM-DD)
        cur.execute('''
        INSERT OR IGNORE INTO TradeData
        (Stock, TradeDate, Open, High, Low, Close, Volume) VALUES (?, ?, ?, ?, ?,
        ?, ?)''', (stock[0], date_formated, row[1], row[2], row[3], row[4], row[5]))
        # ******************************************************************************
        # need to fix: values can't be converted to float/integer due to missing values

    conn.commit()

#data.to_sql('TradeData', conn, if_exists = 'append')


print("Done")
