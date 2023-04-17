#SETUP
!pip install twelvedata

#imports
import json
import requests
from twelvedata import TDClient
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

#init
td = TDClient(apikey=API_KEY)

#Fortune_CSV
fortune_companies = pd.read_csv('Fortune_1000.csv')
f_50 = fortune_companies.copy()
f_50 = f_50[f_50["rank"] <= 50]
f_50 = f_50[["revenue", "profit", "num. of employees", "sector", "city", "state", "rank", "CEO", "Website", "Ticker", "Market Cap" ]]

fortune_tickers = []

for row in f_50["Ticker"]:
    fortune_tickers.append(row)

#ref: https://www.kaggle.com/code/winston56/2022-fortune-500-data-collection/notebook

#EXTRACT from 12_Data API
symbol_list = fortune_tickers
dict_company = {}

for company in symbol_list:
    #Grabs company logo URL
    try:
        logo_data = td.get_logo(symbol=company).as_json()
        #print(json.dumps(logo_data, indent = 2))
        img_url = logo_data["url"]
    except Exception as e:
        print(f"Failed to retrieve logo data for {company}: {str(e)}")
        exit()

    #Grabs exchange, close_price, currency
    try:
        core_url = f"https://api.twelvedata.com/eod?symbol={company}&apikey={API_KEY}"
        result = requests.get(core_url).json()
        print(json.dumps(result, indent =5))
        exchange = result["exchange"]
        close_price = result["close"]
        currency = result["currency"]
    except Exception as e:
        print(f"Failed to retrieve core data for {company}: {str(e)}")
        exit()

    dict_company[company] = {"img_url": img_url, "exchange": exchange, "close_price": close_price, "currency": currency}

#TRANSFORM - Merged two Dataframes together

data = [{'Ticker': key, 'company_img_url': value['img_url'], 'exchange': value['exchange'], "close_price": value["close_price"], "currency": value["currency"]} for key, value in dict_company.items()]

df = pd.DataFrame(data)
merge = pd.merge(df, f_50, on='Ticker')

#LOAD into POSTGRES-DB

#CONNECT TO POSTGRES
try:
    conn = psycopg2.connect(
        host= db_host,
        database= db_name,
        user= user,
        password= db_password,
        port = db_port)
    print("Connected to DB")

    cursor = conn.cursor()

    #Dropping STOCKS table if already exists.
    cursor.execute("DROP TABLE IF EXISTS STOCKS")

    #Creating table as per requirement
    sql ='''CREATE TABLE STOCKS(
    company_name VARCHAR(255) NOT NULL,
    img_url VARCHAR(255),
    exchange VARCHAR(255),
    close_price FLOAT,
    currency VARCHAR(10),
    revenue FLOAT,
    profit FLOAT,
    employee_num INT,
    sector VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(255),
    rank INT,
    ceo_name VARCHAR(255),
    website VARCHAR(255),
    market_cap FLOAT

    )'''

    cursor.execute(sql)
    
    #INSERTING DATA
    insert_query = """INSERT into STOCKS(company_name, 
                    img_url ,
                    exchange, 
                    close_price,
                    currency,
                    revenue,
                    profit,
                    employee_num,
                    sector, 
                    city, 
                    state, 
                    rank, 
                    ceo_name,
                    website , 
                    market_cap              
                    
                    ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""


    data = merge

    cursor.executemany(insert_query, data.values)

    cursor.close()
    print('Process Done!')

    conn.commit()
    print("Table created successfully........")


except psycopg2.DatabaseError as error : 
    print(error)

finally : 
    conn.close()




