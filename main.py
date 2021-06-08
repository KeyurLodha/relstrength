from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
# from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
# import uvicorn
import pandas as pd
pd.set_option('chained_assignment', None)
import mysql.connector as cnx
import datetime as dt
from datetime import timedelta
import numpy as np
import copy
import json
from pytz import timezone

def round_minutes(dt, direction, resolution):
    new_minute = (dt.minute // resolution + (1 if direction == 'up' else 0)) * resolution
    return dt + timedelta(minutes=new_minute - dt.minute)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

templates = Jinja2Templates(directory="templates")

@app.get('/')
def show_form(request: Request):
    return templates.TemplateResponse('form.html', context={'request': request})

@app.get("/api")
def callAPI(request: Request):
    print('POSTED')
    format = "%Y-%m-%d %H:%M"
    now_utc = dt.datetime.strptime(dt.datetime.now().strftime(format), format)
    now_ist = now_utc.astimezone(timezone('Asia/Kolkata'))
    curr_datetime = dt.datetime.strptime(now_ist.strftime(format), format)
    curr_date = curr_datetime.date()
    curr_time = curr_datetime.time()

    if curr_date.weekday()==5:
        curr_date = curr_date - timedelta(days=1)
    elif curr_date.weekday()==6:
        curr_date = curr_date - timedelta(days=2)

    if curr_time<dt.time(9, 30):
        rtn_data = {'scrips': [], 'shortlist_buy': [], 'shortlist_sell': [], 'time': ''}
        payload = json.dumps(rtn_data)
    elif curr_time>dt.time(15, 29):
        start_time = dt.time(15, 15)
        end_time = dt.time(15, 29)
        scrips, shortlist_buy, shortlist_sell = scanner(curr_date, start_time, end_time)
        s1 = scrips.to_dict(orient="records")
        s2 = shortlist_buy.to_dict(orient="records")
        s3 = shortlist_sell.to_dict(orient="records")
        time_str = f'{start_time} - {end_time}'
        rtn_data = {'scrips': s1, 'shortlist_buy': s2, 'shortlist_sell': s3, 'time': time_str}
        payload = json.dumps(rtn_data)
    else:
        if curr_time.minute%15==0:
            start_time = (curr_datetime - timedelta(minutes=15)).time()
            end_time = curr_time
        else:
            end_time_dt = round_minutes(curr_datetime, 'down', 15)
            end_time = end_time_dt.time()
            start_time = (end_time_dt - timedelta(minutes=15)).time()

        scrips, shortlist_buy, shortlist_sell = scanner(curr_date, start_time, end_time)
        s1 = scrips.to_dict(orient="records")
        s2 = shortlist_buy.to_dict(orient="records")
        s3 = shortlist_sell.to_dict(orient="records")
        time_str = f'{start_time} - {end_time}'
        rtn_data = {'scrips': s1, 'shortlist_buy': s2, 'shortlist_sell': s3, 'time': time_str}
        payload = json.dumps(rtn_data)

    return payload

# if __name__ == "__main__":
#     uvicorn.run(app, host="0.0.0.0", port=8000)

##### FUNC DEFNS #######
def get_time(time_list, time_var, input_date):
    time_range = [((dt.datetime.combine(input_date, time_var))-timedelta(minutes=i)).time() for i in range(0, 3)]
    intersect = list(set(time_list).intersection(time_range))

    if len(intersect)==1:
        rt_time = intersect[0]
        time_flag = True
    else:
        intersect.sort()
        idx_list = [abs(ele.minute-time_var.minute) for ele in intersect]
        try:
            idx = idx_list.index(min(idx_list))
            rt_time = intersect[idx]
            time_flag = True
        except ValueError as e:
            rt_time = dt.time(0, 0)
            time_flag = False
    
    return rt_time, time_flag


def scanner(input_date, start_time, end_time):

    try:
        stocks_db = cnx.connect(host="164.52.207.158", user="stock", password="stockdata@data", database='stock_production')

        eq_query = f'select instrument_id, ins_date, open, high, low, close from instrument_scan where date(ins_date)="{input_date}" ;'
        eq_df = pd.read_sql(eq_query,stocks_db, parse_dates=['ins_date'])

        high_low_query = 'select * from instrument_high;'
        high_low_df = pd.read_sql(high_low_query,stocks_db)

        sl_query = 'select id, tradingsymbol from instruments where f_n_o=1 and tradingsymbol not like "%NIFTY%";'
        sl_df = pd.read_sql(sl_query,stocks_db)
        
        stocks_db.close() 
    except Exception as e:
        stocks_db.close()
        print(str(e))

    eq_df.drop_duplicates(subset=['instrument_id', 'ins_date'], inplace=True)
    eq_df['date'] = eq_df['ins_date'].dt.date
    eq_df['time'] = eq_df['ins_date'].dt.time

    eq_df.drop(eq_df[(eq_df['time']<dt.time(9, 15))].index, inplace = True)
    eq_df.drop(eq_df[(eq_df['time']>dt.time(15, 29))].index, inplace = True)
    eq_df.reset_index(inplace=True, drop=True)
    eq_df.sort_values(by=['instrument_id', 'ins_date'], inplace=True)

    temp_st = copy.deepcopy(start_time)
    temp_et = copy.deepcopy(end_time)

    bchmrk_df = eq_df[eq_df['instrument_id']==417]
    bchmrk_time_list = set(list(bchmrk_df['time']))

    if (start_time in bchmrk_time_list) and (end_time in bchmrk_time_list):
        bchmrk_open = bchmrk_df[bchmrk_df['time']==start_time]['open'].to_list()[0]
        bchmrk_close = bchmrk_df[bchmrk_df['time']==end_time]['close'].to_list()[0]
        bchmrk_pc = round(((bchmrk_close - bchmrk_open)/bchmrk_open)*100, 4)
    elif (start_time not in bchmrk_time_list) and (end_time in bchmrk_time_list):
        start_time, time_flag = get_time(bchmrk_time_list, start_time, input_date)
        if time_flag==True:
            bchmrk_open = bchmrk_df[bchmrk_df['time']==start_time]['open'].to_list()[0]
            bchmrk_close = bchmrk_df[bchmrk_df['time']==end_time]['close'].to_list()[0]
            bchmrk_pc = round(((bchmrk_close - bchmrk_open)/bchmrk_open)*100, 4)
        else:
            bchmrk_pc = np.nan
    elif (start_time in bchmrk_time_list) and (end_time not in bchmrk_time_list):
        end_time, time_flag = get_time(bchmrk_time_list, end_time, input_date)
        if time_flag==True:
            bchmrk_open = bchmrk_df[bchmrk_df['time']==start_time]['open'].to_list()[0]
            bchmrk_close = bchmrk_df[bchmrk_df['time']==end_time]['close'].to_list()[0]
            bchmrk_pc = round(((bchmrk_close - bchmrk_open)/bchmrk_open)*100, 4)
        else:
            bchmrk_pc = np.nan
    else:
        start_time, time_flag1 = get_time(bchmrk_time_list, start_time, input_date)
        end_time, time_flag2 = get_time(bchmrk_time_list, end_time, input_date)
        if (time_flag1==True) and (time_flag2==True):
            bchmrk_open = bchmrk_df[bchmrk_df['time']==start_time]['open'].to_list()[0]
            bchmrk_close = bchmrk_df[bchmrk_df['time']==end_time]['close'].to_list()[0]
            bchmrk_pc = round(((bchmrk_close - bchmrk_open)/bchmrk_open)*100, 4)
        else:
            bchmrk_pc = np.nan

    id_dict = dict(sl_df.values)
    params_list = []

    for id, name in id_dict.items():    

        start_time = copy.deepcopy(temp_st)
        end_time = copy.deepcopy(temp_et)
        stock_df = eq_df[eq_df['instrument_id']==id]
        stock_time_list = set(list(stock_df['time']))

        if (start_time in stock_time_list) and (end_time in stock_time_list):
            stock_open = stock_df[stock_df['time']==start_time]['open'].to_list()[0]
            stock_close = stock_df[stock_df['time']==end_time]['close'].to_list()[0]
            stock_pc = round(((stock_close - stock_open)/stock_open)*100, 4)
        elif (start_time not in stock_time_list) and (end_time in stock_time_list):
            start_time, time_flag = get_time(stock_time_list, start_time, input_date)
            if time_flag==True:
                stock_open = stock_df[stock_df['time']==start_time]['open'].to_list()[0]
                stock_close = stock_df[stock_df['time']==end_time]['close'].to_list()[0]
                stock_pc = round(((stock_close - stock_open)/stock_open)*100, 4)
            else:
                stock_open = stock_close = stock_pc = np.nan
        elif (start_time in stock_time_list) and (end_time not in stock_time_list):
            end_time, time_flag = get_time(stock_time_list, end_time, input_date)
            if time_flag==True:
                stock_open = stock_df[stock_df['time']==start_time]['open'].to_list()[0]
                stock_close = stock_df[stock_df['time']==end_time]['close'].to_list()[0]
                stock_pc = round(((stock_close - stock_open)/stock_open)*100, 4)
            else:
                stock_open = stock_close = stock_pc = np.nan
        else:
            start_time, time_flag1 = get_time(stock_time_list, start_time, input_date)
            end_time, time_flag2 = get_time(stock_time_list, end_time, input_date)
            if (time_flag1==True) and (time_flag2==True):
                stock_open = stock_df[stock_df['time']==start_time]['open'].to_list()[0]
                stock_close = stock_df[stock_df['time']==end_time]['close'].to_list()[0]
                stock_pc = round(((stock_close - stock_open)/stock_open)*100, 4)
            else:
                stock_open = stock_close = stock_pc = np.nan

        rs_wo_beta = round(stock_pc - bchmrk_pc, 4)

        today_high = stock_df[(stock_df['time']>=start_time) & (stock_df['time']<=end_time)]['high'].max()
        today_low = stock_df[(stock_df['time']>=start_time) & (stock_df['time']<=end_time)]['low'].min()

        try:
            high_20d = high_low_df[high_low_df['instrument_id']==id]['twentyH'].to_list()[0]
        except IndexError as e:
            high_20d = np.nan

        try:
            high_50d = high_low_df[high_low_df['instrument_id']==id]['fiftyH'].to_list()[0]
        except IndexError as e:
            high_50d = np.nan

        try:
            high_250d = high_low_df[high_low_df['instrument_id']==id]['twofiftyH'].to_list()[0]
        except IndexError as e:
            high_250d = np.nan

        try:
            low_20d = high_low_df[high_low_df['instrument_id']==id]['twentyL'].to_list()[0]
        except IndexError as e:
            low_20d = np.nan

        try:
            low_50d = high_low_df[high_low_df['instrument_id']==id]['fiftyL'].to_list()[0]
        except IndexError as e:
            low_50d = np.nan

        try:
            low_250d = high_low_df[high_low_df['instrument_id']==id]['twofiftyL'].to_list()[0]
        except IndexError as e:
            low_250d = np.nan
        
        high_vs_20d = 'True' if today_high>high_20d else 'False'
        high_vs_50d = 'True' if today_high>high_50d else 'False'
        high_vs_250d = 'True' if today_high>high_250d else 'False'

        low_vs_20d = 'True' if today_low<low_20d else 'False'
        low_vs_50d = 'True' if today_low<low_50d else 'False'
        low_vs_250d = 'True' if today_low<low_250d else 'False'

        params_list.append([id, name, bchmrk_pc, stock_pc, rs_wo_beta, high_vs_20d, high_vs_50d, high_vs_250d, low_vs_20d, low_vs_50d, low_vs_250d])
        
    scrips = pd.DataFrame(params_list, columns=['id', 'name', 'bchmrk_pc', 'stock_pc', 'rs_wo_beta', 'high_vs_20d', 'high_vs_50d', 'high_vs_250d', 'low_vs_20d', 'low_vs_50d', 'low_vs_250d'])
    scrips.sort_values(by='rs_wo_beta', ascending=False, inplace=True)

    shortlist_buy = scrips.head(10)
    shortlist_buy['priority'] = np.nan
    shortlist_buy.loc[shortlist_buy['high_vs_250d']=='True', ['priority']] = 1
    shortlist_buy.loc[(shortlist_buy['high_vs_50d']=='True') & (shortlist_buy['high_vs_250d']=='False'), ['priority']] = 2
    shortlist_buy.loc[(shortlist_buy['high_vs_20d']=='True') & (shortlist_buy['high_vs_50d']=='False') & (shortlist_buy['high_vs_250d']=='False'), ['priority']] = 3
    shortlist_buy.drop(shortlist_buy[shortlist_buy['priority'].isna()].index, inplace=True)
    shortlist_buy.sort_values(by='priority', inplace=True)

    shortlist_sell = scrips.head(10)
    shortlist_sell['priority'] = np.nan
    shortlist_sell.loc[shortlist_sell['low_vs_250d']=='True', ['priority']] = 1
    shortlist_sell.loc[(shortlist_sell['low_vs_50d']=='True') & (shortlist_sell['low_vs_250d']=='False'), ['priority']] = 2
    shortlist_sell.loc[(shortlist_sell['low_vs_20d']=='True') & (shortlist_sell['low_vs_50d']=='False') & (shortlist_sell['low_vs_250d']=='False'), ['priority']] = 3
    shortlist_sell.drop(shortlist_sell[shortlist_sell['priority'].isna()].index, inplace=True)
    shortlist_sell.sort_values(by='priority', inplace=True)

    scrips.fillna('', inplace=True)
    shortlist_buy.fillna('', inplace=True)
    shortlist_sell.fillna('', inplace=True)

    return scrips, shortlist_buy, shortlist_sell