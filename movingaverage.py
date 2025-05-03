import requests
import json
from datetime import date, timedelta


today = date.today()
if today.weekday() == 0:  # Monday → go back 3 days to Friday
    yesterday = today - timedelta(days=3).strftime("%Y-%m-%d")
elif today.weekday() == 6:  # Sunday → go back 2 days to Friday
    yesterday = today - timedelta(days=2).strftime("%Y-%m-%d")
else:  # All other days → go back 1 day
    yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")


today = date.today().strftime("%Y-%m-%d")
nifty100 = {'NSE_EQ|INE117A01022': 'ABB', 'NSE_EQ|INE931S01010': 'ADANIENSOL', 'NSE_EQ|INE423A01024': 'ADANIENT', 'NSE_EQ|INE364U01010': 'ADANIGREEN', 'NSE_EQ|INE742F01042': 'ADANIPORTS', 'NSE_EQ|INE814H01011': 'ADANIPOWER', 'NSE_EQ|INE079A01024': 'AMBUJACEM', 'NSE_EQ|INE437A01024': 'APOLLOHOSP', 'NSE_EQ|INE021A01026': 'ASIANPAINT', 'NSE_EQ|INE192R01011': 'DMART', 'NSE_EQ|INE238A01034': 'AXISBANK', 'NSE_EQ|INE917I01010': 'BAJAJ-AUTO', 'NSE_EQ|INE296A01024': 'BAJFINANCE', 'NSE_EQ|INE918I01026': 'BAJAJFINSV', 'NSE_EQ|INE118A01012': 'BAJAJHLDNG', 'NSE_EQ|INE377Y01014': 'BAJAJHFL', 'NSE_EQ|INE028A01039': 'BANKBARODA', 'NSE_EQ|INE263A01024': 'BEL', 'NSE_EQ|INE029A01011': 'BPCL', 'NSE_EQ|INE397D01024': 'BHARTIARTL', 'NSE_EQ|INE323A01026': 'BOSCHLTD', 'NSE_EQ|INE216A01030': 'BRITANNIA', 'NSE_EQ|INE067A01029': 'CGPOWER', 'NSE_EQ|INE476A01022': 'CANBK', 'NSE_EQ|INE121A01024': 'CHOLAFIN', 'NSE_EQ|INE059A01026': 'CIPLA', 'NSE_EQ|INE522F01014': 'COALINDIA', 'NSE_EQ|INE271C01023': 'DLF', 'NSE_EQ|INE016A01026': 'DABUR', 'NSE_EQ|INE361B01024': 'DIVISLAB', 'NSE_EQ|INE089A01031': 'DRREDDY', 'NSE_EQ|DUM003A01024': 'DUMMYSIEMS', 'NSE_EQ|INE066A01021': 'EICHERMOT', 'NSE_EQ|INE758T01015': 'ETERNAL', 'NSE_EQ|INE129A01019': 'GAIL', 'NSE_EQ|INE102D01028': 'GODREJCP', 'NSE_EQ|INE047A01021': 'GRASIM', 'NSE_EQ|INE860A01027': 'HCLTECH', 'NSE_EQ|INE040A01034': 'HDFCBANK', 'NSE_EQ|INE795G01014': 'HDFCLIFE', 'NSE_EQ|INE176B01034': 'HAVELLS', 'NSE_EQ|INE158A01026': 'HEROMOTOCO', 'NSE_EQ|INE038A01020': 'HINDALCO', 'NSE_EQ|INE066F01020': 'HAL', 'NSE_EQ|INE030A01027': 'HINDUNILVR', 'NSE_EQ|INE0V6F01027': 'HYUNDAI', 'NSE_EQ|INE090A01021': 'ICICIBANK', 'NSE_EQ|INE765G01017': 'ICICIGI', 'NSE_EQ|INE726G01019': 'ICICIPRULI', 'NSE_EQ|INE154A01025': 'ITC', 'NSE_EQ|INE053A01029': 'INDHOTEL', 'NSE_EQ|INE242A01010': 'IOC', 'NSE_EQ|INE053F01010': 'IRFC', 'NSE_EQ|INE095A01012': 'INDUSINDBK', 'NSE_EQ|INE663F01024': 'NAUKRI', 'NSE_EQ|INE009A01021': 'INFY', 'NSE_EQ|INE646L01027': 'INDIGO', 'NSE_EQ|INE121E01018': 'JSWENERGY', 'NSE_EQ|INE019A01038': 'JSWSTEEL', 'NSE_EQ|INE749A01030': 'JINDALSTEL', 'NSE_EQ|INE758E01017': 'JIOFIN', 'NSE_EQ|INE237A01028': 'KOTAKBANK', 'NSE_EQ|INE214T01019': 'LTIM', 'NSE_EQ|INE018A01030': 'LT', 'NSE_EQ|INE0J1Y01017': 'LICI', 'NSE_EQ|INE670K01029': 'LODHA', 'NSE_EQ|INE101A01026': 'M&M', 'NSE_EQ|INE585B01010': 'MARUTI', 'NSE_EQ|INE733E01010': 'NTPC', 'NSE_EQ|INE239A01024': 'NESTLEIND', 'NSE_EQ|INE213A01029': 'ONGC', 'NSE_EQ|INE318A01026': 'PIDILITIND', 'NSE_EQ|INE134E01011': 'PFC', 'NSE_EQ|INE752E01010': 'POWERGRID', 'NSE_EQ|INE160A01022': 'PNB', 'NSE_EQ|INE020B01018': 'RECLTD', 'NSE_EQ|INE002A01018': 'RELIANCE', 'NSE_EQ|INE123W01016': 'SBILIFE', 'NSE_EQ|INE775A01035': 'MOTHERSON', 'NSE_EQ|INE070A01015': 'SHREECEM', 'NSE_EQ|INE721A01047': 'SHRIRAMFIN', 'NSE_EQ|INE003A01024': 'SIEMENS', 'NSE_EQ|INE062A01020': 'SBIN', 'NSE_EQ|INE044A01036': 'SUNPHARMA', 'NSE_EQ|INE00H001014': 'SWIGGY', 'NSE_EQ|INE494B01023': 'TVSMOTOR', 'NSE_EQ|INE467B01029': 'TCS', 'NSE_EQ|INE192A01025': 'TATACONSUM', 'NSE_EQ|INE155A01022': 'TATAMOTORS', 'NSE_EQ|INE245A01021': 'TATAPOWER', 'NSE_EQ|INE081A01020': 'TATASTEEL', 'NSE_EQ|INE669C01036': 'TECHM', 'NSE_EQ|INE280A01028': 'TITAN', 'NSE_EQ|INE685A01028': 'TORNTPHARM', 'NSE_EQ|INE849A01020': 'TRENT', 'NSE_EQ|INE481G01011': 'ULTRACEMCO', 'NSE_EQ|INE854D01024': 'UNITDSPR', 'NSE_EQ|INE200M01039': 'VBL', 'NSE_EQ|INE205A01025': 'VEDL', 'NSE_EQ|INE075A01022': 'WIPRO', 'NSE_EQ|INE010B01027': 'ZYDUSLIFE'}

access_token = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI2VUJUN1ciLCJqdGkiOiI2ODEzZDgyMDc1Mzc2NTFlM2E0M2QyY2YiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlhdCI6MTc0NjEzMDk3NiwiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxNzQ2MTM2ODAwfQ.gb5GpoRZYlAKd2KtbzkcVtdZOQ2em6nOaq1q-WJ33p8"

unit = 'minutes'
interval = 5
day = '2025-04-30'
moving_average = 44
result = {}



def write_to_json(data, filename='sma_trend_output.json'):
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)
        
def check_trend(sma_values):
    if len(sma_values) < 15:
        return "Not enough data"
    
    
    first = sma_values[0]
    rest = sma_values[1:31]
    
    if all(first > val for val in rest):
        return "Rising"
    elif all(first < val for val in rest):
        return "Falling"
    else:
        return "Sideways"

def calculate_sma_series(prices, period,key):
    sma_values = []
    for i in range(len(prices)):
        if i + 1 >= period:
            sma = sum(prices[i + 1 - period : i + 1]) / period
            sma_values.append(sma)
            
    # print(sma_values)    
    trend = check_trend(sma_values)
    sa = sma_values[0]
    ul = (sa/100)*.1 + sa
    ll = sa - (sa/100)*.05
    
    if trend == 'Rising' and sa< prices[0] < ul :   
        print(sa,prices[0],ul)
        result[key]={
                "sma": round(sa, 2),
                "trend": trend,
                "symbol" : nifty100[key],
                "upperlim" : ul,
                "lowerlim": ll
            }
    
    return 


print(day)
for key in nifty100:
    # Get Yesterday's Date
    print(key)

    # API URL (From Date & To Date are both yesterday to get single day's data)
    url = f"https://api.upstox.com/v3/historical-candle/{key}/{unit}/{interval}/{day}/{day}"

    # Set headers
    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    # Make the API request
    response = requests.get(url, headers=headers)
    data = response.json()
    if (
        data.get("status") == "success"
        and "data" in data
        and "candles" in data["data"]
        and isinstance(data["data"]["candles"], list)
        and len(data["data"]["candles"]) > 0
    ):
        closing_prices = [candle[4] for candle in data["data"]["candles"]]
        # print(closing_prices)
        calculate_sma_series(closing_prices, moving_average, key)
    else:
        print(data)
        print(f"Skipping {key}: Invalid or empty data")
    
    
    write_to_json(result)
