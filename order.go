package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"my-upstox-client/marketdatafeedpb"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

var accessToken string = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI2VUJUN1ciLCJqdGkiOiI2ODExY2JhZTJiOTZjYzQyMGUxYjM1Y2MiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaWF0IjoxNzQ1OTk2NzE4LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3NDYwNTA0MDB9.YASe09215mICgy4XqD2Y5Ak-NRTZTzTe16X-H36rBZs"
var amount float64
var lev int

type Candle struct {
	Open      float64
	High      float64
	Low       float64
	Close     float64
	StartTime time.Time
}

var instrumentData = make(map[string]InstrumentDetails)

type InstrumentDetails struct {
	SMA    float64 `json:"sma"`
	Trend  string  `json:"trend"`
	Symbol string  `json:"symbol"`
	UL     float64 `json:"upperlim"`
	SL     float64 `json:"lowerlim"`
}

var candleData = make(map[string]*Candle)

var instrumentKeys []string

var dict = map[string]string{"MARUTI": "NSE_EQ|INE585B01010", "NATIONALUM": "NSE_EQ|INE139A01034", "HINDZINC": "NSE_EQ|INE267A01025", "INOXWIND": "NSE_EQ|INE066P01011", "LAURUSLABS": "NSE_EQ|INE947Q01028", "BAJAJFINSV": "NSE_EQ|INE918I01026", "JIOFIN": "NSE_EQ|INE758E01017", "MANAPPURAM": "NSE_EQ|INE522D01027", "DRREDDY": "NSE_EQ|INE089A01031", "DALBHARAT": "NSE_EQ|INE00R701025", "NHPC": "NSE_EQ|INE848E01016", "BAJAJ-AUTO": "NSE_EQ|INE917I01010", "SHREECEM": "NSE_EQ|INE070A01015", "PAYTM": "NSE_EQ|INE982J01020", "PAGEIND": "NSE_EQ|INE761H01022", "JINDALSTEL": "NSE_EQ|INE749A01030", "COFORGE": "NSE_EQ|INE591G01017", "TVSMOTOR": "NSE_EQ|INE494B01023", "PNB": "NSE_EQ|INE160A01022", "CDSL": "NSE_EQ|INE736A01011", "INDIGO": "NSE_EQ|INE646L01027", "ZYDUSLIFE": "NSE_EQ|INE010B01027", "GODREJ": "NSE_EQ|INE102D01028", "EXIDEIND": "NSE_EQ|INE302A01020", "PFC": "NSE_EQ|INE134E01011", "INFY": "NSE_EQ|INE009A01021", "BIOCON": "NSE_EQ|INE376G01013", "PATANJALI": "NSE_EQ|INE619A01035", "BHARATFORG": "NSE_EQ|INE465A01025", "BERGEPAINT": "NSE_EQ|INE463A01038", "BHARTIARTL": "NSE_EQ|INE397D01024", "DMART": "NSE_EQ|INE192R01011", "ALKEM": "NSE_EQ|INE540L01014", "MOTHERSON": "NSE_EQ|INE775A01035", "KOTAKBANK": "NSE_EQ|INE237A01028", "CIPLA": "NSE_EQ|INE059A01026", "ANGELONE": "NSE_EQ|INE732I01013", "DIVISLAB": "NSE_EQ|INE361B01024", "JUBLFOOD": "NSE_EQ|INE797F01020", "PRESTIGE": "NSE_EQ|INE811K01011", "MFSL": "NSE_EQ|INE180A01020", "AUBANK": "NSE_EQ|INE949L01017", "OFSS": "NSE_EQ|INE881D01027", "HINDUNILVR": "NSE_EQ|INE030A01027", "HDFCLIFE": "NSE_EQ|INE795G01014", "CANBK": "NSE_EQ|INE476A01022", "MCX": "NSE_EQ|INE745G01035", "HINDCOPPER": "NSE_EQ|INE531E01026", "JKCEMENT": "NSE_EQ|INE823G01014", "SHRIRAMFIN": "NSE_EQ|INE721A01047", "BANKBARODA": "NSE_EQ|INE028A01039", "LODHA": "NSE_EQ|INE670K01029", "TITAN": "NSE_EQ|INE280A01028", "HEROMOTOCO": "NSE_EQ|INE158A01026", "SBILIFE": "NSE_EQ|INE123W01016", "CUMMINSIND": "NSE_EQ|INE298A01020", "TATACONSUM": "NSE_EQ|INE192A01025", "AARTIIND": "NSE_EQ|INE769A01020", "SYNGENE": "NSE_EQ|INE398R01022", "TATAMOTORS": "NSE_EQ|INE155A01022", "ABCAPITAL": "NSE_EQ|INE674K01013", "HINDPETRO": "NSE_EQ|INE094A01015", "OIL": "NSE_EQ|INE274J01014", "YESBANK": "NSE_EQ|INE528G01035", "OBEROIRLTY": "NSE_EQ|INE093I01010", "ICICIPRULI": "NSE_EQ|INE726G01019", "ACC": "NSE_EQ|INE012A01025", "SONACOMS": "NSE_EQ|INE073K01018", "INDUSINDBK": "NSE_EQ|INE095A01012", "ASTRAL": "NSE_EQ|INE006I01046", "INDIANB": "NSE_EQ|INE562A01011", "SUPREMEIND": "NSE_EQ|INE195A01028", "TATATECH": "NSE_EQ|INE142M01025", "TRENT": "NSE_EQ|INE849A01020", "TECHM": "NSE_EQ|INE669C01036", "CYIENT": "NSE_EQ|INE136B01020", "BRITANNIA": "NSE_EQ|INE216A01030", "MGL": "NSE_EQ|INE002S01010", "CONCOR": "NSE_EQ|INE111A01025", "SBIN": "NSE_EQ|INE062A01020", "BSE": "NSE_EQ|INE118H01025", "ADANIGREEN": "NSE_EQ|INE364U01010", "AXISBANK": "NSE_EQ|INE238A01034", "TATASTEEL": "NSE_EQ|INE081A01020", "SUNPHARMA": "NSE_EQ|INE044A01036", "MRF": "NSE_EQ|INE883A01011", "WIPRO": "NSE_EQ|INE075A01022", "LTF": "NSE_EQ|INE498L01015", "DIXON": "NSE_EQ|INE935N01020", "SJVN": "NSE_EQ|INE002L01015", "HINDALCO": "NSE_EQ|INE038A01020", "GODREJPROP": "NSE_EQ|INE484J01027", "HUDCO": "NSE_EQ|INE031A01017", "IOC": "NSE_EQ|INE242A01010", "VEDL": "NSE_EQ|INE205A01025", "MAXHEALTH": "NSE_EQ|INE027H01010", "UNIONBANK": "NSE_EQ|INE692A01016", "KPITTECH": "NSE_EQ|INE04I401011", "GRANULES": "NSE_EQ|INE101D01020", "BEL": "NSE_EQ|INE263A01024", "RECLTD": "NSE_EQ|INE020B01018", "TORNTPHARM": "NSE_EQ|INE685A01028", "SRF": "NSE_EQ|INE647A01010", "CHOLAFIN": "NSE_EQ|INE121A08PJ0", "HCLTECH": "NSE_EQ|INE860A01027", "TIINDIA": "NSE_EQ|INE974X01010", "UNITDSPR": "NSE_EQ|INE854D01024", "JSL": "NSE_EQ|INE220G01021", "ADANIPORTS": "NSE_EQ|INE742F01042", "VOLTAS": "NSE_EQ|INE226A01021", "FEDERALBNK": "NSE_EQ|INE171A01029", "RBLBANK": "NSE_EQ|INE976G01028", "GRASIM": "NSE_EQ|INE047A01021", "LUPIN": "NSE_EQ|INE326A01037", "PERSISTENT": "NSE_EQ|INE262H01021", "NMDC": "NSE_EQ|INE584A01023", "BANKINDIA": "NSE_EQ|INE084A01016", "CHAMBLFERT": "NSE_EQ|INE085A01013", "KEI": "NSE_EQ|INE878B01027", "BSOFT": "NSE_EQ|INE836A01035", "HFCL": "NSE_EQ|INE548A01028", "MUTHOOTFIN": "NSE_EQ|INE414G01012", "SBICARD": "NSE_EQ|INE018E01016", "IDEA": "NSE_EQ|INE669E01016", "GMRAIRPORT": "NSE_EQ|INE776C01039", "PHOENIXLTD": "NSE_EQ|INE211B01039", "POLICYBZR": "NSE_EQ|INE417T01026", "TORNTPOWER": "NSE_EQ|INE813H01021", "NCC": "NSE_EQ|INE868B01028", "ONGC": "NSE_EQ|INE213A01029", "IRCTC": "NSE_EQ|INE335Y01020", "ADANIENSOL": "NSE_EQ|INE931S01010", "IRB": "NSE_EQ|INE821I01022", "IRFC": "NSE_EQ|INE053F01010", "BOSCHLTD": "NSE_EQ|INE323A01026", "HDFCAMC": "NSE_EQ|INE127D01025", "ASIANPAINT": "NSE_EQ|INE021A01026", "MPHASIS": "NSE_EQ|INE356A01018", "NTPC": "NSE_EQ|INE733E01010", "LTIM": "NSE_EQ|INE214T01019", "HAVELLS": "NSE_EQ|INE176B01034", "IEX": "NSE_EQ|INE022Q01020", "BANDHANBNK": "NSE_EQ|INE545U01014", "POONAWALLA": "NSE_EQ|INE511C01022", "LICHSGFIN": "NSE_EQ|INE115A01026", "CAMS": "NSE_EQ|INE596I01012", "APLAPOLLO": "NSE_EQ|INE702C01027", "SOLARINDS": "NSE_EQ|INE343H01029", "NYKAA": "NSE_EQ|INE388Y01029", "ABB": "NSE_EQ|INE117A01022", "IIFL": "NSE_EQ|INE530B01024", "NESTLEIND": "NSE_EQ|INE239A01024", "ZOMATO": "NSE_EQ|INE758T01015", "ITC": "NSE_EQ|INE154A01025", "POLYCAB": "NSE_EQ|INE455K01017", "AUROPHARMA": "NSE_EQ|INE406A01037", "M&M": "NSE_EQ|INE101A01026", "APOLLOHOSP": "NSE_EQ|INE437A01024", "ASHOKLEY": "NSE_EQ|INE208A01029", "KALYANKJIL": "NSE_EQ|INE303R01014", "TATAPOWER": "NSE_EQ|INE245A01021", "DEEPAKNTR": "NSE_EQ|INE288B01029", "DELHIVERY": "NSE_EQ|INE148O01028", "RAMCOCEM": "NSE_EQ|INE331A01037", "INDHOTEL": "NSE_EQ|INE053A01029", "ICICIBANK": "NSE_EQ|INE090A01021", "UPL": "NSE_EQ|INE628A01036", "MARICO": "NSE_EQ|INE196A01026", "BALKRISIND": "NSE_EQ|INE787D01026", "LT": "NSE_EQ|INE018A01030", "INDUSTOWER": "NSE_EQ|INE121J01017", "PEL": "NSE_EQ|INE140A01024", "ATGL": "NSE_EQ|INE399L01023", "IDFCFIRSTB": "NSE_EQ|INE092T01019", "PETRONET": "NSE_EQ|INE347G01014", "CGPOWER": "NSE_EQ|INE067A01029", "APOLLOTYRE": "NSE_EQ|INE438A01022", "TITAGARH": "NSE_EQ|INE615H01020", "ADANIENT": "NSE_EQ|INE423A01024", "JSWENERGY": "NSE_EQ|INE121E01018", "JSWSTEEL": "NSE_EQ|INE019A01038", "TATACOMM": "NSE_EQ|INE151A01013", "COLPAL": "NSE_EQ|INE259A01022", "COALINDIA": "NSE_EQ|INE522F01014", "NBCC": "NSE_EQ|INE095N01031", "BAJFINANCE": "NSE_EQ|INE296A01024", "ICICIGI": "NSE_EQ|INE765G01017", "HAL": "NSE_EQ|INE066F01020", "BHEL": "NSE_EQ|INE257A01026", "RELIANCE": "NSE_EQ|INE002A01018", "IGL": "NSE_EQ|INE203G01027", "TCS": "NSE_EQ|INE467B01029", "M&MFIN": "NSE_EQ|INE774D08MG3", "ABFRL": "NSE_EQ|INE647O01011", "AMBUJACEM": "NSE_EQ|INE079A01024", "GAIL": "NSE_EQ|INE129A01019", "LICI": "NSE_EQ|INE0J1Y01017", "ULTRACEMCO": "NSE_EQ|INE481G01011", "CROMPTON": "NSE_EQ|INE299U01018", "HDFCBANK": "NSE_EQ|INE040A01034", "SAIL": "NSE_EQ|INE114A01011", "CESC": "NSE_EQ|INE486A01021", "GLENMARK": "NSE_EQ|INE935A01035", "PIIND": "NSE_EQ|INE603J01030", "SIEMENS": "NSE_EQ|INE003A01024", "IREDA": "NSE_EQ|INE202E01016", "EICHERMOT": "NSE_EQ|INE066A01021", "BPCL": "NSE_EQ|INE029A01011", "TATAELXSI": "NSE_EQ|INE670A01012", "NAUKRI": "NSE_EQ|INE663F01024", "POWERGRID": "NSE_EQ|INE752E01010", "TATACHEM": "NSE_EQ|INE092A01019", "DLF": "NSE_EQ|INE271C01023", "PIDILITIND": "NSE_EQ|INE318A01026", "VBL": "NSE_EQ|INE200M01039", "DABUR": "NSE_EQ|INE016A01026", "ESCORTS": "NSE_EQ|INE042A01014"}
var dict2 = map[string]string{"NSE_EQ|INE585B01010": "MARUTI", "NSE_EQ|INE139A01034": "NATIONALUM", "NSE_EQ|INE267A01025": "HINDZINC", "NSE_EQ|INE066P01011": "INOXWIND", "NSE_EQ|INE947Q01028": "LAURUSLABS", "NSE_EQ|INE918I01026": "BAJAJFINSV", "NSE_EQ|INE758E01017": "JIOFIN", "NSE_EQ|INE522D01027": "MANAPPURAM", "NSE_EQ|INE089A01031": "DRREDDY", "NSE_EQ|INE00R701025": "DALBHARAT", "NSE_EQ|INE848E01016": "NHPC", "NSE_EQ|INE917I01010": "BAJAJ-AUTO", "NSE_EQ|INE070A01015": "SHREECEM", "NSE_EQ|INE982J01020": "PAYTM", "NSE_EQ|INE761H01022": "PAGEIND", "NSE_EQ|INE749A01030": "JINDALSTEL", "NSE_EQ|INE591G01017": "COFORGE", "NSE_EQ|INE494B01023": "TVSMOTOR", "NSE_EQ|INE160A01022": "PNB", "NSE_EQ|INE736A01011": "CDSL", "NSE_EQ|INE646L01027": "INDIGO", "NSE_EQ|INE010B01027": "ZYDUSLIFE", "NSE_EQ|INE102D01028": "GODREJ", "NSE_EQ|INE302A01020": "EXIDEIND", "NSE_EQ|INE134E01011": "PFC", "NSE_EQ|INE009A01021": "INFY", "NSE_EQ|INE376G01013": "BIOCON", "NSE_EQ|INE619A01035": "PATANJALI", "NSE_EQ|INE465A01025": "BHARATFORG", "NSE_EQ|INE463A01038": "BERGEPAINT", "NSE_EQ|INE397D01024": "BHARTIARTL", "NSE_EQ|INE192R01011": "DMART", "NSE_EQ|INE540L01014": "ALKEM", "NSE_EQ|INE775A01035": "MOTHERSON", "NSE_EQ|INE237A01028": "KOTAKBANK", "NSE_EQ|INE059A01026": "CIPLA", "NSE_EQ|INE732I01013": "ANGELONE", "NSE_EQ|INE361B01024": "DIVISLAB", "NSE_EQ|INE797F01020": "JUBLFOOD", "NSE_EQ|INE811K01011": "PRESTIGE", "NSE_EQ|INE180A01020": "MFSL", "NSE_EQ|INE949L01017": "AUBANK", "NSE_EQ|INE881D01027": "OFSS", "NSE_EQ|INE030A01027": "HINDUNILVR", "NSE_EQ|INE795G01014": "HDFCLIFE", "NSE_EQ|INE476A01022": "CANBK", "NSE_EQ|INE745G01035": "MCX", "NSE_EQ|INE531E01026": "HINDCOPPER", "NSE_EQ|INE823G01014": "JKCEMENT", "NSE_EQ|INE721A01047": "SHRIRAMFIN", "NSE_EQ|INE028A01039": "BANKBARODA", "NSE_EQ|INE670K01029": "LODHA", "NSE_EQ|INE280A01028": "TITAN", "NSE_EQ|INE158A01026": "HEROMOTOCO", "NSE_EQ|INE123W01016": "SBILIFE", "NSE_EQ|INE298A01020": "CUMMINSIND", "NSE_EQ|INE192A01025": "TATACONSUM", "NSE_EQ|INE769A01020": "AARTIIND", "NSE_EQ|INE398R01022": "SYNGENE", "NSE_EQ|INE155A01022": "TATAMOTORS", "NSE_EQ|INE674K01013": "ABCAPITAL", "NSE_EQ|INE094A01015": "HINDPETRO", "NSE_EQ|INE274J01014": "OIL", "NSE_EQ|INE528G01035": "YESBANK", "NSE_EQ|INE093I01010": "OBEROIRLTY", "NSE_EQ|INE726G01019": "ICICIPRULI", "NSE_EQ|INE012A01025": "ACC", "NSE_EQ|INE073K01018": "SONACOMS", "NSE_EQ|INE095A01012": "INDUSINDBK", "NSE_EQ|INE006I01046": "ASTRAL", "NSE_EQ|INE562A01011": "INDIANB", "NSE_EQ|INE195A01028": "SUPREMEIND", "NSE_EQ|INE142M01025": "TATATECH", "NSE_EQ|INE849A01020": "TRENT", "NSE_EQ|INE669C01036": "TECHM", "NSE_EQ|INE136B01020": "CYIENT", "NSE_EQ|INE216A01030": "BRITANNIA", "NSE_EQ|INE002S01010": "MGL", "NSE_EQ|INE111A01025": "CONCOR", "NSE_EQ|INE062A01020": "SBIN", "NSE_EQ|INE118H01025": "BSE", "NSE_EQ|INE364U01010": "ADANIGREEN", "NSE_EQ|INE238A01034": "AXISBANK", "NSE_EQ|INE081A01020": "TATASTEEL", "NSE_EQ|INE044A01036": "SUNPHARMA", "NSE_EQ|INE883A01011": "MRF", "NSE_EQ|INE075A01022": "WIPRO", "NSE_EQ|INE498L01015": "LTF", "NSE_EQ|INE935N01020": "DIXON", "NSE_EQ|INE002L01015": "SJVN", "NSE_EQ|INE038A01020": "HINDALCO", "NSE_EQ|INE484J01027": "GODREJPROP", "NSE_EQ|INE031A01017": "HUDCO", "NSE_EQ|INE242A01010": "IOC", "NSE_EQ|INE205A01025": "VEDL", "NSE_EQ|INE027H01010": "MAXHEALTH", "NSE_EQ|INE692A01016": "UNIONBANK", "NSE_EQ|INE04I401011": "KPITTECH", "NSE_EQ|INE101D01020": "GRANULES", "NSE_EQ|INE263A01024": "BEL", "NSE_EQ|INE020B01018": "RECLTD", "NSE_EQ|INE685A01028": "TORNTPHARM", "NSE_EQ|INE647A01010": "SRF", "NSE_EQ|INE121A08PJ0": "CHOLAFIN", "NSE_EQ|INE860A01027": "HCLTECH", "NSE_EQ|INE974X01010": "TIINDIA", "NSE_EQ|INE854D01024": "UNITDSPR", "NSE_EQ|INE220G01021": "JSL", "NSE_EQ|INE742F01042": "ADANIPORTS", "NSE_EQ|INE226A01021": "VOLTAS", "NSE_EQ|INE171A01029": "FEDERALBNK", "NSE_EQ|INE976G01028": "RBLBANK", "NSE_EQ|INE047A01021": "GRASIM", "NSE_EQ|INE326A01037": "LUPIN", "NSE_EQ|INE262H01021": "PERSISTENT", "NSE_EQ|INE584A01023": "NMDC", "NSE_EQ|INE084A01016": "BANKINDIA", "NSE_EQ|INE085A01013": "CHAMBLFERT", "NSE_EQ|INE878B01027": "KEI", "NSE_EQ|INE836A01035": "BSOFT", "NSE_EQ|INE548A01028": "HFCL", "NSE_EQ|INE414G01012": "MUTHOOTFIN", "NSE_EQ|INE018E01016": "SBICARD", "NSE_EQ|INE669E01016": "IDEA", "NSE_EQ|INE776C01039": "GMRAIRPORT", "NSE_EQ|INE211B01039": "PHOENIXLTD", "NSE_EQ|INE417T01026": "POLICYBZR", "NSE_EQ|INE813H01021": "TORNTPOWER", "NSE_EQ|INE868B01028": "NCC", "NSE_EQ|INE213A01029": "ONGC", "NSE_EQ|INE335Y01020": "IRCTC", "NSE_EQ|INE931S01010": "ADANIENSOL", "NSE_EQ|INE821I01022": "IRB", "NSE_EQ|INE053F01010": "IRFC", "NSE_EQ|INE323A01026": "BOSCHLTD", "NSE_EQ|INE127D01025": "HDFCAMC", "NSE_EQ|INE021A01026": "ASIANPAINT", "NSE_EQ|INE356A01018": "MPHASIS", "NSE_EQ|INE733E01010": "NTPC", "NSE_EQ|INE214T01019": "LTIM", "NSE_EQ|INE176B01034": "HAVELLS", "NSE_EQ|INE022Q01020": "IEX", "NSE_EQ|INE545U01014": "BANDHANBNK", "NSE_EQ|INE511C01022": "POONAWALLA", "NSE_EQ|INE115A01026": "LICHSGFIN", "NSE_EQ|INE596I01012": "CAMS", "NSE_EQ|INE702C01027": "APLAPOLLO", "NSE_EQ|INE343H01029": "SOLARINDS", "NSE_EQ|INE388Y01029": "NYKAA", "NSE_EQ|INE117A01022": "ABB", "NSE_EQ|INE530B01024": "IIFL", "NSE_EQ|INE239A01024": "NESTLEIND", "NSE_EQ|INE758T01015": "ZOMATO", "NSE_EQ|INE154A01025": "ITC", "NSE_EQ|INE455K01017": "POLYCAB", "NSE_EQ|INE406A01037": "AUROPHARMA", "NSE_EQ|INE101A01026": "M&M", "NSE_EQ|INE437A01024": "APOLLOHOSP", "NSE_EQ|INE208A01029": "ASHOKLEY", "NSE_EQ|INE303R01014": "KALYANKJIL", "NSE_EQ|INE245A01021": "TATAPOWER", "NSE_EQ|INE288B01029": "DEEPAKNTR", "NSE_EQ|INE148O01028": "DELHIVERY", "NSE_EQ|INE331A01037": "RAMCOCEM", "NSE_EQ|INE053A01029": "INDHOTEL", "NSE_EQ|INE090A01021": "ICICIBANK", "NSE_EQ|INE628A01036": "UPL", "NSE_EQ|INE196A01026": "MARICO", "NSE_EQ|INE787D01026": "BALKRISIND", "NSE_EQ|INE018A01030": "LT", "NSE_EQ|INE121J01017": "INDUSTOWER", "NSE_EQ|INE140A01024": "PEL", "NSE_EQ|INE399L01023": "ATGL", "NSE_EQ|INE092T01019": "IDFCFIRSTB", "NSE_EQ|INE347G01014": "PETRONET", "NSE_EQ|INE067A01029": "CGPOWER", "NSE_EQ|INE438A01022": "APOLLOTYRE", "NSE_EQ|INE615H01020": "TITAGARH", "NSE_EQ|INE423A01024": "ADANIENT", "NSE_EQ|INE121E01018": "JSWENERGY", "NSE_EQ|INE019A01038": "JSWSTEEL", "NSE_EQ|INE151A01013": "TATACOMM", "NSE_EQ|INE259A01022": "COLPAL", "NSE_EQ|INE522F01014": "COALINDIA", "NSE_EQ|INE095N01031": "NBCC", "NSE_EQ|INE296A01024": "BAJFINANCE", "NSE_EQ|INE765G01017": "ICICIGI", "NSE_EQ|INE066F01020": "HAL", "NSE_EQ|INE257A01026": "BHEL", "NSE_EQ|INE002A01018": "RELIANCE", "NSE_EQ|INE203G01027": "IGL", "NSE_EQ|INE467B01029": "TCS", "NSE_EQ|INE774D08MG3": "M&MFIN", "NSE_EQ|INE647O01011": "ABFRL", "NSE_EQ|INE079A01024": "AMBUJACEM", "NSE_EQ|INE129A01019": "GAIL", "NSE_EQ|INE0J1Y01017": "LICI", "NSE_EQ|INE481G01011": "ULTRACEMCO", "NSE_EQ|INE299U01018": "CROMPTON", "NSE_EQ|INE040A01034": "HDFCBANK", "NSE_EQ|INE114A01011": "SAIL", "NSE_EQ|INE486A01021": "CESC", "NSE_EQ|INE935A01035": "GLENMARK", "NSE_EQ|INE603J01030": "PIIND", "NSE_EQ|INE003A01024": "SIEMENS", "NSE_EQ|INE202E01016": "IREDA", "NSE_EQ|INE066A01021": "EICHERMOT", "NSE_EQ|INE029A01011": "BPCL", "NSE_EQ|INE670A01012": "TATAELXSI", "NSE_EQ|INE663F01024": "NAUKRI", "NSE_EQ|INE752E01010": "POWERGRID", "NSE_EQ|INE092A01019": "TATACHEM", "NSE_EQ|INE271C01023": "DLF", "NSE_EQ|INE318A01026": "PIDILITIND", "NSE_EQ|INE200M01039": "VBL", "NSE_EQ|INE016A01026": "DABUR", "NSE_EQ|INE042A01014": "ESCORTS"}

var (
	bullish    = &SafeSlice{}
	placed     = &SafeSlice{}
	insKeys    = &SafeSlice{}
	closetosma = &SafeSlice{}
)

var order10 = make(map[string]Order10)
var order15 = make(map[string]Order10)
var order20 = make(map[string]Order10)

type Order10 struct {
	SL       float64 `json:"sl"`
	USL      float64 `json:"usl"`
	SLGAP    float64 `json:"slgap"`
	OPRICE   float64 `json:"oprice"`
	ORDERID  string  `json:"orderid"`
	OTYPE    string  `json:"otype"`
	QUANTITY int     `json:"quantity"`
}

var stopflag = true
var stopflag1 = true
var stopflag2 = true

type Response struct {
	Status string `json:"status"`
	Data   struct {
		OrderID string `json:"order_id"`
	} `json:"data"`
}

var stockChannel1 = make(chan struct {
	Instrument string
	Price      float64
}, 10000) // Large buffer to avoid blocking

var stockChannel2 = make(chan struct {
	Instrument string
	Price      float64
}, 10000) // Large buffer to avoid blocking

type SafeSlice struct {
	items []string
	mux   sync.RWMutex
}

func (s *SafeSlice) Contains(item string) bool {
	s.mux.RLock()
	defer s.mux.RUnlock()
	for _, v := range s.items {
		if v == item {
			return true
		}
	}
	return false
}

func (s *SafeSlice) Append(item string) {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.items = append(s.items, item)
}

func getIntervalStart(t time.Time) time.Time {
	minutes := (t.Minute() / 5) * 5
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), minutes, 0, 0, t.Location())
}

const (
	authURL = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
)

type MarketDataClient struct {
	conn      *websocket.Conn
	mu        sync.RWMutex
	ctx       context.Context
	cancel    context.CancelFunc
	connected bool
}

func NewMarketDataClient() *MarketDataClient {
	ctx, cancel := context.WithCancel(context.Background())
	return &MarketDataClient{
		ctx:    ctx,
		cancel: cancel,
	}
}

func (c *MarketDataClient) startPingLoop() {
	ticker := time.NewTicker(25 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			if c.connected {
				c.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second))
			}
			c.mu.Unlock()
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *MarketDataClient) Connect() error {
	wsURL, err := getWebSocketURL()
	if err != nil {
		return fmt.Errorf("failed to get WebSocket URL: %v", err)
	}

	log.Printf("Connecting to WebSocket endpoint: %s", wsURL)

	dialer := websocket.Dialer{
		HandshakeTimeout: 15 * time.Second, // Reduce from 1800s
		NetDialContext: (&net.Dialer{
			KeepAlive: 30 * time.Second, // TCP keepalive
		}).DialContext,
	}

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+accessToken)
	headers.Add("Accept", "*/*")

	conn, _, err := dialer.Dial(wsURL, headers)
	if err != nil {
		return fmt.Errorf("WebSocket connection failed: %v", err)
	}

	c.mu.Lock()
	c.conn = conn
	c.connected = true
	c.mu.Unlock()

	log.Println("WebSocket connection established successfully")
	go c.startPingLoop()
	return nil
}

func (c *MarketDataClient) Subscribe(instrumentKeys []string, mode string) error {
	if !c.connected {
		return fmt.Errorf("not connected to WebSocket")
	}

	if len(instrumentKeys) == 0 {
		return fmt.Errorf("no instrument keys provided")
	}

	// Validate mode
	validModes := map[string]bool{
		"ltpc":          true,
		"option_greeks": true,
		"full":          true,
	}
	if !validModes[mode] {
		return fmt.Errorf("invalid mode: %s", mode)
	}

	subscription := map[string]interface{}{
		"guid":   generateGUID(),
		"method": "sub",
		"data": map[string]interface{}{
			"mode":           mode,
			"instrumentKeys": instrumentKeys,
		},
	}

	payload, err := json.Marshal(subscription)
	if err != nil {
		return fmt.Errorf("failed to marshal subscription: %v", err)
	}

	log.Printf("Sending subscription request: %s", string(payload))

	c.mu.Lock()
	defer c.mu.Unlock()

	// Add write timeout
	c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	err = c.conn.WriteMessage(websocket.BinaryMessage, payload)
	c.conn.SetWriteDeadline(time.Time{}) // Clear deadline

	if err != nil {
		return fmt.Errorf("failed to send subscription: %v", err)
	}

	return nil
}
func (c *MarketDataClient) reconnect() {
	c.Disconnect()
	time.Sleep(2 * time.Second)

	for retry := 0; retry < 3; retry++ {
		if err := c.Connect(); err == nil {
			time.Sleep(2 * time.Second)
			if err := c.Subscribe(instrumentKeys, "full"); err != nil {
				log.Printf("Failed to subscribe after reconnect: %v", err)
				continue
			}
			log.Println("Successfully resubscribed after reconnect")
			// Restart the reading loop
			go c.StartReading()
			return
		}
		time.Sleep(time.Duration(retry+1) * 5 * time.Second)
	}
	log.Println("Max reconnection attempts reached")
}

func (c *MarketDataClient) StartReading() {
	defer func() {
		if r := recover(); r != nil {
			log.Println("Recovered in StartReading:", r)
			c.reconnect()
		}
	}()

	if !c.connected {
		log.Println("Not connected, cannot start reading")
		return
	}

	log.Println("Starting to read messages...")

	for {
		select {
		case <-c.ctx.Done():
			log.Println("Stopping message reader")
			return
		default:
			c.conn.SetReadDeadline(time.Now().Add(120 * time.Second))
			messageType, message, err := c.conn.ReadMessage()
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					log.Println("Read timeout - reconnecting...")
					c.reconnect()
					return
				}
				if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
					log.Println("WebSocket closed normally")
					return
				}
				log.Printf("Error reading message: %v - reconnecting...", err)
				c.reconnect()
				return
			}

			switch messageType {
			case websocket.BinaryMessage:
				c.handleBinaryMessage(message)
			case websocket.PingMessage:
				log.Println("Received ping message")
				c.conn.WriteMessage(websocket.PongMessage, nil)
			case websocket.PongMessage:
				log.Println("Received pong message")
			case websocket.CloseMessage:
				log.Println("Received close message")
				c.Disconnect()
				return
			default:
				log.Printf("Received unexpected message type: %d", messageType)
			}
		}
	}
}

func (c *MarketDataClient) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected
}

func (c *MarketDataClient) handleBinaryMessage(message []byte) {
	var feedResponse marketdatafeedpb.FeedResponse

	if err := proto.Unmarshal(message, &feedResponse); err != nil {
		log.Printf("Failed to decode Protobuf: %v", err)
		return
	}

	switch feedResponse.GetType() {
	case marketdatafeedpb.Type_market_info:
		c.handleMarketInfo(&feedResponse)
	case marketdatafeedpb.Type_initial_feed, marketdatafeedpb.Type_live_feed:
		c.handleMarketData(&feedResponse)
	default:
		log.Printf("Received unknown message type: %v", feedResponse.GetType())
	}
}

func (c *MarketDataClient) handleMarketInfo(feedResponse *marketdatafeedpb.FeedResponse) {
	if marketInfo := feedResponse.GetMarketInfo(); marketInfo != nil {
		log.Println("------ Market Status ------")
		for segment, status := range marketInfo.GetSegmentStatus() {
			log.Printf("%s: %s\n", segment, status.String())
		}
		log.Println("--------------------------")
	}
}

func buyten(ltp float64, ins string) {
	url := "https://api-hft.upstox.com/v2/order/place"
	method := "POST"

	quant := int(amount/ltp) * lev
	sloss := ltp * .9965

	payload := fmt.Sprintf(`{
		"quantity": "%d",
		"product": "I",
		"validity": "DAY",
		"price": 0,
		"tag": "string",
		"instrument_token": "%s",
		"order_type": "MARKET",
		"transaction_type": "BUY",
		"disclosed_quantity": 0,
		"trigger_price": 0,
		"is_amo": false
		}`, quant, ins)

	client := &http.Client{}
	req, err := http.NewRequest(method, url, strings.NewReader(payload))

	if err != nil {
		log.Println(err)
		return
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", "Bearer "+accessToken)

	res, err := client.Do(req)
	if err != nil {
		log.Println(err)
		return
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Println(err)
		return
	}
	log.Println("order for", ltp, ins)
	// Parse the JSON response
	var response Response
	err = json.Unmarshal(body, &response)
	if err != nil {
		log.Println("Error parsing JSON:", err)
		return
	}

	// Print the entire response for debugging
	log.Println("Full response:", string(body))

	// Extract and print the order ID
	if response.Status == "success" {
		order10[ins] = Order10{
			SL:       sloss,
			USL:      sloss,
			OPRICE:   ltp,
			ORDERID:  response.Data.OrderID,
			OTYPE:    "BUY",
			QUANTITY: quant,
			SLGAP:    .0035 * ltp,
		}

		stopflag = false
		placed.Append(ins)
		log.Println(order10[ins])

	} else {
		log.Println("Order placement was not successful")
	}

}

func sellten(ltp float64, ins string) {
	url := "https://api-hft.upstox.com/v2/order/place"
	method := "POST"

	quant := int(amount/ltp) * lev
	sloss := ltp * 1.0035
	payload := fmt.Sprintf(`{
		"quantity": "%d",
		"product": "I",
		"validity": "DAY",
		"price": 0,
		"tag": "string",
		"instrument_token": "%s",
		"order_type": "MARKET",
		"transaction_type": "SELL",
		"disclosed_quantity": 0,
		"trigger_price": 0,
		"is_amo": false
		}`, quant, ins)

	client := &http.Client{}
	req, err := http.NewRequest(method, url, strings.NewReader(payload))

	if err != nil {
		log.Println(err)
		return
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", "Bearer "+accessToken)

	res, err := client.Do(req)
	if err != nil {
		log.Println(err)
		return
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Println(err)
		return
	}
	log.Println("order for", ltp, ins)
	// Parse the JSON response
	var response Response
	err = json.Unmarshal(body, &response)
	if err != nil {
		log.Println("Error parsing JSON:", err)
		return
	}

	// Print the entire response for debugging
	log.Println("Full response:", string(body))

	// Extract and print the order ID
	if response.Status == "success" {
		order10[ins] = Order10{
			SL:       sloss,
			USL:      sloss,
			OPRICE:   ltp,
			ORDERID:  response.Data.OrderID,
			OTYPE:    "SELL",
			QUANTITY: quant,
			SLGAP:    .0035 * ltp,
		}

		stopflag = false
		placed.Append(ins)
		log.Println(order10[ins])

	} else {
		log.Println("Order placement was not successful")
	}

}

func stoplossten(ltp float64, ins string) {
	if order10[ins].OTYPE == "BUY" {
		if ltp >= (order10[ins].OPRICE + order10[ins].SLGAP) {

			temp := order10[ins]
			temp.USL += temp.SLGAP
			temp.OPRICE += temp.SLGAP
			order10[ins] = temp
			log.Println(order10[ins])

		}
		if ltp <= order10[ins].USL {
			url := "https://api-hft.upstox.com/v2/order/place"
			method := "POST"

			payload := fmt.Sprintf(`{
				"quantity": "%d",
				"product": "I",
				"validity": "DAY",
				"price": 0,
				"tag": "string",
				"instrument_token": "%s",
				"order_type": "MARKET",
				"transaction_type": "SELL",
				"disclosed_quantity": 0,
				"trigger_price": "0",
				"is_amo": false
				}`, order10[ins].QUANTITY, ins)

			client := &http.Client{}
			req, err := http.NewRequest(method, url, strings.NewReader(payload))

			if err != nil {
				log.Println(err)
				return
			}
			req.Header.Add("Content-Type", "application/json")
			req.Header.Add("Authorization", "Bearer "+accessToken)

			res, err := client.Do(req)
			if err != nil {
				log.Println(err)
				return
			}
			defer res.Body.Close()

			body, err := ioutil.ReadAll(res.Body)
			if err != nil {
				log.Println(err)
				return
			}

			// Parse the JSON response
			var response Response
			err = json.Unmarshal(body, &response)
			if err != nil {
				log.Println("Error parsing JSON:", err)
				return
			}

			// Print the entire response for debugging
			log.Println("Full response:", string(body))
			if response.Status == "success" {
				stopflag = true
				log.Println(order10[ins], ltp, ins)

			}

		}
	} else {
		if ltp <= (order10[ins].OPRICE - order10[ins].SLGAP) {

			temp := order10[ins] // Get a copy of the struct
			temp.USL -= temp.SLGAP
			temp.OPRICE -= temp.SLGAP
			order10[ins] = temp
			log.Println(order10[ins])

		}
		if ltp >= order10[ins].USL {
			url := "https://api-hft.upstox.com/v2/order/place"
			method := "POST"

			payload := fmt.Sprintf(`{
				"quantity": "%d",
				"product": "I",
				"validity": "DAY",
				"price": 0,
				"tag": "string",
				"instrument_token": "%s",
				"order_type": "MARKET",
				"transaction_type": "BUY",
				"disclosed_quantity": 0,
				"trigger_price": "0",
				"is_amo": false
				}`, order10[ins].QUANTITY, ins)

			client := &http.Client{}
			req, err := http.NewRequest(method, url, strings.NewReader(payload))

			if err != nil {
				log.Println(err)
				return
			}
			req.Header.Add("Content-Type", "application/json")
			req.Header.Add("Authorization", "Bearer "+accessToken)

			res, err := client.Do(req)
			if err != nil {
				log.Println(err)
				return
			}
			defer res.Body.Close()

			body, err := ioutil.ReadAll(res.Body)
			if err != nil {
				log.Println(err)
				return
			}

			// Parse the JSON response
			var response Response
			err = json.Unmarshal(body, &response)
			if err != nil {
				log.Println("Error parsing JSON:", err)
				return
			}

			// Print the entire response for debugging
			log.Println("Full response:", string(body))
			if response.Status == "success" {
				stopflag = true
				log.Println(order10[ins], ltp, ins)

			}
		}
	}
}

func buyfifteen(ltp float64, ins string) {
	url := "https://api-hft.upstox.com/v2/order/place"
	method := "POST"

	quant := int(amount/ltp) * lev
	sloss := ltp * .9965

	payload := fmt.Sprintf(`{
			"quantity": "%d",
			"product": "I",
			"validity": "DAY",
			"price": 0,
			"tag": "string",
			"instrument_token": "%s",
			"order_type": "MARKET",
			"transaction_type": "BUY",
			"disclosed_quantity": 0,
			"trigger_price": 0,
			"is_amo": false
			}`, quant, ins)

	client := &http.Client{}
	req, err := http.NewRequest(method, url, strings.NewReader(payload))

	if err != nil {
		log.Println(err)
		return
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", "Bearer "+accessToken)

	res, err := client.Do(req)
	if err != nil {
		log.Println(err)
		return
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Println(err)
		return
	}
	log.Println("order for", ltp, ins)
	// Parse the JSON response
	var response Response
	err = json.Unmarshal(body, &response)
	if err != nil {
		log.Println("Error parsing JSON:", err)
		return
	}

	// Print the entire response for debugging
	log.Println("Full response:", string(body))

	// Extract and print the order ID
	if response.Status == "success" {
		order15[ins] = Order10{
			SL:       sloss,
			USL:      sloss,
			OPRICE:   ltp,
			SLGAP:    .0035 * ltp,
			ORDERID:  response.Data.OrderID,
			OTYPE:    "BUY",
			QUANTITY: quant,
		}

		stopflag1 = false
		fifteenstock.Append(ins) // Append values
		log.Println(order15[ins])

	} else {
		log.Println("Order placement was not successful")
	}

}

func sellfifteen(ltp float64, ins string) {
	url := "https://api-hft.upstox.com/v2/order/place"
	method := "POST"

	quant := int(amount/ltp) * lev
	sloss := 1.0035
	payload := fmt.Sprintf(`{
			"quantity": "%d",
			"product": "I",
			"validity": "DAY",
			"price": 0,
			"tag": "string",
			"instrument_token": "%s",
			"order_type": "MARKET",
			"transaction_type": "SELL",
			"disclosed_quantity": 0,
			"trigger_price": 0,
			"is_amo": false
			}`, quant, ins)

	client := &http.Client{}
	req, err := http.NewRequest(method, url, strings.NewReader(payload))

	if err != nil {
		log.Println(err)
		return
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", "Bearer "+accessToken)

	res, err := client.Do(req)
	if err != nil {
		log.Println(err)
		return
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Println(err)
		return
	}
	log.Println("order for", ltp, ins)
	// Parse the JSON response
	var response Response
	err = json.Unmarshal(body, &response)
	if err != nil {
		log.Println("Error parsing JSON:", err)
		return
	}

	// Print the entire response for debugging
	log.Println("Full response:", string(body))

	// Extract and print the order ID
	if response.Status == "success" {
		order15[ins] = Order10{
			SL:       sloss,
			USL:      sloss,
			OPRICE:   ltp,
			SLGAP:    .0035 * ltp,
			ORDERID:  response.Data.OrderID,
			OTYPE:    "SELL",
			QUANTITY: quant,
		}

		stopflag1 = false
		fifteenstock.Append(ins) // Append values
		log.Println(order15[ins])

	} else {
		log.Println("Order placement was not successful")
	}

}

func stoplossfifteen(ltp float64, ins string) {
	if order15[ins].OTYPE == "BUY" {
		if ltp >= (order15[ins].OPRICE + order15[ins].SLGAP) {

			temp := order15[ins] // Get a copy of the struct
			temp.USL += temp.SLGAP
			temp.OPRICE += temp.SLGAP
			order15[ins] = temp
			log.Println(order15[ins])

		}
		if ltp <= order15[ins].USL {
			url := "https://api-hft.upstox.com/v2/order/place"
			method := "POST"

			payload := fmt.Sprintf(`{
			"quantity": "%d",
			"product": "I",
			"validity": "DAY",
			"price": 0,
			"tag": "string",
			"instrument_token": "%s",
			"order_type": "MARKET",
			"transaction_type": "SELL",
			"disclosed_quantity": 0,
			"trigger_price": "0",
			"is_amo": false
			}`, order15[ins].QUANTITY, ins)

			client := &http.Client{}
			req, err := http.NewRequest(method, url, strings.NewReader(payload))

			if err != nil {
				log.Println(err)
				return
			}
			req.Header.Add("Content-Type", "application/json")
			req.Header.Add("Authorization", "Bearer "+accessToken)

			res, err := client.Do(req)
			if err != nil {
				log.Println(err)
				return
			}
			defer res.Body.Close()

			body, err := ioutil.ReadAll(res.Body)
			if err != nil {
				log.Println(err)
				return
			}

			// Parse the JSON response
			var response Response
			err = json.Unmarshal(body, &response)
			if err != nil {
				log.Println("Error parsing JSON:", err)
				return
			}

			// Print the entire response for debugging
			log.Println("Full response:", string(body))
			if response.Status == "success" {
				stopflag1 = true
				log.Println(order15[ins], ltp, ins)

			}

		}

	} else {

		if ltp <= (order15[ins].OPRICE - order15[ins].SLGAP) {

			temp := order15[ins] // Get a copy of the struct
			temp.USL -= temp.SLGAP
			temp.OPRICE -= temp.SLGAP
			order15[ins] = temp
			log.Println(order15[ins])

		}
		if ltp >= order15[ins].USL {
			url := "https://api-hft.upstox.com/v2/order/place"
			method := "POST"

			payload := fmt.Sprintf(`{
					"quantity": "%d",
					"product": "I",
					"validity": "DAY",
					"price": 0,
					"tag": "string",
					"instrument_token": "%s",
					"order_type": "MARKET",
					"transaction_type": "BUY",
					"disclosed_quantity": 0,
					"trigger_price": "0",
					"is_amo": false
					}`, order15[ins].QUANTITY, ins)

			client := &http.Client{}
			req, err := http.NewRequest(method, url, strings.NewReader(payload))

			if err != nil {
				log.Println(err)
				return
			}
			req.Header.Add("Content-Type", "application/json")
			req.Header.Add("Authorization", "Bearer "+accessToken)

			res, err := client.Do(req)
			if err != nil {
				log.Println(err)
				return
			}
			defer res.Body.Close()

			body, err := ioutil.ReadAll(res.Body)
			if err != nil {
				log.Println(err)
				return
			}

			// Parse the JSON response
			var response Response
			err = json.Unmarshal(body, &response)
			if err != nil {
				log.Println("Error parsing JSON:", err)
				return
			}

			// Print the entire response for debugging
			log.Println("Full response:", string(body))
			if response.Status == "success" {
				stopflag1 = true
				log.Println(order15[ins], ltp, ins)

			}

		}
	}
}

func buytwenty(ltp float64, ins string) {
	url := "https://api-hft.upstox.com/v2/order/place"
	method := "POST"

	quant := int(amount/ltp) * lev
	sloss := ltp * .9965

	payload := fmt.Sprintf(`{
			"quantity": "%d",
			"product": "I",
			"validity": "DAY",
			"price": 0,
			"tag": "string",
			"instrument_token": "%s",
			"order_type": "MARKET",
			"transaction_type": "BUY",
			"disclosed_quantity": 0,
			"trigger_price": 0,
			"is_amo": false
			}`, quant, ins)

	client := &http.Client{}
	req, err := http.NewRequest(method, url, strings.NewReader(payload))

	if err != nil {
		log.Println(err)
		return
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", "Bearer "+accessToken)

	res, err := client.Do(req)
	if err != nil {
		log.Println(err)
		return
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Println(err)
		return
	}

	log.Println("order for", ltp, ins)
	var response Response
	err = json.Unmarshal(body, &response)
	if err != nil {
		log.Println("Error parsing JSON:", err)
		return
	}

	// Print the entire response for debugging
	log.Println("Full response:", string(body))

	// Extract and print the order ID
	if response.Status == "success" {
		order20[ins] = Order10{
			SL:       sloss,
			USL:      sloss,
			OPRICE:   ltp,
			SLGAP:    .0035 * ltp,
			ORDERID:  response.Data.OrderID,
			OTYPE:    "BUY",
			QUANTITY: quant,
		}

		stopflag2 = false
		log.Println(order20[ins])

	} else {
		log.Println("Order placement was not successful")
	}

}

func selltwenty(ltp float64, ins string) {
	url := "https://api-hft.upstox.com/v2/order/place"
	method := "POST"

	quant := int(amount/ltp) * lev
	sloss := 1.0035
	payload := fmt.Sprintf(`{
			"quantity": "%d",
			"product": "I",
			"validity": "DAY",
			"price": 0,
			"tag": "string",
			"instrument_token": "%s",
			"order_type": "MARKET",
			"transaction_type": "SELL",
			"disclosed_quantity": 0,
			"trigger_price": 0,
			"is_amo": false
			}`, quant, ins)

	client := &http.Client{}
	req, err := http.NewRequest(method, url, strings.NewReader(payload))

	if err != nil {
		log.Println(err)
		return
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", "Bearer "+accessToken)

	res, err := client.Do(req)
	if err != nil {
		log.Println(err)
		return
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Println(err)
		return
	}
	log.Println("order for", ltp, ins)
	// Parse the JSON response
	var response Response
	err = json.Unmarshal(body, &response)
	if err != nil {
		log.Println("Error parsing JSON:", err)
		return
	}

	// Print the entire response for debugging
	log.Println("Full response:", string(body))

	// Extract and print the order ID
	if response.Status == "success" {
		order20[ins] = Order10{
			SL:       sloss,
			USL:      sloss,
			OPRICE:   ltp,
			SLGAP:    .0035 * ltp,
			ORDERID:  response.Data.OrderID,
			OTYPE:    "SELL",
			QUANTITY: quant,
		}

		stopflag2 = false
		log.Println(order20[ins])

	} else {
		log.Println("Order placement was not successful")
	}

}

func stoplosstwenty(ltp float64, ins string) {

	if order20[ins].OTYPE == "BUY" {
		if ltp >= (order20[ins].OPRICE + order20[ins].SLGAP) {

			temp := order20[ins] // Get a copy of the struct
			temp.USL += temp.SLGAP
			temp.OPRICE += temp.SLGAP
			order20[ins] = temp // Store the modified struct back in the map
			log.Println(order20[ins])

		}
		if ltp <= order20[ins].USL {
			url := "https://api-hft.upstox.com/v2/order/place"
			method := "POST"

			payload := fmt.Sprintf(`{
				"quantity": "%d",
				"product": "I",
				"validity": "DAY",
				"price": 0,
				"tag": "string",
				"instrument_token": "%s",
				"order_type": "MARKET",
				"transaction_type": "SELL",
				"disclosed_quantity": 0,
				"trigger_price": "0",
				"is_amo": false
				}`, order20[ins].QUANTITY, ins)

			client := &http.Client{}
			req, err := http.NewRequest(method, url, strings.NewReader(payload))

			if err != nil {
				log.Println(err)
				return
			}
			req.Header.Add("Content-Type", "application/json")
			req.Header.Add("Authorization", "Bearer "+accessToken)

			res, err := client.Do(req)
			if err != nil {
				log.Println(err)
				return
			}
			defer res.Body.Close()

			body, err := ioutil.ReadAll(res.Body)
			if err != nil {
				log.Println(err)
				return
			}

			// Parse the JSON response
			var response Response
			err = json.Unmarshal(body, &response)
			if err != nil {
				log.Println("Error parsing JSON:", err)
				return
			}

			// Print the entire response for debugging
			log.Println("Full response:", string(body))
			if response.Status == "success" {
				stopflag2 = true
				log.Println(order20[ins], ltp, ins)

			}

		}

	} else {

		if ltp <= (order20[ins].OPRICE - order20[ins].SLGAP) {

			temp := order20[ins] // Get a copy of the struct
			temp.USL -= temp.SLGAP
			temp.OPRICE -= temp.SLGAP
			order20[ins] = temp // Store the modified struct back in the map
			log.Println(order20[ins])

		}
		if ltp >= order20[ins].USL {
			url := "https://api-hft.upstox.com/v2/order/place"
			method := "POST"

			payload := fmt.Sprintf(`{
				"quantity": "%d",
				"product": "I",
				"validity": "DAY",
				"price": 0,
				"tag": "string",
				"instrument_token": "%s",
				"order_type": "MARKET",
				"transaction_type": "BUY",
				"disclosed_quantity": 0,
				"trigger_price": "0",
				"is_amo": false
				}`, order20[ins].QUANTITY, ins)

			client := &http.Client{}
			req, err := http.NewRequest(method, url, strings.NewReader(payload))

			if err != nil {
				log.Println(err)
				return
			}
			req.Header.Add("Content-Type", "application/json")
			req.Header.Add("Authorization", "Bearer "+accessToken)

			res, err := client.Do(req)
			if err != nil {
				log.Println(err)
				return
			}
			defer res.Body.Close()

			body, err := ioutil.ReadAll(res.Body)
			if err != nil {
				log.Println(err)
				return
			}

			// Parse the JSON response
			var response Response
			err = json.Unmarshal(body, &response)
			if err != nil {
				log.Println("Error parsing JSON:", err)
				return
			}

			// Print the entire response for debugging
			log.Println("Full response:", string(body))
			if response.Status == "success" {
				stopflag2 = true
				log.Println(order20[ins], ltp, ins)

			}

		}
	}
}

func (c *MarketDataClient) handleMarketData(feedResponse *marketdatafeedpb.FeedResponse) {
	for instrument, feed := range feedResponse.GetFeeds() {
		switch feed.GetRequestMode() {
		case marketdatafeedpb.RequestMode_ltpc:
			if ltpc := feed.GetLtpc(); ltpc != nil {
				// log.Printf("\n------ LTPC Data for %s ------\n", instrument)
				log.Printf("Last Traded Price: %.2f\n for %s", ltpc.GetLtp(), instrument)
				ins := instrument
				ltp := ltpc.GetLtp()

				if stopflag {

					if instrumentData[ins].SL < ltp && ltp < instrumentData[ins].UL {
						closetosma.Append(ins)
					}

					ts := time.Now()
					intervalStart := getIntervalStart(ts)
					candle, exists := candleData[ins]
					if !exists || !candle.StartTime.Equal(intervalStart) {
						// Finalize old candle (optional: print or save it)
						if exists {
							log.Printf("Finalized Candle [%s]: %+v\n", ins, *candle)
						}
						if candle.Close > candle.Open && closetosma.Contains(ins) {
							log.Println("Bullish Candle for BUYBUYBUYBUYBUYBUYBUYB", ins)
							log.Println("Bullish Candle for BUYBUYBUYBUYBUYBUYBUYB", ins)
							log.Println("Bullish Candle for BUYBUYBUYBUYBUYBUYBUYB", ins)
							log.Println("Bullish Candle for BUYBUYBUYBUYBUYBUYBUYB", ins)
							log.Println("Bullish Candle for BUYBUYBUYBUYBUYBUYBUYB", ins)
							log.Println("Bullish Candle for BUYBUYBUYBUYBUYBUYBUYB", ins)
						}

						// Start new candle
						candleData[ins] = &Candle{
							Open:      ltp,
							High:      ltp,
							Low:       ltp,
							Close:     ltp,
							StartTime: intervalStart,
						}
					} else {
						// Update ongoing candle
						candle.Close = ltp
						if ltp > candle.High {
							candle.High = ltp
						}
						if ltp < candle.Low {
							candle.Low = ltp
						}
					}

				} else if _, exists := order10[ins]; exists {
					stoplossten(ltp, ins)
				}

				if placed.Contains(ins) {

					stockData := struct {
						Instrument string
						Price      float64
					}{
						Instrument: ins, // Simulated instrument key
						Price:      ltp, // Simulated stock price
					}
					select {
					case stockChannel1 <- stockData: // Non-blocking send
					default:
						// Drop old data if buffer is full to always keep recent stock prices
						<-stockChannel1
						stockChannel1 <- stockData
					}
				}

				if fifteenstock.Contains(ins) {
					stockData := struct {
						Instrument string
						Price      float64
					}{
						Instrument: ins, // Simulated instrument key
						Price:      ltp, // Simulated stock price
					}
					select {
					case stockChannel2 <- stockData: // Non-blocking send
					default:
						// Drop old data if buffer is full to always keep recent stock prices
						<-stockChannel2
						stockChannel2 <- stockData
					}
				}
			}
		}
	}
}

func processStockData1(workerID int, wg *sync.WaitGroup) {
	defer wg.Done()
	for data := range stockChannel1 {

		ins := data.Instrument
		ltp := data.Price

		if stopflag1 {

		} else if _, exists := order15[ins]; exists {
			stoplossfifteen(ltp, ins)

		}

	}
}

func processStockData2(workerID int, wg *sync.WaitGroup) {
	defer wg.Done()
	for data := range stockChannel2 {

		ins := data.Instrument
		ltp := data.Price

		if stopflag2 {

		} else if _, exists := order20[ins]; exists {
			stoplosstwenty(ltp, ins)

		}

	}

}

func (c *MarketDataClient) Disconnect() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil || !c.connected { // Add this check
		return
	}
	if c.connected {
		log.Println("Disconnecting from WebSocket...")
		err := c.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		if err != nil {
			log.Printf("Error sending close message: %v", err)
		}

		err = c.conn.Close()
		if err != nil {
			log.Printf("Error closing WebSocket: %v", err)
		}

		c.connected = false
		c.cancel()
		log.Println("Disconnected successfully")
	}
}

func getWebSocketURL() (string, error) {
	req, err := http.NewRequest(http.MethodGet, authURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Accept", "*/*")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to make request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var result struct {
		Status string `json:"status"`
		Data   struct {
			AuthorizedRedirectUri string `json:"authorizedRedirectUri"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("failed to decode response: %v", err)
	}

	if result.Status != "success" {
		return "", fmt.Errorf("api returned non-success status: %s", result.Status)
	}

	return result.Data.AuthorizedRedirectUri, nil
}

func generateGUID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func (s *SafeSlice) Values() []string {
	s.mux.RLock()
	defer s.mux.RUnlock()
	return append([]string(nil), s.items...) // return a copy for safety
}

func main() {
	if accessToken == "" {
		log.Println("UPSTOX_ACCESS_TOKEN environment variable not set")
		return
	}

	fileContent, err := os.ReadFile("sma_trend_output.json")
	if err != nil {
		fmt.Println("Failed to read file:", err)
		return
	}

	// Declare a map to hold the JSON data

	// Unmarshal the JSON into the map
	if err := json.Unmarshal(fileContent, &instrumentData); err != nil {
		fmt.Println("Failed to parse JSON:", err)
		return
	}

	// Use the data
	for key := range instrumentData {
		insKeys.Append(key) // Append values
	}

	instrumentKeys = insKeys.Values()

	// Set up signal handling for graceful shutdown
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	client := NewMarketDataClient()

	// Connect to WebSocket
	connectAndSubscribe := func() error {
		if err := client.Connect(); err != nil {
			return fmt.Errorf("failed to connect: %v", err)
		}

		if err := client.Subscribe(instrumentKeys, "ltpc"); err != nil {
			client.Disconnect()
			return fmt.Errorf("failed to subscribe: %v", err)
		}
		return nil
	}
	if err := connectAndSubscribe(); err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect()

	runtime.GOMAXPROCS(runtime.NumCPU()) // Enable true parallelism

	var wg sync.WaitGroup
	numWorkers := runtime.NumCPU() / 2

	// Start workers for both channels
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go processStockData1(i, &wg)
	}
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go processStockData2(i+numWorkers, &wg)
	}

	// Start WebSocket processing
	go client.StartReading()

	// Add periodic status logging
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				log.Printf(
					"Status - Goroutines: %d, Channel1: %d, Channel2: %d, placed: %d, FifteenStock: %d",
					runtime.NumGoroutine(),
					len(stockChannel1),
					len(stockChannel2),
					len(bullish.items),
					len(placed.items),
				)
			case <-interrupt:
				return
			}
		}
	}()

	// Wait for interrupt
	<-interrupt
	log.Println("Shutting down...")
}
