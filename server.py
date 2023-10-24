
from flask import Flask, request, jsonify
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import pandas as pd
from datetime import datetime
from flask_cors import CORS
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import pandas as pd
import itertools as it
import socket
from time import sleep
from random import randint, uniform
from threading import Thread


app = Flask(__name__)
CORS(app)


comprehensive_list =["BID_SIZE", "BID", "ASK", "ASK_SIZE", "LAST", "LAST_SIZE", "HIGH", "LOW", "VOLUME","CLOSE",
                     "BID_OPTION_COMPUTATION", "ASK_OPTION_COMPUTATION","LAST_OPTION_COMPUTATION", "MODEL_OPTION",
                     "OPEN","LOW_13_WEEK", "HIGH_13_WEEK", "LOW_26_WEEK", "HIGH_26_WEEK", "LOW_52_WEEK", 
                     "HIGH_52_WEEK", "AVG_VOLUME", "OPEN_INTEREST", "OPTION_HISTORICAL_VOL", "OPTION_IMPLIED_VOL",
                     "OPTION_BID_EXCH","OPTION_ASK_EXCH", "OPTION_CALL_OPEN_INTEREST", "OPTION_PUT_OPEN_INTEREST", 
                     "OPTION_CALL_VOLUME", "OPTION_PUT_VOLUME", "INDEX_FUTURE_PREMIUM", "BID_EXCH", "ASK_EXCH", 
                     "AUCTION_VOLUME", "AUCTION_PRICE", "AUCTION_IMBALANCE", "MARK_PRICE", "BID_EFP_COMPUTATION",
                     "ASK_EFP_COMPUTATION", "LAST_EFP_COMPUTATION",  "OPEN_EFP_COMPUTATION", "HIGH_EFP_COMPUTATION",
                     "LOW_EFP_COMPUTATION", "CLOSE_EFP_COMPUTATION", "LAST_TIMESTAMP", "SHORTABLE", "FUNDAMENTAL_RATIOS",
                     "RT_VOLUME", "HALTED", "BID_YIELD", "ASK_YIELD", "LAST_YIELD", "CUST_OPTION_COMPUTATION", "TRADE_COUNT",
                     "TRADE_RATE", "VOLUME_RATE", "LAST_RTH_TRADE", "RT_HISTORICAL_VOL", "IB_DIVIDENDS",
                     "BOND_FACTOR_MULTIPLIER", "REGULATORY_IMBALANCE", "NEWS_TICK", "SHORT_TERM_VOLUME_3_MIN",
                     "SHORT_TERM_VOLUME_5_MIN", "SHORT_TERM_VOLUME_10_MIN", "DELAYED_BID", "DELAYED_ASK", "DELAYED_LAST",
                     "DELAYED_BID_SIZE", "DELAYED_ASK_SIZE", "DELAYED_LAST_SIZE", "DELAYED_HIGH", "DELAYED_LOW",
                     "DELAYED_VOLUME", "DELAYED_CLOSE", "DELAYED_OPEN", "RT_TRD_VOLUME", "CREDITMAN_MARK_PRICE",
                     "CREDITMAN_SLOW_MARK_PRICE", "DELAYED_BID_OPTION", "DELAYED_ASK_OPTION", "DELAYED_LAST_OPTION",
                     "DELAYED_MODEL_OPTION", "LAST_EXCH", "LAST_REG_TIME", "FUTURES_OPEN_INTEREST", "AVG_OPT_VOLUME",
                     "DELAYED_LAST_TIMESTAMP", "SHORTABLE_SHARES", "DELAYED_HALTED", "REUTERS_2_MUTUAL_FUNDS", "ETF_NAV_CLOSE",
                     "ETF_NAV_PRIOR_CLOSE", "ETF_NAV_BID", "ETF_NAV_ASK", "ETF_NAV_LAST", "ETF_FROZEN_NAV_LAST",
                     "ETF_NAV_HIGH", "ETF_NAV_LOW", "SOCIAL_MARKET_ANALYTICS", "ESTIMATED_IPO_MIDPOINT", "FINAL_IPO_LAST",
                     "NOT_SET"
                    ]
def price_mapper(field):
    return [name for idx,name in enumerate(comprehensive_list) if idx == field][0]

option_chain_dict = {}
option_chains_df=pd.DataFrame()
df_dict = {"reqId":[], 
           "exchange":[],
           "underlyingConId":[],
           "tradingClass":[],
           "multiplier":[],
           "expirations":[],
           "strikes":[]
          }
price_dict={}



class OptionDataApp(EWrapper, EClient):

    def __init__(self):
        EClient.__init__(self, self)
        self.data_dict = {
            'bid_price': [],
            'bid_size': []
        }

    def error(self, reqId, errorCode, errorString, advanced_order_reject_json: str = ""):
        print("Error:", reqId, errorCode, errorString, advanced_order_reject_json)

    def request_option_chain(self, contract):
        self.reqSecDefOptParams(1, contract.exchange, "", contract.tradingClass, contract.conId)

    def securityDefinitionOptionParameter(self, reqId: int, exchange: str, underlyingConId: int, tradingClass: str, multiplier: str, expirations, strikes):
        super().securityDefinitionOptionParameter(reqId, exchange, underlyingConId, tradingClass, multiplier, expirations, strikes)
        print("SecurityDefinitionOptionParameter.", "ReqId:", reqId, "Exchange:", exchange, "Underlying conId:", underlyingConId, "TradingClass:", tradingClass, "Multiplier:", multiplier, "Expirations:", expirations, "Strikes:", str(strikes))

        if tradingClass not in option_chain_dict:
            option_chain_dict[tradingClass] = {}
        option_chain_dict[tradingClass][exchange] = {
            "Expirations": expirations,
            "Strikes": strikes
        }

        df_dict["reqId"].append(reqId)
        df_dict["exchange"].append(exchange)
        df_dict["underlyingConId"].append(underlyingConId)
        df_dict["tradingClass"].append(tradingClass)
        df_dict["multiplier"].append(multiplier)
        df_dict["expirations"].append(expirations)
        df_dict["strikes"].append(strikes)

    def create_option_df(self):
        option_chains_df = pd.DataFrame(df_dict).drop_duplicates(subset=["exchange"], keep="last")
        return option_chains_df

    def contractDetails(self, reqId: int, contractDetails):
        self.contractdet = contractDetails
        print(f"{contractDetails=}")

    def contractDetailsEnd(self, reqId: int):
        self.disconnect()
        print(f"ContractDetailsEnd(ReqId: {reqId})")

    def tickPrice(self, tickerId, field, price, attribs):
        print(" Tick type:", field, " Price:", price)
        price_field_mapping = {1: 'bid_price'}
        if price > 0.0 and field in price_field_mapping:
            price_type = price_field_mapping[field]
            if price_type not in self.data_dict.keys():
                self.data_dict[price_type] = []
            self.data_dict[price_type].append(price)

    def tickSize(self, tickerId, field, size):
        print(" Tick type:", field, " Size:", size)
        size_field_mapping = {0: 'bid_size'}
        if field in size_field_mapping:
            size_type = size_field_mapping[field]
            if size_type not in self.data_dict.keys():
                self.data_dict[size_type] = []
            self.data_dict[size_type].append(size)

    def tickString(self, tickerId, field, value):
        print(" Tick type:", field, " Value:", value)

    def tickGeneric(self, tickerId, field, value):
        print(" Tick type:", field, " value:", value)


    def disconnect_ib(self):
        try:
            self.conn.lock.acquire()
            connection_acquired = True
        except AttributeError:
            print("IB is not connected.")
            connection_acquired = False

        if connection_acquired:
            print("Starting to disconnect from IB")
            try:
                if self.conn.socket is not None:
                    self.conn.socket.shutdown(socket.SHUT_WR)
            finally:
                self.conn.lock.release()
                sleep(1)
                self.disconnect()
                print("Disconnected from IB")
        return None


def get_in_the_money_calls(portfolio_df, option_df, expiry):
    # Merging the portfolio dataframe with the option dataframe
    merged_df = portfolio_df.merge(option_df, left_on='ticker', right_on='tradingClass', how='inner')
    expiry = datetime.strptime(str(expiry), '%Y%m%d')
    
    # Classifying expirations into weekly and monthly
    weekly_expirations, monthly_expirations = zip(*merged_df['expirations'].apply(lambda x: classify_expiration(x, expiry)))
    merged_df['weekly_expirations'] = weekly_expirations
    merged_df['monthly_expirations'] = monthly_expirations

    in_the_money_calls = []
    for _, row in merged_df.iterrows():
        # Set strike_price to the current stock price for the ticker
        strike_price = row['price']
        closest_strikes = sorted(row['strikes'], key=lambda x: abs(x - strike_price))[:10]
        for strike in closest_strikes:
            money_flag = 1 if strike_price > strike else 0
            in_the_money_calls.append({
                'ticker': row['ticker'],
                'strike': strike,
                'exchange': row['exchange'],
                'weekly_expirations': row['weekly_expirations'],
                'monthly_expirations': row['monthly_expirations'],
                'weekly_bids': [uniform(0.9, 1.1) for _ in row['weekly_expirations']],
                'monthly_bids': [uniform(0.9, 1.1) for _ in row['monthly_expirations']],
                'money_flag': money_flag,
            })

    return pd.DataFrame(in_the_money_calls)



def classify_expiration(expiration_set, expiry):
    weekly = set()
    monthly = set()
    
    for exp in expiration_set:
        exp_date = datetime.strptime(str(exp), '%Y%m%d')
        if exp_date > expiry:
            if (exp_date.weekday() == 4) and (15 <= exp_date.day <= 21):
                monthly.add(exp)
            else:
                weekly.add(exp)
    
    weekly = sorted(list(weekly))[:5]
    monthly = sorted(list(monthly))[:5]
    
    return weekly, monthly

def fetch_option_chain(symbol):
    app = OptionDataApp()
    app.connect("127.0.0.1", 4002, clientId=randint(100, 200))
    api_thread = Thread(target=app.run, daemon=True)
    api_thread.start()
    if app.isConnected():
        print("Connected to IB")
    contract = Contract()
    contract.symbol = symbol
    contract.secType = "STK"
    contract.currency = "USD"
    contract.exchange = "SMART"
    contract.primaryExchange = "NASDAQ"
    sleep(3)
    app.reqMarketDataType(3)
    app.reqContractDetails(reqId=2322324, contract=contract)
    sleep(10)
    app.disconnect_ib()
    app.run()
    try:
        contractId = app.contractdet.contract.conId
        print(f"{contractId=}")
    except:
        print("Contract ID not found. Try to run the code again~~")
    sleep(7)
    app.connect("127.0.0.1", 4002, clientId=randint(100, 200))
    api_thread = Thread(target=app.run, daemon=True)
    api_thread.start()
    sleep(3)
    app.reqMarketDataType(3)
    app.reqSecDefOptParams(0, symbol, "", "STK", contractId)
    sleep(15)
    app.disconnect_ib()
    app.run()
    contract.conId = contractId
    print("Creating option chain dataframe")
    option_df = app.create_option_df()
    return option_df


@app.route('/get-option-chain', methods=['POST'])
def get_option_chain():
    # Parse the portfolio input
    portfolio_data = request.json.get('portfolio', [])
    
    # Filter out stocks with quantities below 100
    eligible_stocks = [stock for stock in portfolio_data if stock['quantity'] > 100]


    
    # Placeholder for the results
    results = {}

    portfolio_df = pd.DataFrame(portfolio_data)
    
    option_dfs = []

    # Fetch option chain for each eligible stock
    for stock in eligible_stocks:
        option_df = fetch_option_chain(stock['ticker'])
        option_dfs.append(option_df)

    # Merge all option chains into one dataframe
    option_df = pd.concat(option_dfs)
    option_df = option_df[option_df['exchange'] == 'SMART']

    # Initialize the results dictionary outside the loop
    results = {}

    for stock in eligible_stocks:
        today_date = datetime.today().strftime('%Y%m%d')
        calls_df = get_in_the_money_calls(portfolio_df, option_df, expiry=int(today_date))
        print(calls_df)
        results[stock['ticker']] = calls_df.to_dict(orient='records')

    
    return jsonify(results)

@app.errorhandler(403)
def handle_403(error):
    return str(error), 403


@app.route('/test', methods=['GET'])
def test_route():
    return jsonify({"message": "It works!"})

if __name__ == '__main__':
    app.run(debug=True, port=5001)