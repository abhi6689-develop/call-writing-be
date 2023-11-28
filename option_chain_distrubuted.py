import logging
import ray
import pandas as pd
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from threading import Thread, Event
import time
from datetime import datetime
from random import uniform, randint
from BidPriceWorker import get_bid_price
from fetch_option_prices import FetchBidPrice

pd.set_option("display.max_columns", None)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


# IB API Client Class
class IBApi(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.options_params = {}  # Stores the option parameters keyed by the request id
        self.contract_details_dict = {}
        self.contract_details_received = Event()
        self.options_params_received = {}
        self.connectionStatus = Event()

    def nextRequestId(self):
        """Simple logic to ensure each request has a unique ID"""
        if hasattr(self, "_nextReqId"):
            self._nextReqId += 1
        else:
            self._nextReqId = 1
        return self._nextReqId

    def nextValidId(self, orderId: int):
        """Called when the connection is established and the server provides a valid ID"""
        super().nextValidId(orderId)
        self.connectionStatus.set()

    def reqMarketData(self, contract, reqId=None):
        """Request market data for a contract."""
        if not reqId:
            reqId = self.nextRequestId()
        self.reqMktData(reqId, contract, "", False, False, [])
        return reqId

    # EWrapper callbacks
    def contractDetails(self, reqId, contractDetails):
        super().contractDetails(reqId, contractDetails)
        self.contract_details_dict[
            reqId
        ] = contractDetails  # Store using reqId as the key
        logger.info(f"Received contract details for request {reqId}: {contractDetails}")
        self.contract_details_received.set()  # You may need an event for each reqId if they can be concurrent

    def contractDetailsEnd(self, reqId):
        super().contractDetailsEnd(reqId)
        logger.info(f"Contract details finished for request {reqId}")

    def securityDefinitionOptionParameter(
        self,
        reqId,
        exchange,
        underlyingConId,
        tradingClass,
        multiplier,
        expirations,
        strikes,
    ):
        super().securityDefinitionOptionParameter(
            reqId,
            exchange,
            underlyingConId,
            tradingClass,
            multiplier,
            expirations,
            strikes,
        )
        logger.info(f"Received options parameters for request {reqId}")
        self.options_params[reqId] = {
            "exchange": exchange,
            "expirations": expirations,
            "strikes": strikes,
        }

    def securityDefinitionOptionParameterEnd(self, reqId):
        super().securityDefinitionOptionParameterEnd(reqId)
        logger.info(f"Options parameters finished for request {reqId}")
        self.options_params_received[reqId].set()

    def error(self, reqId, errorCode, errorString, *args):
        super().error(reqId, errorCode, errorString)
        logger.error(f"Error. Id: {reqId}, Code: {errorCode}, Msg: {errorString}")
        if args:
            logger.error(f"Additional error info: {args}")

    def reqOptionsData(self, contract):
        """Custom method to request options data."""
        req_id = self.nextRequestId()
        self.contract_details_received.clear()
        self.reqContractDetails(req_id, contract)

        # Wait for the contract details event to be set
        self.contract_details_received.wait()
        if req_id in self.contract_details_dict:
            contract.conId = self.contract_details_dict[req_id].contract.conId
            options_params_event = Event()
            self.options_params_received[req_id] = options_params_event
            self.reqSecDefOptParams(
                req_id, contract.symbol, "", contract.secType, contract.conId
            )
            options_params_event.wait()


# Function to fetch option chain data
@ray.remote
def fetch_option_chain(stock_symbol, client_id):
    app = IBApi()
    app.connect("127.0.0.1", 4002, clientId=client_id)

    # Start the client thread
    api_thread = Thread(target=app.run, daemon=True)
    api_thread.start()

    # Wait for connection to be established
    if not app.connectionStatus.wait(10):  # Wait up to 10 seconds for the connection
        # raise Exception("Connection to IB Gateway could not be established.")
        print("Connection to IB Gateway could not be established.")

    # Define the stock contract
    contract = Contract()
    contract.symbol = stock_symbol
    contract.secType = "STK"
    contract.exchange = "SMART"
    contract.currency = "USD"

    # Request options data
    app.reqOptionsData(contract)

    # Wait for the contract details to be received
    app.contract_details_received.wait()

    # After contract details are received, wait for the options parameters
    for req_id in app.options_params_received:
        app.options_params_received[req_id].wait()

    # Disconnect from the IB Gateway
    app.disconnect()

    # Process the options parameters to create a DataFrame
    df_option_chain_list = []
    for req_id, params in app.options_params.items():
        con_id = app.contract_details_dict[req_id].contract.conId
        for expiration in params["expirations"]:
            for strike in params["strikes"]:
                df_option_chain_list.append(
                    {
                        "ticker": stock_symbol,
                        "Exchange": params["exchange"],
                        "Expiries": expiration,
                        "Strikes": strike,
                        "ConId": con_id,
                    }
                )

    return pd.DataFrame(df_option_chain_list)


def get_in_the_money_calls(portfolio_df, option_df, expiry):
    # Merging the portfolio dataframe with the option dataframe
    merged_df = portfolio_df.merge(
        option_df, left_on="ticker", right_on="ticker", how="inner"
    )
    expiry = datetime.strptime(str(expiry), "%Y%m%d")

    # Classifying expirations into weekly and monthly
    weekly_expirations, monthly_expirations = zip(
        *merged_df["Expiries"].apply(lambda x: classify_expiration(x, expiry))
    )
    merged_df["weekly_expirations"] = weekly_expirations
    merged_df["monthly_expirations"] = monthly_expirations

    in_the_money_calls = []
    for _, row in merged_df.iterrows():
        # Set strike_price to the current stock price for the ticker
        strike_price = row["price"]
        closest_strikes = sorted(row["Strikes"], key=lambda x: abs(x - strike_price))[
            :10
        ]
        for strike in closest_strikes:
            money_flag = 1 if strike_price > strike else 0
            in_the_money_calls.append(
                {
                    "ticker": row["ticker"],
                    "strike": strike,
                    "exchange": row["Exchange"],
                    "weekly_expirations": row["weekly_expirations"],
                    "monthly_expirations": row["monthly_expirations"],
                    "money_flag": money_flag,
                }
            )

    return pd.DataFrame(in_the_money_calls)


def classify_expiration(expiration_set, expiry):
    weekly = set()
    monthly = set()

    for exp in expiration_set:
        exp_date = datetime.strptime(str(exp), "%Y%m%d")
        if exp_date > expiry:
            if (exp_date.weekday() == 4) and (15 <= exp_date.day <= 21):
                monthly.add(exp)
            else:
                weekly.add(exp)

    weekly = sorted(list(weekly))[:5]
    monthly = sorted(list(monthly))[:5]

    return weekly, monthly


def create_option_contracts(filtered_df):
    """
    This function takes a DataFrame of filtered options and returns a list of IB option contracts.

    :param filtered_df: DataFrame with the necessary option information
    :return: List of option contracts
    """
    option_contracts = {}

    for index, row in filtered_df.iterrows():
        contract = Contract()
        contract.symbol = row["ticker"]
        contract.secType = "OPT"
        contract.exchange = row["Exchange"]
        contract.currency = "USD"
        contract.strike = row["Strike"]
        contract.lastTradeDateOrContractMonth = row["Expiry"].replace(
            "-", ""
        )  # Format the expiry as needed
        contract.right = "P"
        contract.multiplier = 100  # 100 shares per contract
        contract.conId = row["ConId"]

        # Add the created contract to the list
        option_contracts[row["uuid"]] = contract

    return option_contracts


def filter_closest_options(option_df, portfolio_df):
    """
    Filters the option_df to find the 10 closest options for each stock based on the current stock price.

    :param option_df: DataFrame containing option data with 'ticker', 'Exchange', 'Expiries', and 'Strikes' columns
    :param portfolio_df: DataFrame containing portfolio data with 'ticker' and 'price' columns
    :return: DataFrame containing the filtered options
    """
    # Create an empty DataFrame to hold the filtered options
    filtered_options = pd.DataFrame()

    for _, row in portfolio_df.iterrows():
        ticker = row["ticker"]
        current_price = row["price"]

        # Filter the option_df for the current ticker
        ticker_options = option_df[option_df["ticker"] == ticker]

        # Find the 5 closest strikes above the current price
        closest_above = ticker_options[
            ticker_options["Strikes"] >= current_price
        ].nsmallest(5, "Strikes")

        # Find the 5 closest strikes below the current price
        closest_below = ticker_options[
            ticker_options["Strikes"] <= current_price
        ].nlargest(5, "Strikes")

        # Concatenate the above and below DataFrames
        closest_options = pd.concat([closest_below, closest_above]).sort_values(
            by="Strikes"
        )

        # Add the concatenated result to the filtered options DataFrame
        filtered_options = pd.concat([filtered_options, closest_options])

    return filtered_options.reset_index(drop=True)


def expand_option_combinations(filtered_options_df):
    """
    Expands the filtered options DataFrame to have a row for each expiration and strike combination.

    :param filtered_options_df: DataFrame containing the filtered options with 'ticker', 'Exchange', 'Strikes', and 'Expiries' columns
    :return: DataFrame with expanded options
    """
    expanded_options = []

    for index, row in filtered_options_df.iterrows():
        for expiry, conId in zip(row["Expiries"], row["ConId"]):
            expanded_options.append(
                {
                    "ticker": row["ticker"],
                    "Exchange": "SMART",
                    "Strike": row["Strikes"],
                    "Expiry": expiry,
                    "ConId": conId,
                }
            )

    return pd.DataFrame(expanded_options)


def classify_and_filter_options(df):
    # Convert Expiry to datetime
    df["Expiry"] = pd.to_datetime(df["Expiry"], format="%Y%m%d")

    # Define a function to classify the option as weekly or monthly
    def classify_option(expiry_date):
        # Monthly options typically expire on the third Friday of the month
        # Calculate the third Friday of the month
        third_friday = pd.date_range(
            start=expiry_date.strftime("%Y-%m-01"), periods=3, freq="WOM-3FRI"
        )[0]
        return "Monthly" if expiry_date == third_friday else "Weekly"

    # Apply the classification
    df["OptionType"] = df["Expiry"].apply(classify_option)

    # Filter for the next 5 monthly and weekly options
    # Get the current date
    current_date = pd.to_datetime("today")

    # Create a mask for dates that are after the current date
    future_mask = df["Expiry"] > current_date

    # Apply the mask and sort by Expiry
    df_future = df[future_mask].sort_values(by="Expiry")

    # Group by ticker and Strike and classify
    def filter_top_5(group):
        return group.groupby("OptionType").head(5)

    # Apply the groupby filter
    filtered_df = (
        df_future.groupby(["ticker", "Strike"])
        .apply(filter_top_5)
        .reset_index(drop=True)
    )
    filtered_df["Expiry"] = filtered_df["Expiry"].dt.strftime("%Y%m%d")

    return filtered_df


def get_option_chain(stocks):
    ray.init(ignore_reinit_error=True)
    #ramdomly assign client ids
    client_ids = [randint(100, 10000) for _ in range(len(stocks))]

    #start tasks sequentially

    # results = []
    
    # for stock, client_id in zip(stocks, client_ids):
    #     result = fetch_option_chain(stock, client_id)
    #     results.append(result)
    # df_combined = pd.concat(results, ignore_index=True)

    # Start tasks in parallel
    futures = [
        fetch_option_chain.remote(stock, client_id)
        for stock, client_id in zip(stocks, client_ids)
    ]
    results = ray.get(futures)
    df_combined = pd.concat(results, ignore_index=True)
    ray.shutdown()
    return df_combined



if __name__ == "__main__":
    # Initialize ray
    ray.init(ignore_reinit_error=True)

    start = time.time()
    # List of stock symbols
    stocks = [
        "AAPL",
        "GOOG",
        "MSFT",
        "TSLA",
        "AMZN",
        "META",
        "NVDA",
        "PYPL",
        "INTC",
        "NFLX",
        "ADBE",
        "ORCL",
        "IBM",
    ]

    # Assign unique client IDs for each task
    client_ids = list(range(1, len(stocks) + 1))

    # Start tasks in parallel
    futures = [
        fetch_option_chain.remote(stock, client_id)
        for stock, client_id in zip(stocks, client_ids)
    ]
    results = ray.get(futures)
    df_combined = pd.concat(results, ignore_index=True)
    print(df_combined)

    end = time.time()
    print(f"Time taken: {end - start} seconds")
    ray.shutdown()
