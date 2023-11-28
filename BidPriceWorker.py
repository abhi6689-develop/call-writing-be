import threading
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import time
import random
import logging
import ray
import hashlib
import os

# Setup logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

MAX_RETRIES = 2
BASE_RETRY_DELAY = 2  # in seconds

# First, define a class that inherits from EWrapper and EClient
class BidPriceApp(EWrapper, EClient):
    def __init__(self, client_id):
        EClient.__init__(self, self)
        self.reqId_to_price = {}
        self.reqId_to_event = {}
        self.client_id = client_id
        self.connected_event = threading.Event()  # Use to wait for successful connection
        self.reqId_to_option_computation = {} 
        self.reqId_to_volume = {} 

    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self.connected_event.set()  # Set the event when connection is established and ID is received

    def tickPrice(self, reqId, tickType, price, attrib):
        # Handle bid and ask prices, considering delayed data tick types
        if tickType in [1,66]:  # 1 and 2 for real-time, 66 and 67 for delayed bid and ask
            price_type = 'BID' if tickType in [1, 66] else 'ASK'
            if reqId in self.reqId_to_event:  # Check if reqId matches expected
                self.reqId_to_price[reqId] = price
                logger.info(f"Received price: {price} for reqId: {reqId}")
                self.reqId_to_event[reqId].set()
    
    def tickSize(self, reqId, tickType, size):
    # Handle real-time volume (tickType 0) and delayed volume (tickType 74)
        if tickType in [0, 74]:
            volume_type = 'VOLUME'
            if reqId in self.reqId_to_event:  # Check if reqId matches expected
                self.reqId_to_volume[reqId] = size
                logger.info(f"Received {volume_type} size: {size} for reqId: {reqId}")
                self.reqId_to_event[reqId].set()  # Signal that volume has been received



    def tickOptionComputation(self, reqId, tickType, impliedVol, delta,
                              optPrice, pvDividend, gamma, vega, theta, undPrice):
        self.reqId_to_option_computation[reqId] = {
            "impliedVol": impliedVol,
            "delta": delta,
            "optPrice": optPrice,
            "pvDividend": pvDividend,
            "gamma": gamma,
            "vega": vega,
            "theta": theta,
            "undPrice": undPrice
        }
        logger.info(f"Option computation for reqId {reqId}: {self.reqId_to_option_computation[reqId]}")
        if reqId in self.reqId_to_event:
            self.reqId_to_event[reqId].set()

    def error(self, reqId, errorCode, errorString, *args):
        super().error(reqId, errorCode, errorString)
        logger.error(f"Error. Id: {reqId}, Code: {errorCode}, Msg: {errorString}")
        if errorCode in [10167, 200]:  # Delayed data permission or no security definition
            if reqId in self.reqId_to_event:
                self.reqId_to_event[reqId].set()


    def connect_and_start(self):
        try:
            self.connect('127.0.0.1', 4002, self.client_id)
            self.thread = threading.Thread(target=self.run)
            self.thread.start()
            self.connected_event.wait(10)  # Wait up to 10 seconds for connection to be established
            if not self.connected_event.is_set():
                logger.error("Failed to connect within the timeout period.")
                raise Exception("Connection timeout.")
        except Exception as e:
            logger.exception("Exception during connection and start: " + str(e))
            raise

    def stop(self):
        try:
            if self.conn:  # Check if self.conn is not None
                self.disconnect()
            if self.thread:  # Also check if self.thread is not None before joining
                self.thread.join()
        except Exception as e:
            logger.exception("Exception during stop: " + str(e))


# Define the function to fetch the bid price with a timeout
def fetch_bid_price(contract: Contract, client_id, timeout=10):
    app = BidPriceApp(client_id)
    reqId = random.randint(1000, 1000000)
    app.reqId_to_event[reqId] = threading.Event()
    
    try:
        app.connect_and_start()
        
        retry_delay = BASE_RETRY_DELAY
        for _ in range(MAX_RETRIES):
            try:
                app.reqMarketDataType(3)  # Delayed data
                app.reqMktData(reqId, contract, "", False, False, [])

                price_received = app.reqId_to_event[reqId].wait(timeout)
                if price_received:
                    bid_price = app.reqId_to_price.get(reqId, None)
                    volume = app.reqId_to_volume.get(reqId, None) 
                    print("Bid price received: ", bid_price)
                    return bid_price, volume
                else:
                    logger.info(f"Timeout reached for contract: {contract.symbol}. Retrying...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
            except Exception as e:
                logger.exception(f"Exception during market data request: {e}")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
    finally:
        #if there is an active connection, disconnect and stop the client
        if app.isConnected():
            app.stop()

    logger.error(f"Failed to receive bid price after {MAX_RETRIES} retries.")
    return None, None


# Now, set up Ray for multiprocessing
ray.init(ignore_reinit_error=True)

# Define a Ray remote function to fetch the bid price in parallel
@ray.remote
def get_bid_price(uuid, contract):
    unique_str = f"{time.time()}-{ray.get_runtime_context().task_id.hex()}-{random.randint(0, 10000)}"
    hash_obj = hashlib.sha256(unique_str.encode('utf-8'))
    client_id = int(hash_obj.hexdigest(), base=16) % 10000
    bid_price, volume = fetch_bid_price(contract, client_id)
    return uuid, bid_price, volume


if __name__ == '__main__':
    pass 
