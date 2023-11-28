from adaptive.base_v2.connection import AlgoSeekDBConnection
from adaptive.base.core_base import CoreBase

class FetchBidPrice(CoreBase):
    def __init__(self, env: str, assigned_tickers) -> None:
        super().__init__(env)
        self.assigned_tickers = assigned_tickers

    def get_option_list(self):
        self.adc = AlgoSeekDBConnection()
        self.adc.connect()

        # Ensure the list of tickers is formatted correctly for the SQL query
        formatted_tickers = tuple(self.assigned_tickers) if len(self.assigned_tickers) > 1 else f"('{self.assigned_tickers[0]}')"

        query = f"""
            SELECT *
            FROM USOptionsMarketData.TradeAndQuoteMinuteBar
            WHERE
                (Ticker, Strike, CallPut, ExpirationDate, BarDateTime) IN (
                    SELECT Ticker, Strike, CallPut, ExpirationDate, MAX(BarDateTime)
                    FROM USOptionsMarketData.TradeAndQuoteMinuteBar
                    WHERE toDate(BarDateTime) = '2023-11-14'
                    GROUP BY Ticker, Strike, CallPut, ExpirationDate
                )
            AND Ticker IN {formatted_tickers}
        """

        price_df = self.adc.execute(query=query)
        price_df.to_csv('price_df.csv')
        
        self.adc.disconnect()

        return price_df



if __name__ == "__main__":
    tickers = ['AAPL', 'TSLA', 'MSFT', 'AMZN', 'GOOG', 'FB', 'NVDA', 'PYPL', 'INTC', 'NFLX']
    bidprices = FetchBidPrice('dev', tickers)
    combined_df = bidprices.get_option_list()
    print(combined_df)
