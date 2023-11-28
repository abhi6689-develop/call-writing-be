
from flask import Flask, request, jsonify
import pandas as pd
from datetime import datetime, timedelta
from flask_cors import CORS
import pandas as pd
from random import uniform
from option_chain_distrubuted import get_option_chain


app = Flask(__name__)
CORS(app)


def get_in_the_money_calls(portfolio_df, option_df, expiry):
    # Convert 'Expiries' in option_df to datetime format
    option_df['Expiries'] = pd.to_datetime(option_df['Expiries'], format='%Y%m%d')

    # Merging the portfolio dataframe with the option dataframe
    merged_df = pd.merge(portfolio_df, option_df, on='ticker', how='inner')

    # Filter options by expiry
    expiry_datetime = datetime.strptime(str(expiry), '%Y%m%d')
    merged_df = merged_df[merged_df['Expiries'] >= expiry_datetime]

    in_the_money_calls = []
    for ticker in merged_df['ticker'].unique():
        ticker_df = merged_df[merged_df['ticker'] == ticker]
        for strike in ticker_df['Strikes'].unique():
            strike_df = ticker_df[ticker_df['Strikes'] == strike]

            # Use a list comprehension to flatten the list of lists
            weekly_expirations = [date for sublist in strike_df['Expiries'].apply(lambda x: classify_expiration(x, expiry_datetime)[0]) for date in sublist if date is not None]
            monthly_expirations = [date for sublist in strike_df['Expiries'].apply(lambda x: classify_expiration(x, expiry_datetime)[1]) for date in sublist if date is not None]

            # Remove duplicates and sort
            weekly_expirations = sorted(list(set(weekly_expirations)))
            monthly_expirations = sorted(list(set(monthly_expirations)))

            current_price = portfolio_df[portfolio_df['ticker'] == ticker]['price'].iloc[0]
            money_flag = 1 if current_price > strike else 0
            in_the_money_calls.append({
                'ticker': ticker,
                'strike': strike,
                'exchange': strike_df['Exchange'].iloc[0],
                'weekly_expirations': weekly_expirations,
                'monthly_expirations': monthly_expirations,
                'weekly_bids': [uniform(0.9, 1.1) for _ in range(len(set(weekly_expirations)))],
                'monthly_bids': [uniform(0.9, 1.1) for _ in range(len(set(monthly_expirations)))],
                'money_flag': money_flag,
            })

    return pd.DataFrame(in_the_money_calls)

def nearest_friday(exp_date):
    """
    Find the nearest Friday to the given date.
    If the date is Friday, return the same date.
    """
    friday = 4  # Friday is represented by 4 in Python's datetime module (Monday is 0)
    days_until_friday = (friday - exp_date.weekday()) % 7
    nearest_friday_date = exp_date + timedelta(days=days_until_friday)
    return nearest_friday_date

def classify_expiration(exp_date, expiry):
    weekly = []
    monthly = []

    # Find the nearest Friday to the expiration date
    near_friday = nearest_friday(exp_date)

    if exp_date > expiry:
        if (near_friday.weekday() == 4) and (15 <= near_friday.day <= 21):
            # Considered as a monthly expiration
            monthly.append(exp_date.strftime('%Y%m%d'))
        else:
            # Considered as a weekly expiration
            weekly.append(exp_date.strftime('%Y%m%d'))

    return weekly, monthly


@app.route('/get-option-chain', methods=['POST'])
def return_option_chain():
    # Parse the portfolio input
    portfolio_data = request.json.get('portfolio', [])

    # Filter out stocks with quantities below 100
    eligible_stocks = [stock for stock in portfolio_data if stock['quantity'] >= 100]

    portfolio_df = pd.DataFrame(portfolio_data)
    tickers = [stock['ticker'] for stock in eligible_stocks]
    option_df = get_option_chain(tickers)

    today_date = datetime.today().strftime('%Y%m%d')
    calls_df = get_in_the_money_calls(portfolio_df, option_df, expiry=int(today_date))

    print(calls_df)

    # Organize the results by ticker
    results = {ticker: calls_df[calls_df['ticker'] == ticker].to_dict(orient='records') for ticker in tickers}

    print(results)

    return jsonify(results)


@app.errorhandler(403)
def handle_403(error):
    return str(error), 403


@app.route('/test', methods=['GET'])
def test_route():
    return jsonify({"message": "It works!"})

if __name__ == '__main__':
    app.run(debug=True, port=5001)