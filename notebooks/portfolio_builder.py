# imports
import pandas as pd
import yfinance as yf
from typing import Optional
from datetime import datetime

class Trade():

    def __init__(self,
                 ticker: str,
                 qty: int,
                 date: datetime,
                 price: Optional[float]=None) -> None:
        self.ticker = ticker
        self.is_cash = False
        if self.ticker == 'CASH':
            self.is_cash = True
        self.qty = qty
        if isinstance(date, str):
            self.date = datetime.strptime(date, '%Y-%m-%d').date()
        else:
            self.date = date
        if self.is_cash:
            self.price = 1
        elif not price and not self.is_cash:
            self.price = self._get_fill_price()
        else:
            self.price = price
    
    def _get_fill_price(self) -> float:
        """
        Gets the fill price for the trade using yfinance and this trade's ticker.
        """
        return yf.Ticker(self.ticker).history(period='1d', start=self.date)['Close'][0]
    
    def __repr__(self) -> str:
        return f'{self.ticker} {self.qty} {self.date} {self.price}'
    
    def __str__(self) -> str:
        return f'{self.ticker} {self.qty} {self.date} {self.price}'

class PortfolioBuilder():
    """
    Accepts on construction a series of TRADEs and backs out a portfolio of those
    trades, offering various methods for PM analytics as well as data export methods.
    """

    # accepts any number of TRADEs
    def __init__(self, 
                 trades,
                 backup_price_data: Optional[pd.DataFrame]=None) -> None:
        """
        Constructor for PortfolioBuilder class.
        """
        self.trades = list()
        if isinstance(trades, Trade):
            trades = [trades]
        for trade in trades:
            self.trades.append(trade)
        if backup_price_data is not None:
            self.backup_price_data = backup_price_data
        self._transactions = self._build_transactions()
        self._portfolio = self._build_portfolio()

    def _build_transactions(self) -> pd.DataFrame:
        """
        Builds the transactions from the list of trades.
        """
        transactions = pd.DataFrame()
        for trade in self.trades:
            transactions = pd.concat([transactions, pd.DataFrame({'ticker': [trade.ticker],
                                                                  'qty': [trade.qty],
                                                                  'price': [trade.price],
                                                                  'date': [trade.date]})])
        return transactions
    
    def _build_portfolio(self, up_to: Optional[datetime]=None) -> pd.DataFrame:
        """
        Uses the transactions log to build a current portfolio.
        """
        
        portfolio = pd.DataFrame(columns=['Ticker', 'Weight', 'Value', 'Qty', 'Avg', 'Current', 'Gain/Loss $', 'Gain/Loss %'])
        if up_to:
            transactions = self._transactions[self._transactions['date'] <= up_to]
        else:
            transactions = self._transactions.copy()
        for ticker in transactions['ticker'].unique():
            ticker_transactions = transactions[transactions['ticker'] == ticker]
            total_qty = ticker_transactions['qty'].sum()
            if total_qty <= 0:
                continue
            # avg price is the weighted average of the prices of the transactions with prices > 0
            avg_price = (ticker_transactions[ticker_transactions['price'] > 0]['qty'] * ticker_transactions[ticker_transactions['price'] > 0]['price']).sum() / total_qty
            if ticker == 'CASH':
                current_price = 1
            else:
                try:
                    if up_to:
                        current_price = yf.Ticker(ticker).history(period='1d', start=up_to)['Close'][0]
                    else:
                        current_price = yf.Ticker(ticker).history(period='1d')['Close'][0]
                except Exception as e:
                    try:
                        if up_to:
                            current_price = self.backup_price_data[self.backup_price_data['Date'] <= up_to][ticker].values[0]
                        else:
                            current_price = self.backup_price_data[self.backup_price_data['Ticker'] == ticker]['Price'].iloc[-1]
                    except Exception as e:
                        # print(f'Could not get current price for {ticker}. Continuing with the rest of the portfolio.')
                        # print(e)
                        continue
            gain_loss = (current_price - avg_price) * total_qty
            gain_loss_pct = (current_price - avg_price) / avg_price
            portfolio = pd.concat([portfolio, pd.DataFrame({'Ticker': [ticker],
                                                            'Weight': [None],
                                                            'Value': [None],
                                                            'Qty': [total_qty],
                                                            'Avg': [avg_price],
                                                            'Current': [current_price],
                                                            'Gain/Loss $': [gain_loss],
                                                            'Gain/Loss %': [gain_loss_pct]})])
        portfolio['Value'] = portfolio['Current'] * portfolio['Qty']
        portfolio['Weight'] = portfolio['Value'] / portfolio['Value'].sum()
        return portfolio
    
    def get_portfolio(self, up_to: Optional[datetime]=None) -> pd.DataFrame:
        """
        Returns the portfolio.
        """
        if up_to:
            return self._build_portfolio(up_to)
        else:
            return self._portfolio
    
    def get_transactions(self) -> pd.DataFrame:
        """
        Returns the transactions.
        """
        return self._transactions
            
    def get_value_over_time(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """
        
        """
        date_range = pd.date_range(start=start_date, end=end_date)
        date_range = [date.date() for date in date_range]
        value_over_time = pd.DataFrame(index=date_range, columns=['Value'])
        quantities_over_time = self.get_holdings_over_time(start_date, end_date)
        for ticker in quantities_over_time.columns:
            price_df = self.get_price_df(ticker, start_date, end_date)
            value_over_time['Value'] += quantities_over_time[ticker] * price_df

        return value_over_time
    
    def get_price_df(self, ticker: str, start_date: datetime, end_date: datetime) -> list:
        """
        
        """
        
        price_df = pd.DataFrame(index=[d.date() for d in pd.date_range(start=start_date, end=end_date)])
        
        if ticker in self.backup_price_data.columns:
            tmp_price_df = self.backup_price_data[self.backup_price_data['Date'] <= end_date].drop(columns=[c for c in self.backup_price_data.columns if c != ticker and c != 'Date'])
        else:
            tmp_price_df = yf.Ticker(ticker).history(period='1d', start=start_date, end=end_date)
            tmp_price_df.index = tmp_price_df.index.date
            tmp_price_df.drop(columns=[c for c in tmp_price_df.columns if c != 'Close'], inplace=True)

        # merge on the dates that we have
        price_df = price_df.merge(tmp_price_df, how='left', left_index=True, right_index=True)
        # fill forward
        price_df = price_df.fillna(method='ffill')
        # fill backward
        price_df = price_df.fillna(method='bfill')
        return price_df

    def get_holdings_over_time(self, start_date, end_date) -> pd.DataFrame:
        """
        Makes a dataframe with one column per unique ticker. Gets tickers from self.trades.
        The value for each column is the quantity of that ticker held on that date.
        """
        holdings = pd.DataFrame(columns=self._transactions['ticker'].unique())
        for date in pd.date_range(start=start_date, end=end_date):
            holdings = pd.concat([holdings, self._get_holdings_on_date(date)])
        holdings.index = [d.date() for d in pd.date_range(start=start_date, end=end_date)]
        return holdings
    
    def _get_holdings_on_date(self, date: datetime) -> pd.DataFrame:
        """
        Returns a dataframe with one row and one column per unique ticker. The value for each column
        is the quantity of that ticker held on that date.
        """
        holdings = pd.DataFrame(columns=self._transactions['ticker'].unique())
        for ticker in holdings.columns:
            holdings[ticker] = [self._get_qty_on_date(ticker, date)]
        return holdings
    
    def _get_qty_on_date(self, ticker: str, date: datetime) -> int:
        """
        Returns the quantity of a given ticker held on a given date.
        """
        qty = 0
        for trade in self.trades:
            if trade.ticker == ticker and trade.date <= date:
                qty += trade.qty
        qty = max(qty, 0)
        return qty
        