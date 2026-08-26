import yfinance as yf
import pandas as pd

stock = input("Stock name: ")
apple=yf.download(stock, period='5d',interval="1m")
apple.to_parquet(f"{stock}.parquet", engine="pyarrow", compression="snappy")
