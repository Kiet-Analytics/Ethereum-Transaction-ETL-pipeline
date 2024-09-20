import pandas as pd
import time
import requests
from datetime import datetime, timedelta
from config import ETHERSCAN_API_KEY

URL_BASE = 'https://api.etherscan.io/api'

#Function 1: get block number from specified timestamp

def get_block_number(timestamp):
  url = f'{URL_BASE}?module=block&action=getblocknobytime&timestamp={timestamp}&closest=before&apikey={ETHERSCAN_API_KEY}'
  response = requests.get(url)
  block_num = response.json()['result']
  return int(block_num)


#Function2: get block range by using Function1

def get_yesterday_block_range():
  yesterday = datetime.utcnow() - timedelta(days=1)
  timestamp_start_day = int(datetime(yesterday.year, yesterday.month, yesterday.day).timestamp())
  timestamp_end_day = int((datetime(yesterday.year, yesterday.month, yesterday.day) + timedelta(days=1)).timestamp()) - 1
  start_block = get_block_number(timestamp_start_day)
  end_block = get_block_number(timestamp_end_day)
  return start_block, end_block


#function3: get data from block transactions

def get_block_transactions(block_num):
  url = f'{BASE_URL}?module=proxy&action=eth_getBlockByNumber&tag={hex(block_number)}&boolean=true&apikey={ETHERSCAN_API_KEY}'
  for _ in range(5): #try 5 times if failed
    try:
      response = requests.get(url)
      block_data = response.json()
      if 'result' in block_data:
        return block_data['result']['transactions'] # list chứa các dictionaries - 1 dictionary represents a row

    except Exception as e:
      print(f"Lỗi khi lấy dữ liệu từ block number: {block_num}")
      time.sleep(1) #wait for 2 seconds before try again
  return []
    

#function4: fetch daily data

def fetch_daily_data(output_file):
  start_block, end_block = get_yesterday_block_range()
  transactions = []
  for i in range(start_block, end_block + 1):
    transactions.extend(get_block_transactions(i))
    time.sleep(0.2)
    
  df = pd.DataFrame(transactions)
  df.to_csv(output_file, mode='w', header=True, index=False)
  return df
  

#Collect data daily and save to CSV file:
if __name__ == "__main__":
  fetch_daily_data('data/daily_ethereum_transactions.csv')
