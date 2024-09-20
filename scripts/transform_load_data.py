import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_CONNECTION


#---------Function1---------#


def transform_data(filepath):
  df = pd.read_csv(filepath, header=0)
  columns = ['blockHash', 'blockNumber', 'from', 'gas', 'gasPrice', 'maxFeePerGas',
             'maxPriorityFeePerGas', 'hash', 'input', 'to',
             'transactionIndex', 'value', 'type']

  # Filtering columns
  df = df[columns]


  # Replacing null with 0x0 at maxFeePerGas | maxPriorityFeePerGas
  df['maxFeePerGas'].replace(np.nan, '0x0', inplace=True)
  df['maxPriorityFeePerGas'].replace(np.nan, '0x0', inplace=True)


  # Convert Hex to ints
  cols_to_int = ['blockNumber', 'gas', 'gasPrice', 'maxFeePerGas', 'maxPriorityFeePerGas', 'transactionIndex', 'value', 'type']
  for col in cols_to_int:
    df[col] = df[col].apply(lambda x : int(x,16))


  # Create columns 'Txn Fee (ETH)' based on 'type'
  df['Txn Fee (ETH)'] = 0
  df.loc[df['type']==0, 'Txn Fee (ETH)']  = (df['gas'] * df['gasPrice']) / pow(10,18)
  df.loc[df['type']==2, 'Txn Fee (ETH)'] = (df['gas'] * (df['gasPrice'] + df['maxPriorityFeePerGas'])) / pow(10,18)


  # Convert 'value' from Wei to ETH
  df['value'] = df['value'] / pow(10,18)


  # Update method for transaction based on 'input' column
  df['Method hash'] = df['input'].apply(lambda x: x[:10])
  function_dict = {
    "0x38ed1739": "swapExactTokensForTokens",
    "0x7ff36ab5": "swapExactETHForTokens",
    "0x18cbafe5": "swapExactTokensForETH",
    "0x5c11d795": "swapTokensForExactTokens",
    "0x8807b3a0": "swapETHForExactTokens",
    "0x0234b3b2": "swapTokensForExactETH",
    "0x3d5c8e83": "addLiquidity",
    "0x3b1a4a3b": "removeLiquidity",
    "0x0f0c91d0": "addLiquidityETH",
    "0x4f8c5a05": "removeLiquidityETH",
    "0x02fef0b3": "mint",
    "0x7c022d65": "burn",
    "0x4a07b12d": "transfer",
    "0xa9059cbb": "transfer",
    "0x095ea7b3": "approve",
    "0x70a08231": "balanceOf(address)",
    "0x313ce567": "decimals()",
    "0x18160ddd": "totalSupply()",
    "0x040c81a5": "allowance(address,address)"
}

  df['Method'] = df['Method hash'].map(function_dict)
  df['Method'].replace(np.nan, 'Customed Method', inplace=True)
  df.drop('input', axis=1, inplace=True)
  df['value'] = pd.to_numeric(df['value'])
  return df


#---------Function2---------#
def save_to_db(df, table_name):
  engine = create_engine(DB_CONNECTION)
  data.to_sql(table_name, con=engine, if_exists='append', index=False)
  print("All transactions loaded")

#running script
if __name__ == "__main__":
  data = transform_data('data/daily_ethereum_transactions.csv')
  save_to_db(data, "transactions")
