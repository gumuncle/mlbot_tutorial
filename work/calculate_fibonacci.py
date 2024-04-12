import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor
import concurrent.futures

# フィボナッチリトレースメントレベルを計算する関数
def calculate_fibonacci_for_row(args):
    row, df, n = args
    end_date = row.name
    start_date = end_date - pd.Timedelta(days=n)
    recent_df = df.loc[start_date:end_date]
    
    recent_high = recent_df['High'].max()
    recent_low = recent_df['Low'].min()
    
    fib_23_6 = recent_high - (recent_high - recent_low) * 0.236
    fib_38_2 = recent_high - (recent_high - recent_low) * 0.382
    fib_61_8 = recent_high - (recent_high - recent_low) * 0.618
    fib_78_6 = recent_high - (recent_high - recent_low) * 0.786
    
    return [fib_23_6, fib_38_2, fib_61_8, fib_78_6]

# データフレームの準備
# df = pd.read_csv('your_ohlcv_data.csv')
# df['Date'] = pd.to_datetime(df['Date'])
# df.set_index('Date', inplace=True)

# 並列計算の実行
def parallel_fibonacci_calculation(df, n=30):
    with ProcessPoolExecutor() as executor:
        # 各行に対して関数を並列に適用
        futures = [executor.submit(calculate_fibonacci_for_row, (row, df, n)) for index, row in df.iterrows()]
        results = []
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
            
    # 結果をデータフレームに追加
    fib_columns = ['Fib_23_6', 'Fib_38_2', 'Fib_61_8', 'Fib_78_6']
    df[fib_columns] = pd.DataFrame(results, index=df.index)
    
    return df

# 関数の実行
#df = parallel_fibonacci_calculation(df, n=30)
