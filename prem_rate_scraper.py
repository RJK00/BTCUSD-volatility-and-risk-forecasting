import os
import sys
from bs4 import BeautifulSoup as bs
from urllib.request import urlopen
from urllib.parse import urljoin
from gzip import BadGzipFile
import pandas as pd
# import tqdm

url = f'https://public.bybit.com/premium_index/BTCUSD/'

def main(url):
    html = urlopen(url).read()
    soup = bs(html)
    print(html)

    tags = soup('a')

    for tag in tags:
        href = (tag.get('href', None))
        # only retrieve files from 2022 and 2023
        if href.startswith("BTCUSD2022") | href.startswith("BTCUSD2023"):
            file_url = urljoin(url, href)
            print(f'processing file: {href}')
            try:
                file_df = pd.read_csv(file_url, compression='gzip', delimiter=',', header=0, usecols=[
                                      'start_at', 'symbol', 'close'])
            except BadGzipFile:
                file_df = pd.read_csv(file_url, delimiter=',', header=0, usecols=[
                                      'start_at', 'symbol', 'close'])
                continue
            try:
                combined_df = pd.concat(
                    [combined_df, file_df], ignore_index=True)
            except NameError:
                combined_df = file_df
        else:
            pass
    return combined_df

if __name__ == "__main__":
    df = main(url)
    df.to_csv("data/premium_index/BTCUSD2022-2023_premium_rates_1m.csv", index=False)
    # print("")
