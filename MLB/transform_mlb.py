from urllib.request import urlopen
from bs4 import BeautifulSoup
import urllib.request
import requests
from lxml import html
from lxml.html import parse
from lxml import etree
import pandas as pd


# Read data from file
df = pd.read_csv("phillies_stats_all-time-by-season.csv", skiprows=1)

# Data cleanup - column name header appear many times
filter_condition = df['PLAYERPLAYER'] == 'PLAYERPLAYER'

# Delete rows based on the filter condition
df = df[~filter_condition]  # Use ~ to negate the filter condition
# Rename bad column
df = df.rename(columns={'caret-upcaret-downHRcaret-upcaret-downHR': 'HRHR'})
# rename columns
df.columns = [col[:len(col)//2] for col in df.columns]

df['G'] = df['G'].astype(int)
df['R'] = df['R'].astype(int)
df['H'] = df['H'].astype(int)
df['HR'] = df['HR'].astype(int)

df.size
sorted_df = df.sort_values(by=['HR'], ascending=False).head(10)

print(sorted_df)

