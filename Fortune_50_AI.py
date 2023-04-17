import requests, re
import pandas as pd
from bs4 import BeautifulSoup

url = "https://www.forbes.com/lists/ai50/?sh=418f0eb7290f"

response = requests.get(url)
html = response.text

soup = BeautifulSoup(html, "html.parser")

#HEADINGS
headings = soup.find_all("span", {"class": "header-content-text"})
header_list = []
for header in headings:
  header_list.append(header.text.strip())

#ROWS
rows = soup.find_all("div", {"class": "table-cell"})
rows_list = []

for row in rows:
  rows_list.append(row.text)

column_numbers = len(header_list)

rows = []

for i in range(0, len(rows_list), column_numbers):
  chunk = rows_list[i:i + column_numbers]
  rows.append(chunk)

#DATAFRAME
df = pd.DataFrame(rows, columns=header_list)

df.head()
