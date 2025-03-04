from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

OUTPUT_FOLDER = "."
OUTPUT_FILE_NAME = "open_insider_10y.csv"

def get_data_as_df(soup):
    table = soup.find('table', {'class': 'tinytable'})  # Adjust class or ID if needed

    headers = [header.text.strip() for header in table.find_all('th')]

    # Extract rows
    rows = table.find_all('tr')
    data = []
    for row in rows[1:]:  # Skip the header row
        cells = row.find_all('td')
        data.append([cell.text.strip() for cell in cells])

    for i in range(len(headers)):
        headers[i] = headers[i].replace("\xa0", " ")

    # Convert to a DataFrame
    df = pd.DataFrame(data, columns=headers)

    return df

delta = timedelta(days=180)
end_date = datetime.strptime("04-03-2025", "%d-%m-%Y")
counter = 0


soups = []
while counter < 10 * 365:
    start_date = end_date - delta
    print(f"Scraping interval ({start_date}, {end_date})")
    url = f'http://openinsider.com/screener?s=&o=&pl=&ph=&ll=&lh=&fd=-1&fdr={start_date.month}%2F{start_date.day}%2F{start_date.year}+-+{end_date.month}%2F{end_date.day}%2F{end_date.year}&td=0&tdr=&fdlyl=&fdlyh=&daysago=&xp=1&excludeDerivRelated=1&vl=100&vh=&ocl=&och=&sic1=-1&sicl=100&sich=9999&isofficer=1&iscob=1&isceo=1&ispres=1&iscoo=1&iscfo=1&isgc=1&isvp=1&grp=0&nfl=&nfh=&nil=&nih=&nol=&noh=&v2l=&v2h=&oc2l=&oc2h=&sortcol=0&cnt=1000&page=1'
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    soups.append(soup)
    end_date = start_date
    counter += 180

dfs = []
for soup in soups: dfs.append(get_data_as_df(soup))

df = pd.concat(dfs)

df.to_csv(os.path.join(OUTPUT_FOLDER, OUTPUT_FILE_NAME), index=False)