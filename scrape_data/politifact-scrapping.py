import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime

def safe_extract(element, selector, attribute=None):
    try:
        found = element.select_one(selector)
        if found:
            return found.get(attribute) if attribute else found.get_text(strip=True)
    except:
        pass
    return ""

def scrape_politifact(max_pages=5):
    base_url = "https://www.politifact.com/factchecks/list/"
    fact_checks = []

    for page in range(1, max_pages + 1):
        url = f"{base_url}?page={page}"
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')

        for article in soup.find_all('li', class_='o-listicle__item'):
            statement = safe_extract(article, '.m-statement__quote')
            source = safe_extract(article, '.m-statement__meta a')
            rating = safe_extract(article, '.m-statement__content img', 'alt')

            footer = safe_extract(article, '.m-statement__footer')
            author, date_str = "", ""
            if '•' in footer:
                author, date_str = footer.split('•')
            else:
                date_str = footer

            author = author.strip()
            date_str = date_str.strip()

            try:
                date = datetime.strptime(date_str, '%B %d, %Y')
                date_formatted = date.strftime('%Y-%m-%d')
            except:
                date_formatted = date_str

            fact_checks.append({
                'statement': statement,
                'source': source,
                'author': author,
                'date': date_formatted,
                'rating': rating
            })

    return fact_checks

def save_to_csv(fact_checks, filename):
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['statement', 'source', 'author', 'date', 'rating']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for check in fact_checks:
            writer.writerow(check)

    print(f"Data saved to {filename}")

if __name__ == "__main__":
    fact_checks = scrape_politifact(max_pages=5)

    filename = f"politifact_factchecks_{datetime.now().strftime('%Y%m%d')}.csv"
    save_to_csv(fact_checks, filename)

    print(f"Scraped {len(fact_checks)} fact checks from PolitiFact")

# import requests
# from bs4 import BeautifulSoup
# import pandas as pd
# from datetime import datetime
# import pyarrow as pa
# import pyarrow.parquet as pq

# def scrape_politifact(start_date, end_date, max_pages=10):
#     base_url = "https://www.politifact.com/factchecks/list/"
#     fact_checks = []

#     for page in range(1, max_pages + 1):
#         url = f"{base_url}?page={page}"
#         response = requests.get(url)
#         soup = BeautifulSoup(response.content, 'html.parser')

#         for article in soup.find_all('li', class_='o-listicle__item'):
#             footer_text = article.find('div', class_='m-statement__body').find('footer').text.strip()

#             # Extract only the date part
#             date_str = footer_text.split('•')[-1].strip()
#             date = datetime.strptime(date_str, '%B %d, %Y')  # Use '%B' for full month name

#             if start_date <= date <= end_date:
#                 statement = article.find('div', class_='m-statement__quote').text.strip()
#                 source = article.find('div', class_='m-statement__meta').find('a').text.strip()
#                 rating = article.find('div', class_='m-statement__content').find('img')['alt']

#                 fact_checks.append({
#                     'statement': statement,
#                     'source': source,
#                     'date': date,
#                     'rating': rating
#                 })
#             elif date < start_date:
#                 return fact_checks  # Stop if we've gone past the start date

#     return fact_checks


# def save_to_parquet(fact_checks, filename):
#     df = pd.DataFrame(fact_checks)
#     table = pa.Table.from_pandas(df)
#     pq.write_table(table, filename)
#     print(f"Data saved to {filename}")

# if __name__ == "__main__":
#     # Example: Scrape data from 2014 to 2016
#     start_date = datetime(2020, 1, 1)
#     end_date = datetime(2024, 9, 15)

#     fact_checks = scrape_politifact(start_date, end_date)

#     filename = f"politifact_factchecks_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.parquet"
#     save_to_parquet(fact_checks, filename)

#     print(f"Scraped {len(fact_checks)} fact checks from PolitiFact")



