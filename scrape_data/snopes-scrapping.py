import requests
from bs4 import BeautifulSoup
import logging
import csv
import re
import time
from urllib.parse import urljoin

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def scrape_snopes_fact_check_urls(base_url, num_pages=2):
    all_article_links = []
    current_page = 1
    next_page_url = base_url

    while next_page_url and current_page <= num_pages:
        logging.info(f"Scraping page {current_page}: {next_page_url}")
        try:
            response = requests.get(next_page_url)
            response.raise_for_status()
        except requests.RequestException as e:
            logging.error(f"Error fetching page {current_page}: {e}")
            break

        soup = BeautifulSoup(response.content, 'html.parser')
        article_links = [urljoin(base_url, link['href']) for link in soup.find_all('a', class_='outer_article_link_wrapper', href=True)]
        all_article_links.extend(article_links)
        logging.info(f"Found {len(article_links)} URLs on page {current_page}")

        # Check for the "Next" button to proceed to the next page
        next_button = soup.find('a', class_='next-button')
        if next_button and 'href' in next_button.attrs:
            next_page_url = urljoin(base_url, next_button['href'])  # Get the URL of the next page
            current_page += 1
        else:
            next_page_url = None  # No more pages to scrape

        time.sleep(2)  # Be respectful to the website

    logging.info(f"Total {len(all_article_links)} URLs found across {num_pages} pages")
    return all_article_links

def fetch_article_html(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        logging.error(f"Error fetching the article: {e}")
        return None

def clean_rating(rating_text):
    if rating_text:
        cleaned_text = ' '.join(rating_text.split())
        cleaned_text = re.sub(r'<.*?>', '', cleaned_text)
        rating_match = re.match(r'^[^\s]+', cleaned_text)
        return rating_match.group(0) if rating_match else 'N/A'
    return 'N/A'

def extract_article_data(article_html):
    soup = BeautifulSoup(article_html, 'html.parser')

    title = soup.find('h1').text.strip() if soup.find('h1') else 'N/A'
    author = soup.find('h3', class_='author_name').text.strip() if soup.find('h3', class_='author_name') else 'N/A'
    date = soup.find('h3', class_='publish_date').text.strip() if soup.find('h3', class_='publish_date') else 'N/A'
    claim = soup.find('div', class_='claim_cont').text.strip() if soup.find('div', class_='claim_cont') else 'N/A'
    raw_rating = soup.find('div', class_='rating_title_wrap').text.strip() if soup.find('div', class_='rating_title_wrap') else 'N/A'
    rating = clean_rating(raw_rating)

    return {
        'title': title,
        'author': author,
        'date': date,
        'claim': claim,
        'rating': rating
    }

def save_to_csv(data, filename):
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['title', 'author', 'date', 'claim', 'rating']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in data:
                writer.writerow(row)
        logging.info(f"Data successfully saved to {filename}")
    except IOError as e:
        logging.error(f"Error saving data to CSV: {e}")

def main():
    base_url = "https://www.snopes.com/fact-check/"
    output_file = "snopes_factchecks_data.csv"
    num_pages = 8 # Set number of pages to scrape

    urls = scrape_snopes_fact_check_urls(base_url, num_pages)
    all_article_data = []

    logging.info(f"Starting to scrape {len(urls)} articles")
    for i, url in enumerate(urls, 1):
        logging.info(f"Scraping article {i} of {len(urls)}")
        article_html = fetch_article_html(url)
        if article_html:
            article_data = extract_article_data(article_html)
            all_article_data.append(article_data)
            time.sleep(2)  # Be respectful to the website
        else:
            logging.error(f"Failed to fetch the article from {url}. Skipping.")

    save_to_csv(all_article_data, output_file)
    logging.info(f"Scraping completed. {len(all_article_data)} articles scraped and saved.")

if __name__ == "__main__":
    main()
