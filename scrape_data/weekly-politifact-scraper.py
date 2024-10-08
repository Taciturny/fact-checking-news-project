import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
import time
import random
from datetime import datetime
from nltk.tokenize import sent_tokenize
from nltk.corpus import stopwords
import numpy as np
from scipy.spatial.distance import cosine
from prefect import task, flow
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the path to the data folder
DATA_FOLDER = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')

@task(name="safe_extract")
def safe_extract(element, selector, attribute=None):
    found = element.select_one(selector) if element else None
    if not found:
        return "N/A"
    if attribute:
        return found.get(attribute, "N/A")
    return found.get_text(strip=True)

def summarize(text, num_sentences=3):
    sentences = sent_tokenize(text)
    stop_words = set(stopwords.words('english'))

    sentence_vectors = []
    for sentence in sentences:
        words = [word.lower() for word in sentence.split() if word.lower() not in stop_words]
        sentence_vectors.append(words)

    similarity_matrix = np.zeros((len(sentences), len(sentences)))
    for i in range(len(sentences)):
        for j in range(len(sentences)):
            if i != j:
                similarity_matrix[i][j] = sentence_similarity(sentence_vectors[i], sentence_vectors[j])

    sentence_scores = similarity_matrix.sum(axis=1)
    ranked_sentences = [sentences[i] for i in np.argsort(sentence_scores)[::-1][:num_sentences]]

    return ' '.join(ranked_sentences)

def sentence_similarity(sent1, sent2):
    all_words = list(set(sent1 + sent2))
    vector1 = [0] * len(all_words)
    vector2 = [0] * len(all_words)

    for w in sent1:
        vector1[all_words.index(w)] += 1
    for w in sent2:
        vector2[all_words.index(w)] += 1

    return 1 - cosine(vector1, vector2)

@task(name="scrape_article_page")
def scrape_article_page(url):
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')

        article = soup.find('article', class_='m-textblock')
        if article:
            paragraphs = article.find_all('p')
            text = ' '.join([p.get_text() for p in paragraphs])
            summary = summarize(text)
            return {'summary': summary}
        else:
            return {'summary': "N/A"}
    except Exception as e:
        logging.error(f"Error scraping article page: {e}")
        return {'summary': "N/A"}

@task(name="load_existing_data")
def load_existing_data(filename):
    file_path = os.path.join(DATA_FOLDER, filename)
    try:
        df = pd.read_csv(file_path)
        return df.to_dict('records')
    except pd.errors.EmptyDataError:
        logging.warning(f"The file at {file_path} is empty. Starting fresh.")
        return []
    except FileNotFoundError:
        logging.info(f"No existing file found at {file_path}. Starting fresh.")
        return []
    except Exception as e:
        logging.error(f"Error loading data from {file_path}: {str(e)}")
        return []

@task(name="scrape_politifact")
def scrape_politifact(base_url, num_pages, existing_data):
    fact_checks = []
    existing_links = set(item['link'] for item in existing_data)

    for page in range(1, num_pages + 1):
        url = f"{base_url}?page={page}"
        logging.info(f"Scraping page {page}...")
        response = requests.get(url)
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.content, 'html.parser')

        articles = soup.find_all('article', class_='m-statement')

        for article in articles:
            try:
                link_element = article.select_one('.m-statement__content a')
                link = link_element['href'] if link_element else "N/A"
                full_link = f"https://www.politifact.com{link}" if link != "N/A" else "N/A"

                if full_link in existing_links:
                    logging.info(f"Encountered existing article: {full_link}. Stopping scrape.")
                    return fact_checks  # Stop scraping and return collected fact checks

                claim = safe_extract(article, '.m-statement__quote')
                verdict = safe_extract(article, '.m-statement__meter img', 'alt')
                source_element = article.select_one('.m-statement__meta .m-statement__name')
                source = source_element.get_text(strip=True) if source_element else "N/A"

                article_content = scrape_article_page(full_link) if full_link != "N/A" else {'summary': "N/A"}

                fact_checks.append({
                    'claim': claim,
                    'verdict': verdict,
                    'summary': article_content['summary'],
                    'source': source,
                    'link': full_link,
                })

                logging.info(f"Scraped new article: {claim[:50]}...")
                time.sleep(random.uniform(1, 2))
            except Exception as e:
                logging.error(f"Error processing an article: {e}")

        if not fact_checks:
            logging.info("No new articles found. Stopping scraping.")
            break

    return fact_checks


@task(name="save_to_csv")
def save_to_csv(new_data, filename):
    file_path = os.path.join(DATA_FOLDER, filename)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # Read existing data
    try:
        existing_df = pd.read_csv(file_path, encoding='utf-8')
    except FileNotFoundError:
        existing_df = pd.DataFrame()

    # Create DataFrame from new data
    new_df = pd.DataFrame(new_data)

    # Combine new data with existing data, new data on top
    combined_df = pd.concat([new_df, existing_df]).drop_duplicates(subset=['link'], keep='first')

    # Save combined data
    combined_df.to_csv(file_path, index=False, encoding='utf-8')

    return len(new_df)


@flow(name="politifact_scraper")
def main_flow():
    logging.info("Starting PolitiFact scraper flow")
    base_url = 'https://www.politifact.com/factchecks/list/'
    num_pages = 1
    csv_filename = 'politifact_fact_checks.csv'   #'politifact_fact_checks.csv' new_df

    logging.info(f"Script started at {datetime.now()}")

    existing_data = load_existing_data(csv_filename)
    logging.info(f"Loaded {len(existing_data)} existing fact checks")

    new_fact_checks = scrape_politifact(base_url, num_pages, existing_data)
    logging.info(f"Scraped {len(new_fact_checks)} new fact checks")

    if new_fact_checks:
        num_saved = save_to_csv(new_fact_checks, csv_filename)
        logging.info(f"Saved {num_saved} new fact checks to {csv_filename}")
    else:
        logging.info("No new fact checks found.")

    logging.info("Scraping completed.")
    logging.info(f"Script completed at {datetime.now()}")

if __name__ == "__main__":
    main_flow()


if __name__ == "__main__":
    flow.from_source(
        "https://github.com/Taciturny/fact-checking-news-project.git",  # Replace with your repo URL
        entrypoint="scrape_data/weekly-politifact-scraper.py:main_flow"  # Path to your flow file and function name
    ).deploy(
        name="politifact-scraper",
        work_pool_name="politifact-scraper",  # Make sure this work pool exists in Prefect
        build=False,  # Set to False if no Docker image is being built
        cron="0 15 * * 2",
        # cron="0 7 * * 2", # This schedules the flow to run every Tuesday at 7 AM
        ignore_warnings=True
    )


# if __name__ == "__main__":
#     main_flow.serve(
#         name="politifact_tuesday_scraper",
#         crcron="0 14 * * 2",
#         tags=["politifact", "scraper"],
#         work_queue_name="politifact-scraper"
#     )

# prefect deployment apply main_flow-deployment.yaml
#  prefect work-pool create politifact-scraper
# prefect worker start --pool politifact-scraper
# prefect worker start --queue politifact-scraper

#  prefect deployment run 'politifact_scraper/weekly-politifact-scraper'
# if __name__ == "__main__":
#     deployment = Deployment.build_from_flow(
#         flow=main_flow,
#         name="politifact_sunday_scraper",
#         schedule=CronSchedule(cron="0 14 * * 0"),  # Run at 2 PM (14:00) every Sunday
#         work_queue_name="politifact-scraper"
#     )
#     deployment.apply()
