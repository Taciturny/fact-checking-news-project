# import requests
# from bs4 import BeautifulSoup
# import pandas as pd
# import time
# import random
# from datetime import datetime
# import csv
# from nltk.tokenize import sent_tokenize
# from nltk.corpus import stopwords
# from nltk.cluster.util import cosine_distance
# import numpy as np

# def safe_extract(element, selector, attribute=None):
#     found = element.select_one(selector) if element else None
#     if not found:
#         return "N/A"
#     if attribute:
#         return found.get(attribute, "N/A")
#     return found.get_text(strip=True)

# def summarize(text, num_sentences=3):
#     sentences = sent_tokenize(text)
#     stop_words = set(stopwords.words('english'))

#     sentence_vectors = []
#     for sentence in sentences:
#         words = [word.lower() for word in sentence.split() if word.lower() not in stop_words]
#         sentence_vectors.append(words)

#     similarity_matrix = np.zeros((len(sentences), len(sentences)))
#     for i in range(len(sentences)):
#         for j in range(len(sentences)):
#             if i != j:
#                 similarity_matrix[i][j] = sentence_similarity(sentence_vectors[i], sentence_vectors[j])

#     sentence_scores = similarity_matrix.sum(axis=1)
#     ranked_sentences = [sentences[i] for i in np.argsort(sentence_scores)[::-1][:num_sentences]]

#     return ' '.join(ranked_sentences)

# def sentence_similarity(sent1, sent2):
#     all_words = list(set(sent1 + sent2))
#     vector1 = [0] * len(all_words)
#     vector2 = [0] * len(all_words)

#     for w in sent1:
#         vector1[all_words.index(w)] += 1
#     for w in sent2:
#         vector2[all_words.index(w)] += 1

#     return 1 - cosine_distance(vector1, vector2)

# def scrape_article_page(url):
#     try:
#         response = requests.get(url)
#         soup = BeautifulSoup(response.content, 'html.parser')

#         article = soup.find('article', class_='m-textblock')
#         if article:
#             paragraphs = article.find_all('p')
#             text = ' '.join([p.get_text() for p in paragraphs])
#             summary = summarize(text)
#             return {'summary': summary}
#         else:
#             return {'summary': "N/A"}
#     except Exception as e:
#         print(f"Error scraping article page: {e}")
#         return {'summary': "N/A"}

# def scrape_politifact(base_url, num_pages):
#     fact_checks = []
#     for page in range(1, num_pages + 1):
#         url = f"{base_url}?page={page}"
#         print(f"Scraping page {page}...")
#         response = requests.get(url)
#         soup = BeautifulSoup(response.content, 'html.parser')

#         for article in soup.find_all('article', class_='m-statement'):
#             try:
#                 claim = safe_extract(article, '.m-statement__quote')
#                 verdict = safe_extract(article, '.m-statement__meter img', 'alt')
#                 source_element = article.select_one('.m-statement__meta .m-statement__name')
#                 source = source_element.get_text(strip=True) if source_element else "N/A"
#                 link_element = article.select_one('.m-statement__content a')
#                 link = link_element['href'] if link_element else "N/A"
#                 full_link = f"https://www.politifact.com{link}" if link != "N/A" else "N/A"

#                 article_content = scrape_article_page(full_link) if full_link != "N/A" else {'summary': "N/A"}

#                 fact_checks.append({
#                     'claim': claim,
#                     'verdict': verdict,
#                     'summary': article_content['summary'],
#                     'source': source,
#                     'link': full_link
#                 })

#                 time.sleep(random.uniform(1, 1))
#             except Exception as e:
#                 print(f"Error processing an article: {e}")
#     return fact_checks

# def update_database(new_data, database_path='./data/politifact_fact_checks.csv'):
#     try:
#         existing_db = pd.read_csv(database_path)
#     except FileNotFoundError:
#         existing_db = pd.DataFrame(columns=['claim', 'verdict', 'summary', 'source', 'link'])

#     new_df = pd.DataFrame(new_data)
#     updated_db = pd.concat([existing_db, new_df], ignore_index=True)
#     updated_db = updated_db.drop_duplicates(subset='link', keep='last')
#     updated_db.to_csv(database_path, index=False, quoting=csv.QUOTE_ALL)

#     print(f"Database updated. Total entries: {len(updated_db)}")

# def main():
#     base_url = 'https://www.politifact.com/factchecks/list/'
#     num_pages = 10  # Adjust this number to scrape more or fewer pages

#     print("Starting database update...")
#     print(f"Script started at {datetime.now()}")
#     new_fact_checks = scrape_politifact(base_url, num_pages)
#     update_database(new_fact_checks)
#     print("Update completed.")
#     print(f"Script completed at {datetime.now()}")

# if __name__ == "__main__":
#     main()





import os
from prefect import flow, task
# from prefect.task_runners import ThreadPoolTaskRunner
import logging
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
from datetime import datetime
import csv
from nltk.tokenize import sent_tokenize
from nltk.corpus import stopwords
from nltk.cluster.util import cosine_distance
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

    return 1 - cosine_distance(vector1, vector2)

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
        print(f"Error scraping article page: {e}")
        return {'summary': "N/A"}

@task(name="get_existing_entries")
def get_existing_entries(database_path='politifact_fact_checks.csv'):
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)
        full_database_path = os.path.join(project_root, 'data', database_path)

        if os.path.exists(full_database_path):
            existing_db = pd.read_csv(full_database_path)
            logging.info(f"Found {len(existing_db)} existing entries in the database.")
            return set(existing_db['link']), set(existing_db['claim'])
        else:
            logging.warning(f"Database file not found at {full_database_path}. Starting with empty sets.")
            return set(), set()
    except Exception as e:
        logging.error(f"Error reading existing entries: {str(e)}")
        return set(), set()

@task(name="scrape_politifact")
def scrape_politifact(base_url, num_pages, existing_links, existing_claims):
    fact_checks = []
    new_articles_count = 0
    consecutive_existing_articles = 0
    max_consecutive_existing = 30  # Stop after finding 30 consecutive existing articles (1 full page)

    for page in range(1, num_pages + 1):
        url = f"{base_url}?page={page}"
        print(f"Scraping page {page}...")
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')

        articles = soup.find_all('article', class_='m-statement')
        page_new_articles = 0

        for article in articles:
            try:
                claim = safe_extract(article, '.m-statement__quote')
                link_element = article.select_one('.m-statement__content a')
                link = link_element['href'] if link_element else "N/A"
                full_link = f"https://www.politifact.com{link}" if link != "N/A" else "N/A"

                if full_link in existing_links or claim in existing_claims:
                    consecutive_existing_articles += 1
                    if consecutive_existing_articles >= max_consecutive_existing:
                        print(f"Found {max_consecutive_existing} consecutive existing articles. Stopping the scrape.")
                        return fact_checks
                    continue

                consecutive_existing_articles = 0  # Reset the counter when a new article is found

                verdict = safe_extract(article, '.m-statement__meter img', 'alt')
                source_element = article.select_one('.m-statement__meta .m-statement__name')
                source = source_element.get_text(strip=True) if source_element else "N/A"

                article_content = scrape_article_page(full_link) if full_link != "N/A" else {'summary': "N/A"}

                fact_checks.append({
                    'claim': claim,
                    'verdict': verdict,
                    'summary': article_content['summary'],
                    'source': source,
                    'link': full_link
                })

                new_articles_count += 1
                page_new_articles += 1
                print(f"Scraped new article: {claim[:50]}...")
                time.sleep(random.uniform(1, 2))
            except Exception as e:
                print(f"Error processing an article: {e}")

        print(f"Scraped {new_articles_count} new articles so far.")

        if page_new_articles == 0:
            print("No new articles on this page. Stopping the scrape.")
            break

    return fact_checks


@task(name="update_database")
def update_database(new_data, database_path='politifact_fact_checks.csv'):
    logging.info("Starting database update...")
    try:
        # Construct the full path to the database file
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)
        full_database_path = os.path.join(project_root, 'data', database_path)

        if os.path.exists(full_database_path):
            existing_db = pd.read_csv(full_database_path)
            logging.info(f"Loaded existing database with {len(existing_db)} entries.")
        else:
            logging.warning(f"Database file not found. Creating new database.")
            existing_db = pd.DataFrame(columns=['claim', 'verdict', 'summary', 'source', 'link'])

        new_df = pd.DataFrame(new_data)
        updated_db = pd.concat([existing_db, new_df], ignore_index=True)
        updated_db = updated_db.drop_duplicates(subset=['link', 'claim'], keep='last')

        # Ensure the directory exists
        os.makedirs(os.path.dirname(full_database_path), exist_ok=True)

        updated_db.to_csv(full_database_path, index=False, quoting=csv.QUOTE_ALL)

        print(f"Database updated. Total entries: {len(updated_db)}")
        logging.info(f"Database updated. Total entries: {len(updated_db)}")
        return len(updated_db)
    except Exception as e:
        logging.error(f"Error updating database: {str(e)}")
        raise

# @flow(task_runner=ThreadPoolTaskRunner(max_workers=10))
@flow(name="politifact_scraper")
def main_flow():
    logging.info("Starting PolitiFact scraper flow")
    base_url = 'https://www.politifact.com/factchecks/list/'
    num_pages = 2

    print("Starting database update...")
    print(f"Script started at {datetime.now()}")
    logging.info(f"Script started at {datetime.now()}")

    existing_links, existing_claims = get_existing_entries()
    logging.info(f"Found {len(existing_links)} existing links and {len(existing_claims)} existing claims.")

    new_fact_checks = scrape_politifact(base_url, num_pages, existing_links, existing_claims)

    if new_fact_checks is None:
        logging.error("Failed to scrape fact checks. scrape_politifact returned None.")
        print("Failed to scrape fact checks.")
        return

    logging.info(f"Scraped {len(new_fact_checks)} new fact checks.")

    if new_fact_checks:
        total_entries = update_database(new_fact_checks)
        logging.info(f"Added {len(new_fact_checks)} new fact checks. Total entries: {total_entries}")
        print(f"Added {len(new_fact_checks)} new fact checks.")
    else:
        print("No new fact checks found.")
        logging.info("No new fact checks found. Database not updated.")

    print("Update completed.")
    print(f"Script completed at {datetime.now()}")
    logging.info(f"Script completed at {datetime.now()}")

if __name__ == "__main__":
    flow.from_source(
        "https://github.com/Taciturny/fact-checking-news-project.git",  # Replace with your repo URL
        entrypoint="scrape_data/weekly-politifact-scraper.py:main_flow"  # Path to your flow file and function name
    ).deploy(
        name="weekly-politifact-scraper",
        work_pool_name="politifact_pool",  # Make sure this work pool exists in Prefect
        build=False,  # Set to False if no Docker image is being built
        cron="0 7 * * 2", # This schedules the flow to run every Tuesday at 7 AM
        ignore_warnings=True
    )

# main_flow.serve(name="weekly-politifact-scraper", cron="0 6 * * 2")
#     schedule = CronSchedule(cron="0 14 * * 1,3,4")
# @flow(task_runner=ThreadPoolTaskRunner())
# def main_flow():
#     base_url = 'https://www.politifact.com/factchecks/list/'
#     num_pages = 5

#     print("Starting database update...")
#     print(f"Flow started at {datetime.now()}")

#     existing_links, existing_claims = get_existing_entries()
#     new_fact_checks = scrape_politifact(base_url, num_pages, existing_links, existing_claims)

#     if new_fact_checks:
#         update_database(new_fact_checks)
#         print(f"Added {len(new_fact_checks)} new fact checks.")
#     else:
#         print("No new fact checks found.")

#     print("Update completed.")
#     print(f"Flow completed at {datetime.now()}")

# if __name__ == "__main__":
#     schedule = CronSchedule(cron="0 14 * * 1,3,4")
#     main_flow.serve(name="weekly-politifact-scraper", schedule=schedule)




# import requests
# from bs4 import BeautifulSoup
# import pandas as pd
# import time
# import random
# from datetime import datetime, timedelta
# import csv
# from nltk.tokenize import sent_tokenize
# from nltk.corpus import stopwords
# from nltk.cluster.util import cosine_distance
# import numpy as np

# def safe_extract(element, selector, attribute=None):
#     found = element.select_one(selector) if element else None
#     if not found:
#         return "N/A"
#     if attribute:
#         return found.get(attribute, "N/A")
#     return found.get_text(strip=True)

# def summarize(text, num_sentences=3):
#     sentences = sent_tokenize(text)
#     stop_words = set(stopwords.words('english'))

#     sentence_vectors = []
#     for sentence in sentences:
#         words = [word.lower() for word in sentence.split() if word.lower() not in stop_words]
#         sentence_vectors.append(words)

#     similarity_matrix = np.zeros((len(sentences), len(sentences)))
#     for i in range(len(sentences)):
#         for j in range(len(sentences)):
#             if i != j:
#                 similarity_matrix[i][j] = sentence_similarity(sentence_vectors[i], sentence_vectors[j])

#     sentence_scores = similarity_matrix.sum(axis=1)
#     ranked_sentences = [sentences[i] for i in np.argsort(sentence_scores)[::-1][:num_sentences]]

#     return ' '.join(ranked_sentences)

# def sentence_similarity(sent1, sent2):
#     all_words = list(set(sent1 + sent2))
#     vector1 = [0] * len(all_words)
#     vector2 = [0] * len(all_words)

#     for w in sent1:
#         vector1[all_words.index(w)] += 1
#     for w in sent2:
#         vector2[all_words.index(w)] += 1

#     return 1 - cosine_distance(vector1, vector2)

# def scrape_article_page(url):
#     try:
#         response = requests.get(url)
#         soup = BeautifulSoup(response.content, 'html.parser')

#         article = soup.find('article', class_='m-textblock')
#         if article:
#             paragraphs = article.find_all('p')
#             text = ' '.join([p.get_text() for p in paragraphs])
#             summary = summarize(text)
#             return {'summary': summary}
#         else:
#             return {'summary': "N/A"}
#     except Exception as e:
#         print(f"Error scraping article page: {e}")
#         return {'summary': "N/A"}

# def get_existing_entries(database_path='../data/new_data.csv'):
#     try:
#         existing_db = pd.read_csv(database_path)
#         return set(existing_db['link']), set(existing_db['claim'])
#     except FileNotFoundError:
#         return set(), set()

# def scrape_politifact(base_url, num_pages, existing_links, existing_claims):
#     fact_checks = []
#     new_articles_count = 0
#     consecutive_existing_articles = 0
#     max_consecutive_existing = 60  # Stop after finding 60 consecutive existing articles (2 full pages)

#     for page in range(1, num_pages + 1):
#         url = f"{base_url}?page={page}"
#         print(f"Scraping page {page}...")
#         response = requests.get(url)
#         soup = BeautifulSoup(response.content, 'html.parser')

#         articles = soup.find_all('article', class_='m-statement')
#         page_new_articles = 0

#         for article in articles:
#             try:
#                 claim = safe_extract(article, '.m-statement__quote')
#                 link_element = article.select_one('.m-statement__content a')
#                 link = link_element['href'] if link_element else "N/A"
#                 full_link = f"https://www.politifact.com{link}" if link != "N/A" else "N/A"

#                 if full_link in existing_links or claim in existing_claims:
#                     consecutive_existing_articles += 1
#                     if consecutive_existing_articles >= max_consecutive_existing:
#                         print(f"Found {max_consecutive_existing} consecutive existing articles. Stopping the scrape.")
#                         return fact_checks
#                     continue

#                 consecutive_existing_articles = 0  # Reset the counter when a new article is found

#                 verdict = safe_extract(article, '.m-statement__meter img', 'alt')
#                 source_element = article.select_one('.m-statement__meta .m-statement__name')
#                 source = source_element.get_text(strip=True) if source_element else "N/A"

#                 article_content = scrape_article_page(full_link) if full_link != "N/A" else {'summary': "N/A"}

#                 fact_checks.append({
#                     'claim': claim,
#                     'verdict': verdict,
#                     'summary': article_content['summary'],
#                     'source': source,
#                     'link': full_link
#                 })

#                 new_articles_count += 1
#                 page_new_articles += 1
#                 print(f"Scraped new article: {claim[:50]}...")
#                 time.sleep(random.uniform(1, 2))
#             except Exception as e:
#                 print(f"Error processing an article: {e}")

#         print(f"Scraped {new_articles_count} new articles so far.")

#         if page_new_articles == 0:
#             print("No new articles on this page. Stopping the scrape.")
#             break

#     return fact_checks

# def update_database(new_data, database_path='../data/new_data.csv'):
#     try:
#         existing_db = pd.read_csv(database_path)
#     except FileNotFoundError:
#         existing_db = pd.DataFrame(columns=['claim', 'verdict', 'summary', 'source', 'link'])

#     new_df = pd.DataFrame(new_data)
#     updated_db = pd.concat([new_df, existing_db], ignore_index=True)
#     updated_db = updated_db.drop_duplicates(subset=['link', 'claim'], keep='first')
#     updated_db.to_csv(database_path, index=False, quoting=csv.QUOTE_ALL)

#     print(f"Database updated. Total entries: {len(updated_db)}")

# def main():
#     base_url = 'https://www.politifact.com/factchecks/list/'
#     num_pages = 1  # Set to a high number to ensure we capture all new articles

#     print("Starting database update...")
#     print(f"Script started at {datetime.now()}")

#     existing_links, existing_claims = get_existing_entries()

#     new_fact_checks = scrape_politifact(base_url, num_pages, existing_links, existing_claims)

#     if new_fact_checks:
#         update_database(new_fact_checks)
#         print(f"Added {len(new_fact_checks)} new fact checks.")
#     else:
#         print("No new fact checks found.")

#     print("Update completed.")
#     print(f"Script completed at {datetime.now()}")

# if __name__ == "__main__":
#     main()



# @task(name="scrape_politifact")
# def scrape_politifact(base_url, num_pages, existing_links, existing_claims):
#     fact_checks = []
#     try:
#         new_articles_count = 0
#         consecutive_existing_articles = 0
#         max_consecutive_existing = 60  # Stop after finding 60 consecutive existing articles (2 full pages)

#         for page in range(1, num_pages + 1):
#             url = f"{base_url}?page={page}"
#             print(f"Scraping page {page}...")
#             response = requests.get(url)
#             soup = BeautifulSoup(response.content, 'html.parser')

#             articles = soup.find_all('article', class_='m-statement')
#             page_new_articles = 0

#             for article in articles:
#                 try:
#                     claim = safe_extract(article, '.m-statement__quote')
#                     link_element = article.select_one('.m-statement__content a')
#                     link = link_element['href'] if link_element else "N/A"
#                     full_link = f"https://www.politifact.com{link}" if link != "N/A" else "N/A"

#                     if full_link in existing_links or claim in existing_claims:
#                         consecutive_existing_articles += 1
#                         if consecutive_existing_articles >= max_consecutive_existing:
#                             print(f"Found {max_consecutive_existing} consecutive existing articles. Stopping the scrape.")
#                             logging.info(f"Scraped {len(fact_checks)} new fact checks.")
#                             return fact_checks
#                         continue

#                     consecutive_existing_articles = 0  # Reset the counter when a new article is found

#                     verdict = safe_extract(article, '.m-statement__meter img', 'alt')
#                     source_element = article.select_one('.m-statement__meta .m-statement__name')
#                     source = source_element.get_text(strip=True) if source_element else "N/A"

#                     article_content = scrape_article_page(full_link) if full_link != "N/A" else {'summary': "N/A"}

#                     fact_checks.append({
#                         'claim': claim,
#                         'verdict': verdict,
#                         'summary': article_content['summary'],
#                         'source': source,
#                         'link': full_link
#                     })

#                     new_articles_count += 1
#                     page_new_articles += 1
#                     # print(f"Scraped new article: {claim[:50]}...")
#                     time.sleep(random.uniform(1, 2))
#                 except Exception as e:
#                     print(f"Error processing an article: {e}")

#             print(f"Scraped {new_articles_count} new articles so far.")

#             if page_new_articles == 0:
#                 print("No new articles on this page. Stopping the scrape.")
#                 break
#         return fact_checks  # Return the list, even if it's empty
#     except Exception as e:
#         logging.error(f"Error occurred while scraping: {str(e)}")
#         return []


# def update_database(new_data, database_path='../data/politifact_fact_checks.csv'):
#     logging.info("Starting database update...")
#     try:
#         existing_db = pd.read_csv(database_path)
#         logging.info(f"Loaded existing database with {len(existing_db)} entries.")
#     except FileNotFoundError:
#         logging.warning(f"Database file not found. Creating new database.")
#         existing_db = pd.DataFrame(columns=['claim', 'verdict', 'summary', 'source', 'link'])

#     new_df = pd.DataFrame(new_data)
#     updated_db = pd.concat([existing_db, new_df], ignore_index=True)
#     updated_db = updated_db.drop_duplicates(subset=['link', 'claim'], keep='last')
#     updated_db.to_csv(database_path, index=False, quoting=csv.QUOTE_ALL)

#     print(f"Database updated. Total entries: {len(updated_db)}")
#     logging.info(f"Database updated. Total entries: {len(updated_db)}")
#     return len(updated_db)


# def get_existing_entries(database_path='../data/politifact_fact_checks.csv'):
#     try:
#         existing_db = pd.read_csv(database_path)
#         logging.info(f"Found {len(existing_db)} existing entries in the database.")
#         return set(existing_db['link']), set(existing_db['claim'])
#     except FileNotFoundError:
#         logging.warning(f"Database file not found at {database_path}. Starting with empty sets.")
#         return set(), set()
