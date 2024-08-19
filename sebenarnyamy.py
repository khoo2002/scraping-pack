import os
import logging
import requests
from bs4 import BeautifulSoup
from fpdf import FPDF
import duckdb
import urllib3

# Suppress insecure request warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Constants
XML_URL_TEMPLATE = "https://sebenarnya.my/wp-sitemap-posts-post-{}.xml"
PDF_STORE_PATH = "../uningest/"
DATABASE_STORE_PATH = "../database/"
NUMBER_PAGE_START = 1

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Ensure necessary directories exist
os.makedirs(PDF_STORE_PATH, exist_ok=True)
os.makedirs(DATABASE_STORE_PATH, exist_ok=True)

class SebenarnyaMYData:
    def __init__(self, db_path=os.path.join(DATABASE_STORE_PATH, "SebenarnyaMY.db")):
        self.database = db_path
        self._initialize_db()

    def _initialize_db(self):
        try:
            with duckdb.connect(self.database) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS SebenarnyaMY (
                        number INTEGER PRIMARY KEY,
                        title VARCHAR NOT NULL,
                        date DATE NOT NULL,
                        url VARCHAR NOT NULL
                    );
                    CREATE SEQUENCE IF NOT EXISTS seq_number START 1;
                """)
            logger.info("Database initialized.")
        except Exception as e:
            logger.error(f"Error initializing database: {e}")

    def create_record(self, title, date, url):
        try:
            with duckdb.connect(self.database) as conn:
                query = "INSERT INTO SebenarnyaMY (number, title, date, url) VALUES (NEXTVAL('seq_number'), ?, ?, ?)"
                conn.execute(query, (title, date, url))
            logger.info(f"Record created: {title}, {date}, {url}")
        except Exception as e:
            logger.error(f"Error creating record: {e}")

    def read_records(self):
        try:
            with duckdb.connect(self.database) as conn:
                return conn.execute("SELECT * FROM SebenarnyaMY").fetchall()
        except Exception as e:
            logger.error(f"Error reading records: {e}")
            return []

    def update_record(self, number, new_title=None, new_date=None, new_url=None):
        updates = []
        parameters = []

        if new_title:
            updates.append("title = ?")
            parameters.append(new_title)
        if new_date:
            updates.append("date = ?")
            parameters.append(new_date)
        if new_url:
            updates.append("url = ?")
            parameters.append(new_url)

        if updates:
            parameters.append(number)
            query = f"UPDATE SebenarnyaMY SET {', '.join(updates)} WHERE number = ?"
            try:
                with duckdb.connect(self.database) as conn:
                    conn.execute(query, parameters)
                logger.info(f"Record updated: {number}")
            except Exception as e:
                logger.error(f"Error updating record: {e}")

    def delete_record(self, number):
        try:
            with duckdb.connect(self.database) as conn:
                conn.execute("DELETE FROM SebenarnyaMY WHERE number = ?", (number,))
            logger.info(f"Record deleted: {number}")
        except Exception as e:
            logger.error(f"Error deleting record: {e}")

    def get_latest_records(self, limit=10):
        try:
            with duckdb.connect(self.database) as conn:
                query = "SELECT * FROM SebenarnyaMY ORDER BY date DESC LIMIT ?"
                return conn.execute(query, (limit,)).fetchall()
        except Exception as e:
            logger.error(f"Error getting latest records: {e}")
            return []

    def is_link_in_database(self, url):
        query = "SELECT 1 FROM SebenarnyaMY WHERE url = ? LIMIT 1"
        try:
            with duckdb.connect(self.database) as conn:
                result = conn.execute(query, (url,)).fetchone()
            return result is not None
        except Exception as e:
            logger.error(f"Error checking link in database: {e}")
            return False

def fetch_html(url):
    try:
        response = requests.get(url, verify=False)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        logger.error(f"Error fetching {url}: {e}")
        return ""

def parse_html(html):
    try:
        soup = BeautifulSoup(html, 'html.parser')
        return soup
    except Exception as e:
        logger.error(f"Error parsing HTML: {e}")
        return None

def extract_info_from_soup(soup, link):
    try:
        title_tag = soup.find('h1', {'class': 'entry-title'})
        title = title_tag.get_text(strip=True) if title_tag else link.split('/')[-2]

        date_tag = soup.find('time', {'class': 'entry-date'})
        date = date_tag.get_text(strip=True) if date_tag else "26/11/2002"

        content_div = soup.find('div', {'class': 'td-post-content'})
        content_text = ""
        if content_div:
            tags_of_interest = ['p', 'ol', 'ul', 'li']
            elements = content_div.find_all(tags_of_interest)
            content_text += f'Source: Sebenarnya My ({link})\nTitle: {title}\nDate: {date}\n'
            for element in elements:
                content_text += element.get_text(strip=True) + ' '

        return title, date, content_text
    except Exception as e:
        logger.error(f"Error extracting info from soup: {e}")
        return "", "", ""

def format_title(title):
    try:
        title_part = title.split(':')[-1].strip()
        max_length = 40
        if len(title_part) > max_length:
            title_part = title_part[:max_length]
        return title_part.replace('/', '-').replace('\\', '-')
    except Exception as e:
        logger.error(f"Error formatting title: {e}")
        return "untitled"

def save_text_to_pdf(text, output_file):
    try:
        pdf = FPDF()
        pdf.add_page()
        pdf.set_font("Arial", size=12)
        latin_text = text.encode('latin-1', 'replace').decode('latin-1')
        cell_width = 190
        cell_height = 10
        pdf.multi_cell(cell_width, cell_height, txt=latin_text, align='L')
        pdf.output(output_file)
        logger.info(f"PDF saved: {output_file}")
    except Exception as e:
        logger.error(f"Error saving PDF: {e}")

def main():
    sebenarnyaMYData = SebenarnyaMYData()
    page_number = NUMBER_PAGE_START

    while True:
        xml_url = XML_URL_TEMPLATE.format(page_number)
        xml_content = fetch_html(xml_url)

        if not xml_content:
            logger.info(f"Stopping. No content found at page {page_number}.")
            break

        xml_parts = xml_content.split("<loc>")
        logger.info(f"Page {page_number}")
        logger.info(f"Total links: {len(xml_parts) - 1}")

        for part in xml_parts[1:]:
            link = part.split("</loc>")[0].strip()
            logger.info(f"Processing link: {link}")

            if not sebenarnyaMYData.is_link_in_database(link):
                html_content = fetch_html(link)
                soup = parse_html(html_content)
                if soup:
                    title, date, content_text = extract_info_from_soup(soup, link)

                    formatted_date = date.split('/')[2] + "-" + date.split('/')[1] + "-" + date.split('/')[0]
                    sanitized_title = format_title(title)
                    filename = os.path.join(PDF_STORE_PATH, f"{formatted_date}_{sanitized_title}.pdf")

                    save_text_to_pdf(content_text, filename)
                    sebenarnyaMYData.create_record(title, date, link)
                    logger.info(f"Added: {link}")
                else:
                    logger.warning(f"Failed to parse HTML for link: {link}")
            else:
                logger.info(f"Already in database: {link}")

        page_number += 1

    logger.info("Sebenarnya My Scrap - update done!")

if __name__ == "__main__":
    main()
