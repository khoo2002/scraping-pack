import requests
from bs4 import BeautifulSoup
from fpdf import FPDF
from PyPDF2 import PdfReader, PdfWriter
import os
import duckdb
from datetime import datetime
import urllib3
import logging

# Suppress insecure request warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
PDF_STORE_PATH = "../uningest/"
DATABASE_STORE_PATH = "../database/"

# Ensure necessary directories exist
os.makedirs(PDF_STORE_PATH, exist_ok=True)
os.makedirs(DATABASE_STORE_PATH, exist_ok=True)

class PMOSpeechData:
    def __init__(self, db_path=os.path.join(DATABASE_STORE_PATH, "PMOSpeech.db")):
        self.database = db_path
        self._initialize_db()

    def _initialize_db(self):
        try:
            with duckdb.connect(self.database) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS PMO_speech_data (
                        number INTEGER PRIMARY KEY,
                        title VARCHAR NOT NULL,
                        date DATE NOT NULL,
                        url VARCHAR NOT NULL,
                        pdf_path VARCHAR NOT NULL
                    );
                    CREATE SEQUENCE IF NOT EXISTS seq_number START 1;
                """)
            logger.info("Database initialized.")
        except Exception as e:
            logger.error(f"Error initializing database: {e}")

    def create_record(self, title, date, url, pdf_path):
        try:
            with duckdb.connect(self.database) as conn:
                query = "INSERT INTO PMO_speech_data (number, title, date, url, pdf_path) VALUES (NEXTVAL('seq_number'), ?, ?, ?, ?)"
                conn.execute(query, (title, date, url, pdf_path))
            logger.info(f"Record created: {title}, {date}, {url}, {pdf_path}")
        except Exception as e:
            logger.error(f"Error creating record: {e}")

    def read_records(self):
        try:
            with duckdb.connect(self.database) as conn:
                return conn.execute("SELECT * FROM PMO_speech_data").fetchall()
        except Exception as e:
            logger.error(f"Error reading records: {e}")
            return []

    def update_record(self, number, new_title=None, new_date=None, new_url=None, new_pdf_path=None):
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
        if new_pdf_path:
            updates.append("pdf_path = ?")
            parameters.append(new_pdf_path)

        if updates:
            parameters.append(number)
            query = f"UPDATE PMO_speech_data SET {', '.join(updates)} WHERE number = ?"
            try:
                with duckdb.connect(self.database) as conn:
                    conn.execute(query, parameters)
                logger.info(f"Record updated: {number}")
            except Exception as e:
                logger.error(f"Error updating record: {e}")

    def delete_record(self, number):
        try:
            with duckdb.connect(self.database) as conn:
                conn.execute("DELETE FROM PMO_speech_data WHERE number = ?", (number,))
            logger.info(f"Record deleted: {number}")
        except Exception as e:
            logger.error(f"Error deleting record: {e}")

    def get_latest_records(self, limit=10):
        try:
            with duckdb.connect(self.database) as conn:
                query = "SELECT * FROM PMO_speech_data ORDER BY date DESC LIMIT ?"
                return conn.execute(query, (limit,)).fetchall()
        except Exception as e:
            logger.error(f"Error getting latest records: {e}")
            return []

    def is_link_in_database(self, url):
        query = "SELECT 1 FROM PMO_speech_data WHERE url = ? LIMIT 1;"
        try:
            with duckdb.connect(self.database) as conn:
                result = conn.execute(query, (url,)).fetchone()
            return result is not None
        except Exception as e:
            logger.error(f"Error checking link in database: {e}")
            return False

def download_pdf(url, filename):
    try:
        response = requests.get(url, stream=True, verify=False)
        response.raise_for_status()
        with open(filename, 'wb') as f:
            f.write(response.content)
        logger.info(f"PDF downloaded successfully as {filename}.")
    except requests.RequestException as e:
        logger.error(f"Failed to download the PDF from {url}: {e}")

def text_to_pdf(text, output_file):
    try:
        pdf = FPDF()
        pdf.add_page()
        pdf.set_font("Arial", size=12)
        latin_text = text.encode('latin-1', 'replace').decode('latin-1')
        cell_width = 190
        cell_height = 10
        pdf.multi_cell(cell_width, cell_height, txt=latin_text, align='L')
        pdf.output(output_file)
        logger.info(f"Text saved to PDF as {output_file}.")
    except Exception as e:
        logger.error(f"Error converting text to PDF: {e}")

def merge_pdfs(pdf_list, output):
    try:
        pdf_writer = PdfWriter()
        for pdf in pdf_list:
            pdf_reader = PdfReader(pdf)
            for page_num in range(len(pdf_reader.pages)):
                page = pdf_reader.pages[page_num]
                pdf_writer.add_page(page)
        with open(output, 'wb') as out:
            pdf_writer.write(out)
        logger.info(f"PDFs merged into {output}.")
    except Exception as e:
        logger.error(f"Error merging PDFs: {e}")

def delete_pdf(*filenames):
    for filename in filenames:
        try:
            os.remove(filename)
            logger.info(f"Deleted {filename}")
        except FileNotFoundError:
            logger.warning(f"{filename} not found")
        except Exception as e:
            logger.error(f"Error deleting {filename}: {e}")

def get_request_from_sublink(link):
    try:
        response = requests.get(link, verify=False)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        logger.error(f"Error fetching content from {link}: {e}")
        return ""

def get_html(url):
    try:
        response = requests.get(url, verify=False)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        return soup.prettify()
    except requests.RequestException as e:
        logger.error(f"Error fetching HTML from {url}: {e}")
        return ""

def count_tr_elements(html):
    try:
        soup = BeautifulSoup(html, 'html.parser')
        td_elements = soup.findAll('tr')
        return len(td_elements) - 1
    except Exception as e:
        logger.error(f"Error counting 'tr' elements: {e}")
        return 0

def get_n_tr_elements(html, n):
    try:
        soup = BeautifulSoup(html, 'html.parser')
        td_elements = soup.findAll('tr')
        return td_elements[n]
    except IndexError:
        logger.error(f"Index {n} out of range for 'tr' elements")
        return None
    except Exception as e:
        logger.error(f"Error getting 'tr' element {n}: {e}")
        return None

def get_info_from_tr(tr_element):
    try:
        td_elements = tr_element.findAll('td')
        link = td_elements[0].find('a')['href']
        title = td_elements[0].text.strip()
        date = td_elements[1].text.strip()
        return link, title, date
    except Exception as e:
        logger.error(f"Error extracting info from 'tr' element: {e}")
        return "", "", ""

def get_info_from_sublink(link, title, date):
    html = get_request_from_sublink(link)
    if html:
        soup = BeautifulSoup(html, 'html.parser')
        content = soup.find(id='primary').main.article
        if content:
            entry_content_div = content.find('div', {'class': 'entry-content'})
            content_text = ''
            if entry_content_div:
                tags_of_interest = ['p', 'ol', 'ul', 'li']
                elements = entry_content_div.find_all(tags_of_interest)
                content_text += f'Source: {link}\nTitle: {title}\nDate: {date}\n'
                for element in elements:
                    content_text += element.get_text(strip=True) + ' '

            pdf_link = content.find('object', class_='wp-block-file__embed')
            if pdf_link:
                pdf_link = pdf_link['data']
                title_part = title.split(':')[-1].strip()
                max_length = 40
                if len(title_part) > max_length:
                    title_part = title_part[:max_length]
                sanitized_title = title_part.replace('/', '-').replace('\\', '-')
                filename = os.path.join(PDF_STORE_PATH, f"{date}_{sanitized_title}.pdf")
                filenameTmp1 = f"tmp_{date}_{sanitized_title}.pdf"
                filenameTmp2 = f"{date}_laterreplace.pdf"

                download_pdf(pdf_link, filenameTmp1)
                text_to_pdf(content_text, filenameTmp2)
                merge_pdfs([filenameTmp1, filenameTmp2], filename)
                delete_pdf(filenameTmp1, filenameTmp2)
            else:
                title_part = title.split(':')[-1].strip()
                max_length = 40
                if len(title_part) > max_length:
                    title_part = title_part[:max_length]
                sanitized_title = title_part.replace('/', '-').replace('\\', '-')
                filename = os.path.join(PDF_STORE_PATH, f"{date}_{sanitized_title}.pdf")
                text_to_pdf(content_text, filename)

            return title, date, filename
    return None

if __name__ == "__main__":
    pmodatabase = PMOSpeechData()
    html = get_html('https://www.pmo.gov.my/speech/')
    if html:
        for i in range(1, count_tr_elements(html) + 1):
            tr_element = get_n_tr_elements(html, i)
            if tr_element:
                link_url, title, date = get_info_from_tr(tr_element)
                if link_url and not pmodatabase.is_link_in_database(link_url):
                    try:
                        date_obj = datetime.strptime(date, "%d %b %Y")
                        formatted_date = date_obj.strftime("%Y-%m-%d")
                        title, date, filename = get_info_from_sublink(link_url, title, date)
                        if title and date and filename:
                            pmodatabase.create_record(title, formatted_date, link_url, filename)
                            logger.info(f"{date} - {title} - {link_url} saved in database.")
                        else:
                            logger.warning(f"Failed to process: {link_url}")
                    except ValueError as e:
                        logger.error(f"Date parsing error for {date}: {e}")
                else:
                    logger.info(f"{date} - {title} - {link_url} already in database.")
    logger.info("PMOScrap - update done!")
