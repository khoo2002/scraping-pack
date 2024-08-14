import requests
from bs4 import BeautifulSoup
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from PyPDF2 import PdfReader, PdfWriter
from fpdf import FPDF
import io
import os
import duckdb
from datetime import datetime



pdf_store_path = "../uningest/"
database_store_path = "../database/"
# Check if the pdf_store_path exists
if not os.path.exists(pdf_store_path):
    # If it doesn't exist, create the directory
    os.makedirs(pdf_store_path)

if not os.path.exists(database_store_path):
    # If it doesn't exist, create the directory
    os.makedirs(database_store_path)


class PMOSpeechData:
    database = os.path.join(database_store_path,"PMOSpeech.db")
    def __init__(self, db_path=database):
        self.database = db_path
        self._initialize_db()

    def _initialize_db(self):
        """
        Initialize the database by creating the table if it does not exist.
        """
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

    def create_record(self, title, date, url, pdf_path):
        """
        Create a new record in the PMO_speech_data table.
        """
        with duckdb.connect(self.database) as conn:
            query = "INSERT INTO PMO_speech_data (number, title, date, url, pdf_path) VALUES (NEXTVAL('seq_number'),?, ?, ?, ?)"
            conn.execute(query, (title, date, url, pdf_path))

    def read_records(self):
        """
        Read all records from the PMO_speech_data table.
        """
        with duckdb.connect(self.database) as conn:
            return conn.execute("SELECT * FROM PMO_speech_data").fetchall()

    def update_record(self, number, new_title=None, new_date=None, new_url=None, new_pdf_path=None):
        """
        Update an existing record in the PMO_speech_data table.
        """
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

        parameters.append(number)
        query = f"UPDATE PMO_speech_data SET {', '.join(updates)} WHERE number = ?"

        with duckdb.connect(self.database) as conn:
            conn.execute(query, parameters)

    def delete_record(self, number):
        """
        Delete a record from the PMO_speech_data table.
        """
        with duckdb.connect(self.database) as conn:
            conn.execute("DELETE FROM PMO_speech_data WHERE number = ?", (number,))

    def get_latest_records(self, limit=10):
        """
        Get the latest records from the database, ordered by date.

        :param limit: The maximum number of records to return.
        :return: A list of the latest records, up to the specified limit.
        """
        with duckdb.connect(self.database) as conn:
            query = "SELECT * FROM PMO_speech_data ORDER BY date DESC LIMIT ?"
            return conn.execute(query, (limit,)).fetchall()

    def is_link_in_database(self, url):
        """
        Check if a given URL exists in the PMO_speech_data table.
        """
        query = "SELECT 1 FROM PMO_speech_data WHERE url = ? LIMIT 1;"
        with duckdb.connect(self.database) as conn:
            result = conn.execute(query, (url,)).fetchone()
            return result is not None


def download_pdf(url, filename):
    # Send a GET request to the URL
    response = requests.get(url, stream=True, verify=False)
    # Check if the request was successful
    if response.status_code == 200:
        # Open a local file in binary write mode
        with open(filename, 'wb') as f:
            f.write(response.content)
        print(f"PDF downloaded successfully as {filename}.")
    else:
        print("Failed to download the PDF.")

def text_to_pdf(text, output_file):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=12)
    latin_text = text.encode('latin-1', 'replace').decode('latin-1')
    # Define the width and height of the multi_cell
    cell_width = 190  # Adjust the width to fit your layout
    cell_height = 10  # Adjust the height based on your font size and preference
    pdf.multi_cell(cell_width, cell_height, txt=latin_text, align='L')
    pdf.output(output_file)

def merge_pdfs(pdf_list, output):
    pdf_writer = PdfWriter()
    for pdf in pdf_list:
        pdf_reader = PdfReader(pdf)
        for page_num in range(len(pdf_reader.pages)):
            page = pdf_reader.pages[page_num]
            pdf_writer.add_page(page)
    with open(output, 'wb') as out:
        pdf_writer.write(out)

def delete_pdf(*filenames):
    for filename in filenames:
        try:
            os.remove(filename)
            print(f"Deleted {filename}")
        except FileNotFoundError:
            print(f"{filename} not found")
        except Exception as e:
            print(f"Error deleting {filename}: {e}")

def get_request_from_sublink(link):
    response = requests.get(link,verify=False)
    return response.text

def get_html(url):
    response = requests.get(url,verify=False)
    soup = BeautifulSoup(response.text, 'html.parser')
    return soup.prettify()

def count_tr_elements(html):
    soup = BeautifulSoup(html, 'html.parser')
    td_elements = soup.findAll('tr')
    return len(td_elements)-1

def get_n_tr_elements(html,n):
    soup = BeautifulSoup(html, 'html.parser')
    td_elements = soup.findAll('tr')
    return td_elements[n]

def get_info_from_tr(tr_element):
    td_elements = tr_element.findAll('td')
    link = td_elements[0].find('a')['href']
    title = td_elements[0].text.replace("\n","")
    title = title.strip()
    date = td_elements[1].text.replace("\n","")
    date = date.strip()

    return link, title, date

def get_info_from_sublink(link,title,date):
    html = get_request_from_sublink(link)
    soup = BeautifulSoup(html, 'html.parser')
    content = soup.find(id='primary').main.article
    if(content!=None):
        entry_content_div = content.find('div', {'class': 'entry-content'})
        # Extracting text from the 'div'
        # Specify the tags you're interested in
        tags_of_interest = ['p', 'ol', 'ul', 'li']  # Add more tags as needed
        if(entry_content_div!=None):
            # Find all elements of the specified tags
            elements = entry_content_div.find_all(tags_of_interest)

            # Extract and print the text from each element
            content_text = ''
            content_text += 'Source: '+ link +'\nTitle: ' + title + '\nDate: ' + date + '\n'
            if(elements != None):
                for element in elements:
                    content_text += element.get_text(strip=True) + ' '

        pdf_link = content.find('object', class_='wp-block-file__embed')['data']
        # Split the title at ':' and take the part after it if it exists, otherwise use the whole title
        title_part = title.split(':')[-1].strip()
        # Limit the title part to a maximum of 50 characters to ensure the filename is not too long
        max_length = 40
        if len(title_part) > max_length:
            title_part = title_part[:max_length]
        # Sanitize the title to remove/replace characters not allowed in filenames
        # This is a basic example; you might need to expand it based on your requirements
        sanitized_title = title_part.replace('/', '-').replace('\\', '-')
        # Use the sanitized and possibly shortened title in the filename

        filename = "{}_{}.pdf".format(date, sanitized_title)
        filename = os.path.join(pdf_store_path, filename)
        filenameTmp1 = "tmp_{}_{}.pdf".format(date, sanitized_title)
        filenameTmp2 = f"{date}_laterreplace.pdf"
        if(pdf_link != None):
            download_pdf(pdf_link, filenameTmp1)
            text_to_pdf(content_text, filenameTmp2)
            merge_pdfs([filenameTmp1, filenameTmp2], filename)
            delete_pdf(filenameTmp1, filenameTmp2)
        else:
            text_to_pdf(content_text, filename)
        return title, date, filename
    return None

if __name__ == "__main__":
    pmodatabase = PMOSpeechData()
    html = get_html('https://www.pmo.gov.my/speech/')
    for i in range(1,count_tr_elements(html)+1):
        link_url, title, date = get_info_from_tr(get_n_tr_elements(html,i))
        if(not pmodatabase.is_link_in_database(link_url)):
            date_obj = datetime.strptime(date, "%d %b %Y")
            formatted_date = date_obj.strftime("%Y-%m-%d")
            title, date, filename = get_info_from_sublink(link_url,title,date)
            pmodatabase.create_record(title,formatted_date,link_url,filename)
            print(f"{date} - {title} - {link_url} saved in database - I am so tired ;_;")
        else:
            print(f"{date} - {title} - {link_url} already in database - Continue digging")

    print("PMOScrap - update done! - So tired")
    exit()
