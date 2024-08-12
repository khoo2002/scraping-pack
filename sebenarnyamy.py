import requests
from bs4 import BeautifulSoup
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from PyPDF2 import PdfReader, PdfWriter
from fpdf import FPDF
# import DownloadSaveAsLocalPDF
import requests
import os
import duckdb

str = "https://sebenarnya.my/wp-sitemap-posts-post-{}.xml"
numberPage = 1

pdf_store_path = "../uningest/"
database_store_path = "../database/"
# Check if the pdf_store_path exists
if not os.path.exists(pdf_store_path):
    # If it doesn't exist, create the directory
    os.makedirs(pdf_store_path)

if not os.path.exists(database_store_path):
    # If it doesn't exist, create the directory
    os.makedirs(database_store_path)


class SebenarnyaMYData:
    database = os.path.join(database_store_path,"SebenarnyaMY.db")
    def __init__(self, db_path=database):
        self.database = db_path
        self._initialize_db()

    def _initialize_db(self):
        """
        Initialize the database by creating the table if it does not exist.
        """
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

    def create_record(self, title, date, url):
        """
        Create a new record in the PMO_speech_data table.
        """
        with duckdb.connect(self.database) as conn:
            query = "INSERT INTO SebenarnyaMY (number, title, date, url) VALUES (NEXTVAL('seq_number'),?, ?, ?)"
            conn.execute(query, (title, date, url))

    def read_records(self):
        """
        Read all records from the PMO_speech_data table.
        """
        with duckdb.connect(self.database) as conn:
            return conn.execute("SELECT * FROM SebenarnyaMY").fetchall()

    def update_record(self, number, new_title=None, new_date=None, new_url=None):
        """
        Update an existing record in the SebenarnyaMY table.
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
        
        
        parameters.append(number)
        query = f"UPDATE PMO_speech_data SET {', '.join(updates)} WHERE number = ?"
        
        with duckdb.connect(self.database) as conn:
            conn.execute(query, parameters)

    def delete_record(self, number):
        """
        Delete a record from the PMO_speech_data table.
        """
        with duckdb.connect(self.database) as conn:
            conn.execute("DELETE FROM SebenarnyaMY WHERE number = ?", (number,))

    def get_latest_records(self, limit=10):
        """
        Get the latest records from the database, ordered by date.
        
        :param limit: The maximum number of records to return.
        :return: A list of the latest records, up to the specified limit.
        """
        with duckdb.connect(self.database) as conn:
            query = "SELECT * FROM SebenarnyaMY ORDER BY date DESC LIMIT ?"
            return conn.execute(query, (limit,)).fetchall()
    
    def is_link_in_database(self, url):
        """
        Check if a given URL is already in the SebenarnyaMY table.
        """
        query = "SELECT 1 FROM SebenarnyaMY WHERE url = ? LIMIT 1"
        
        with duckdb.connect(self.database) as conn:
            result = conn.execute(query, (url,)).fetchone()
        
        return result is not None

def get_request_from_sublink(link):
    response = requests.get(link,verify=False)
    return response.text

def get_html(url):
    response = requests.get(url,verify=False)
    soup = BeautifulSoup(response.text, 'html.parser')
    return soup.prettify()
    
def get_info_from_sublink(link):
    html = get_request_from_sublink(link)
    soup = BeautifulSoup(html, 'html.parser')
    # content = soup.find(id='primary').main.article
    title = soup.find('h1', {'class': 'entry-title'}).get_text(strip=True)
    date = soup.find('time', {'class': 'entry-date'}).get_text(strip=True)
    content_div_text = soup.find('div', {'class': 'td-post-content'})
    # Extracting text from the 'div'
    # Specify the tags you're interested in
    tags_of_interest = ['p', 'ol', 'ul', 'li']  # Add more tags as needed
    
    # Find all elements of the specified tags
    elements = content_div_text.find_all(tags_of_interest)

    # Extract and print the text from each element
    content_text = ''
    content_text += 'Source: '+ link +'\nTitle: ' + title + '\nDate: ' + date + '\n'
    for element in elements:
        content_text += element.get_text(strip=True) + ' '
    
    # pdf_link = content.find('object', class_='wp-block-file__embed')['data']
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
    formatted_date = date.split('/')[2]+"-"+date.split('/')[1]+"-"+date.split('/')[0]

    filename = "{}_{}.pdf".format(formatted_date, sanitized_title)
    filename = os.path.join(pdf_store_path, filename)
    
    # filenameTmp1 = "tmp_{}_{}.pdf".format(date, sanitized_title)
    # filenameTmp2 = f"{date}_laterreplace.pdf"
    # download_pdf(pdf_link, filenameTmp1)
    text_to_pdf(content_text, filename)
    # merge_pdfs([filenameTmp1, filenameTmp2], filename)
    # delete_pdf(filenameTmp1, filenameTmp2)
    return title, formatted_date, filename

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

if __name__ == "__main__":
    sebenarnyaMYData = SebenarnyaMYData()
    while True:
        response = requests.get(str.format(numberPage))
        if response.status_code == 200:
            xml = response.text
            xml = xml.split("<loc>")
            print("Page ", numberPage)
            print("Total links: ", len(xml))
            for i in range(0, len(xml)):
                link = xml[i].split("</loc>")[0]
                if(sebenarnyaMYData.is_link_in_database(link) == False):
                    title, date, filename = get_info_from_sublink(link)
                    sebenarnyaMYData.create_record(title, date, link)
                    print("Added: ", link)
                else:
                    print("Already in database: ", link)
            numberPage += 1
        else:
            numberPage -= 1
            break
    print("Sebenarnya My Scrap - update done! - So tired")
    exit()


