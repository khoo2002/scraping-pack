import DownloadSaveAsLocalPDF
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
                url VARCHAR NOT NULL
            );
            
            CREATE SEQUENCE IF NOT EXISTS seq_number START 1;
            """)

    def create_record(self, url):
        """
        Create a new record in the PMO_speech_data table.
        """
        with duckdb.connect(self.database) as conn:
            query = "INSERT INTO SebenarnyaMY (number, url) VALUES (NEXTVAL('seq_number'),?)"
            conn.execute(query, (url))

    def read_records(self):
        """
        Read all records from the PMO_speech_data table.
        """
        with duckdb.connect(self.database) as conn:
            return conn.execute("SELECT * FROM SebenarnyaMY").fetchall()

    def update_record(self, number, new_url=None):
        """
        Update an existing record in the SebenarnyaMY table.
        """
        updates = []
        parameters = []
        
        if new_url:
            updates.append("url = ?")
            parameters.append(new_url)
        
        parameters.append(number)
        query = f"UPDATE SebenarnyaMY SET {', '.join(updates)} WHERE number = ?"
        
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

sebenarnyaMYData = SebenarnyaMYData()
while True:
    response = requests.get(str.format(numberPage))
    if response.status_code == 200:
        xml = response.text
        xml = xml.split("<loc>")
        print("Page ", numberPage)
        print("Total links: ", len(xml))
        for i in range(1, len(xml)):
            link = xml[i].split("</loc>")[0]
            if(sebenarnyaMYData.is_link_in_database(link) == False):
                sebenarnyaMYData.create_record(link)
                print("Added: ", link)
                DownloadSaveAsLocalPDF.DownloadSaveAsLocalPDF(link, pdf_store_path, 3)
            else:
                print("Already in database: ", link)
        numberPage += 1
    else:
        numberPage -= 1
        break
print("Total page: ", numberPage)


