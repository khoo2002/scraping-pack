from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import json
import time
import os

class DownloadSaveAsLocalPDF:
    def __init__(self, web_link, download_path, seconds=10):
        self.web_link = web_link
        self.download_path = download_path
        self.seconds = seconds
        self.download_save_as_local_pdf()
    
    def download_save_as_local_pdf(self):
        os.makedirs(self.download_path, exist_ok=True)
        chrome_options = Options()
        settings = {
            "recentDestinations": [{"id": "Save as PDF", "origin": "local"}],
            "selectedDestinationId": "Save as PDF",
            "version": 2,
            "isHeaderFooterEnabled": False,
            "customMargins": {},
            "marginsType": 2,
            "scaling": 175,
            "scalingType": 3,
            "scalingTypePdf": 3,
            "isCssBackgroundEnabled": True
        }
        prefs = {
            'printing.print_preview_sticky_settings.appState': json.dumps(settings),
            'savefile.default_directory': self.download_path,
            'download.directory_upgrade': True,
            'download.extensions_to_open': 'pdf',
            'safebrowsing.enabled': True
        }
        chrome_options.add_experimental_option('prefs', prefs)
        chrome_options.add_argument('--kiosk-printing')

        browser = webdriver.Chrome(options=chrome_options)
        browser.get(self.web_link)
        browser.execute_script("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(self.seconds)
        browser.execute_script('window.print();')
        browser.execute_script("""
                            document.querySelectorAll('print-preview-app')[0].shadowRoot.getElementById('sidebar').shadowRoot.querySelectorAll('print-preview-button-strip')[0].shadowRoot.querySelectorAll('div.controls')[0].querySelectorAll('cr-button.action-button')[0].click();
                            """)
        browser.close() 