import requests
import os
import pandas as pd
from pathlib import Path
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options


class DataRetriever:
    def __init__(self, output_dir='./data'):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        self.driver = None
        self.wait = None

    # Download a file using HTTP request
    def download_file_http(self, url, filename, subfolder):
        
        try:
            save_dir = self.output_dir / subfolder
            save_dir.mkdir(exist_ok=True) # Create subfolder if it doesn't exist
            filepath = save_dir / filename
            
            print(f'Downloading: {filename}')
            print(f'From: {url}')
            
            response = self.session.get(url, stream=True, timeout=30)
            response.raise_for_status()
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            print(f'Downloaded: {filename}')
            return filepath
            
        except Exception as e:
            print(f'Failed to download {filename}: {str(e)}')
            return None

    # Set up Selenium driver
    def setup_selenium_driver(self, subfolder):
            # Set up Chrome options for Selenium script
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            
            # Specify anti-detection options
            chrome_options.add_argument("--disable-web-security")
            chrome_options.add_argument("--allow-running-insecure-content")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            
            # Set download directory
            download_path = str((self.output_dir / subfolder).absolute())
            Path(download_path).mkdir(exist_ok=True)
            print(f'Setting download directory to: {download_path}')
            
            prefs = {
                "download.default_directory": download_path,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True,
                "safebrowsing.disable_download_protection": True,
                "download.extensions_to_open": "",
                "download.open_pdf_in_system_reader": False,
                "plugins.always_open_pdf_externally": True
            }
            
            chrome_options.add_experimental_option("prefs", prefs)
            
            # Initialise WebDriver
            try:
                self.driver = webdriver.Chrome(options=chrome_options)
                self.wait = WebDriverWait(self.driver, 20)                
                print('Chrome WebDriver initialized')
                return True
            except Exception as e:
                print(f'Failed to initialize WebDriver: {str(e)}')
                return False

    # Close WebDriver
    def close_driver(self):
    
        if self.driver:
            self.driver.quit()
            self.driver = None
            self.wait = None

    # Specify timeout for download
    def wait_for_download(self, download_dir, timeout=120):
        print('Waiting for download to complete...')
        
        start_time = time.time()
        initial_files = set(os.listdir(download_dir))
        
        while time.time() - start_time < timeout:
            current_files = set(os.listdir(download_dir))
            new_files = current_files - initial_files
            
            if new_files:
                # Check if any files are still downloading (.crdownload extension)
                downloading = [f for f in new_files if f.endswith('.crdownload')]
                if not downloading:
                    print(f'Download complete: {list(new_files)}')
                    return list(new_files)
            
            # Show progress every 10 seconds
            elapsed = time.time() - start_time
            if int(elapsed) % 10 == 0 and elapsed > 0:
                print(f'Waiting... ({elapsed:.0f}s elapsed)')
            
            time.sleep(1)
        
        print(f'Download timeout after {timeout} seconds')
        return []

    # Retrieve NGER data
    def retrieve_cer_nger_data(self):
        '''
        How it works:
        Find the Download CSV button and click it
        '''
        print('\n=== Task 1: Retrieving CER NGER Data ===')
        
        if not self.setup_selenium_driver('cer_nger'):
            return []
        
        downloaded_files = []
        
        try:
            url = 'https://data.cer.gov.au/datasets/NGER/ID0243'
            print(f"Loading: {url}")
            self.driver.get(url)
            
            # Wait for page to load
            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
            time.sleep(5)  # Wait for dynamic content to load
            
            # Set window size
            self.driver.set_window_size(1920, 1080)
            time.sleep(1)
            
            target_filename = 'NGER.ID0243.csv'
            
            # Try to click "Download CSV" button
            print('Looking for Download CSV button...')
            
            # Use selector to find the Download CSV button
            # selector = "//button[contains(., 'Download CSV')]"
        
            # try:
            #     download_button = self.driver.find_element(By.XPATH, selector)
            #     print(f'Found Download button using: {selector}')
            # except:
            #     raise Exception('Could not find Download CSV button with the selector')

            selectors = [
                    "//span[contains(text(), 'Download CSV')]/parent::*/parent::button",
                    "//button[contains(., 'Download CSV')]",
                    "//button//span[contains(text(), 'Download CSV')]",
                    "//button[contains(@class, 'k-button') and contains(., 'Download CSV')]",
                    "[data-id='2b426e36-82a4-4b6c-b2f0-53fd6aeed5d1']",  # From HTML
                    "//span[contains(text(), 'Download')]/ancestor::button",
                    "//div[contains(text(), 'NGER.ID0243.csv')]/following-sibling::div//button"
                ]
                
            download_button = None
            for selector in selectors:
                try:
                    if selector.startswith('['):
                        download_button = self.driver.find_element(By.CSS_SELECTOR, selector)
                    else:
                        download_button = self.driver.find_element(By.XPATH, selector)
                    print(f"Found Download button using: {selector}")
                    break
                except:
                    continue
                
            # Scroll to the Download CSV button
            print('Found Download CSV button')
            self.driver.execute_script('arguments[0].scrollIntoView(true);', download_button)
            time.sleep(2)
            
            # Click the Download CSV button
            download_button.click()
            print('Clicked Download CSV button')
            
            # Wait for download to complete
            download_dir = self.output_dir / 'cer_nger'
            downloaded_file = self.wait_for_download(download_dir, timeout=60)
            
            # Check if the file is downloaded
            if downloaded_file:
                print(f'Downloaded: {target_filename}')
            else:
                print('Download failed')
        
        except Exception as e:
            print(f"Error retrieving NGER data: {str(e)}")
        
        finally:
            self.close_driver()
        
        print(f"\nSuccessfully downloaded {len(downloaded_file)} NGER file")
        return downloaded_file


    # Retrieve CER Renewable Energy Data
    def retrieve_cer_renewable_data(self):
        '''
        How it works:
        Download the files using HTTP requests
        '''
        print('\n=== Task 2: Retrieving CER Renewable Energy Data ===')
        
        # Define the exact 5 files we want with their direct URLs
        # target_files = [
        #     {
        #         'url': 'https://cer.gov.au/document/total-lgcs-rec-registry-0',
        #         'filename': 'Total_LGCs_in_the_REC_Registry.csv'
        #     },
        #     {
        #         'url': 'https://cer.gov.au/document/power-stations-and-projects-probable',
        #         'filename': 'Power_stations_and_projects_probable.csv'
        #     },
        #     {
        #         'url': 'https://cer.gov.au/document/power-stations-and-projects-committed',
        #         'filename': 'Power_stations_and_projects_committed.csv'
        #     },
        #     {
        #         'url': 'https://cer.gov.au/document/power-stations-and-projects-accredited',
        #         'filename': 'Power_stations_and_projects_accredited.csv'
        #     },
        #     {
        #         'url': 'https://cer.gov.au/document/total-lgcs-and-capacity-accredited-power-stations-2025',
        #         'filename': 'Total_LGCs_and_capacity_of_accredited_power_stations_in_2025.csv'
        #     }
        # ]
        
        # downloaded_files = []
        
        # for i, target in enumerate(target_files, 1):
        #     print(f"\nDownloading {i}/5: {target['filename']}")
            
        #     filepath = self.download_file_http(
        #         target['url'], 
        #         target['filename'], 
        #         'cer_renewable'
        #     )
            
        #     if filepath:
        #         downloaded_files.append(filepath)
            
        #     # Wait for 1 second to avoid overloading the server
        #     time.sleep(1)
        url = 'https://cer.gov.au/markets/reports-and-data/large-scale-renewable-energy-data'

        response = requests.get(url, stream = True)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all CSV download links from body
        downloaded_files = []
        file_links = []

        div_tags = soup.find_all('div', class_ = 'cer-accordion__body__item')
        for div in div_tags:
            file_tags = div.find_all('a', class_ = 'cer-button button--secondary')
            for file in file_tags:
                href = file.get('href', '')
                text = file.get_text(strip = True).lower()

                if 'csv' in text:
                    file_links.append(href)
        
        print(file_links)
        
        for i, href in enumerate(file_links, 1):
            full_url = f'https://cer.gov.au{href}'
            filename = full_url.split('/')[-1] + '.csv'

            print(f'Downloading {i}/5: {filename}')
            filepath = self.download_file_http(full_url, filename, 'cer_renewable')

            if filepath:
                downloaded_files.append(filepath)
            else:
                print(f'Failed to download {filename}')

            # Wait for 1 second to avoid overloading the server
            time.sleep(1)

        
        print(f"\nSuccessfully downloaded {len(downloaded_files)}/5 CER files")
        return downloaded_files

    # Retrieve ABS Economy and Industry Data
    def retrieve_abs_data(self):
        '''
        How it works:
        Use Selenium to find the download link and click it
        '''
        print('\n=== Task 3: Retrieving ABS Economy and Industry Data ===')
        
        if not self.setup_selenium_driver('abs_data'):
            return []
        
        try:
            url = 'https://www.abs.gov.au/methodologies/data-region-methodology/2011-24'
            print(f'Loading: {url}')
            self.driver.get(url)
            
            # Wait for page to load
            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
            time.sleep(3)
            
            # Set window size to desktop to avoid mobile elements
            self.driver.set_window_size(1920, 1080)
            time.sleep(1)
            
            # Scroll to data downloads section
            downloads_section = self.driver.find_element(By.ID, 'data-downloads')
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", downloads_section)
            time.sleep(2)
            print('Found and scrolled to data downloads section')
            
            # Target the specific file
            target_href = '/methodologies/data-region-methodology/2011-24/14100DO0003_2011-24.xlsx'
            
            # Find the download link and click it
            download_link = self.driver.find_element(By.CSS_SELECTOR, f'a[href="{target_href}"]')
            
            print(f'Found ABS file: {download_link.text.strip()}')
            print(f'Downloading in progress...')
            
            # Scroll to element and click
            self.driver.execute_script('arguments[0].scrollIntoView(true);', download_link)
            time.sleep(2)
            download_link.click()
            print('Clicked download link')    
            
            # Wait for download
            download_dir = self.output_dir / 'abs_data'
            downloaded_file = self.wait_for_download(download_dir, timeout=180)
            
            # Check if the file is downloaded
            if downloaded_file:
                print(f'Download ABS file')
            else:
                print('Download failed')
        
        except Exception as e:
            print(f'Error retrieving NGER data: {str(e)}')
        
        finally:
            self.close_driver()
        
        print(f'\nSuccessfully downloaded {len(downloaded_file)} ABS file')
        return downloaded_file
    
    # Function to run the whole script
    def run_script(self):
        print(f'Output directory: {self.output_dir.absolute()}')

        # Run all tasks
        task1 = self.retrieve_cer_nger_data()
        task2 = self.retrieve_cer_renewable_data()
        task3 = self.retrieve_abs_data()

        # Summary
        total_files = len(task1) + len(task2) + len(task3)
        print(f'Total files downloaded: {total_files}/7')
        print(f'  CER NGER (Selenium): {len(task1)}/1')
        print(f'  CER Renewable (HTTP): {len(task2)}/5')
        print(f'  ABS Economy (Selenium): {len(task3)}/1')
        
        # List all downloaded files
        print(f'\nFiles in: {self.output_dir.absolute()}')

        cer_nger_dir = self.output_dir / 'cer_nger'
        if cer_nger_dir.exists():
            print(f'\nCER NGER files:')
            for file in sorted(cer_nger_dir.glob('*.csv')):
                print(f'    {file.name}')
        
        cer_dir = self.output_dir / 'cer_renewable'
        if cer_dir.exists():
            print(f'\nCER Renewable files:')
            for file in sorted(cer_dir.glob('*.csv')):
                print(f'    {file.name}')
        
        abs_dir = self.output_dir / 'abs_data'
        if abs_dir.exists():
            print(f'\nABS Economy files:')
            for file in sorted(abs_dir.glob('*.xlsx')):
                print(f'    {file.name}')
        
        if total_files == 7:
            print('\nAll datasets downloaded successfully!')
        else:
            print(f'\nExpected 7 files, got {total_files}')

    
def main():
    retriever = DataRetriever()
    retriever.retrieve_cer_renewable_data()
    # retriever.run_script()

if __name__ == '__main__':
    main()
                  