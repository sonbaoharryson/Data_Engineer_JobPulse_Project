import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from typing import List, Dict
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Output to console
        # Uncomment the line below to also save logs to a file
        # logging.FileHandler('scraper.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


class ITViecScraper:
    def __init__(self, headless: bool = True):
        """
        Initialize the scraper with configurable options.
        
        Args:
            headless: Run Chrome in headless mode
        """
        self.headless = headless
        # Cache driver path on initialization
        logger.info("Initializing ChromeDriver...")
        self._driver_path = ChromeDriverManager().install()
    
    def _get_chrome_options(self) -> Options:
        """Configure Chrome options for optimal performance."""
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        return chrome_options
    
    def _init_driver(self) -> webdriver.Chrome:
        """Initialize a new Chrome driver instance."""
        options = self._get_chrome_options()
        return webdriver.Chrome(
            service=Service(self._driver_path), 
            options=options
        )
    
    def scrape_jobs(self, url: str) -> List[Dict[str, str]]:
        """Main method to scrape jobs from ITViec."""
        chrome_options = self._get_chrome_options()
        
        # Initialize driver for listing page
        driver = webdriver.Chrome(service=Service(self._driver_path), options=chrome_options)
        driver.get(url)
        time.sleep(5)
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        jobs = soup.find_all('div', class_='ipy-2')
        driver.close()
        
        logger.info(f"Found {len(jobs)} jobs")
        
        job_data = []
        
        for idx, job in enumerate(jobs, 1):
            try:
                logger.info(f"Processing job {idx}/{len(jobs)}")
                
                job_url = job.find('h3', class_='imt-3 text-break')['data-url'].split('?lab_feature=')[0]
                title = job.find('h3').text.strip()
                
                company = job.find('div', class_='imy-3 d-flex align-items-center').span.text.strip()
                logo = job.find('div', class_='imy-3 d-flex align-items-center').a.img['data-src']
                mode = job.find('div', class_='text-rich-grey flex-shrink-0').text.strip()
                location = job.find('div', class_='text-rich-grey text-truncate text-nowrap stretched-link position-relative')['title']
                
                tags = ', '.join([f'{a.text.strip()}' for a in job.find('div', class_='imt-4 imb-3 d-flex igap-1').find_all('a')])
                
                # Create new driver for each job detail to avoid bot detection
                driver = webdriver.Chrome(service=Service(self._driver_path), options=chrome_options)
                driver.get(job_url)
                time.sleep(5)
                soup = BeautifulSoup(driver.page_source, "html.parser")
                driver.close()
                
                try:
                    job_description = soup.find_all('div', class_='imy-5 paragraph')[0]
                    job_requirement = soup.find_all('div', class_='imy-5 paragraph')[1]
                except Exception:
                    logger.warning(f"Could not get job description or requirement {job_url}")
                
                try:
                    descriptions = ' '.join([
                        f"{p.get_text(strip=True)}"
                        for p in job_description.find_all("li")
                    ]) or ' '.join([
                        f"{p.get_text(strip=True)}"
                        for p in job_description.find_all("p")
                    ])
                except Exception:
                    logger.warning(f"Could not get job description details {job_url}")

                try:
                    requirements = ' '.join([
                        f"{p.get_text(strip=True)}"
                        for p in job_requirement.find_all("li")
                    ]) or ' '.join([
                        f"{p.get_text(strip=True)}"
                        for p in job_requirement.find_all("p")
                    ])
                except Exception:
                    logger.warning(f"Could not get job requirements details {job_url}")
          
                job_data.append({
                    'title': title,
                    'company': company,
                    'logo': logo,
                    'url': job_url,
                    'location': location,
                    'mode': mode,
                    'tags': tags,
                    'descriptions': descriptions,
                    'requirements': requirements
                })
            except Exception as e:
                logger.error(f"Error processing job, skipping... {e}")
                continue
        
        logger.info(f"Scraping completed. Total jobs scraped: {len(job_data)}")
        return job_data