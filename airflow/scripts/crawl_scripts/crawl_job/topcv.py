import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from typing import List, Dict, Optional
import logging
from .helpers.extracting_info import _safe_text, _safe_attr, _safe_find

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


class TopCVScraper:
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
        """Configure Chrome options."""
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36")
        return chrome_options
    
    def _init_driver(self) -> webdriver.Chrome:
        """Initialize Chrome WebDriver."""
        return webdriver.Chrome(
            service=Service(self._driver_path),
            options=self._get_chrome_options()
        )

    def _extract_job_info(self, job) -> tuple:
        """Extract basic job information from job listing."""
        title = _safe_text(_safe_find(job, 'h3')).strip().split('?ta_source')[0]
        company = _safe_text(_safe_find(job, 'a', class_='company')).strip()
        
        img_tag = job.find('img')
        logo = img_tag.get('src') or img_tag.get('data-src', '')
        logo = logo.replace('https://cdn-new.topcv.vn/unsafe/150x/', '')
        
        job_url = _safe_attr(_safe_find(job, 'a'), 'href').strip()
        location = _safe_text(_safe_find(job.find('label', class_='address'), 'span'))
        salary = _safe_text(_safe_find(job.find('label', class_='title-salary'), 'span')) or _safe_text(_safe_find(job.find('label', class_='salary'), 'span'))
        exp = _safe_text(_safe_find(job.find('label', class_='exp'), 'span')).strip()
        return title, company, logo, job_url, location, salary, exp
    

    def _parse_brand_job(self, soup) -> tuple:
        descriptions = requirements = edu = type_of_work = ''

        divs = (
            soup.find_all('div', class_='box-info')
            or soup.find_all('div', class_='premium-job-description')
        )

        for div in divs:
            h2_tag = (
                _safe_find(div, 'h2', class_="premium-job-description__box--title")
                or _safe_find(div, 'h2', class_="title")
            )
            if not h2_tag:
                continue

            title_text = _safe_text(h2_tag)
            content = (
                _safe_find(div, 'div', class_='premium-job-description__box--content')
                or _safe_find(div, 'div', class_='content-tab')
            )

            if "Mô tả công việc" in title_text:
                descriptions = _safe_text(content) if content else ''
            elif "Yêu cầu ứng viên" in title_text:
                requirements = _safe_text(content) if content else ''
            elif "Thông tin" in title_text:
                for item in div.find_all('div', class_='box-item'):
                    strong = item.find('strong')
                    span = item.find('span')
                    if strong and span:
                        label = _safe_text(strong)
                        value = _safe_text(span)
                        if label == "Học vấn":
                            edu = value
                        elif label == "Hình thức làm việc":
                            type_of_work = value

        if not edu or not type_of_work:
            for div in soup.find_all('div', class_='premium-job-general-information__content'):
                title_div = _safe_find(div, 'div', 'general-information-data__label')
                value_div = _safe_find(div, 'div', 'general-information-data__value')
                if title_div and value_div:
                    label = _safe_text(title_div)
                    value = _safe_text(value_div)
                    if label == "Hình thức làm việc":
                        type_of_work = value
                    elif label == "Học vấn":
                        edu = value
        return descriptions, requirements, edu, type_of_work

    
    def _parse_job_detail(self, soup) -> tuple:
        """Parse job details from standard job page."""
        descriptions = requirements = edu = type_of_work = ''
        
        for div in soup.find_all('div', class_='job-description__item'):
            h3_tag = _safe_find(div, "h3")
            content = _safe_find(div, 'div', 'job-description__item--content')
            if h3_tag and content:
                title = _safe_text(h3_tag)
                if title == "Mô tả công việc":
                    descriptions = _safe_text(content)
                elif title == "Yêu cầu ứng viên":
                    requirements = _safe_text(content)
        
        for div in soup.find_all('div', class_='box-general-group'):
            title_div = _safe_find(div, 'div', 'box-general-group-info-title')
            value_div = _safe_find(div, 'div', 'box-general-group-info-value')
            if title_div and value_div:
                label = _safe_text(title_div)
                value = _safe_text(value_div)
                if label == "Hình thức làm việc":
                    type_of_work = value
                elif label == "Học vấn":
                    edu = value
        
        return descriptions, requirements, edu, type_of_work
    
    def scrape_jobs(self, url: str) -> List[Dict[str, str]]:
        """Main method to scrape jobs from TopCV."""        
        # Initialize driver for listing page
        driver = self._init_driver()
        
        driver.get(url)
        time.sleep(3)
        
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")
        time.sleep(3)
        jobs = soup.find_all('div', class_='job-item-search-result')
        driver.quit()
        
        logger.info(f"Found {len(jobs)} jobs")
        
        job_data: List[Dict[str, Optional[str]]] = []
        
        for idx, job in enumerate(jobs, 1):
            try:
                logger.info(f"Processing job {idx}/{len(jobs)}")
                
                data = {
                    'title': None,
                    'company': None,
                    'logo': None,
                    'url': None,
                    'location': None,
                    'salary': None,
                    'descriptions': None,
                    'requirements': None,
                    'experience': None,
                    'education': None,
                    'type_of_work': None
                }

                title, company, logo, job_url, location, salary, exp = self._extract_job_info(job)
                data['title'] = title
                data['company'] = company
                data['logo'] = logo
                data['url'] = job_url
                data['location'] = location
                data['salary'] = salary
                data['experience'] = exp
                # Create new driver for each job detail to avoid bot detection
                if job_url:
                    try:
                        detail_driver = self._init_driver()
                        detail_driver.get(job_url)
                        time.sleep(3)
                        job_soup = BeautifulSoup(detail_driver.page_source, "html.parser")
                        
                        if job_url.startswith('https://www.topcv.vn/brand'):
                            descriptions, requirements, edu, type_of_work = self._parse_brand_job(job_soup)
                        elif job_url.startswith('https://www.topcv.vn/viec-lam'):
                            descriptions, requirements, edu, type_of_work = self._parse_job_detail(job_soup)
                        else:
                            descriptions = requirements = edu = type_of_work = ''
                        
                        data['descriptions'] = descriptions
                        data['requirements'] = requirements
                        data['education'] = edu
                        data['type_of_work'] = type_of_work
                    finally:
                        detail_driver.quit()

            except Exception as e:
                logger.error(f"Error processing job, skipping... {e}")
            
            if data['url']:
                job_data.append(data)
        
        logger.info(f"Scraping completed. Total jobs scraped: {len(job_data)}")
        return job_data