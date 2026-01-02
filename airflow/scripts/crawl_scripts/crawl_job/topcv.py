import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
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
    
    def _extract_job_info(self, job) -> tuple:
        """Extract basic job information from job listing."""
        title = job.find('h3').text.strip().split('?ta_source')[0]
        company = job.find('a', class_='company').text.strip()
        
        img_tag = job.find('img')
        logo = img_tag.get('src') or img_tag.get('data-src', '')
        logo = logo.replace('https://cdn-new.topcv.vn/unsafe/150x/', '')
        job_url = job.find('a')['href'].strip()
        location = job.find('label', class_='address').text.strip()
        salary = job.find('label', class_='title-salary').text.strip()
        
        return title, company, logo, job_url, location, salary
    
    def _parse_brand_job(self, soup) -> tuple:
        """Parse job details from brand page."""
        descriptions = requirements = exp = edu = type_of_work = ''
        divs = soup.find_all('div', class_='box-info') or soup.find_all('div', class_='premium-job-description__box')
        
        for div in divs:
            h2_tag = div.find('h2', class_="title")
            if not h2_tag:
                continue
            
            title_text = h2_tag.text.strip()
            content = div.find('div', class_='content-tab')
            if "Mô tả công việc" in title_text:
                descriptions = content.text.strip() if content else ''
            elif "Yêu cầu ứng viên" in title_text:
                requirements = content.text.strip() if content else ''
            elif "Thông tin" in title_text:
                for item in div.find_all('div', class_='box-item'):
                    strong = item.find('strong')
                    span = item.find('span')
                    if strong and span:
                        label = strong.text.strip()
                        value = span.text.strip()
                        if label == "Kinh nghiệm":
                            exp = value
                        elif label == "Học vấn":
                            edu = value
                        elif label == "Hình thức làm việc":
                            type_of_work = value
        return descriptions, requirements, exp, edu, type_of_work
    
    def _parse_job_detail(self, soup) -> tuple:
        """Parse job details from standard job page."""
        descriptions = requirements = exp = edu = type_of_work = ''
        
        for div in soup.find_all('div', class_='job-description__item'):
            h3_tag = div.find("h3")
            content = div.find('div', class_='job-description__item--content')
            if h3_tag and content:
                title = h3_tag.get_text(strip=True)
                if title == "Mô tả công việc":
                    descriptions = content.text.strip()
                elif title == "Yêu cầu ứng viên":
                    requirements = content.text.strip()
        
        exp_divs = soup.find_all('div', class_='job-detail__info--section-content-value')
        if exp_divs:
            exp = exp_divs[-1].text.strip()
        
        for div in soup.find_all('div', class_='box-general-group'):
            title_div = div.find('div', class_='box-general-group-info-title')
            value_div = div.find('div', class_='box-general-group-info-value')
            if title_div and value_div:
                label = title_div.text.strip()
                value = value_div.text.strip()
                if label == "Hình thức làm việc":
                    type_of_work = value
                elif label == "Học vấn":
                    edu = value
        
        return descriptions, requirements, exp, edu, type_of_work
    
    def scrape_jobs(self, url: str) -> List[Dict[str, str]]:
        """Main method to scrape jobs from TopCV."""
        chrome_options = self._get_chrome_options()
        
        # Initialize driver for listing page
        driver = webdriver.Chrome(service=Service(self._driver_path), options=chrome_options)
        driver.get(url)
        time.sleep(5)
        
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")
        time.sleep(5)
        jobs = soup.find_all('div', class_='job-item-search-result')
        driver.quit()
        
        logger.info(f"Found {len(jobs)} jobs")
        
        job_data = []
        
        for idx, job in enumerate(jobs, 1):
            try:
                logger.info(f"Processing job {idx}/{len(jobs)}")
                
                title, company, logo, job_url, location, salary = self._extract_job_info(job)
                
                # Create new driver for each job detail to avoid bot detection
                driver = webdriver.Chrome(service=Service(self._driver_path), options=chrome_options)
                driver.get(job_url)
                job_soup = BeautifulSoup(driver.page_source, "html.parser")
                time.sleep(5)
                driver.quit()
                
                if job_url.startswith('https://www.topcv.vn/brand'):
                    descriptions, requirements, exp, edu, type_of_work = self._parse_brand_job(job_soup)
                elif job_url.startswith('https://www.topcv.vn/viec-lam'):
                    descriptions, requirements, exp, edu, type_of_work = self._parse_job_detail(job_soup)
                else:
                    descriptions = requirements = exp = edu = type_of_work = ''
                
                job_data.append({
                    'title': title,
                    'company': company,
                    'logo': logo,
                    'url': job_url,
                    'location': location,
                    'salary': salary,
                    'descriptions': descriptions,
                    'requirements': requirements,
                    'experience': exp,
                    'education': edu,
                    'type_of_work': type_of_work
                })
            except Exception as e:
                logger.error(f"Error processing job, skipping... {e}")
                continue
        
        logger.info(f"Scraping completed. Total jobs scraped: {len(job_data)}")
        return job_data