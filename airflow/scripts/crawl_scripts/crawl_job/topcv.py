import time
import random
import logging
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from typing import List, Dict, Optional
from .helpers.extracting_info import _safe_text, _safe_attr, _safe_find

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
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
        logger.info("Initializing ChromeDriver...")
        return webdriver.Chrome(
            service=Service(self._driver_path),
            options=self._get_chrome_options()
        )

    def _extract_job_info(self, job) -> tuple:
        """Extract basic job information from job listing."""
        title = _safe_text(_safe_find(job, 'h3'))
        company = _safe_text(_safe_find(job, 'a', class_='company'))
        img_tag = job.find('img')
        logo = img_tag.get('src') or img_tag.get('data-src', '')
        logo = (logo or "")
        job_url = _safe_attr(_safe_find(job, 'a'), 'href').split('?ta_source')[0]
        location = _safe_text(_safe_find(job.find('label', class_='address'), 'span'))
        salary = _safe_text(_safe_find(job.find('label', class_='title-salary'), 'span')) or _safe_text(_safe_find(job.find('label', class_='salary'), 'span'))
        exp = _safe_text(_safe_find(job.find('label', class_='exp'), 'span'))
        return title, company, logo, job_url, location, salary, exp


    def _parse_brand_job(self, soup) -> tuple:
        """Parse job details from brand job page."""
        def extract_general_info(div):
            label = div.select_one('.general-information-data__label')
            value = div.select_one('.general-information-data__value')
            if label and value:
                return label, value
            
            label = div.find('strong')
            value = div.find('span')
            if label and value:
                return label, value
            
            return None, None
        
        def extract_description_requirement(div):
            h2 = div.select_one("h2.premium-job-description__box--title")
            content_div = div.select_one("div.premium-job-description__box--content")
            content = content_div if content_div else None
            if h2 and content:
                return h2, content
            
            h2 = div.select_one("h2.title")
            content_div = div.select_one("div.content-tab")
            content = content_div if content_div else None
            if h2 and content:
                return h2, content
            
            return None, None
        
        descriptions = requirements = edu = type_of_work = None

        # Parse descriptions and requirements
        for div in soup.select("div.premium-job-description__box, div.box-info"):
            title, content = extract_description_requirement(div)

            if not title:
                continue
            if "Mô tả công việc" == title.get_text(strip=True):
                descriptions = _safe_text(content)
            elif "Yêu cầu ứng viên" == title.get_text(strip=True):
                requirements = _safe_text(content)
                
            if descriptions and requirements:
                break

        # Parse general info (education, type of work)
        for div in soup.select("div.general-information-data, div.box-item"):
            label, value = extract_general_info(div)

            if not label:
                continue
            if label.get_text(strip=True) == "Hình thức làm việc":
                type_of_work = _safe_text(value)
            elif label.get_text(strip=True) == "Học vấn":
                edu = _safe_text(value)
            
            if type_of_work and edu:
                break

        return descriptions, requirements, edu, type_of_work

    def _parse_job_detail(self, soup) -> tuple:
        """Parse job details from standard job page."""

        descriptions = requirements = edu = type_of_work = None

        for div in soup.select("div.job-description__item"):
            h3 = _safe_find(div, "h3")
            content = _safe_find(div, "div", "job-description__item--content")

            if not h3:
                continue

            title = h3.get_text(strip=True)

            if title == "Mô tả công việc":
                descriptions = _safe_text(content)

            elif title == "Yêu cầu ứng viên":
                requirements = _safe_text(content)

            if descriptions and requirements:
                break

        for div in soup.find_all('div', class_='box-general-group-info'):
            title_div = _safe_find(div, 'div', 'box-general-group-info-title')
            value_div = _safe_find(div, 'div', 'box-general-group-info-value')
            if not title_div:
                continue
            
            label = title_div.get_text(strip=True)
            value = _safe_text(value_div)
            if label == "Hình thức làm việc":
                type_of_work = value
            elif label == "Học vấn":
                edu = value

            if type_of_work and edu:
                break

        return descriptions, requirements, edu, type_of_work

    
    def scrape_jobs(self, url: str) -> List[Dict[str, str]]:
        """Main method to scrape jobs from TopCV."""        
        # Initialize driver for listing page
        driver = self._init_driver()
        driver.get(url)

        WebDriverWait(driver, 30).until(
            lambda d: d.find_elements(By.CSS_SELECTOR, "div.job-item-search-result")
        )
        time.sleep(0.5 + random.uniform(0.5, 2.5))
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")
        jobs = soup.find_all('div', class_='job-item-search-result')
        driver.quit()

        logger.info(f"Found {len(jobs)} jobs")
        
        job_data: List[Dict[str, Optional[str]]] = []
        detail_driver = self._init_driver()

        for idx, job in enumerate(jobs, 1):
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
            try:
                logger.info(f"Processing job {idx}/{len(jobs)}")

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
                        detail_driver.get(job_url)
                        try:
                            WebDriverWait(detail_driver, 30).until(
                                lambda d: d.execute_script("return document.body.innerText.length") > 250
                            )
                        except TimeoutException:
                            logger.warning(f"Timeout waiting for job details to load for URL: {job_url}")
                            #detail_driver.quit()
                            time.sleep(0.5 + random.uniform(0.5, 1.5))
                            continue

                        job_soup = BeautifulSoup(detail_driver.page_source, "html.parser")

                        job_cat_div = job_soup.find("div", string=lambda x: x and "Chuyên môn:" in x)
                        data["job_cat"] = ", ".join([job_cat.text.strip() for job_cat in job_cat_div.find_next("div").find_all("a")]) if job_cat_div else None
                        
                        if 'topcv.vn/brand/' in job_url.strip():
                            descriptions, requirements, edu, type_of_work = self._parse_brand_job(job_soup)
                        elif 'topcv.vn/viec-lam/' in job_url.strip():
                            job_cat_div = job_soup.find("div", string="Chuyên môn:")
                            descriptions, requirements, edu, type_of_work = self._parse_job_detail(job_soup)
                        else:
                            descriptions = requirements = edu = type_of_work = None
                            
                        data['descriptions'] = descriptions
                        data['requirements'] = requirements
                        data['education'] = edu
                        data['type_of_work'] = type_of_work
                    finally:
                        detail_driver.delete_all_cookies()
                        detail_driver.execute_cdp_cmd("Network.clearBrowserCookies", {})
                        detail_driver.execute_cdp_cmd("Network.clearBrowserCache", {})
                        time.sleep(0.5 + random.uniform(0.5, 1.5))
            except Exception as e:
                logger.error(f"Error processing job, skipping... {e}")
            
            if data['url'] and data['requirements'] and data['descriptions'] and data['experience']:
                job_data.append(data)

        detail_driver.quit()
        
        logger.info(f"Scraping completed. Total jobs scraped: {len(job_data)}")
        return job_data