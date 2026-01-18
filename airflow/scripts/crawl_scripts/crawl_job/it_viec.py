import time
import logging
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
from selenium import webdriver
from .helpers.extracting_info import _safe_text, _safe_attr, _safe_find
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- LOGGING ---------------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ITViecScraper:
    def __init__(self, headless: bool = True):
        self.headless = headless
        logger.info("Initializing ChromeDriver...")
        self._driver_path = ChromeDriverManager().install()

    # ---------------- DRIVER SETUP ---------------- #
    def _get_chrome_options(self) -> Options:
        options = Options()
        if self.headless:
            options.add_argument("--headless=new")

        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
        )
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        return options

    def _init_driver(self) -> webdriver.Chrome:
        return webdriver.Chrome(
            service=Service(self._driver_path),
            options=self._get_chrome_options()
        )

    def _extract_text(self, section) -> Optional[str]:
        try:
            items = section.find_all(["p", "li"], recursive=True)
            texts = [i.get_text() for i in items if i.get_text(strip=True)]
            return " ".join(texts) if texts else None
        except Exception:
            return None

    def scrape_jobs(self, url: str) -> List[Dict[str, Optional[str]]]:
        driver = self._init_driver()
        driver.get(url)
        time.sleep(3)

        soup = BeautifulSoup(driver.page_source, "html.parser")
        jobs = soup.find_all("div", class_="ipy-2")
        driver.quit()

        logger.info(f"Found {len(jobs)} jobs")
        job_data: List[Dict[str, Optional[str]]] = []
        job_url = []

        for idx, job in enumerate(jobs, 1):
            logger.info(f"Processing job {idx}/{len(jobs)}")

            data = {
                "title": None,
                "company": None,
                "logo": None,
                "url": None,
                "location": None,
                "mode": None,
                "tags": None,
                "descriptions": None,
                "requirements": None
            }

            try:
                title_el = _safe_find(job, "h3")
                data["title"] = _safe_text(title_el)

                url_el = _safe_find(job, "h3", class_="imt-3 text-break")
                raw_url = _safe_attr(url_el, "data-url")
                data["url"] = raw_url.split("?lab_feature=")[0] if raw_url else None

                company_el = _safe_find(
                    job, "div", class_="imy-3 d-flex align-items-center"
                )
                data["company"] = _safe_text(
                    _safe_find(company_el, "span")
                )

                data["logo"] = _safe_attr(
                    _safe_find(company_el, "img"), "data-src"
                )

                data["mode"] = _safe_text(
                    _safe_find(job, "div", class_="text-rich-grey flex-shrink-0")
                )

                location_el = _safe_find(
                    job,
                    "div",
                    class_="text-rich-grey text-truncate text-nowrap stretched-link position-relative"
                )
                data["location"] = _safe_attr(location_el, "title")

                tag_container = _safe_find(
                    job, "div", class_="imt-4 imb-3 d-flex igap-1"
                )
                if tag_container:
                    tags = [
                        _safe_text(a)
                        for a in tag_container.find_all("a")
                        if _safe_text(a)
                    ]
                    data["tags"] = ", ".join(tags) if tags else None

                # -------- DETAIL PAGE (NEW DRIVER) -------- #
                if data["url"] and data["url"] not in job_url:
                    job_url.append(data["url"])
                    detail_driver = self._init_driver()
                    try:
                        detail_driver.get(data["url"])
                        time.sleep(3)
                        detail_soup = BeautifulSoup(
                            detail_driver.page_source, "html.parser"
                        )

                        sections = detail_soup.find_all(
                            "div", class_="imy-5 paragraph"
                        )

                        if len(sections) > 0:
                            data["descriptions"] = self._extract_text(sections[0])
                        if len(sections) > 1:
                            data["requirements"] = self._extract_text(sections[1])

                    finally:
                        detail_driver.quit()
                else:
                    continue
            except Exception as e:
                logger.error(f"Job skipped due to unexpected error: {e}")
            
            if data['url']:
                job_data.append(data)

        logger.info(f"Scraping completed. Total jobs scraped: {len(job_data)}")
        return job_data