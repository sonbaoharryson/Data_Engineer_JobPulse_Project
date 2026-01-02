import os
import sys
sys.path.insert(1, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from crawl_scripts.crawl_job.topcv import TopCVScraper

data = TopCVScraper(headless=False).scrape_jobs("https://www.topcv.vn/tim-viec-lam-data-analyst")
print(data)