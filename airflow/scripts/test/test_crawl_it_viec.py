import os
import sys
sys.path.insert(1, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from crawl_scripts.crawl_job.it_viec import ITViecScraper
import pandas as pd
data = ITViecScraper(headless=False).scrape_jobs('https://itviec.com/it-jobs/data-science')
print(data)

df = pd.DataFrame(data)
df.head(20)