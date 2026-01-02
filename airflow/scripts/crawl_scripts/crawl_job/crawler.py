from .topcv import TopCVScraper
from .it_viec import ITViecScraper

class Crawler:
    def __init__(self, source):
        self.source = source

    def crawler(self, url):
        if self.source == "itviec":
            return ITViecScraper().scrape_jobs(url)
        elif self.source == "topcv":
            return TopCVScraper().scrape_jobs(url)
        else:
            raise ValueError(f"Unsupported source: {self.source}")