import requests
import hashlib
import imghdr
import base64
from io import BytesIO
from PIL import Image
from rembg import remove


class ImageDownloader:
    def __init__(self, timeout: int = 20):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0",
                "Accept": "image/*",
            }
        )
        self.timeout = timeout

    def process_urls(self, urls: list[str]) -> dict:
        results = {}

        for url in urls:
            try:
                results[url] = self._process_single(url)
            except Exception as e:
                results[url] = None
                print(f"[FAIL] {url} -> {e}")

        return results

    def _process_single(self, url: str) -> str:
        """
        Final output:
        data:image/png;base64,...
        """
        content = self._download(url)

        # Remove background only if needed
        content = self._remove_background_if_needed(content)

        # Convert to base64 (PNG)
        encoded = base64.b64encode(content).decode("utf-8")
        return encoded

    def _download(self, url: str) -> bytes:
        r = self.session.get(url, timeout=self.timeout)
        r.raise_for_status()
        return r.content

    def _remove_background_if_needed(self, content: bytes) -> bytes:
        img = Image.open(BytesIO(content))

        # If already transparent → keep as is
        if img.mode in ("RGBA", "LA"):
            buffer = BytesIO()
            img.save(buffer, format="PNG", optimize=True)
            return buffer.getvalue()

        # Otherwise remove background
        output = remove(content)

        out_img = Image.open(BytesIO(output)).convert("RGBA")
        buffer = BytesIO()
        out_img.save(buffer, format="PNG", optimize=True)

        return buffer.getvalue()
