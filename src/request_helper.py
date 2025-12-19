import os
import urllib.parse
from curl_cffi import requests

import re
import json as json_lib

class CleanResponse:
    """Wrapper to handle HTML-wrapped JSON responses from ScrapingAnt."""
    def __init__(self, original_response):
        self._response = original_response

    def __getattr__(self, name):
        return getattr(self._response, name)
        
    def json(self, **kwargs):
        try:
            return self._response.json(**kwargs)
        except Exception:
            # Fallback: Try to extract JSON from <pre> tag
            content = self._response.text
            match = re.search(r'<pre>(.*?)</pre>', content, re.DOTALL)
            if match:
                clean_text = match.group(1).strip()
                # Handle HTML entities if needed? Usually not for simple JSON.
                # But let's unescape just in case using html module?
                # For now, just try load.
                return json_lib.loads(clean_text)
            raise

def make_request(target_url, params=None, headers=None, use_api=True, method="GET", timeout=60):
    """
    Centralized request handler.
    - If use_api=True and SCRAPER_API_KEY exists: Routes via ScrapingAnt.
    - Else: Routes via Direct/Proxy using curl_cffi with Chrome impersonation.
    """
    
    # Generic Environment Variables
    api_key = os.environ.get("SCRAPER_API_KEY") 
    base_url = os.environ.get("SCRAPER_BASE_URL")
    proxy_url = os.environ.get("PROXY_URL")

    # 1. API Path (ScraperAPI)
    if use_api and api_key:
        # Construct full target URL with params manually for the 'url' parameter
        if params:
            query_string = urllib.parse.urlencode(params)
            final_target_url = f"{target_url}?{query_string}"
        else:
            final_target_url = target_url

        # ScraperAPI Parameters
        # User provided: {'api_key': '...', 'url': '...'}
        payload = {
            'api_key': api_key,
            'url': final_target_url
        }
        
        # ScraperAPI supports keep_headers if we really need them, 
        # but often standard is fine. 
        # If headers passed, we can add 'keep_headers': 'true' and pass headers to request.
        if headers:
            payload['keep_headers'] = 'true'
        
        try:
            # ScraperAPI usually returns raw content, so we don't strictly need CleanResponse 
            # unless they change behavior. But standard requests is fine.
            
            response = requests.request(
                method=method,
                url=base_url,
                params=payload,
                headers=headers if headers else None,
                timeout=timeout
            )
            
            if not response.ok:
                print(f"[ERROR] ScraperAPI Error {response.status_code}: {response.text}")
                
            # ScraperAPI returns content directly.
            # We can use CleanResponse if we suspect wrapping, but usually it's raw.
            # Let's return raw response for now as per user snippet.
            return response
            
        except Exception as e:
            raise RuntimeError(f"ScraperAPI Request Failed: {e}")

    # 2. Direct / Local Proxy Path
    else:
        # Format proxy for curl_cffi
        proxies = None
        if proxy_url:
            if not proxy_url.startswith(("http://", "https://")):
                proxy_url = f"http://{proxy_url}"
            proxies = {"http": proxy_url, "https": proxy_url}

        try:
            response = requests.request(
                method=method,
                url=target_url,
                params=params,
                headers=headers,
                proxies=proxies,
                impersonate="chrome110", # Bypass WAF
                timeout=timeout/2
            )
            return CleanResponse(response)
            
        except Exception as e:
            raise RuntimeError(f"Direct Request Failed (WAF/Network): {e}")
