import asyncio
import logging
import re
import sys
from typing import IO
import urllib.error
import urllib.parse
import requests

import aiofiles
import aiohttp
from aiohttp import ClientSession

import pathlib
import time
from datetime import datetime, timedelta


logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level=logging.INFO,
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger("areq")
#logging.getLogger("chardet.charsetprober").disabled = True

HREF_RE = re.compile(r'href="(.*?)"')
include_filters = ("en.wikipedia", "wiki")
exclude_filters = [".m.", "index", "/File:", "/Category:", "/Talk:", "/Wikipedia:", "/Template:", "/Help:", "/Special:", "/static/", "/w/", "stats.wikimedia", '/UTC', "/Template_talk:"]

ALL_FOUND_URLS = set()
ALL_FOUND_URLS_MAP = dict()
ALL_CHECKED_URLS = set()
TARGET_PATH = list()


async def fetch_html(url: str, session: ClientSession, **kwargs) -> str:
    """GET request wrapper to fetch page HTML.
    kwargs are passed to `session.request()`.
    """

    resp = await session.request(method="GET", url=url, **kwargs)
    resp.raise_for_status()
    #logger.info("Got response [%s] for URL: %s", resp.status, url)
    html = await resp.text()    
    html = html.split("bodyContent")
    html = html[1].split("footer")
    html = html[0]
    return html

async def parse(url: str, session: ClientSession, **kwargs) -> set:
    """Find HREFs in the HTML of `url`."""  
    
    exclude_filters.append(url)
    found = set()
    try:
        html = await fetch_html(url=url, session=session, **kwargs)        
    except (
        aiohttp.ClientError,
        aiohttp.http_exceptions.HttpProcessingError,
    ) as e:
        logger.error(
            "aiohttp exception for %s [%s]: %s",
            url,
            getattr(e, "status", None),
            getattr(e, "message", None),
        )
        return found
    except Exception as e:
        logger.exception(
            "Non-aiohttp exception occured:  %s", getattr(e, "__dict__", {})
        )
        return found
    else:
        for link in HREF_RE.findall(html):
            
            try:
                abslink = urllib.parse.urljoin(url, link)
            except (urllib.error.URLError, ValueError):
                logger.exception("Error parsing URL: %s", link)

            if all(inc in abslink for inc in include_filters):
                if all(exc not in abslink for exc in exclude_filters):                        
                    found.add(abslink)                  
        
        logger.info("Found %d links for %s", len(found), url)
        
        if found:            
            ALL_FOUND_URLS_MAP[url] = found
            ALL_FOUND_URLS.update(found)            
        return found

async def write_one(file: IO, url: str, **kwargs) -> None:
    """Write the found HREFs from `url` to `file`."""
    res = await parse(url=url, **kwargs)
    if not res:
        return None
    async with aiofiles.open(file, "a") as f:        
        for p in res:
            await f.write(f"{url}\t{p}\n")
        logger.info("Wrote results for source URL: %s", url)
    
async def bulk_crawl_and_write(file: IO, urls: set, **kwargs) -> None:
    """Crawl & write concurrently to `file` for multiple `urls`."""
    async with ClientSession() as session:
        tasks = []
        for url in urls:
            tasks.append(
                write_one(file=file, url=url, session=session, **kwargs)
            )
        await asyncio.gather(*tasks)


async def temp1(target, urls):
    BULK_SIZE = 500
    starting_url = urls[0]
    urls_queue = list(urls)
    urls = list()
    while True:
        print(len(urls_queue))     
        async with ClientSession() as session:
            tasks = []
            for url in urls_queue[:BULK_SIZE]:                
                tasks.append(parse(url=url, session=session))                
            del urls_queue[:BULK_SIZE]

            await asyncio.gather(*tasks)
            
            latest_found_urls = ALL_FOUND_URLS.symmetric_difference(ALL_CHECKED_URLS)
            print("lastest found: ", len(latest_found_urls))
            print("pages_analyzed: ", len(ALL_FOUND_URLS_MAP))
            if latest_found_urls:
                if target_url_found_check(target=target, latest_found_urls=latest_found_urls, starting_url=starting_url): return                        
                ALL_CHECKED_URLS.update(latest_found_urls)
                urls.extend(latest_found_urls)
        if len(urls_queue) < BULK_SIZE:
            urls_queue = list(urls)            
            urls = list()               
                

def target_url_found_check(target, latest_found_urls, starting_url):
    if target in latest_found_urls:
        current_target = target

        while current_target != starting_url:
            for url, url_link_set in ALL_FOUND_URLS_MAP.items():
                if current_target in url_link_set:
                    TARGET_PATH.append(url)
                    current_target = url
                    break
            
        print(f"\nFOUND {target}!\nurls found: {len(ALL_FOUND_URLS)}\nurls checked: {len(ALL_CHECKED_URLS)}\n")        
        return True
    else: 
        print(f"----------------------------------------------not found----------------------------------------------")        
        return False



if __name__ == "__main__":
    perf_start = time.perf_counter()
    #assert sys.version_info >= (3, 7), "Script requires Python 3.7+."
    #here = pathlib.Path(__file__).parent

    here = pathlib.PurePath()
    
    # with open(here.joinpath("urls.txt")) as infile:
    #     urls = set(map(str.strip, infile))   
    
    timestamp = datetime.now()
    timestamp = timestamp.strftime(r"%b %d %H-%M-%S")
    outpath = here.joinpath(f"Wiki path search - {timestamp}.txt")
    with open(outpath, "w") as outfile:
            outfile.write(f"\nNumber of links \t\t to find this page \t\t\t\t\t\t\t\t from this one \t\t\t\t\t\t\t\t full path\n\n")

    number_attempts = 1
    links_number_results = list()
    urls_total = 0
    pages_total = 0

    for _ in range(number_attempts):
        include_filters = ("en.wikipedia", "wiki")
        exclude_filters = [".m.", "index", "/File:", "/Category:", "/Talk:", "/Wikipedia:", "/Template:", "/Help:", "/Special:", "/static/", "/w/", "stats.wikimedia", '/UTC', "/Template_talk:"]
        ALL_FOUND_URLS = set()
        ALL_FOUND_URLS_MAP = dict()
        ALL_CHECKED_URLS = set()
        TARGET_PATH = list()

        starting_url = requests.get("https://en.wikipedia.org/wiki/Special:Random").url
        urls = (starting_url, )
        #urls = ("https://en.wikipedia.org/wiki/Roigheim", )        
        target_url = "https://en.wikipedia.org/wiki/Adolf_Hitler"
        target_url = requests.get("https://en.wikipedia.org/wiki/Special:Random").url

        asyncio.get_event_loop().run_until_complete(temp1(target=target_url, urls=urls)) 

        TARGET_PATH.reverse()
        TARGET_PATH.append(target_url)
        target_path_titles = list()
        for element in TARGET_PATH:
            target_path_titles.append(element.rsplit('/', 1)[1])
        names = '  ->  '.join(target_path_titles)        
    
        with open(outpath, "a") as outfile:
            outfile.write(f"{len(TARGET_PATH) - 1} \t {target_url} \t {starting_url} \t\t {names}\n")

        links_number_results.append(len(TARGET_PATH) - 1)
        urls_total += len(ALL_FOUND_URLS)
        pages_total += len(ALL_FOUND_URLS_MAP)
    
    avg_links = sum(links_number_results)/number_attempts
    
    time_elapsed = timedelta(seconds=(time.perf_counter() - perf_start))
    with open(outpath, "a") as outfile:
            outfile.write(f"\n\tTotal number of urls found: {urls_total}\n\tTotal number of pages analyzed: {pages_total}\
            \n\tNumber of attempts: {number_attempts}\n\tAverage number of links: {avg_links}\n\n\tTime Elapsed: {time_elapsed}")


        