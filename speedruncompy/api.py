import base64, json
from .exceptions import *
import logging
from ReturnThread import ReturnThread
from requests import Response, get, post, ReadTimeout
from time import sleep
from typing import Callable, Any
from env import PROXIES, USE_PROXY

API_URI = "https://www.speedrun.com/api/v2/"
API_V1_URI = "https://www.speedrun.com/api/v1/"
LANG = "en"
ACCEPT = "application/json"
TIMEOUT = 60
MAX_ATTEMPTS = 10

cookie = {}
proxyNum = -1
usableIPs = []
usableProxies = []

_log = logging.getLogger("speedruncompy")
_main_log = logging.getLogger("SpeedStats-V3")

def findUsableProxies():
    _main_log.info("Finding usable proxies:")
    
    proxyIPThreads = {}
    for proxy in PROXIES:
        t = ReturnThread(target=getIP, args = (proxy, ))
        proxyIPThreads[proxy] = t
        t.start()

    for proxy, t in proxyIPThreads.items():
        ip = t.join()
        if ip not in usableIPs:
            _main_log.info(f"IP of {proxy} is {ip}, which is not in use. Adding.")
            usableIPs.append(ip)
            usableProxies.append(proxy)
        else:
            _main_log.info(f"IP of {proxy} is {ip}, which already being used. Skipping.")
    
    _main_log.info(f"Found {len(usableProxies)} proxies.")
    return len(usableProxies)

def getProxyUri():
    if (not USE_PROXY):
       return ""
    
    if len(usableProxies) == 0:
        findUsableProxies()
    
    global proxyNum
    proxyNum = (proxyNum + 1) % len(usableProxies)
    return f'https://{usableProxies[proxyNum]}.herokuapp.com/'

def setSessId(phpsessionid):
    global cookie
    cookie.update({"PHPSESSID": phpsessionid})

def getIP(proxy: str):
    _header = {"Accept-Language": LANG, "Accept": ACCEPT}

    response = get(url=f"https://{proxy}.herokuapp.com/ip4only.me/api/", headers=_header, timeout=TIMEOUT)
    return response.content.decode('utf-8').split(',')[1]


def doGet(endpoint: str, params: dict = {}):
    _header = {"Accept-Language": LANG, "Accept": ACCEPT}
    # Params passed to the API by the site are json-base64 encoded, even though std params are supported.
    # We will do the same in case param support is retracted.
    paramsjson = bytes(json.dumps(params, separators=(",", ":")).strip(), "utf-8")
    _r = base64.urlsafe_b64encode(paramsjson).replace(b"=", b"")
    _log.debug(f"GET {API_URI}{endpoint} w/ params {paramsjson}")

    attempt = 0
    while attempt < MAX_ATTEMPTS:
        try:
            response = get(url=f"{getProxyUri()}{API_URI}{endpoint}", headers=_header, params={"_r": _r}, timeout=TIMEOUT)
            return response
        except Exception:
            print(f"Attempt {attempt + 1} of {MAX_ATTEMPTS} failed due to timeout. Retrying...")
            attempt += 1

def doGetV1(endpoint: str, params: dict = {}):
    _header = {"Accept-Language": LANG, "Accept": ACCEPT}
    paramsjson = bytes(json.dumps(params, separators=(",", ":")).strip(), "utf-8")
    _log.debug(f"GET {API_V1_URI}{endpoint} w/ params {paramsjson}")
    
    attempt = 0
    while attempt < MAX_ATTEMPTS:
        try:
            response = get(url=f"{getProxyUri()}{API_V1_URI}{endpoint}{buildParams(params)}", headers=_header, timeout=TIMEOUT)
            return response
        except Exception:
            print(f"Attempt {attempt + 1} of {MAX_ATTEMPTS} failed due to timeout. Retrying...")
            attempt += 1

def doPost(endpoint:str, params: dict = {}, _setCookie=True):
    global cookie
    _header = {"Accept-Language": LANG, "Accept": ACCEPT}
    _log.debug(f"POST {API_URI}{endpoint} w/ params {params}")

    attempt = 0
    while attempt < MAX_ATTEMPTS:
        try:
            response = post(url=f"{getProxyUri()}{API_URI}{endpoint}", headers=_header, cookies=cookie, json=params, timeout=TIMEOUT)
            return response
        except Exception:
            print(f"Attempt {attempt + 1} of {MAX_ATTEMPTS} failed due to timeout. Retrying...")
            attempt += 1

    if _setCookie and response.cookies:
        cookie = response.cookies
    return response

def buildParams(params):
    output = "?"
    for key, value in params.items():
        output += "{}={}&".format(key, value)
    return output

class BaseRequest():
    def __init__(self, method: Callable[[str, dict[str, Any]], Response], endpoint, **params):
        self.method = method
        self.endpoint = endpoint
        self.params = params
    
    def updateParams(self, **kwargs):
        """Updates parameters using values set in kwargs"""
        self.params.update(kwargs)

    def perform(self, retries=MAX_ATTEMPTS, delay=TIMEOUT) -> dict:
        try:
            self.response = self.method(self.endpoint, self.params)

            if (self.response.status_code >= 500 and self.response.status_code <= 599) or self.response.status_code == 408 or self.response.status_code == 429:
                if retries > 0:
                    _log.error(f"SRC returned error {self.response.status_code} {self.response.content}. Retrying with delay {delay}:")
                    for attempt in range(1, retries+1):
                        self.response = self.method(self.endpoint, self.params)
                        if not ((self.response.status_code >= 500 and self.response.status_code <= 599) or self.response.status_code == 408 or self.response.status_code == 429): 
                            break
                        _log.error(f"Retry {attempt} returned error {self.response.status_code} {self.response.content}")
                        sleep(delay)
                    else:
                        if self.response.status_code == 408: raise RequestTimeout(self)
                        elif self.response.status_code == 429: raise RateLimitExceeded(self)
                        else: raise ServerException(self)

            if self.response.status_code == 400: raise BadRequest(self)
            if self.response.status_code == 401: raise Unauthorized(self)
            if self.response.status_code == 403: raise Forbidden(self)
            if self.response.status_code == 404: raise NotFound(self)
            if self.response.status_code == 405: raise MethodNotAllowed(self)
            if self.response.status_code == 408: raise RequestTimeout(self)
            if self.response.status_code == 429: raise RateLimitExceeded(self)

            if self.response.status_code < 200 or self.response.status_code > 299:
                _log.error(f"Unknown response error returned from SRC! {self.response.status_code} {self.response.content}")
                raise APIException(self)

            return json.loads(self.response.content)
        except ServerException as e:
            _log.error(f"ServerException caught: {e}")
            return None

class GetRequest(BaseRequest):
    def __init__(self, endpoint, **params) -> None:
        super().__init__(method=doGet, endpoint=endpoint, **params)

class GetRequestV1(BaseRequest):
    def __init__(self, endpoint, **params) -> None:
        super().__init__(method=doGetV1, endpoint=endpoint, **params)

class PostRequest(BaseRequest):
    def __init__(self, endpoint, **params) -> None:
        super().__init__(method=doPost, endpoint=endpoint, **params)
