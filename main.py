#!/usr/bin/env python3
"""
Optimized CNPJ Scraper with Dashboard
Enhanced version with session reuse, caching, and improved performance
"""

import os
import time
import asyncio
import signal
import threading
import random
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass
import re

from dotenv import load_dotenv
import colorama
from colorama import Fore, Style
import aiohttp
from fake_useragent import UserAgent

from playwright.async_api import async_playwright
from parsel import Selector

# Load environment variables
load_dotenv('config.env')

# Initialize colorama for Windows
colorama.init()

# Configure logging (file only, no console output)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs.txt')
    ]
)
logger = logging.getLogger(__name__)

USER_AGENT = [
    # Windows Chrome
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/115.0.0.0 Safari/537.36",

    # Windows Edge
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/115.0.0.0 Safari/537.36 Edg/115.0.1901.183",

    # Mac Safari
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "Version/16.0 Safari/605.1.15",

    # Linux Chrome
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/114.0.0.0 Safari/537.36",

    # iPhone Safari
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_5 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "Version/16.0 Mobile/15E148 Safari/604.1",

    # Android Chrome
    "Mozilla/5.0 (Linux; Android 13; Pixel 7 Pro) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/115.0.0.0 Mobile Safari/537.36",
]

STOPWORDS = {"da", "do", "dos", "das", "de", "me", "epp", "lt", "ltda", "sa", "s", "ass", "com"}

# CNPJ validation pattern
CNPJ_PATTERN = re.compile(r'^\d{14}$')

# Web scraping configuration
WEB_SCRAPING_TIMEOUT = 15000  # 15 seconds
WEB_SCRAPING_MAX_RETRIES = 3
WEB_SCRAPING_RATE_LIMIT = 1.0  # 1 second between requests

def validate_cnpj(cnpj: str) -> bool:
    """Validate CNPJ format (14 digits)"""
    if not cnpj or not isinstance(cnpj, str):
        return False
    return bool(CNPJ_PATTERN.match(cnpj.strip()))

def extract_phone_from_html(selector: Selector) -> str:
    """Extract phone number from HTML with improved validation"""
    text = selector.get()
    phones = re.findall(r'\(?\d{2}\)?\s?\d{4,5}-?\d{4}', text)
    if phones:
        phone = phones[0].strip()
        # Additional validation: ensure it's not a CNPJ number
        digits = re.sub(r'\D', '', phone)
        if len(digits) == 10 or len(digits) == 11:  # Valid phone length
            return phone
    return ""

def extract_email_from_html(selector: Selector) -> str:
    """Extract email from HTML with improved validation"""
    text = selector.get()
    emails = re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', text)
    if emails:
        email = emails[0].strip().lower()
        # Basic email validation
        if '@' in email and '.' in email and len(email) > 5:
            return email
    return ""

def build_nome_api(raw_name: str) -> str:
    if not raw_name:
        return ""
    
    # Replace non-alphanumeric with spaces
    clean_name = re.sub(r"[^A-Za-z0-9]+", " ", raw_name)
    
    # Split into words, remove stopwords, capitalize
    words = [
        w.capitalize() 
        for w in clean_name.split() 
        if w and w.lower() not in STOPWORDS
    ]
    
    return "-".join(words)

def format_br_phone(number: str) -> str:
    if not number:
        return ""

    results = []

    # split on commas
    parts = re.split(r",", number)

    for part in parts:
        digits = re.sub(r"\D", "", part)

        # Handle country code (Brazil = 55)
        if digits.startswith("55") and len(digits) > 11:
            digits = digits[2:]

        if len(digits) == 11:
            # Mobile
            results.append(f"({digits[:2]}) {digits[2:7]}-{digits[7:]}")
        elif len(digits) == 10:
            # Landline
            results.append(f"({digits[:2]}) {digits[2:6]}-{digits[6:]}")
        elif len(digits) == 9:
            # Just return as is, but better format like XXXXX-XXXX
            results.append(f"{digits[:5]}-{digits[5:]}")
        else:
            # keep raw digits if nothing matches
            if digits:
                results.append(digits)

    # Remove duplicates & join
    return ", ".join(sorted(set(results)))

@dataclass
class CNPJData:
    """Data structure for CNPJ information"""
    cnpj: str
    nome_empresa: str = ""
    nome_api_puxada: str = ""
    natureza: str = ""
    situacao: str = ""
    porte: str = ""
    mei: str = "?"
    telefone: str = ""
    email: str = ""
    source: str = ""

class SimpleCache:
    """Simple in-memory cache with TTL"""
    
    def __init__(self, ttl_seconds: int = 3600):
        self.cache = {}
        self.ttl = ttl_seconds
    
    def get(self, key: str) -> Optional[any]:
        """Get value from cache"""
        if key in self.cache:
            value, timestamp = self.cache[key]
            if time.time() - timestamp < self.ttl:
                return value
            else:
                del self.cache[key]
        return None
    
    def set(self, key: str, value: any):
        """Set value in cache"""
        self.cache[key] = (value, time.time())
    
    def clear(self):
        """Clear cache"""
        self.cache.clear()
    
    def size(self) -> int:
        """Get cache size"""
        return len(self.cache)

class Dashboard:
    """Real-time dashboard for scraping progress"""
    
    def __init__(self):
        self.total = 0
        self.done = 0
        self.pending = 0
        self.in_progress = 0
        self.errors = 0
        self.proxy_status = "Unknown"
        self.proxy_ip = "Unknown"
        self.last_check = "Never"
        self.start_time = time.time()
        self.terminate_requested = False
        
        # Setup signal handler for Ctrl+C
        signal.signal(signal.SIGINT, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle Ctrl+C termination"""
        print(f"\n{Fore.RED}Termination requested! Stopping all processes...{Style.RESET_ALL}")
        self.terminate_requested = True
    
    def update_proxy_status(self, status: str, ip: str = "Unknown"):
        """Update proxy status"""
        self.proxy_status = status
        self.proxy_ip = ip
        self.last_check = datetime.now().strftime("%H:%M:%S")
    
    def update_counts(self, done: int = None, pending: int = None, 
                     in_progress: int = None, errors: int = None):
        """Update dashboard counters"""
        if done is not None:
            self.done = done
        if pending is not None:
            self.pending = pending
        if in_progress is not None:
            self.in_progress = in_progress
        if errors is not None:
            self.errors = errors
    
    def display(self):
        """Display the dashboard"""
        os.system('cls' if os.name == 'nt' else 'clear')
        
        elapsed = time.time() - self.start_time
        rate = self.done / elapsed if elapsed > 0 else 0
        
        print(f"{Fore.CYAN}{'='*60}")
        print(f"OPTIMIZED CNPJ SCRAPER DASHBOARD")
        print(f"{'='*60}{Style.RESET_ALL}")
        print()
        
        # Proxy Status
        proxy_color = Fore.GREEN if self.proxy_status == "Connected" else Fore.RED
        print(f"{Fore.YELLOW}PROXY STATUS:{Style.RESET_ALL}")
        print(f"  Status: {proxy_color}{self.proxy_status}{Style.RESET_ALL}")
        print(f"  IP: {self.proxy_ip}")
        print(f"  Last Check: {self.last_check}")
        print()
        
        # API Status
        print(f"{Fore.YELLOW}API STATUS:{Style.RESET_ALL}")
        print(f"  CNPJá: {'✅ Enabled' if self.config.get('CNPJA_ENABLED', True) else '❌ Disabled'}")
        print(f"  BrasilAPI: {'✅ Enabled' if self.config.get('BRASIL_API_ENABLED', False) else '❌ Disabled'}")
        print(f"  ReceitaWS: {'✅ Enabled' if self.config.get('RECEITA_WS_ENABLED', False) else '❌ Disabled'}")
        print(f"  CNPJ.ws: {'✅ Enabled' if self.config.get('CNPJ_WS_ENABLED', False) else '❌ Disabled'}")
        print(f"  Minha Receita: {'✅ Enabled' if self.config.get('MINHA_RECEITA_ENABLED', False) else '❌ Disabled'}")
        print()
        
        # Additional Scraping Status
        print(f"{Fore.YELLOW}ADDITIONAL SCRAPING STATUS:{Style.RESET_ALL}")
        if self.config.get('ENABLE_ADDITIONAL_SCRAPING', True):
            print(f"  Status: ✅ Enabled")
        else:
            print(f"  Status: ❌ Disabled")
        print()
        
        # Counters
        print(f"{Fore.YELLOW}PROGRESS:{Style.RESET_ALL}")
        print(f"  Total: {Fore.CYAN}{self.total}{Style.RESET_ALL}")
        print(f"  Done: {Fore.GREEN}{self.done}{Style.RESET_ALL}")
        print(f"  Pending: {Fore.YELLOW}{self.pending}{Style.RESET_ALL}")
        print(f"  In Progress: {Fore.BLUE}{self.in_progress}{Style.RESET_ALL}")
        print(f"  Errors: {Fore.RED}{self.errors}{Style.RESET_ALL}")
        print()
        
        # Progress Bar
        if self.total > 0:
            progress = (self.done / self.total) * 100
            bar_length = 40
            filled_length = int(bar_length * progress / 100)
            bar = '█' * filled_length + '-' * (bar_length - filled_length)
            print(f"{Fore.YELLOW}OVERALL PROGRESS:{Style.RESET_ALL}")
            print(f"  [{bar}] {progress:.1f}% ({self.done}/{self.total})")
            print(f"  Rate: {rate:.2f} CNPJs/second")
            print()

        # Runtime
        hours, remainder = divmod(elapsed, 3600)
        minutes, seconds = divmod(remainder, 60)
        print(f"{Fore.YELLOW}RUNTIME:{Style.RESET_ALL} {int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}")
        print()
        
        if self.terminate_requested:
            print(f"{Fore.RED}TERMINATION IN PROGRESS...{Style.RESET_ALL}")

class OptimizedCNPJScraper:
    """Optimized CNPJ scraper class with session reuse and caching"""
    
    def __init__(self):
        self.config = self._load_config()
        self.dashboard = Dashboard()
        self.dashboard.config = self.config  # Pass config to dashboard
        self.session = None
        self.ua = UserAgent()
        self.cache = SimpleCache(ttl_seconds=3600)  # 1 hour cache
        
        # File paths
        self.input_file = self.config['INPUT_FILE']
        self.result_file = self.config['RESULT_FILE']
        self.done_file = self.config['DONE_FILE']
        self.error_file = self.config['ERROR_FILE']
        
        # Initialize files
        self._init_files()
        
        # Load CNPJs
        self.cnpjs = self._load_cnpjs()
        self.dashboard.total = len(self.cnpjs)
        self.dashboard.pending = len(self.cnpjs)
        
        # Load completed CNPJs
        self.completed_cnpjs = self._load_completed_cnpjs()
        
    def _load_config(self) -> Dict:
        """Load configuration from environment"""
        config = {
            'PROXY_URL': os.getenv('PROXY_URL'),
            'BATCH_SIZE': int(os.getenv('BATCH_SIZE', 50)),
            'MAX_CONCURRENCY': int(os.getenv('MAX_CONCURRENCY', 20)),
            'REQUESTS_PER_SECOND': int(os.getenv('REQUESTS_PER_SECOND', 10)),
            'RETRY_ATTEMPTS': int(os.getenv('RETRY_ATTEMPTS', 5)),
            'RETRY_DELAY_MIN': float(os.getenv('RETRY_DELAY_MIN', 0.5)),
            'RETRY_DELAY_MAX': float(os.getenv('RETRY_DELAY_MAX', 2.0)),
            'INPUT_FILE': os.getenv('INPUT_FILE', 'input.txt'),
            'RESULT_FILE': os.getenv('RESULT_FILE', 'result.txt'),
            'DONE_FILE': os.getenv('DONE_FILE', 'done.txt'),
            'ERROR_FILE': os.getenv('ERROR_FILE', 'errors.txt'),
            'CACHE_TTL': int(os.getenv('CACHE_TTL', 3600)),
            'CONNECTION_TIMEOUT': int(os.getenv('CONNECTION_TIMEOUT', 10)),
            'REQUEST_TIMEOUT': int(os.getenv('REQUEST_TIMEOUT', 15)),
            'ENABLE_ADDITIONAL_SCRAPING': os.getenv('ENABLE_ADDITIONAL_SCRAPING', 'true').lower() == 'true',
            'MIN_JITTER': float(os.getenv('MIN_JITTER', 0.5)),
            'MAX_JITTER': float(os.getenv('MAX_JITTER', 2.0)),
            # API Configuration
            'CNPJA_ENABLED': os.getenv('CNPJA_ENABLED', 'false').lower() == 'true',
            'BRASIL_API_ENABLED': os.getenv('BRASIL_API_ENABLED', 'false').lower() == 'true',
            'RECEITA_WS_ENABLED': os.getenv('RECEITA_WS_ENABLED', 'false').lower() == 'true',
            'CNPJ_WS_ENABLED': os.getenv('CNPJ_WS_ENABLED', 'true').lower() == 'true',
            'MINHA_RECEITA_ENABLED': os.getenv('MINHA_RECEITA_ENABLED', 'false').lower() == 'true',
        }
        
        # Validate configuration
        self._validate_config(config)
        return config
    
    def _validate_config(self, config: Dict):
        """Validate configuration values"""
        errors = []
        
        # Validate numeric values
        if config['BATCH_SIZE'] <= 0:
            errors.append("BATCH_SIZE must be greater than 0")
        if config['MAX_CONCURRENCY'] <= 0:
            errors.append("MAX_CONCURRENCY must be greater than 0")
        if config['REQUESTS_PER_SECOND'] <= 0:
            errors.append("REQUESTS_PER_SECOND must be greater than 0")
        if config['RETRY_ATTEMPTS'] < 0:
            errors.append("RETRY_ATTEMPTS must be non-negative")
        if config['CACHE_TTL'] <= 0:
            errors.append("CACHE_TTL must be greater than 0")
        if config['CONNECTION_TIMEOUT'] <= 0:
            errors.append("CONNECTION_TIMEOUT must be greater than 0")
        if config['REQUEST_TIMEOUT'] <= 0:
            errors.append("REQUEST_TIMEOUT must be greater than 0")
        
        # Check if at least one API is enabled
        apis_enabled = any([
            config['CNPJA_ENABLED'],
            config['BRASIL_API_ENABLED'],
            config['RECEITA_WS_ENABLED'],
            config['CNPJ_WS_ENABLED'],
            config['MINHA_RECEITA_ENABLED']
        ])
        
        if not apis_enabled and not config['ENABLE_ADDITIONAL_SCRAPING']:
            errors.append("At least one API must be enabled or web scraping must be enabled")
        
        if errors:
            error_msg = "Configuration errors:\n" + "\n".join(f"  - {error}" for error in errors)
            raise ValueError(error_msg)
    
    def _init_files(self):
        """Initialize result files"""
        # Create result file header if it doesn't exist
        if not os.path.exists(self.result_file):
            with open(self.result_file, 'w', encoding='utf-8') as f:
                f.write("OPTIMIZED CNPJ SCRAPING RESULTS\n")
                f.write("=" * 50 + "\n\n")
        
        # Create done file if it doesn't exist
        if not os.path.exists(self.done_file):
            open(self.done_file, 'w').close()
        
        # Create error file if it doesn't exist
        if not os.path.exists(self.error_file):
            open(self.error_file, 'w').close()
    
    def _load_cnpjs(self) -> List[str]:
        """Load CNPJs from input file with validation"""
        if not os.path.exists(self.input_file):
            print(f"{Fore.RED}Error: {self.input_file} not found!{Style.RESET_ALL}")
            return []
        
        with open(self.input_file, 'r') as f:
            raw_cnpjs = [line.strip() for line in f if line.strip()]
        
        # Validate CNPJs
        valid_cnpjs = []
        invalid_cnpjs = []
        
        for cnpj in raw_cnpjs:
            if validate_cnpj(cnpj):
                valid_cnpjs.append(cnpj)
            else:
                invalid_cnpjs.append(cnpj)
        
        if invalid_cnpjs:
            logger.warning(f"{len(invalid_cnpjs)} invalid CNPJs found and skipped")
            for invalid in invalid_cnpjs[:5]:  # Show first 5 invalid CNPJs
                logger.warning(f"  Invalid CNPJ: {invalid}")
            if len(invalid_cnpjs) > 5:
                logger.warning(f"  ... and {len(invalid_cnpjs) - 5} more")
        
        logger.info(f"Loaded {len(valid_cnpjs)} valid CNPJs from {self.input_file}")
        return valid_cnpjs
    
    def _load_completed_cnpjs(self) -> set:
        """Load already completed CNPJs"""
        if not os.path.exists(self.done_file):
            return set()
        
        with open(self.done_file, 'r') as f:
            completed = {line.strip() for line in f if line.strip()}
        
        logger.info(f"Found {len(completed)} already completed CNPJs")
        return completed
    
    async def create_session(self):
        """Create optimized aiohttp session with connection pooling"""
        connector = aiohttp.TCPConnector(
            limit=100,  # Total connection pool size
            limit_per_host=30,  # Connections per host
            ttl_dns_cache=300,  # DNS cache TTL
            use_dns_cache=True,
            keepalive_timeout=30,
            enable_cleanup_closed=True
        )
        
        timeout = aiohttp.ClientTimeout(
            total=self.config['REQUEST_TIMEOUT'],
            connect=self.config['CONNECTION_TIMEOUT']
        )
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={'User-Agent': self.ua.random}
        )
    
    async def close_session(self):
        """Close aiohttp session"""
        if self.session:
            await self.session.close()
    
    async def test_proxy(self) -> Tuple[bool, str]:
        """Test proxy connectivity"""
        if not self.config['PROXY_URL']:
            return True, "No proxy configured"
        
        try:
            async with self.session.get('http://ifconfig.me/ip', 
                                     proxy=self.config['PROXY_URL']) as response:
                if response.status == 200:
                    ip = await response.text()
                    return True, ip.strip()
                else:
                    return False, f"HTTP {response.status}"
        except Exception as e:
            return False, str(e)
    
    async def make_request(self, url: str, cache_key: str = None) -> Optional[Dict]:
        """Make HTTP request with caching and retry logic"""
        # Check cache first
        if cache_key:
            cached_data = self.cache.get(cache_key)
            if cached_data:
                return cached_data
        
        for attempt in range(self.config['RETRY_ATTEMPTS']):
            try:
                # Always use proxy if configured
                if self.config['PROXY_URL']:
                    response = await self.session.get(url, proxy=self.config['PROXY_URL'])
                else:
                    response = await self.session.get(url)
                
                if response.status == 200:
                    data = await response.json()
                    
                    # Cache the result
                    if cache_key:
                        self.cache.set(cache_key, data)
                    
                    return data
                else:
                    # Silent fail for non-200 responses
                    pass
                    
            except Exception as e:
                # Silent fail for request exceptions
                if attempt < self.config['RETRY_ATTEMPTS'] - 1:
                    delay = self.config['RETRY_DELAY_MIN'] * (2 ** attempt)
                    await asyncio.sleep(delay)
        
        return None

    async def get_from_cnpja(self, cnpj: str) -> Optional[CNPJData]:
        """Get data from CNPJá API"""
        try:
            url = f"https://open.cnpja.com/office/{cnpj}"
            cache_key = f"cnpja_{cnpj}"
            
            data = await self.make_request(url, cache_key=cache_key)
            
            if data and isinstance(data, dict):
                cnpj_data = CNPJData(cnpj=cnpj, source="CNPJá")

                cnpj_data.nome_empresa = data.get("company", {}).get("name", "")

                cidade = data.get("address", {}).get("city", "")
                estado = data.get("address", {}).get("state", "")
                cnpj_data.nome_api_puxada = f"{cnpj_data.nome_empresa}-{cidade}-{estado}"

                cnpj_data.natureza = data.get("company", {}).get("nature", {}).get("text", "")

                situacao_text = data.get("status", {}).get("text", "")
                situacao_data = data.get("status", {}).get("statusDate", "")
                cnpj_data.situacao = f"{situacao_text} desde {situacao_data}" if situacao_text else ""

                cnpj_data.porte = data.get("company", {}).get("size", {}).get("text", "")
                # Combine all available telephone numbers from CNPJá
                phones = data.get("phones", [])
                cnpj_data.telefone = ", ".join(
                    f"{p.get('area','')}{p.get('number','')}" for p in phones
                )

                emails = data.get("emails", [])
                cnpj_data.email = emails[0].get("address", "N/A") if emails else "N/A"

                cnpj_data.mei = "Sim" if data.get("company", {}).get("simei", {}).get("optant") else "Nao"
                
                return cnpj_data
            
        except Exception as e:
            # Silent fail for CNPJá
            pass
        
        return None
    
    async def get_from_brasil_api(self, cnpj: str) -> Optional[CNPJData]:
        """Get data from BrasilAPI"""
        try:
            url = f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}"
            cache_key = f"brasil_api_{cnpj}"
            
            data = await self.make_request(url, cache_key=cache_key)
            
            if data and isinstance(data, dict):
                cnpj_data = CNPJData(cnpj=cnpj, source="BrasilAPI")

                cnpj_data.nome_empresa = data.get('razao_social', '')

                cidade = data.get("municipio", "")
                estado = data.get("uf", "")
                cnpj_data.nome_api_puxada = f"{cnpj_data.nome_empresa}-{cidade}-{estado}"

                cnpj_data.natureza = data.get('natureza_juridica', '')

                cnpj_data.situacao = self._format_situacao(
                    data.get("descricao_situacao_cadastral", ""),
                    data.get("data_situacao_cadastral", ""),
                    data.get("data_inicio_atividade", "")
                )

                cnpj_data.porte = data.get("porte", "")
                # Combine all available telephone numbers from BrasilAPI
                cnpj_data.telefone = self._combine_telefones(
                    data.get("ddd_telefone_1", ""),
                    data.get("ddd_telefone_2", ""),
                )

                cnpj_data.email = data.get('email', '')

                cnpj_data.mei = "Sim" if data.get("opcao_pelo_mei") else "Nao"
                
                return cnpj_data
            
        except Exception as e:
            # Silent fail for BrasilAPI
            pass
        
        return None

    async def get_from_receita_ws(self, cnpj: str) -> Optional[CNPJData]:
        """Get data from ReceitaWS"""
        try:
            url = f"https://receitaws.com.br/v1/cnpj/{cnpj}"
            cache_key = f"receita_ws_{cnpj}"
            
            data = await self.make_request(url, cache_key=cache_key)
            
            if data and isinstance(data, dict):
                cnpj_data = CNPJData(cnpj=cnpj, source="ReceitaWS")

                cnpj_data.nome_empresa = data.get('nome', '')

                cidade = data.get("municipio", "")
                estado = data.get("uf", "")
                cnpj_data.nome_api_puxada = f"{cnpj_data.nome_empresa}-{cidade}-{estado}"

                cnpj_data.natureza = data.get("natureza_juridica", "")

                cnpj_data.situacao = self._format_situacao(
                    data.get("situacao", ""),
                    data.get("data_situacao", ""),
                    data.get("abertura", "")
                )

                cnpj_data.porte = data.get("porte", "")
                # Combine all available telephone numbers from ReceitaWS
                raw_tel = data.get("telefone", "")
                cnpj_data.telefone = self._combine_telefones(*raw_tel.split("/")) if raw_tel else ""

                cnpj_data.email = data.get('email', '')

                cnpj_data.mei = "Sim" if data.get("simei", {}).get("optante") else "Nao"
                
                return cnpj_data
            
        except Exception as e:
            # Silent fail for ReceitaWS
            pass
        
        return None

    async def get_from_cnpj_ws(self, cnpj: str) -> Optional[CNPJData]:
        """Get data from CNPJ.ws API"""
        try:
            url = f"https://cnpj.ws/cnpj/{cnpj}"
            cache_key = f"cnpj_ws_{cnpj}"
            
            data = await self.make_request(url, cache_key=cache_key)
            
            if data and isinstance(data, dict):
                cnpj_data = CNPJData(cnpj=cnpj, source="CNPJ.ws")
                
                cnpj_data.nome_empresa = data.get("razao_social", "")

                cidade = data.get("estabelecimento", {}).get("cidade", {}).get("nome", "")
                estado = data.get("estabelecimento", {}).get("estado", {}).get("sigla", "")
                cnpj_data.nome_api_puxada = f"{cnpj_data.nome_empresa}-{cidade}-{estado}"

                cnpj_data.natureza = data.get("natureza_juridica", {}).get("descricao", "")

                estabelecimento = data.get("estabelecimento", {})
                cnpj_data.situacao = self._format_situacao(
                    estabelecimento.get("situacao_cadastral", ""),
                    estabelecimento.get("data_situacao_cadastral", ""),
                    estabelecimento.get("data_inicio_atividade", "")
                )

                cnpj_data.porte = data.get("porte", {}).get("descricao", "")
                # Combine all available telephone numbers
                cnpj_data.telefone = self._combine_telefones(
                    f'{estabelecimento.get("ddd1", "")}{estabelecimento.get("telefone1", "")}',
                    f'{estabelecimento.get("ddd2", "")}{estabelecimento.get("telefone2", "")}'
                )

                cnpj_data.email = data.get('email', '')

                simples = data.get("simples", {})
                cnpj_data.mei = "Sim" if simples.get("mei") == "Sim" else "Nao"
                
                return cnpj_data
            
        except Exception as e:
            # Silent fail for CNPJ.ws
            pass
        
        return None

    async def get_from_minha_receita(self, cnpj: str) -> Optional[CNPJData]:
        """Get data from Minha Receita API (last fallback)"""
        try:
            url = f"https://minhareceita.org/api/cnpj/{cnpj}"
            cache_key = f"minha_receita_{cnpj}"
            
            data = await self.make_request(url, cache_key=cache_key)
            
            if data and isinstance(data, dict):
                cnpj_data = CNPJData(cnpj=cnpj, source="Minha Receita")

                cnpj_data.nome_empresa = data.get("razao_social", "")

                cidade = data.get("municipio", "")
                estado = data.get("uf", "")
                cnpj_data.nome_api_puxada = f"{cnpj_data.nome_empresa}-{cidade}-{estado}"

                cnpj_data.natureza = data.get('natureza_juridica', '')

                cnpj_data.situacao = self._format_situacao(
                    data.get("descricao_situacao_cadastral", ""),
                    data.get("data_situacao_cadastral", ""),
                    data.get("data_inicio_atividade", "")
                )

                cnpj_data.porte = data.get("porte", "")
                # Combine all available telephone numbers from Minha Receita
                cnpj_data.telefone = self._combine_telefones(
                    data.get("ddd_telefone_1", ""),
                    data.get("ddd_telefone_2", "")
                )

                cnpj_data.email = data.get('email', '')

                cnpj_data.mei = "Sim" if data.get("opcao_pelo_mei") else "Nao"
                
                return cnpj_data
            
        except Exception as e:
            # Silent fail for Minha Receita
            pass
        
        return None
    
    def _is_data_complete(self, data: CNPJData) -> bool:
        """Check if API data is complete (has company name and basic info)"""
        if not data:
            return False
        
        # Must have company name
        if not data.nome_empresa or data.nome_empresa.strip() == "":
            return False
        
        # Should have at least one of: nature, status, or porte
        has_basic_info = (data.natureza and data.natureza.strip() != "") or \
                        (data.situacao and data.situacao.strip() != "") or \
                        (data.porte and data.porte.strip() != "")
        
        return has_basic_info
    
    def _has_contact_data(self, data: CNPJData) -> bool:
        """Check if contact data (phone/email) is available"""
        if not data:
            return False
        
        has_phone = data.telefone and data.telefone.strip() != "" and data.telefone != "N/A"
        has_email = data.email and data.email.strip() != "" and data.email != "N/A"
        
        return has_phone or has_email
    
    def _format_situacao(self, situacao: str, data_situacao: str = None, data_abertura: str = None) -> str:
        """Format situacao field to show status with date"""
        if not situacao or situacao.strip() == "":
            return ""
        
        # Clean up situacao
        situacao = situacao.strip()
        
        # Add date if available
        date_to_use = data_situacao or data_abertura
        if date_to_use and date_to_use.strip() != "":
            # Format date if it's in a different format
            try:
                # If it's already in YYYY-MM-DD format, use as is
                if len(date_to_use) == 10 and date_to_use[4] == '-' and date_to_use[7] == '-':
                    return f"{situacao} desde {date_to_use}"
                # If it's in DD/MM/YYYY format, convert
                elif len(date_to_use) == 10 and date_to_use[2] == '/' and date_to_use[5] == '/':
                    day, month, year = date_to_use.split('/')
                    return f"{situacao} desde {year}-{month}-{day}"
                else:
                    return f"{situacao} desde {date_to_use}"
            except:
                return f"{situacao} desde {date_to_use}"
        
        return situacao
    
    def _combine_telefones(self, *telefones) -> str:
        """Combine multiple telephone numbers into a single comma-separated string"""
        # Filter out empty or None values and strip whitespace
        valid_telefones = []
        for telefone in telefones:
            if telefone and str(telefone).strip():
                clean_telefone = str(telefone).strip()
                # Avoid duplicates
                if clean_telefone not in valid_telefones:
                    valid_telefones.append(clean_telefone)

        # Return comma-separated string
        return ", ".join(valid_telefones) if valid_telefones else ""
    
    def _merge_cnpj_data(self, existing_data: CNPJData, new_data: CNPJData) -> CNPJData:
        """Merge new data into existing data, preserving already retrieved fields"""
        if not new_data:
            return existing_data
        
        # Only update fields that are empty in existing data
        if not existing_data.nome_empresa or existing_data.nome_empresa.strip() == "":
            existing_data.nome_empresa = new_data.nome_empresa
        
        if not existing_data.nome_api_puxada or existing_data.nome_api_puxada.strip() == "":
            existing_data.nome_api_puxada = new_data.nome_api_puxada
        
        if not existing_data.natureza or existing_data.natureza.strip() == "":
            existing_data.natureza = new_data.natureza
        
        if not existing_data.situacao or existing_data.situacao.strip() == "":
            existing_data.situacao = new_data.situacao
        
        if not existing_data.porte or existing_data.porte.strip() == "":
            existing_data.porte = new_data.porte
        
        if not existing_data.mei or existing_data.mei.strip() == "" or existing_data.mei == "?":
            existing_data.mei = new_data.mei
        
        if not existing_data.telefone or existing_data.telefone.strip() == "":
            existing_data.telefone = new_data.telefone
        
        if not existing_data.email or existing_data.email.strip() == "":
            existing_data.email = new_data.email
        
        # Update source to reflect the combination
        if existing_data.source and new_data.source:
            if existing_data.source != new_data.source:
                existing_data.source = f"{existing_data.source} + {new_data.source}"

        elif new_data.source:
            existing_data.source = new_data.source
        
        return existing_data

    async def scrape_cnpj(self, cnpj: str) -> CNPJData:
        """Main scraping function with optimized 3-step strategy"""
        
        # Initialize data structure
        data = CNPJData(cnpj=cnpj)
        
        # STEP 1: Sequential API Fallback (CNPJá → BrasilAPI → ReceitaWS → CNPJ.ws → Minha Receita)
        
        # Try CNPJá first
        if self.config['CNPJA_ENABLED']:
            api_data = await self.get_from_cnpja(cnpj)
            if api_data and self._is_data_complete(api_data):
                data = self._merge_cnpj_data(data, api_data)
                data.source = "CNPJá"
        
        # Fallback to BrasilAPI (only if we don't have complete data)
        if not data.nome_empresa or data.nome_empresa.strip() == "":
            if self.config['BRASIL_API_ENABLED']:
                api_data = await self.get_from_brasil_api(cnpj)
                if api_data and self._is_data_complete(api_data):
                    data = self._merge_cnpj_data(data, api_data)
                    data.source = "BrasilAPI"
        
        # Fallback to ReceitaWS (only if we don't have complete data)
        if not data.nome_empresa or data.nome_empresa.strip() == "":
            if self.config['RECEITA_WS_ENABLED']:
                api_data = await self.get_from_receita_ws(cnpj)
                if api_data and self._is_data_complete(api_data):
                    data = self._merge_cnpj_data(data, api_data)
                    data.source = "ReceitaWS"
                elif api_data:
                    # If we got partial data from ReceitaWS, use it
                    data = self._merge_cnpj_data(data, api_data)
                    data.source = "ReceitaWS (partial)"
        
        # Fallback to CNPJ.ws (only if we don't have complete data)
        if not data.nome_empresa or data.nome_empresa.strip() == "":
            if self.config['CNPJ_WS_ENABLED']:
                api_data = await self.get_from_cnpj_ws(cnpj)
                if api_data and self._is_data_complete(api_data):
                    data = self._merge_cnpj_data(data, api_data)
                    data.source = "CNPJ.ws"
        
        # Last fallback to Minha Receita (only if we don't have complete data)
        if not data.nome_empresa or data.nome_empresa.strip() == "":
            if self.config['MINHA_RECEITA_ENABLED']:
                api_data = await self.get_from_minha_receita(cnpj)
                if api_data and self._is_data_complete(api_data):
                    data = self._merge_cnpj_data(data, api_data)
                    data.source = "Minha Receita"
        
        # STEP 2: Granular Web Scraping Enrichment (only for missing data)
        
        # Check what contact data is missing after API calls
        missing_phone = not data.telefone or data.telefone.strip() == "" or data.telefone == "N/A"
        missing_email = not data.email or data.email.strip() == "" or data.email == "N/A"
        
        # Only run web scraping if we're missing data AND web scraping is enabled
        if (missing_phone or missing_email) and self.config['ENABLE_ADDITIONAL_SCRAPING']:
            logger.info(f"Missing contact data for {cnpj}: phone={missing_phone}, email={missing_email}")
            
            # Try web scraping for missing phone first
            if missing_phone:
                logger.info("Running web scraping to find missing phone number...")
                phone_data = await self.scrape_additional_info(cnpj, look_for_phone=True, look_for_email=False)
                
                if phone_data and phone_data.get('telefone'):
                    data.telefone = phone_data['telefone']
                    logger.info(f"Found phone via web scraping: {phone_data['telefone']}")
                    missing_phone = False  # Update status
                else:
                    logger.warning("Web scraping didn't find phone number")
            
            # Try web scraping for missing email (only if still missing)
            if missing_email:
                logger.info("Running web scraping to find missing email...")
                email_data = await self.scrape_additional_info(cnpj, look_for_phone=False, look_for_email=True)
                
                if email_data and email_data.get('email'):
                    data.email = email_data['email']
                    logger.info(f"Found email via web scraping: {email_data['email']}")
                else:
                    logger.warning("Web scraping didn't find email")
            
            # Update source to reflect web scraping was used
            data.source = f"{data.source} + Web Scraping" if data.source else "Web Scraping"
            
        else:
            if not missing_phone and not missing_email:
                logger.info("Contact data already complete from APIs, skipping web scraping")
            elif not self.config['ENABLE_ADDITIONAL_SCRAPING']:
                logger.info("Web scraping disabled in config, using API data only")
        
        # Final data validation
        if not data.telefone or data.telefone.strip() == "":
            data.telefone = "N/A"
        if not data.email or data.email.strip() == "":
            data.email = "N/A"
        
        return data
    
    async def scrape_additional_info(self, cnpj: str, look_for_phone: bool = True, look_for_email: bool = True) -> Optional[Dict]:
        """Scrape additional information from web sources when APIs don't provide complete data (CNPJ-only)"""
        if not self.config['ENABLE_ADDITIONAL_SCRAPING']:
            return None
        
        try:
            enriched_data = await self.scrape_cnpj_sites(cnpj, look_for_phone, look_for_email)
            return enriched_data
            
        except Exception as e:
            # Silent fail for additional info scraping
            pass
        
        return None
    
    async def scrape_cnpj_sites(self, cnpj: str, look_for_phone: bool = True, look_for_email: bool = True) -> Optional[Dict]:
        """Scrape CNPJ sites using Playwright with improved error handling and rate limiting"""
        import re

        # CNPJ sites to scrape (using the same sites as the simple test function for consistency)
        urls = [
            f"https://cnpj.biz/{cnpj}",
            f"https://www.consultacnpj.com/cnpj{cnpj}",
            f"https://empresacnpj.com/cnpj/{cnpj}",
        ]
        
        if not urls:
            return None

        out = {"tel": "", "email": ""}
        browser = None

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context(user_agent=random.choice(USER_AGENT))

                async def process_url(url):
                    nonlocal out
                    # Stop early if we found what we're looking for
                    if (not look_for_phone or out["tel"]) and (not look_for_email or out["email"]):
                        return
                    
                    page = None
                    try:
                        page = await context.new_page()
                        
                        # Block unnecessary resources for faster loading
                        await page.route("**/*", lambda route: (
                            route.abort() if route.request.resource_type in ["image", "font", "stylesheet", "media"]
                            else route.continue_()
                        ))
                        
                        # Navigate with timeout
                        await page.goto(url, wait_until="domcontentloaded", timeout=WEB_SCRAPING_TIMEOUT)
                        
                        # Get page content
                        html = await page.content()
                        sel = Selector(text=html)
                        
                        # Extract contact information based on what we're looking for
                        if look_for_phone and not out["tel"]:
                            tel = extract_phone_from_html(sel)
                            if tel:
                                out["tel"] = tel
                                logger.info(f"Found phone on {url}: {tel}")
                        
                        if look_for_email and not out["email"]:
                            email = extract_email_from_html(sel)
                            if email:
                                out["email"] = email
                                logger.info(f"Found email on {url}: {email}")
                            
                    except Exception as e:
                        # Log error but continue with other URLs
                        pass
                    finally:
                        if page:
                            await page.close()
                    
                    # Rate limiting between requests
                    await asyncio.sleep(WEB_SCRAPING_RATE_LIMIT)

                # Process URLs with limited concurrency and retry logic
                semaphore = asyncio.Semaphore(3)  # Max 3 concurrent requests
                
                async def process_with_semaphore(url):
                    async with semaphore:
                        for attempt in range(WEB_SCRAPING_MAX_RETRIES):
                            try:
                                await process_url(url)
                                # Stop processing if we found what we need
                                if (not look_for_phone or out["tel"]) and (not look_for_email or out["email"]):
                                    break
                            except Exception as e:
                                if attempt < WEB_SCRAPING_MAX_RETRIES - 1:
                                    await asyncio.sleep(1 * (attempt + 1))  # Exponential backoff
                                else:
                                    # Final attempt failed, continue
                                    pass
                
                # Process all URLs concurrently
                await asyncio.gather(*[process_with_semaphore(url) for url in urls])

        except Exception as e:
            # Log Playwright errors but don't crash
            pass
        finally:
            # Ensure browser is closed
            if browser:
                try:
                    await browser.close()
                except:
                    pass
        
        # Convert to the expected format
        result = {}
        if out["tel"]:
            result["telefone"] = out["tel"]
        if out["email"]:
            result["email"] = out["email"]
        
        return result if result else None
    
    def get_random_jitter(self) -> float:
        """Get random jitter delay between configured min and max values"""
        return random.uniform(self.config['MIN_JITTER'], self.config['MAX_JITTER'])
    
    def get_proxy_url(self) -> Optional[str]:
        """Get the configured proxy URL for rotating proxy service"""
        return self.config['PROXY_URL'] if self.config['PROXY_URL'] else None
    
    def save_result(self, data: CNPJData):
        """Save result to file"""
        nome_api = build_nome_api(data.nome_api_puxada)
        telefone = format_br_phone(data.telefone)
        
        result_text = f"""NOME LEGAL: {data.nome_empresa}
NOME API DE PUXADA: {nome_api}
CNPJ: {data.cnpj}
NATUREZA: {data.natureza}
SITUACAO: {data.situacao}
PORTE: {data.porte}
MEI: {data.mei}
TEL: {telefone}
EMAIL: {data.email}
{'-'*50}
"""
        
        with open(self.result_file, 'a', encoding='utf-8') as f:
            f.write(result_text)
    
    def mark_done(self, cnpj: str):
        """Mark CNPJ as completed"""
        with open(self.done_file, 'a') as f:
            f.write(f"{cnpj}\n")
    
    def mark_error(self, cnpj: str, error: str):
        """Mark CNPJ as error"""
        with open(self.error_file, 'a') as f:
            f.write(f"{cnpj}: {error}\n")
    
    async def process_batch(self, batch: List[str]):
        """Process a batch of CNPJs with optimized concurrency"""
        semaphore = asyncio.Semaphore(self.config['MAX_CONCURRENCY'])
        
        async def process_single(cnpj: str):
            async with semaphore:
                if self.dashboard.terminate_requested:
                    return
                
                try:
                    self.dashboard.in_progress += 1
                    self.dashboard.update_counts(in_progress=self.dashboard.in_progress)
                    
                    # Skip if already completed
                    if cnpj in self.completed_cnpjs:
                        self.dashboard.done += 1
                        self.dashboard.in_progress -= 1
                        self.dashboard.update_counts(
                            done=self.dashboard.done,
                            in_progress=self.dashboard.in_progress
                        )
                        return
                    
                    # Scrape data
                    data = await self.scrape_cnpj(cnpj)
                    
                    # Save result
                    self.save_result(data)
                    self.mark_done(cnpj)
                    self.completed_cnpjs.add(cnpj)
                    
                    # Update dashboard
                    self.dashboard.done += 1
                    self.dashboard.in_progress -= 1
                    self.dashboard.update_counts(
                        done=self.dashboard.done,
                        in_progress=self.dashboard.in_progress
                    )
                    
                    # Rate limiting
                    await asyncio.sleep(1 / self.config['REQUESTS_PER_SECOND'])
                    
                except Exception as e:
                    self.dashboard.errors += 1
                    self.dashboard.in_progress -= 1
                    self.mark_error(cnpj, str(e))
                    self.dashboard.update_counts(
                        errors=self.dashboard.errors,
                        in_progress=self.dashboard.in_progress
                    )
        
        # Process batch concurrently
        tasks = [process_single(cnpj) for cnpj in batch]
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def run(self):
        """Main execution function"""
        logger.info("Starting Optimized CNPJ Scraper...")
        print(f"{Fore.CYAN}Starting Optimized CNPJ Scraper...{Style.RESET_ALL}")
        
        # Create optimized session
        await self.create_session()
        
        try:
            # Test proxy
            logger.info("Testing proxy connectivity...")
            print("Testing proxy connectivity...")
            proxy_ok, proxy_ip = await self.test_proxy()
            self.dashboard.update_proxy_status(
                "Connected" if proxy_ok else "Failed",
                proxy_ip
            )
            
            # Filter out completed CNPJs
            pending_cnpjs = [cnpj for cnpj in self.cnpjs if cnpj not in self.completed_cnpjs]
            self.dashboard.pending = len(pending_cnpjs)
            
            logger.info(f"Ready to process {len(pending_cnpjs)} CNPJs")
            print(f"{Fore.GREEN}Ready to process {len(pending_cnpjs)} CNPJs{Style.RESET_ALL}")
            print(f"{Fore.YELLOW}Press Ctrl+C to terminate{Style.RESET_ALL}")
            
            # Start dashboard update thread
            def update_dashboard():
                while not self.dashboard.terminate_requested:
                    self.dashboard.display()
                    time.sleep(1)
            
            dashboard_thread = threading.Thread(target=update_dashboard, daemon=True)
            dashboard_thread.start()
            
            # Process in batches
            for i in range(0, len(pending_cnpjs), self.config['BATCH_SIZE']):
                if self.dashboard.terminate_requested:
                    break
                
                batch = pending_cnpjs[i:i + self.config['BATCH_SIZE']]
                logger.info(f"Processing batch {i//self.config['BATCH_SIZE'] + 1}/{(len(pending_cnpjs) + self.config['BATCH_SIZE'] - 1)//self.config['BATCH_SIZE']}")
                await self.process_batch(batch)
                
                # Small delay between batches
                await asyncio.sleep(1)
            
            # Final update
            self.dashboard.display()
            logger.info("Scraping completed successfully!")
            print(f"\n{Fore.GREEN}Scraping completed!{Style.RESET_ALL}")
            print(f"Results saved to: {self.result_file}")
            print(f"Completed CNPJs: {self.done_file}")
            print(f"Errors: {self.error_file}")
            print(f"Cache size: {self.cache.size()} entries")
            
        except Exception as e:
            logger.error(f"Error during scraping: {str(e)}")
            print(f"{Fore.RED}Error during scraping: {str(e)}{Style.RESET_ALL}")
        finally:
            # Always close session
            await self.close_session()
            await self.cleanup()
    
    async def cleanup(self):
        """Cleanup resources"""
        try:
            # Clear cache if it's too large
            if self.cache.size() > 1000:
                logger.info("Clearing cache due to size limit")
                self.cache.clear()
            
            # Log final statistics
            logger.info(f"Final statistics - Cache size: {self.cache.size()}, Completed: {self.dashboard.done}, Errors: {self.dashboard.errors}")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")

    async def test_web_scraping(self, cnpj: str = "25962788000100"):
        """Test web scraping functionality with a sample CNPJ using Playwright"""
        print(f"{Fore.CYAN}Testing Web Scraping for CNPJ: {cnpj}{Style.RESET_ALL}")
        print("=" * 60)
        
        try:
            # Test individual sites (using the same URLs as the main scraper)
            test_sites = [
                ("cnpj.biz", f"https://cnpj.biz/{cnpj}"),
                ("consultacnpj.com", f"https://www.consultacnpj.com/cnpj{cnpj}"),
                ("empresacnpj.com", f"https://empresacnpj.com/cnpj/{cnpj}")
            ]
            
            for site_name, site_url in test_sites:
                print(f"\n{Fore.YELLOW}Testing {site_name}:{Style.RESET_ALL}")
                
                try:
                    async with async_playwright() as p:
                        browser = await p.chromium.launch(headless=True)
                        context = await browser.new_context(user_agent=random.choice(USER_AGENT))
                        page = await context.new_page()
                        
                        try:
                            await page.route("**/*", lambda route: (
                                route.abort() if route.request.resource_type in ["image", "font", "stylesheet", "media"]
                                else route.continue_()
                            ))
                            await page.goto(site_url, wait_until="domcontentloaded", timeout=15000)
                            html = await page.content()
                            sel = Selector(text=html)
                            tel = extract_phone_from_html(sel)
                            email = extract_email_from_html(sel)
                            
                            print(f"Content length: {len(html)} characters")
                            
                            if tel or email:
                                result = {}
                                if tel:
                                    result["telefone"] = tel
                                if email:
                                    result["email"] = email
                                print(f"{Fore.GREEN}✅ Extracted data: {result}{Style.RESET_ALL}")
                            else:
                                print(f"{Fore.RED}❌ No data extracted{Style.RESET_ALL}")
                        
                        finally:
                            await page.close()
                            await browser.close()
                
                except Exception as e:
                    print(f"{Fore.RED}❌ Error: {str(e)}{Style.RESET_ALL}")
                
                # Small delay between tests
                await asyncio.sleep(2)
            
            # Test the full scraping process
            print(f"\n{Fore.CYAN}Testing full scraping process:{Style.RESET_ALL}")
            result = await self.scrape_additional_info(cnpj)
            
            if result:
                print(f"{Fore.GREEN}✅ Full scraping result: {result}{Style.RESET_ALL}")
            else:
                print(f"{Fore.RED}❌ Full scraping failed{Style.RESET_ALL}")
        
        except Exception as e:
            print(f"{Fore.RED}❌ Test error: {str(e)}{Style.RESET_ALL}")

async def main():
    """Main entry point"""
    import sys
    
    # Check if test mode is requested
    if len(sys.argv) > 1 and sys.argv[1] == "--test-scraping":
        scraper = OptimizedCNPJScraper()
        await scraper.test_web_scraping()
        return
    
    # Check if simple test mode is requested
    if len(sys.argv) > 1 and sys.argv[1] == "--simple-test":
        cnpj_test = "07134405000161"
        result = await scrape_public_pages_for_contacts(cnpj_test)
        print(result)
        return
    
    # Check if check-installation mode is requested
    if len(sys.argv) > 1 and sys.argv[1] == "--check-installation":
        await check_playwright_installation()
        return
    
    # Check if validate-config mode is requested
    if len(sys.argv) > 1 and sys.argv[1] == "--validate-config":
        await validate_configuration()
        return
    
    scraper = OptimizedCNPJScraper()
    await scraper.run()

async def validate_configuration():
    """Validate configuration and show status"""
    print(f"{Fore.CYAN}Validating Configuration...{Style.RESET_ALL}")
    
    try:
        # Try to create scraper instance (this will validate config)
        scraper = OptimizedCNPJScraper()
        
        print(f"{Fore.GREEN}✅ Configuration is valid!{Style.RESET_ALL}")
        print()
        
        # Show configuration summary
        print(f"{Fore.YELLOW}Configuration Summary:{Style.RESET_ALL}")
        print(f"  Input file: {scraper.config['INPUT_FILE']}")
        print(f"  Batch size: {scraper.config['BATCH_SIZE']}")
        print(f"  Max concurrency: {scraper.config['MAX_CONCURRENCY']}")
        print(f"  Requests per second: {scraper.config['REQUESTS_PER_SECOND']}")
        print(f"  Cache TTL: {scraper.config['CACHE_TTL']} seconds")
        print(f"  Web scraping: {'✅ Enabled' if scraper.config['ENABLE_ADDITIONAL_SCRAPING'] else '❌ Disabled'}")
        print()
        
        # Show API status
        print(f"{Fore.YELLOW}API Status:{Style.RESET_ALL}")
        apis = [
            ('CNPJá', scraper.config['CNPJA_ENABLED']),
            ('BrasilAPI', scraper.config['BRASIL_API_ENABLED']),
            ('ReceitaWS', scraper.config['RECEITA_WS_ENABLED']),
            ('CNPJ.ws', scraper.config['CNPJ_WS_ENABLED']),
            ('Minha Receita', scraper.config['MINHA_RECEITA_ENABLED'])
        ]
        
        for name, enabled in apis:
            status = "✅ Enabled" if enabled else "❌ Disabled"
            print(f"  {name}: {status}")
        
        # Proxy Status
        if scraper.config['PROXY_URL']:
            print(f"  Proxy: ✅ Configured")
        else:
            print(f"  Proxy: ❌ Not configured")
        
        print()
        print(f"{Fore.GREEN}Configuration validation completed successfully!{Style.RESET_ALL}")
        
    except Exception as e:
        print(f"{Fore.RED}❌ Configuration validation failed:{Style.RESET_ALL}")
        print(f"  Error: {str(e)}")
        print()
        print(f"{Fore.YELLOW}Please check your config.env file and fix the issues above.{Style.RESET_ALL}")

async def check_playwright_installation():
    """Check if Playwright is properly installed"""
    print(f"{Fore.CYAN}Checking Playwright installation...{Style.RESET_ALL}")
    
    try:
        from playwright.async_api import async_playwright
        print(f"{Fore.GREEN}✅ Playwright package is installed{Style.RESET_ALL}")
        
        async with async_playwright() as p:
            try:
                browser = await p.chromium.launch(headless=True)
                await browser.close()
                print(f"{Fore.GREEN}✅ Chromium browser is available{Style.RESET_ALL}")
            except Exception as e:
                print(f"{Fore.RED}❌ Chromium browser not found. Run: playwright install chromium{Style.RESET_ALL}")
                print(f"Error: {str(e)}")
                return
        
        print(f"{Fore.GREEN}✅ Playwright installation is complete!{Style.RESET_ALL}")
        
    except ImportError:
        print(f"{Fore.RED}❌ Playwright not installed. Run: pip install playwright{Style.RESET_ALL}")
    except Exception as e:
        print(f"{Fore.RED}❌ Error checking Playwright: {str(e)}{Style.RESET_ALL}")

async def scrape_public_pages_for_contacts(cnpj: str) -> dict:
    """Simple function to scrape public pages for contacts using Playwright"""
    urls = [
        f"https://cnpj.biz/{cnpj}",
        f"https://www.consultacnpj.com/cnpj{cnpj}",
        f"https://empresacnpj.com/cnpj/{cnpj}",
    ]

    out = {"tel": "", "email": ""}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=random.choice(USER_AGENT))

        async def process_url(url):
            nonlocal out
            # Stop early if we found both phone and email
            if out["tel"] and out["email"]:
                return
            page = await context.new_page()
            try:
                await page.route("**/*", lambda route: (
                    route.abort() if route.request.resource_type in ["image", "font", "stylesheet", "media"]
                    else route.continue_()
                ))
                await page.goto(url, wait_until="domcontentloaded", timeout=15000)
                html = await page.content()
                sel = Selector(text=html)
                
                # Extract phone if not found yet
                if not out["tel"]:
                    tel = extract_phone_from_html(sel)
                    if tel:
                        out["tel"] = tel
                        print(f"Found phone on {url}: {tel}")
                
                # Extract email if not found yet
                if not out["email"]:
                    email = extract_email_from_html(sel)
                    if email:
                        out["email"] = email
                        print(f"Found email on {url}: {email}")
                        
            except Exception:
                pass
            finally:
                await page.close()

        # run with limited concurrency (max 3 like p-limit)
        await asyncio.gather(*[process_url(u) for u in urls])

        await browser.close()
    return out

if __name__ == "__main__":
    asyncio.run(main())