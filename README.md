# CNPJ Scraper

A comprehensive CNPJ (Brazilian company registration) scraper that enriches data with live information from multiple APIs and web scraping.

## Features

- **Real-time Dashboard**: Live progress monitoring with proxy status
- **Multiple Data Sources**: CNPJ.ws + BrasilAPI + ReceitaWS + Web scraping
- **Playwright-based Scraping**: Modern headless browser automation for reliable web scraping
- **Proxy Support**: Configurable proxy for web scraping
- **Batch Processing**: Configurable batch sizes with rate limiting
- **Resumable**: Continues from where it left off if interrupted
- **Error Handling**: Comprehensive error handling and logging
- **Ctrl+C Termination**: Graceful shutdown on interruption

### 1. Install Python

Python 3.8+ (Latest version-3.13 prefered!)

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Install Playwright Browsers

After installing dependencies, install the required browsers:

```bash
playwright install chromium
```

### 4. Configure Environment

Edit `config.env` with your settings:

### 5. Prepare CNPJ List

Add CNPJs to `input.txt` (one per line):

```
25962788000229
00000000000191
11444777000161
```

### 6. Run Scraper

```bash
python main.py
```

Or click directly "start.bat" file

## Testing Web Scraping

To test and debug the web scraping functionality:

```bash
# Test web scraping with default CNPJ
python main.py --test-scraping

# Test simple scraping function
python main.py --simple-test

# Test new smart scraping logic
python test_logic.py

# Check Playwright installation
python main.py --check-installation

# Validate configuration
python main.py --validate-config

# Monitor performance
python monitor_performance.py
```

This will:
- Test each website individually (cnpj.biz, consultacnpj.com, empresacnpj.com)
- Show detailed debugging information
- Display content lengths and extracted data from each site
- Test the full scraping process using Playwright

Use this to:
- Verify Playwright installation
- Check if websites are accessible
- Debug extraction patterns
- Validate the scraping logic

**Note**: If you encounter Playwright-related errors, run `python main.py --check-installation` to verify your setup.

## Logging

The scraper now includes comprehensive logging:
- **Log file**: `logs.txt` - Contains detailed logs of all operations
- **Console output**: Real-time progress and status updates
- **Error tracking**: Detailed error logging for debugging
- **Performance metrics**: Logs processing times and success rates

## Recent Updates

- **Granular Web Scraping**: Web scraping now runs separately for phone and email, stopping when each is found
- **Smart Scraping Strategy**: Web scraping only runs when APIs don't provide phone/email data
- **Efficient API Fallback**: Skips unnecessary API calls when data is already complete
- **Enhanced Validation**: CNPJ format validation and configuration validation
- **Improved Logging**: Comprehensive logging system with file output
- **Rate Limiting**: Web scraping rate limiting and retry mechanisms
- **Resource Management**: Automatic cleanup and cache management
- **Playwright Integration**: Replaced BeautifulSoup with Playwright for more reliable web scraping
- **Simplified URL Set**: Now uses a fixed set of reliable CNPJ websites (cnpj.biz, consultacnpj.com, empresacnpj.com)
- **Improved Error Handling**: Better error handling for browser automation
- **Installation Scripts**: Added install.bat for easy setup on Windows

## Dashboard Features

The scraper includes a real-time dashboard showing:

- **Proxy Status**: Connection status, IP address, last check time
- **Progress Counters**: Total, done, pending, in-progress, errors
- **Progress Bar**: Visual progress indicator
- **Runtime**: Elapsed time and processing rate
- **Termination**: Ctrl+C to stop all processes immediately

## Data Sources Strategy

### Step 1: Sequential API Fallback (Basic CNPJ Data + Contact Information)

**Smart API Strategy:**
- **Efficient Fallback**: Only calls next API if previous APIs didn't provide complete data
- **Data Preservation**: Existing data is never overwritten by subsequent API calls
- **Contact Data Priority**: APIs are checked for phone/email data first before web scraping

**Sequential fallback order:**

1. **CNPJá** - Primary API
2. **BrasilAPI** - Secondary API
3. **ReceitaWS** - Tertiary API
4. **CNPJ.ws** - Fourth option
5. **Minha Receita** - Last resort API

**Fallback logic:** If API is down or returns incomplete data → try next API in sequence

**API Configuration:** Each API can be individually enabled/disabled via config.env:

- `CNPJA_ENABLED=true/false` - Enable/disable CNPJá API
- `BRASIL_API_ENABLED=true/false` - Enable/disable BrasilAPI
- `RECEITA_WS_ENABLED=true/false` - Enable/disable ReceitaWS API
- `CNPJ_WS_ENABLED=true/false` - Enable/disable CNPJ.ws API
- `MINHA_RECEITA_ENABLED=true/false` - Enable/disable Minha Receita API

**Proxy Configuration:** All APIs and web scraping will automatically use the proxy when `PROXY_URL` is configured in the environment file.

### Step 2: Smart Web Scraping Enrichment (only when needed)

**Phone/Email Enrichment Strategy:**

- **Granular Execution**: Web scraping runs separately for phone and email
- **Smart Stop Logic**: If phone is found, stops searching for phone; if email is found, stops searching for email
- **Efficient Processing**: Only searches for what's missing, avoids unnecessary processing
- **Sequential Approach**: Searches for phone first, then email (if still missing)

**Web Scraping Details:**

- **Sites**: cnpj.biz, consultacnpj.com, empresacnpj.com
- **Multi-site approach**: Tries all sites per CNPJ until finding phone/email data
- **Smart data combination**: Merges results from multiple sites, prioritizing valid phone numbers over CNPJ numbers
- **Playwright-based scraping**:
- **Headless browser**: Uses Playwright with headless Chromium for reliable HTML parsing
- **Resource blocking**: Blocks images, fonts, stylesheets, and media for faster loading
- **User-Agent rotation**: Uses consistent modern Chrome user agent
- **Regex patterns**: Brazilian phone number and email extraction patterns

## Configuration Options

### Proxy

- `PROXY_URL`: Proxy server URL (all APIs and web scraping will use this proxy when configured)

### Additional Scraping Settings

**URL Configuration:**

The scraper uses a fixed set of reliable CNPJ websites:
- **cnpj.biz**: Primary CNPJ lookup site
- **consultacnpj.com**: Secondary CNPJ lookup site  
- **empresacnpj.com**: Tertiary CNPJ lookup site

The scraper uses Playwright for additional web scraping:

### Performance

- `BATCH_SIZE`: Number of CNPJs per batch (default: 50)
- `MAX_CONCURRENCY`: Maximum concurrent requests (default: 20)
- `REQUESTS_PER_SECOND`: Rate limiting (default: 10)

### Retry Settings

- `RETRY_ATTEMPTS`: Number of retry attempts (default: 5)
- `RETRY_DELAY_MIN`: Minimum delay between retries (default: 0.5s)
- `RETRY_DELAY_MAX`: Maximum delay between retries (default: 2.0s)

## File Management

- **input.txt**: Add CNPJs to process
- **result.txt**: Final results (appended incrementally)
- **done.txt**: Completed CNPJs (for resuming)
- **errors.txt**: Error log

## Legal Considerations

- Respect rate limits of APIs
- Use data responsibly and in compliance with Brazilian laws
- Some APIs may have usage restrictions
- Web scraping should be done ethically

## Support

For issues and questions:

1. Check the error logs in `errors.txt`
2. Verify configuration in `config.env`
3. Ensure all dependencies are installed
