from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
import csv
import datetime
import os
import time
import re
import requests

def check_website_accessibility(url):
    """Check if the website is accessible"""
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            print("✅ Website is accessible")
            return True
        else:
            print(f"⚠️ Website returned status code: {response.status_code}")
            return False
    except requests.RequestException as e:
        print(f"❌ Failed to access website: {e}")
        return False

def setup_driver(headless=False):
    """Configure the Chrome driver with anti-detection"""
    chrome_options = Options()
    if headless:
        chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    try:
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(60)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        return driver
    except WebDriverException as e:
        print(f"❌ Failed to initialize ChromeDriver: {e}")
        return None

def find_http_ping_buttons(driver, wait):
    """🔍 Find ALL HTTP Ping buttons on the page"""
    print("🔍 Searching for HTTP Ping buttons...")
    selectors = [
        "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'http ping')]",
        "//button[contains(text(), 'HTTP Ping')]",
        "//button[contains(text(), 'HTTP') and contains(text(), 'Ping')]",
        "//*[contains(text(), 'HTTP Ping')]//button",
        "//button[contains(@class, 'ping') or contains(@class, 'Ping')]",
        "//button[@type='button' and contains(text(), 'Ping')]",
    ]
    all_buttons = []
    for selector in selectors:
        try:
            buttons = driver.find_elements(By.XPATH, selector)
            all_buttons.extend(buttons)
        except:
            continue
    unique_buttons = []
    seen_locations = set()
    for button in all_buttons:
        try:
            if button.is_enabled() and button.is_displayed():
                location = (button.location['x'], button.location['y'])
                if location not in seen_locations:
                    seen_locations.add(location)
                    unique_buttons.append(button)
        except:
            continue
    print(f"✅ Found {len(unique_buttons)} HTTP Ping buttons")
    return unique_buttons

def click_specific_http_ping_button(driver, wait, button_index, button_list):
    """🖱️ Click a specific HTTP Ping button by index"""
    if button_index >= len(button_list):
        print(f"❌ Button index {button_index} not available (only {len(button_list)} buttons)")
        return False
    button = button_list[button_index]
    button_type = f"HTTP Ping #{button_index + 1}"
    print(f"🔍 Clicking {button_type}...")
    try:
        driver.execute_script("arguments[0].scrollIntoView(true);", button)
        time.sleep(0.5)
        driver.save_screenshot(f'huawei_before_http_ping_{button_index + 1}.png')
        print(f"📸 Screenshot BEFORE {button_type} click saved")
        ActionChains(driver).move_to_element(button).click().perform()
        print(f"🖱️ AUTOMATIC CLICK on {button_type}!")
        time.sleep(1)
        driver.save_screenshot(f'huawei_after_http_ping_{button_index + 1}.png')
        print(f"📸 Screenshot AFTER {button_type} click saved")
        return True
    except Exception as e:
        print(f"❌ Error clicking {button_type}: {e}")
        return False

def wait_for_latency_data(driver, wait, max_wait=45, min_cells=8):
    """⏱️ Smart wait for latency data to appear"""
    print(f"⏳ Waiting for latency data (max {max_wait}s)...")
    start_time = time.time()
    consecutive_stable_count = 0
    previous_cell_count = 0
    while time.time() - start_time < max_wait:
        latency_cells = driver.find_elements(By.XPATH, """
            //td[contains(text(), '.') or contains(@data-value, '.') or text()='...' or text()='N/A']
            | //span[contains(text(), '.') or contains(@data-value, '.') or text()='...' or text()='N/A']
            | //*[contains(@class, 'latency') or contains(@class, 'ping')]
        """)
        current_cell_count = len(latency_cells)
        if current_cell_count >= min_cells:
            if current_cell_count == previous_cell_count:
                consecutive_stable_count += 1
                if consecutive_stable_count >= 1:
                    print(f"✅ Data loaded! Found {current_cell_count} stable latency cells")
                    for i, cell in enumerate(latency_cells[:10], 1):
                        text = cell.text.strip()
                        print(f"DEBUG: Cell {i} content: '{text}'")
                    return True
            else:
                consecutive_stable_count = 0
        previous_cell_count = current_cell_count
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(0.5)
        print(f"⏳ Waiting... ({(time.time() - start_time):.1f}s) - Cells: {current_cell_count}")
    print("⚠️ Timeout waiting for data")
    return False

def extract_and_save_data(driver, ping_type, timestamp, output_file):
    """Extract and save latency data for a given ping type"""
    file_exists = os.path.exists(output_file)
    rows_found = 0
    with open(output_file, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(['timestamp', 'provider', 'ping_type', 'region', 'datacenter', 'latency_ms'])
        tables = driver.find_elements(By.TAG_NAME, "table")
        print(f"📊 Found {len(tables)} tables for {ping_type}")
        for table_idx, table in enumerate(tables):
            rows = table.find_elements(By.TAG_NAME, "tr")
            for i, row in enumerate(rows[1:], 1):
                try:
                    cells = row.find_elements(By.XPATH, ".//td | .//th")
                    if len(cells) >= 2:
                        region_text = cells[0].text.strip()
                        latency_cell = cells[1]
                        latency_text = latency_cell.text.strip()
                        print(f"DEBUG: Raw latency for {region_text}: '{latency_text}'")
                        if not any(c.isdigit() for c in latency_text):
                            spans = latency_cell.find_elements(By.TAG_NAME, "span")
                            for span in spans:
                                st = span.text.strip()
                                if any(c.isdigit() for c in st):
                                    latency_text = st
                                    print(f"DEBUG: Span latency: '{st}'")
                                    break
                        if not any(c.isdigit() for c in latency_text):
                            data_val = (latency_cell.get_attribute('data-value') or
                                        latency_cell.get_attribute('data-latency') or
                                        latency_cell.get_attribute('title') or
                                        latency_cell.get_attribute('innerText'))
                            if data_val and any(c.isdigit() for c in data_val):
                                latency_text = data_val
                                print(f"DEBUG: Attribute latency: '{data_val}'")
                        if (region_text and
                            region_text not in ['Region', ''] and
                            any(keyword in region_text.lower() for keyword in ['ap-', 'na-', 'la-', 'sa-', 'af-', 'cn-'])):
                            region_code = extract_region_code(region_text)
                            datacenter_name = get_datacenter_name(region_code) if region_code else region_text
                            latency_clean = extract_latency_value(latency_text)
                            if latency_clean:
                                writer.writerow([timestamp, 'cloudping Huawei', ping_type, region_code or region_text, datacenter_name, latency_clean])
                                print(f"✓ {ping_type} - {datacenter_name}: {latency_clean}ms")
                                rows_found += 1
                            else:
                                print(f"⚠️ {ping_type} - No latency for: {region_text}")
                except Exception as e:
                    print(f"⚠️ Error in row {i}, table {table_idx + 1}: {e}")
                    continue
    if rows_found == 0:
        print(f"=== JavaScript fallback for {ping_type} ===")
        try:
            script = """
            var data = [];
            var rows = document.querySelectorAll('table tr');
            rows.forEach(function(row, index) {
                if (index === 0) return;
                var cells = row.querySelectorAll('td, th');
                if (cells.length >= 2) {
                    var region = cells[0].innerText.trim();
                    var latency = cells[1].innerText.trim();
                    if (region && !region.includes('Region')) {
                        data.push({
                            region: region,
                            latency: latency,
                            innerHTML: cells[1].innerHTML
                        });
                    }
                }
            });
            return data;
            """
            js_data = driver.execute_script(script)
            print(f"JS found {len(js_data)} rows")
            with open(output_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                for item in js_data:
                    region_code = extract_region_code(item['region'])
                    datacenter_name = get_datacenter_name(region_code) if region_code else item['region']
                    latency_clean = extract_latency_value(item['latency'])
                    print(f"DEBUG JS: Region: {item['region']}, Latency: {item['latency']}")
                    if latency_clean and any(kw in item['region'].lower() for kw in ['ap-', 'na-', 'la-', 'sa-', 'af-', 'cn-']):
                        writer.writerow([timestamp, 'cloudping Huawei', ping_type, region_code or item['region'], datacenter_name, latency_clean])
                        print(f"✓ JS {ping_type}: {datacenter_name} - {latency_clean}ms")
                        rows_found += 1
        except Exception as e:
            print(f"JS fallback error: {e}")
    return rows_found

def scrape_huawei_cloudping_selenium():
    url = "https://www.cloudping.cloud/huawei"
    driver = None
    max_retries = 3
    retry_delay = 5
    if not check_website_accessibility(url):
        print("❌ Aborting due to website inaccessibility")
        return False
    for attempt in range(1, max_retries + 1):
        try:
            print(f"\n🚀 === ATTEMPT {attempt}/{max_retries} ===")
            driver = setup_driver(headless=False)
            if not driver:
                print("❌ Failed to set up driver")
                continue
            print(f"🌐 Loading: {url}")
            driver.get(url)
            wait = WebDriverWait(driver, 10)
            timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            output_file = 'huawei_cloudping_latency.csv'
            total_rows_found = 0
            time.sleep(1)
            http_ping_buttons = find_http_ping_buttons(driver, wait)
            if len(http_ping_buttons) < 2:
                print(f"❌ Only found {len(http_ping_buttons)} HTTP Ping buttons. Need 2.")
                with open(f'huawei_attempt_{attempt}_state.html', 'w', encoding='utf-8') as f:
                    f.write(driver.page_source)
                print(f"📄 Saved attempt {attempt} HTML")
                if attempt < max_retries:
                    time.sleep(retry_delay)
                    driver.quit()
                    continue
                return False
            print(f"✅ Found {len(http_ping_buttons)} HTTP Ping buttons. Processing both...")
            print("\n=== PROCESSING FIRST HTTP PING BUTTON ===")
            if click_specific_http_ping_button(driver, wait, 0, http_ping_buttons):
                if wait_for_latency_data(driver, wait):
                    rows_found = extract_and_save_data(driver, "HTTP_Ping_1", timestamp, output_file)
                    total_rows_found += rows_found
                    print(f"✅ First HTTP Ping: Saved {rows_found} rows")
                    print("⏳ Waiting before second button...")
                    time.sleep(3)
                    with open(f'huawei_after_first_ping.html', 'w', encoding='utf-8') as f:
                        f.write(driver.page_source)
                    print("📄 Saved HTML after first ping")
                else:
                    print("❌ First HTTP Ping: No data loaded")
                    with open(f'huawei_after_first_ping_failed.html', 'w', encoding='utf-8') as f:
                        f.write(driver.page_source)
                    print("📄 Saved HTML after first ping failure")
            print("\n=== PROCESSING SECOND HTTP PING BUTTON ===")
            http_ping_buttons = find_http_ping_buttons(driver, wait)
            if len(http_ping_buttons) >= 2:
                if click_specific_http_ping_button(driver, wait, 1, http_ping_buttons):
                    if wait_for_latency_data(driver, wait):
                        rows_found = extract_and_save_data(driver, "HTTP_Ping_2", timestamp, output_file)
                        total_rows_found += rows_found
                        print(f"✅ Second HTTP Ping: Saved {rows_found} rows")
                    else:
                        print("❌ Second HTTP Ping: No data loaded")
                        with open(f'huawei_after_second_ping_failed.html', 'w', encoding='utf-8') as f:
                            f.write(driver.page_source)
                        print("📄 Saved HTML after second ping failure")
                else:
                    print("❌ Failed to click second HTTP Ping button")
            else:
                print(f"❌ Only found {len(http_ping_buttons)} buttons for second click")
            driver.save_screenshot('huawei_final_state.png')
            print("📸 Final screenshot saved")
            with open('huawei_cloudping_final.html', 'w', encoding='utf-8') as f:
                f.write(driver.page_source)
            print("📄 Final HTML saved")
            print(f"\n🎉 TOTAL: {total_rows_found} rows saved to {output_file}")
            return total_rows_found > 0
        except Exception as e:
            print(f"💥 Error in attempt {attempt}: {e}")
            import traceback
            traceback.print_exc()
            if attempt < max_retries:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            if driver:
                driver.quit()
                driver = None
        finally:
            if driver:
                driver.quit()
    print("❌ Failed after all attempts")
    return False

def extract_region_code(region_text):
    """Extract region code from text"""
    patterns = [
        r'([a-z]{2}-[a-z]+-\d+)',
        r'([a-z]{2}-[a-z]+\d-\d+)'
    ]
    for pattern in patterns:
        match = re.search(pattern, region_text, re.IGNORECASE)
        if match:
            return match.group(1)
    return None

def extract_latency_value(latency_text):
    """Extract numeric latency value"""
    if not latency_text:
        return None
    match = re.search(r'(\d+\.?\d*)', latency_text)
    return match.group(1) if match else None

def get_datacenter_name(region):
    """Convert region code to datacenter name"""
    if not region:
        return "Unknown"
    datacenter_map = {
        'ap-southeast-1': 'Hong Kong, China',
        'ap-southeast-2': 'Bangkok, Thailand',
        'ap-southeast-3': 'Singapore',
        'na-mexico-1': 'Mexico City 1, Mexico',
        'la-north-2': 'Mexico City 2, Mexico',
        'la-south-2': 'Santiago, Chile',
        'sa-brazil-1': 'Sao Paulo, Brazil',
        'af-south-1': 'Johannesburg, South Africa',
        'cn-north-1': 'Beijing 1',
        'cn-north-4': 'Beijing 4',
        'cn-north-9': 'Wulanchabu',
        'cn-south-1': 'Guangzhou',
        'cn-southwest-2': 'Guiyang 1',
        'cn-east-3': 'Shanghai 1',
        'cn-east-2': 'Shanghai 2'
    }
    return datacenter_map.get(region.lower(), region)

if __name__ == "__main__":
    print("=== HUAWEI CLOUDPING SCRAPER - DUAL HTTP PING ===")
    success = scrape_huawei_cloudping_selenium()
    if success:
        print("\n🎊 ✅ SUCCESS! Check 'huawei_cloudping_latency.csv'")
        print("Files generated:")
        print(" 📸 huawei_before_http_ping_1.png, huawei_after_http_ping_1.png")
        print(" 📸 huawei_before_http_ping_2.png, huawei_after_http_ping_2.png")
        print(" 📸 huawei_final_state.png")
        print(" 📄 huawei_after_first_ping.html, huawei_cloudping_final.html")
    else:
        print("\n❌ FAILED. Check screenshots and HTML for debugging")