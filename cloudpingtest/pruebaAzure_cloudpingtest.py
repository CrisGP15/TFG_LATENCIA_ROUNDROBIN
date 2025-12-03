from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import csv
import datetime
import os
import time
import re
import subprocess
import sys

def kill_chrome_processes():
    """Kill existing Chrome processes to avoid conflicts"""
    try:
        subprocess.run(['pkill', '-f', 'chrome'], capture_output=True)
        subprocess.run(['pkill', '-f', 'chromedriver'], capture_output=True)
        time.sleep(2)
        print("🧹 Cleaned existing Chrome processes")
    except:
        pass

def setup_driver():
    """Configure Chrome driver with better error handling"""
    chrome_options = Options()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('--disable-plugins')
    chrome_options.add_argument('--disable-images')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_argument('--disable-web-security')
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument('--ignore-ssl-errors')
    chrome_options.add_argument('--disable-notifications')
    driver = None
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            print(f"🚗 Starting Chrome driver (attempt {attempt + 1}/{max_attempts})")
            driver = webdriver.Chrome(options=chrome_options)
            driver.set_page_load_timeout(60)
            driver.set_script_timeout(30)
            driver.implicitly_wait(5)
            driver.get("about:blank")
            print("✅ Chrome driver started successfully")
            return driver
        except Exception as e:
            print(f"❌ Driver attempt {attempt + 1} failed: {e}")
            if driver:
                try:
                    driver.quit()
                except:
                    pass
            time.sleep(5)
    raise Exception("Failed to start Chrome driver after multiple attempts")

def extract_mean_latency_only(row_cells):
    """Extract ONLY the Mean latency value from the specific column"""
    if len(row_cells) < 8:  # Need at least up to Mean column
        return None, None, None
    try:
        # Column indices based on table structure:
        # 0: #, 1: Region Name, 2: Region Code, 3: Mean, 4: Median, 5: Min, 6: Max, 7+: Tests
        row_number = row_cells[0].text.strip()
        region_name = row_cells[1].text.strip()
        region_code = row_cells[2].text.strip()
        mean_cell = row_cells[3]  # SPECIFICALLY the Mean column (index 3)
        mean_text = mean_cell.text.strip()
        # Extract numeric value from Mean cell
        mean_ms = extract_latency_value(mean_text)
        # Validate: row number must be numeric, and mean must be valid
        if (re.match(r'^\d+$', row_number) and
                mean_ms and
                float(mean_ms) > 0 and
                float(mean_ms) < 1000):  # Reasonable latency threshold
            print(f"DEBUG: Row {row_number} | {region_name} | Mean cell text: '{mean_text}' -> {mean_ms}ms")
            return region_name, region_code, mean_ms
        print(f"⚠️ Invalid data - Row: {row_number}, Mean text: '{mean_text}', Extracted: {mean_ms}")
        return None, None, None
    except Exception as e:
        print(f"❌ Error extracting mean: {e}")
        return None, None, None

def scrape_cloudpingtest_azure():
    url = "https://cloudpingtest.com/azure"
    driver = None
    try:
        kill_chrome_processes()
        print("🚀 Starting Azure CloudPingTest scraper...")
        driver = setup_driver()
        # Load page
        max_retries = 3
        for attempt in range(max_retries):
            try:
                print(f"🌐 Loading page (attempt {attempt + 1}/{max_retries})")
                driver.get(url)
                WebDriverWait(driver, 30).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
                print("✅ Page loaded successfully")
                time.sleep(3)
                break
            except TimeoutException:
                print(f"⏰ Timeout loading page (attempt {attempt + 1})")
                if attempt == max_retries - 1:
                    raise
                time.sleep(10)
        driver.save_screenshot('azure_initial.png')
        print("📸 Initial screenshot saved")
        # Try to start test
        print("🔍 Looking for Start button...")
        start_xpaths = [
            "//button[contains(text(), 'Start')]",
            "//button[contains(text(), 'start')]",
            "//*[contains(@class, 'start')]",
            "//button[contains(@onclick, 'start')]"
        ]
        start_success = False
        for xpath in start_xpaths:
            try:
                elements = driver.find_elements(By.XPATH, xpath)
                if elements:
                    print(f"🎯 Found start element: {xpath}")
                    elements[0].click()
                    start_success = True
                    print("✅ Start button clicked")
                    break
            except:
                continue
        if not start_success:
            print("⚠️ No start button found. Waiting for auto-start...")
            time.sleep(30)
        # Wait for data
        print("⏳ Waiting for ping data...")
        max_wait = 240
        start_time = time.time()
        while (time.time() - start_time) < max_wait:
            ms_elements = driver.find_elements(By.XPATH, "//td[contains(text(), 'ms')]")
            print(f"📊 Found {len(ms_elements)} 'ms' elements")
            if len(ms_elements) > 50:
                print("✅ Sufficient data detected")
                time.sleep(15)  # Wait for Mean calculations
                break
            time.sleep(10)
        # Extract ONLY Mean latency
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        output_file = 'azure_cloudpingtest_latency.csv'
        file_exists = os.path.exists(output_file)
        rows_found = 0
        with open(output_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(['timestamp', 'provider', 'region_name', 'region_code', 'mean_ms'])
            tables = driver.find_elements(By.TAG_NAME, "table")
            for table_idx, table in enumerate(tables):
                try:
                    rows = table.find_elements(By.TAG_NAME, "tr")
                    print(f"\n📋 Processing table {table_idx + 1} with {len(rows)} rows")
                    for row_idx, row in enumerate(rows[1:], 1):  # Skip header row
                        cells = row.find_elements(By.TAG_NAME, "td")
                        region_name, region_code, mean_ms = extract_mean_latency_only(cells)
                        if region_name and region_code and mean_ms:
                            writer.writerow([timestamp, 'cloudpingtest Azure', region_name, region_code, mean_ms])
                            print(f"✓ SAVED: cloudpingtest Azure - {region_name} ({region_code}): {mean_ms}ms")
                            rows_found += 1
                        else:
                            print(f"⚠️ Skipped row {row_idx}: Invalid mean data")
                except Exception as e:
                    print(f"Error processing table {table_idx}: {e}")
                    continue
        print(f"\n✅ Extraction complete: {rows_found} regions saved to {output_file}")
        # Save final state and table HTML for verification
        driver.save_screenshot('azure_final.png')
        try:
            table = driver.find_element(By.TAG_NAME, "table")
            with open('azure_table_final.html', 'w', encoding='utf-8') as f:
                f.write(table.get_attribute('outerHTML'))
            print("📄 Table HTML saved as 'azure_table_final.html'")
        except:
            pass
        return rows_found > 0
    except Exception as e:
        print(f"❌ Critical error: {e}")
        import traceback
        traceback.print_exc()
        if driver:
            try:
                driver.save_screenshot('azure_error.png')
            except:
                pass
        return False
    finally:
        if driver:
            try:
                driver.quit()
                kill_chrome_processes()
            except:
                pass

def extract_latency_value(text):
    """Extract numeric latency value from Mean cell text"""
    if not text:
        return None
    # Look for "X ms" pattern in Mean cell
    match = re.search(r'(\d+(?:\.\d+)?)\s*ms', text)
    return match.group(1) if match else None

if __name__ == "__main__":
    print("=== AZURE CLOUDPINGTEST SCRAPER - MEAN LATENCY ONLY ===")
    success = scrape_cloudpingtest_azure()
    if success:
        print("🎉 SUCCESS! Check 'azure_cloudpingtest_latency.csv'")
    else:
        print("💥 FAILED. Check screenshots and HTML files.")