from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException
import csv
import datetime
import os
import time
import re

def setup_driver():
    """Configure Chrome driver"""
    chrome_options = Options()
    # chrome_options.add_argument('--headless') # Uncomment for headless mode
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('--start-maximized')
    chrome_options.add_argument('--disable-web-security')
    chrome_options.add_argument('--ignore-certificate-errors')
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(180)
    return driver

def scrape_cloudping_net_azure():
    url = "https://cloudping.net/"
    driver = None
    try:
        print("Starting browser...")
        driver = setup_driver()
        print(f"Loading page: {url}")
        # Attempt to load the page with retry mechanism
        max_retries = 5
        for attempt in range(max_retries):
            try:
                driver.get(url)
                time.sleep(5)  # Wait for initial page load
                break
            except Exception as e:
                print(f"Page load failed (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt == max_retries - 1:
                    raise Exception("Failed to load page after multiple attempts")
                time.sleep(15)
        # Check for CAPTCHA
        if 'captcha' in driver.page_source.lower():
            print("CAPTCHA detected, pausing for manual intervention...")
            time.sleep(60)
        # Handle any consent popup
        try:
            consent_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Accept') or contains(text(), 'Agree')]")
            consent_button.click()
            print("Clicked consent button")
            time.sleep(2)
        except:
            print("No consent button found")
        # Switch to Azure tab with retry mechanism
        print("Selecting Azure tab...")
        max_click_retries = 3
        for attempt in range(max_click_retries):
            try:
                azure_tab = WebDriverWait(driver, 30).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Azure')]"))
                )
                driver.execute_script("arguments[0].scrollIntoView(true);", azure_tab)
                ActionChains(driver).move_to_element(azure_tab).click().perform()
                print("Clicked Azure tab")
                break
            except StaleElementReferenceException:
                print(f"Stale element detected for Azure tab (attempt {attempt + 1}/{max_click_retries}). Retrying...")
                time.sleep(2)
            except TimeoutException:
                print(f"Azure tab not clickable (attempt {attempt + 1}/{max_click_retries}). Retrying...")
                time.sleep(2)
            if attempt == max_click_retries - 1:
                print("Attempting JavaScript click on Azure tab...")
                try:
                    azure_tab = driver.find_element(By.XPATH, "//button[contains(text(), 'Azure')]")
                    driver.execute_script("arguments[0].click();", azure_tab)
                    print("JavaScript click on Azure tab successful")
                    break
                except Exception as e:
                    raise Exception(f"Failed to click Azure tab after multiple attempts: {e}")
        time.sleep(15)  # Ensure Azure content loads
        # Click the "Azure Ping" button
        print("Searching for 'Azure Ping' button...")
        try:
            ping_button = WebDriverWait(driver, 30).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'azure ping')]"))
            )
            driver.execute_script("arguments[0].scrollIntoView(true);", ping_button)
            time.sleep(0.5)
            driver.save_screenshot('azure_cloudping_net_before_azure_ping.png')
            print("📸 Screenshot BEFORE Azure Ping click saved")
            ActionChains(driver).move_to_element(ping_button).click().perform()
            print("🖱️ Clicked 'Azure Ping' button!")
            time.sleep(1)
            driver.save_screenshot('azure_cloudping_net_after_azure_ping.png')
            print("📸 Screenshot AFTER Azure Ping click saved")
        except Exception as e:
            print(f"Error clicking 'Azure Ping' button: {e}")
            raise Exception("Failed to click 'Azure Ping' button")
        # Wait for the Azure latency data to load
        print("Waiting for Azure latency data to load...")
        wait = WebDriverWait(driver, 60)
        wait.until(EC.presence_of_all_elements_located((By.XPATH, "//*[contains(text(), 'Azure Ping Results')]")))
        # Monitor for sufficient latency data
        max_wait = 300  # 300 seconds
        start_time = time.time()
        latency_elements = []
        while len(latency_elements) < 20 and (time.time() - start_time) < max_wait:
            try:
                latency_elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'ms') or contains(text(), 'Failed')]")
                print(f"Found {len(latency_elements)} elements with latency data or Failed status")
                if len(latency_elements) >= 20:  # Expect ~41 regions
                    time.sleep(5)  # Wait for stable data
                    break
                time.sleep(2)
            except:
                print("Latency data not ready, retrying...")
                time.sleep(2)
        if len(latency_elements) < 20:
            print("Warning: Insufficient latency data loaded. Saving available data.")
        # Save screenshot for debugging
        driver.save_screenshot('azure_cloudping_net_loaded.png')
        print("Screenshot saved as 'azure_cloudping_net_loaded.png'")
        # Save the HTML of the relevant section for debugging
        try:
            data_container = driver.find_element(By.XPATH, "//*[contains(text(), 'Azure Ping Results')]//following-sibling::*")
            with open('azure_cloudping_net_data_section.html', 'w', encoding='utf-8') as f:
                f.write(data_container.get_attribute('outerHTML'))
            print("Data section HTML saved as 'azure_cloudping_net_data_section.html'")
        except:
            print("Could not save data section HTML")
        # Log all elements containing 'ms' or 'Failed' for debugging
        try:
            all_ms_elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'ms') or contains(text(), 'Failed')]")
            print("All elements containing 'ms' or 'Failed':")
            for i, element in enumerate(all_ms_elements, 1):
                print(f"Element {i}: {element.text}")
                try:
                    parent = element.find_element(By.XPATH, "./..")
                    print(f"Parent of Element {i}: {parent.text}")
                except:
                    print(f"No parent found for Element {i}")
        except:
            print("Could not log elements containing 'ms' or 'Failed'")
        # Get timestamp for CSV
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        output_file = 'azure_cloudpingnet_latency.csv'
        file_exists = os.path.exists(output_file)
        rows_found = 0
        with open(output_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(['timestamp', 'provider', 'region', 'datacenter', 'latency_ms'])
            # Extract data
            print("Raw latency data:")
            for i, element in enumerate(latency_elements, 1):
                try:
                    parent = element.find_element(By.XPATH, "./..")
                    text = parent.text.strip()
                except:
                    text = element.text.strip()
                print(f"Element {i}: {text}")
                # Skip non-region elements (e.g., headers, summaries)
                if not text or text in ['ms', 'Failed', 'North America', 'South America', 'Europe', 'Asia', 'Australia', 'Africa', 'Middle East'] or 'Tested' in text or 'Fastest' in text or 'Slowest' in text:
                    print(f"⚠️ Skipping element '{text}': header or invalid")
                    continue
                try:
                    # Handle "Failed" cases
                    if text.endswith('Failed'):
                        region_name = text.replace('Failed', '').strip()
                        print(f"Processing: Region: '{region_name}', Status: Failed")
                        writer.writerow([timestamp, 'cloudpingnet Azure', region_name, region_name, 'Failed'])
                        print(f"✓ SAVED: cloudpingnet Azure - {region_name} - Failed")
                        rows_found += 1
                        continue
                    # Expected formats:
                    # 1. "Canada Central 151.30 ms"
                    # 2. "Switzerland North Run 2/3: 64.02 ms"
                    # 3. "Canada Central151.30ms" (no space before ms)
                    match = re.match(r'(.+?)(?:(?:\s*(?:Run \d/\d:)?\s*|\s*)(\d+\.\d+\s*ms))$', text)
                    if not match:
                        match = re.match(r'(.+?)(\d+\.\d+ms)$', text)  # Handle "Canada Central151.30ms"
                    if match:
                        region_name = match.group(1).strip()
                        latency = match.group(2)
                        print(f"Processing: Region: '{region_name}', Latency: '{latency}'")
                        # Extract numeric latency
                        latency_clean = extract_latency_value(latency) if latency else None
                        if latency_clean and float(latency_clean) > 1:  # Threshold at 1 ms
                            writer.writerow([timestamp, 'cloudpingnet Azure', region_name, region_name, latency_clean])
                            print(f"✓ SAVED: cloudpingnet Azure - {region_name} - {latency_clean}ms")
                            rows_found += 1
                        else:
                            print(f"⚠️ Invalid or unrealistic latency for {region_name}: {latency}")
                    else:
                        print(f"⚠️ Skipping element '{text}': invalid format")
                except Exception as e:
                    print(f"Error processing element '{text}': {e}")
                    continue
        print(f"Total rows saved: {rows_found}")
        if rows_found == 0:
            print("💡 POSSIBLE SOLUTIONS:")
            print("1. Check 'azure_cloudping_net_loaded.png' to verify page content.")
            print("2. Check 'azure_cloudping_net_before_azure_ping.png' and 'azure_cloudping_net_after_azure_ping.png' to verify button click.")
            print("3. Inspect 'azure_cloudping_net_final.html' and 'azure_cloudping_net_data_section.html' for data structure.")
            print("4. Increase max_wait time (e.g., 400 seconds) if data is still loading.")
            print("5. Ensure the browser is not blocked by the website (try non-headless mode).")
        with open('azure_cloudping_net_final.html', 'w', encoding='utf-8') as f:
            f.write(driver.page_source)
            print("HTML saved as 'azure_cloudping_net_final.html'")
        return rows_found > 0
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        if driver:
            driver.save_screenshot('azure_cloudping_net_error.png')
            print("Screenshot saved as 'azure_cloudping_net_error.png'")
            with open('azure_cloudping_net_error.html', 'w', encoding='utf-8') as f:
                f.write(driver.page_source)
            print("HTML saved as 'azure_cloudping_net_error.html'")
        return False
    finally:
        if driver:
            print("Closing browser...")
            driver.quit()

def extract_latency_value(latency_text):
    """Extract numeric latency value from text (e.g., '123.45 ms' or '123 ms' -> '123.45' or '123')"""
    if not latency_text:
        return None
    match = re.search(r'(\d+\.?\d*)\s*ms', latency_text)
    return match.group(1) if match else None

if __name__ == "__main__":
    print("=== AZURE CLOUDPING.NET SCRAPER ===")
    success = scrape_cloudping_net_azure()
    if success:
        print("✅ Data extracted successfully!")
    else:
        print("❌ Failed to extract data.")