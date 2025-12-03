from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
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

def scrape_cloudping_net():
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
        # Click the "AWS Ping" button
        print("Searching for 'AWS Ping' button...")
        wait = WebDriverWait(driver, 60)
        try:
            ping_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'aws ping')]")))
            driver.execute_script("arguments[0].scrollIntoView(true);", ping_button)
            time.sleep(0.5)
            driver.save_screenshot('cloudping_net_before_aws_ping.png')
            print("📸 Screenshot BEFORE AWS Ping click saved")
            ActionChains(driver).move_to_element(ping_button).click().perform()
            print("🖱️ Clicked 'AWS Ping' button!")
            time.sleep(1)
            driver.save_screenshot('cloudping_net_after_aws_ping.png')
            print("📸 Screenshot AFTER AWS Ping click saved")
        except Exception as e:
            print(f"Error clicking 'AWS Ping' button: {e}")
            raise Exception("Failed to click 'AWS Ping' button")
        # Wait for the latency data to load
        print("Waiting for latency data to load...")
        wait = WebDriverWait(driver, 60)
        wait.until(EC.presence_of_all_elements_located((By.XPATH, "//*[contains(text(), 'ms')]")))
        # Monitor for sufficient latency data
        max_wait = 240  # Increased to 240 seconds
        start_time = time.time()
        latency_elements = []
        while len(latency_elements) < 30 and (time.time() - start_time) < max_wait:
            try:
                # Try a more specific selector for region entries
                latency_elements = driver.find_elements(By.XPATH, "//div[contains(@class, 'region') or contains(@class, 'aws')]//*[contains(text(), 'ms')]")
                print(f"Found {len(latency_elements)} elements with latency data")
                if len(latency_elements) >= 30:  # Expect ~34 regions
                    time.sleep(5)  # Wait for stable data
                    break
                time.sleep(2)
            except:
                print("Latency data not ready, retrying...")
                time.sleep(2)
        if len(latency_elements) < 30:
            print("Warning: Insufficient latency data loaded. Saving available data.")
        # Save screenshot for debugging
        driver.save_screenshot('cloudping_net_loaded.png')
        print("Screenshot saved as 'cloudping_net_loaded.png'")
        # Save the HTML of the relevant section for debugging
        try:
            data_container = driver.find_element(By.XPATH, "//*[contains(text(), 'AWS Ping Results')]//following-sibling::*[contains(., 'ms')]")
            with open('cloudping_net_data_section.html', 'w', encoding='utf-8') as f:
                f.write(data_container.get_attribute('outerHTML'))
            print("Data section HTML saved as 'cloudping_net_data_section.html'")
        except:
            print("Could not save data section HTML")
        # Log all elements containing 'ms' for debugging
        try:
            all_ms_elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'ms')]")
            print("All elements containing 'ms':")
            for i, element in enumerate(all_ms_elements, 1):
                print(f"MS Element {i}: {element.text}")
                try:
                    parent = element.find_element(By.XPATH, "./..")
                    print(f"Parent of MS Element {i}: {parent.text}")
                except:
                    print(f"No parent found for MS Element {i}")
        except:
            print("Could not log elements containing 'ms'")
        # Get timestamp for CSV
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        output_file = 'aws_cloudpingnet_latency.csv'
        file_exists = os.path.exists(output_file)
        rows_found = 0
        with open(output_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(['timestamp', 'provider', 'region', 'datacenter', 'latency_ms'])
            # Extract data
            print("Raw latency data:")
            for i, element in enumerate(latency_elements, 1):
                text = element.text.strip()
                print(f"Element {i}: {text}")
                try:
                    # Try parent element if the data is split
                    parent = element.find_element(By.XPATH, "./..")
                    parent_text = parent.text.strip()
                    print(f"Parent Element {i}: {parent_text}")
                    # Expected format: "ca-central-1 (Canada Central) 138.60 ms"
                    match = re.match(r'(\w+-\w+-\d+)\s*\((.*?)\)\s*(\d+\.\d+\s*ms)', parent_text)
                    if not match:
                        # Try alternative format: "ca-central-1 - Canada Central: 138.60ms"
                        match = re.match(r'(\w+-\w+-\d+)\s*[-:]\s*(.*?)\s*(\d+\.\d+\s*ms)', parent_text)
                    if match:
                        region_code = match.group(1)
                        datacenter_name = match.group(2)
                        mean_latency = match.group(3)
                        print(f"Processing: Region Code: '{region_code}', Datacenter: '{datacenter_name}', Latency: '{mean_latency}'")
                        # Extract numeric latency
                        latency_clean = extract_latency_value(mean_latency)
                        if latency_clean and float(latency_clean) > 10:
                            writer.writerow([timestamp, 'cloudpingnet AWS', region_code, datacenter_name, latency_clean])
                            print(f"✓ SAVED: cloudpingnet AWS - {datacenter_name} ({region_code}) - {latency_clean}ms")
                            rows_found += 1
                        else:
                            print(f"⚠️ Invalid or unrealistic latency for {datacenter_name}: {mean_latency}")
                    else:
                        print(f"⚠️ Skipping element with parent text '{parent_text}': invalid format")
                except Exception as e:
                    print(f"Error processing element '{text}': {e}")
                    continue
        print(f"Total rows saved: {rows_found}")
        if rows_found == 0:
            print("💡 POSSIBLE SOLUTIONS:")
            print("1. Check 'cloudping_net_loaded.png' to verify page content.")
            print("2. Check 'cloudping_net_before_aws_ping.png' and 'cloudping_net_after_aws_ping.png' to verify button click.")
            print("3. Inspect 'cloudping_net_final.html' and 'cloudping_net_data_section.html' for data structure.")
            print("4. Increase max_wait time (e.g., 300 seconds) if data is still loading.")
            print("5. Ensure the browser is not blocked by the website (try non-headless mode).")
        # Save full HTML for debugging
        with open('cloudping_net_final.html', 'w', encoding='utf-8') as f:
            f.write(driver.page_source)
            print("HTML saved as 'cloudping_net_final.html'")
        return rows_found > 0
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        if driver:
            driver.save_screenshot('cloudping_net_error.png')
            print("Screenshot saved as 'cloudping_net_error.png'")
            with open('cloudping_net_error.html', 'w', encoding='utf-8') as f:
                f.write(driver.page_source)
            print("HTML saved as 'cloudping_net_error.html'")
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
    print("=== AWS CLOUDPING.NET SCRAPER ===")
    success = scrape_cloudping_net()
    if success:
        print("✅ Data extracted successfully!")
    else:
        print("❌ Failed to extract data.")