from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')  # Avoid detection
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(180)  # Increased timeout
    return driver

def scrape_cloudpingtest_selenium():
    url = "https://cloudpingtest.com/aws"
    driver = None
    try:
        print("Starting browser...")
        driver = setup_driver()
        print(f"Loading page: {url}")
        # Attempt to load the page with retry mechanism
        max_retries = 3
        for attempt in range(max_retries):
            try:
                driver.get(url)
                break
            except Exception as e:
                print(f"Page load failed (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt == max_retries - 1:
                    raise Exception("Failed to load page after multiple attempts")
                time.sleep(10)  # Longer delay before retry
        # Wait for the table to load
        print("Waiting for table to load...")
        wait = WebDriverWait(driver, 30)
        wait.until(EC.presence_of_element_located((By.XPATH, "//table")))
        # Monitor table for first test (test0) completion
        print("Waiting for first test (test0) data...")
        max_wait = 180  # Increased to allow more time
        start_time = time.time()
        rows_with_valid_latency = 0
        while rows_with_valid_latency < 10 and (time.time() - start_time) < max_wait:
            try:
                table = driver.find_element(By.XPATH, "//table")
                rows = table.find_elements(By.TAG_NAME, "tr")
                rows_with_valid_latency = sum(1 for row in rows[1:] if row.find_elements(By.XPATH, ".//td[4][contains(text(), 'ms') and number(translate(text(), ' ms', '')) > 10]"))
                print(f"Rows with valid latency (>10ms): {rows_with_valid_latency}")
                if rows_with_valid_latency >= 10:
                    time.sleep(5)  # Wait for stable values
                    break
                time.sleep(2)
            except:
                print("Table not ready, retrying...")
                time.sleep(2)
        if rows_with_valid_latency < 10:
            print("Warning: First test did not complete in time. Saving available data.")
        # Save screenshot for debugging
        driver.save_screenshot('cloudpingtest_loaded.png')
        print("Screenshot saved as 'cloudpingtest_loaded.png'")
        # Get timestamp for CSV
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        output_file = 'aws_cloudpingtest_latency.csv'
        file_exists = os.path.exists(output_file)
        rows_found = 0
        with open(output_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(['timestamp', 'provider', 'region', 'datacenter', 'latency_ms'])
            # Extract data from the table
            try:
                table = driver.find_element(By.XPATH, "//table")
                rows = table.find_elements(By.TAG_NAME, "tr")
                print(f"Found {len(rows)} rows in the table")
                # Log raw table data for debugging
                print("Raw table data:")
                for i, row in enumerate(rows[1:], 1):
                    cells = row.find_elements(By.TAG_NAME, "td")
                    cell_texts = [cell.text.strip() for cell in cells]
                    print(f"Row {i}: {cell_texts}")
                # Process rows (skip header)
                for row in rows[1:]:
                    try:
                        cells = row.find_elements(By.TAG_NAME, "td")
                        if len(cells) >= 4:  # Expect Row Number, Region Name, Region Code, Mean Latency
                            region_name = cells[1].text.strip()  # Second column
                            region_code = cells[2].text.strip()  # Third column
                            mean_latency = cells[3].text.strip()  # Fourth column
                            print(f"Processing: Region Name: '{region_name}', Region Code: '{region_code}', Latency: '{mean_latency}'")
                            # Validate data
                            if region_name and region_code and mean_latency and 'ms' in mean_latency:
                                latency_clean = extract_latency_value(mean_latency)
                                datacenter_name = get_datacenter_name(region_code)
                                if latency_clean and float(latency_clean) > 10:
                                    writer.writerow([timestamp, 'cloudpingtest AWS', region_code, datacenter_name, latency_clean])
                                    print(f"✓ SAVED: cloudpingtest AWS - {datacenter_name} ({region_code}) - {latency_clean}ms")
                                    rows_found += 1
                                else:
                                    print(f"⚠️ Invalid or unrealistic latency for {region_name}: {mean_latency}")
                            else:
                                print(f"⚠️ Skipping row with region {region_name}: incomplete or invalid data")
                    except Exception as e:
                        print(f"Error processing row: {e}")
                        continue
            except Exception as e:
                print(f"Error accessing table: {e}")
        print(f"Total rows saved: {rows_found}")
        if rows_found == 0:
            print("💡 POSSIBLE SOLUTIONS:")
            print("1. Check 'cloudpingtest_loaded.png' to verify table content.")
            print("2. Inspect 'cloudpingtest_final.html' for table structure.")
            print("3. Increase max_wait time (e.g., 240 seconds) if data is still loading.")
            print("4. Ensure the browser is not blocked by the website (try non-headless mode).")
        # Save HTML for debugging
        with open('cloudpingtest_final.html', 'w', encoding='utf-8') as f:
            f.write(driver.page_source)
            print("HTML saved as 'cloudpingtest_final.html'")
        return rows_found > 0
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        if driver:
            driver.save_screenshot('cloudpingtest_error.png')
            print("Screenshot saved as 'cloudpingtest_error.png'")
            with open('cloudpingtest_error.html', 'w', encoding='utf-8') as f:
                f.write(driver.page_source)
            print("HTML saved as 'cloudpingtest_error.html'")
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

def get_datacenter_name(region):
    """Map region code to datacenter name"""
    if not region:
        return "Unknown"
    datacenter_map = {
        'eu-south-2': 'Spain', 'eu-central-2': 'Zürich', 'eu-south-1': 'Milan',
        'eu-west-2': 'London', 'eu-central-1': 'Frankfurt', 'eu-west-1': 'Ireland',
        'eu-north-1': 'Stockholm', 'il-central-1': 'Israel', 'me-central-1': 'UAE',
        'us-east-2': 'Ohio', 'me-south-1': 'Bahrain', 'mx-central-1': 'Mexico Central',
        'ca-west-1': 'Calgary', 'af-south-1': 'Cape Town', 'us-east-1': 'N. Virginia',
        'eu-west-3': 'Paris', 'ca-central-1': 'Canada Central', 'ap-south-2': 'Hyderabad',
        'sa-east-1': 'São Paulo', 'ap-south-1': 'Mumbai', 'us-west-1': 'N. California',
        'us-west-2': 'Oregon', 'ap-northeast-3': 'Osaka', 'ap-southeast-4': 'Melbourne',
        'ap-northeast-1': 'Tokyo', 'ap-east-2': 'Taipei', 'ap-east-1': 'Hong Kong',
        'ap-northeast-2': 'Seoul', 'ap-southeast-7': 'Thailand', 'ap-southeast-3': 'Jakarta',
        'ap-southeast-1': 'Singapore', 'ap-southeast-5': 'Malaysia', 'ap-southeast-2': 'Sydney',
        'CloudFront CDN': 'CloudFront CDN'
    }
    return datacenter_map.get(region, region)

def extract_region_code(region_text):
    """Extract region code from region name or text"""
    if not region_text:
        return None
    region_map = {
        'US East (N. Virginia)': 'us-east-1',
        'US East (Ohio)': 'us-east-2',
        'US West (N. California)': 'us-west-1',
        'US West (Oregon)': 'us-west-2',
        'Africa (Cape Town)': 'af-south-1',
        'Asia Pacific (Hong Kong)': 'ap-east-1',
        'Asia Pacific (Hyderabad)': 'ap-south-2',
        'Asia Pacific (Jakarta)': 'ap-southeast-3',
        'Asia Pacific (Malaysia)': 'ap-southeast-5',
        'Asia Pacific (Melbourne)': 'ap-southeast-4',
        'Asia Pacific (Mumbai)': 'ap-south-1',
        'Asia Pacific (Osaka)': 'ap-northeast-3',
        'Asia Pacific (Seoul)': 'ap-northeast-2',
        'Asia Pacific (Singapore)': 'ap-southeast-1',
        'Asia Pacific (Sydney)': 'ap-southeast-2',
        'Asia Pacific (Taipei)': 'ap-east-2',
        'Asia Pacific (Thailand)': 'ap-southeast-7',
        'Asia Pacific (Tokyo)': 'ap-northeast-1',
        'Canada (Central)': 'ca-central-1',
        'Canada (Calgary)': 'ca-west-1',
        'Europe (Frankfurt)': 'eu-central-1',
        'Europe (Ireland)': 'eu-west-1',
        'Europe (London)': 'eu-west-2',
        'Europe (Milan)': 'eu-south-1',
        'Europe (Paris)': 'eu-west-3',
        'Europe (Spain)': 'eu-south-2',
        'Europe (Stockholm)': 'eu-north-1',
        'Europe (Zurich)': 'eu-central-2',
        'Mexico (Central)': 'mx-central-1',
        'Middle East (Bahrain)': 'me-south-1',
        'Middle East (UAE)': 'me-central-1',
        'Israel (Tel Aviv)': 'il-central-1',
        'South America (São Paulo)': 'sa-east-1',
        'AWS GovCloud (US-East)': 'us-gov-east-1',
        'AWS GovCloud (US)': 'us-gov-west-1'
    }
    for key, value in region_map.items():
        if key.lower() in region_text.lower():
            return value
    match = re.search(r'([a-z]{2}-[a-z]+-\d+|[a-z]{2}-[a-z]+\d-\d+)', region_text)
    return match.group(1) if match else None

if __name__ == "__main__":
    print("=== AWS CLOUDPINGTEST SCRAPER ===")
    success = scrape_cloudpingtest_selenium()
    if success:
        print("✅ Data extracted successfully!")
    else:
        print("❌ Failed to extract data.")