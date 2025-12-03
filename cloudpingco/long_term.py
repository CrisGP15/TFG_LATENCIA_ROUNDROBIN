import time
import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import csv
import os
import re

#scraper automatizado que mide latencias entre regiones de AWS cada 10 minutos durante 10 días
def setup_driver():
    """Configure Chrome driver"""
    chrome_options = Options()
    # chrome_options.add_argument('--headless')  # Uncomment for headless mode if needed
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

def scrape_cloudping_co():
    url = "https://www.cloudping.co/"
    driver = None
    try:
        print("Starting browser...")
        driver = setup_driver()
        print(f"Loading page: {url}")
        # Load page with retry mechanism
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
        # Wait for the table to load
        print("Waiting for table to load...")
        wait = WebDriverWait(driver, 60)
        table = wait.until(EC.presence_of_element_located((By.XPATH, "//table")))
        # Save screenshot and HTML for debugging (optional, can comment out)
        driver.save_screenshot('cloudping_co_loaded.png')
        print("Screenshot saved as 'cloudping_co_loaded.png'")
        with open('cloudping_co_raw.html', 'w', encoding='utf-8') as f:
            f.write(driver.page_source)
        print("Raw HTML saved as 'cloudping_co_raw.html'")
        # Get timestamp for CSV
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        output_file = 'cloudping_co_data.csv'
        file_exists = os.path.exists(output_file)
        rows_found = 0
        with open(output_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(['timestamp', 'provider', 'from_region', 'to_region', 'latency_ms'])
            # Extract headers (to regions)
            try:
                header_row = table.find_element(By.XPATH, ".//tr[1]")
                headers = header_row.find_elements(By.TAG_NAME, "th")
                if not headers:
                    headers = header_row.find_elements(By.TAG_NAME, "td")
                to_regions = [header.text.strip() for header in headers[1:]]  # Skip 'To \ From'
                print(f"To regions: {to_regions}")
                if not to_regions:
                    raise ValueError("No headers found in the table")
                # Extract data rows
                rows = table.find_elements(By.XPATH, ".//tr")[1:]  # Skip header row
                print(f"Found {len(rows)} data rows in the table")
                # Process data rows
                for row_index, row in enumerate(rows, 1):
                    cells = row.find_elements(By.TAG_NAME, "td")
                    cell_texts = [cell.text.strip() for cell in cells]
                    if len(cells) < 2:
                        print(f"⚠️ Skipping invalid row {row_index + 1}: {cell_texts}")
                        continue
                    from_region = cell_texts[0]
                    latencies = cell_texts[1:]
                    if len(latencies) != len(to_regions):
                        print(f"⚠️ Skipping row {row_index + 1} with mismatched columns: {cell_texts}")
                        continue
                    for j, latency_text in enumerate(latencies):
                        to_region = to_regions[j]
                        if latency_text == '':
                            print(f"⚠️ Skipping empty latency for {from_region} to {to_region}")
                            continue
                        latency_clean = extract_latency_value(latency_text)
                        if latency_clean and float(latency_clean) > 0:
                            writer.writerow([timestamp, 'cloudpingco AWS', from_region, to_region, latency_clean])
                            print(f"✓ SAVED: cloudpingco AWS - {from_region} to {to_region} - {latency_clean}ms")
                            rows_found += 1
                        else:
                            print(f"⚠️ Invalid latency for {from_region} to {to_region}: {latency_text}")
            except Exception as e:
                print(f"Error accessing table: {e}")
        print(f"Total rows saved: {rows_found}")
        with open('cloudping_co_final.html', 'w', encoding='utf-8') as f:
            f.write(driver.page_source)
            print("Final HTML saved as 'cloudping_co_final.html'")
        return rows_found > 0
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        if driver:
            driver.save_screenshot('cloudping_co_error.png')
            print("Screenshot saved as 'cloudping_co_error.png'")
            with open('cloudping_co_error.html', 'w', encoding='utf-8') as f:
                f.write(driver.page_source)
            print("Error HTML saved as 'cloudping_co_error.html'")
        return False
    finally:
        if driver:
            print("Closing browser...")
            driver.quit()

def extract_latency_value(latency_text):
    """Extract numeric latency value from text (e.g., '123.45ms' -> '123.45')"""
    if not latency_text:
        return None
    match = re.search(r'(\d+\.?\d*)\s*ms', latency_text)
    return match.group(1) if match else None

if __name__ == "__main__":
    print("=== CLOUDPING.CO LONG-TERM SCRAPER ===")
    start_time = datetime.datetime.now()
    end_time = start_time + datetime.timedelta(days=10)  # Run for 10 days
    interval_minutes = 10  # Every 10 minutes
    iteration = 0

    while datetime.datetime.now() < end_time:
        print(f"\nIteration {iteration + 1} at {datetime.datetime.now()}")
        success = scrape_cloudping_co()
        if success:
            print("✅ Data extracted successfully!")
        else:
            print("❌ Failed to extract data.")
        
        iteration += 1
        # Sleep for the interval (10 minutes = 600 seconds)
        time.sleep(interval_minutes * 60)
    
    print(f"\nScraping completed after 10 days. Total iterations: {iteration}")