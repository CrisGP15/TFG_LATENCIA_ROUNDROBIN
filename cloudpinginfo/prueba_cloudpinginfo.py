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
import traceback

def setup_driver():
    chrome_options = Options()
    # chrome_options.add_argument('--headless')  # Descomenta si quieres headless
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(180)
    return driver

# =================================================================
# LIMPIEZA TOTAL DE TEXTOS POLÍTICOS (nueva función estrella)
# =================================================================
def clean_political_text(text):
    if not text:
        return text
    # Elimina cualquier cosa que empiece por "Doing business with", con o sin paréntesis
    cleaned = re.sub(r'\(?\s*Doing business with[^()]*\)?', '', text, flags=re.IGNORECASE)
    # Elimina paréntesis vacíos residuales
    cleaned = re.sub(r'\(\s*\)', '', cleaned)
    cleaned = re.sub(r'\s{2,}', ' ', cleaned).strip()
    return cleaned

# =================================================================
# PARSEO DE REGIÓN + DATACENTER (ahora infalible)
# =================================================================
def parse_region(raw_text):
    text = clean_political_text(raw_text)

    # Mapeo manual de regiones conocidas polémicas → nombre real bonito
    KNOWN_REGIONS = {
        'il-central-1': 'Tel Aviv',
        'me-west1': 'Tel Aviv',
        'me-south-1': 'Bahrain',
        'me-central-1': 'UAE',
        'me-central2': 'Dammam',         # o Riyadh / Saudi Arabia
        'global': 'Global',
    }

    # Caso especial: Global Load Balancer
    if 'global' in text.lower() and 'load balancer' in text.lower():
        return 'global', 'Global External HTTPS Load Balancer'

    # Separamos código de región del nombre entre paréntesis
    match = re.match(r'^([^\(]+?)\s*(?:\(([^()]+)\))?\s*$', text.strip())

    if not match:
        region_code = text.strip()
        return region_code, KNOWN_REGIONS.get(region_code, region_code)

    region_code = match.group(1).strip()
    location = match.group(2).strip() if match.group(2) else None

    # Si hay nombre entre paréntesis → usarlo
    if location and location not in ['Global', 'Global External HTTPS Load Balancer']:
        final_location = location
    else:
        final_location = KNOWN_REGIONS.get(region_code, region_code)

    return region_code, final_location

# =================================================================
# Extracción de latencia
# =================================================================
def extract_latency_value(latency_text):
    if not latency_text:
        return None
    match = re.search(r'(\d+\.?\d*)\s*ms', latency_text)
    return match.group(1) if match else None

# =================================================================
# SCRAPER PRINCIPAL
# =================================================================
def scrape_cloudping_info():
    driver = None
    try:
        print("Iniciando navegador...")
        driver = setup_driver()
        driver.get("https://www.cloudping.info/")
        time.sleep(8)

        # Consentimiento
        try:
            btn = driver.find_element(By.XPATH, "//button[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'accept') or contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'agree')]")
            btn.click()
            time.sleep(2)
        except:
            pass

        # Click HTTP Ping
        print("Lanzando HTTP Ping...")
        driver.find_element(By.XPATH, "//button[contains(text(), 'HTTP Ping')]").click()
        time.sleep(5)

        # Espera datos
        wait = WebDriverWait(driver, 600)
        wait.until(EC.presence_of_all_elements_located((By.XPATH, "//td[contains(text(), 'ms')]")))

        print("Esperando datos estables...")
        start = time.time()
        while time.time() - start < 500:
            lats = driver.find_elements(By.XPATH, "//td[contains(text(), 'ms')]")
            pend = driver.find_elements(By.XPATH, "//td[contains(text(), 'pinging') or contains(text(), 'connecting')]")
            print(f"{int(time.time()-start)}s → {len(lats)} latencias | {len(pend)} pendientes")

            if len(lats) >= 100 and len(pend) < 10:
                time.sleep(6)
                break
            time.sleep(4)

        # Guardar screenshot final
        driver.save_screenshot("cloudping_final.png")

        # Extraer y guardar CSV
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        file_exists = os.path.exists('cloudpinginfo_latency.csv')

        with open('cloudpinginfo_latency.csv', 'a', newline='', encoding='utf-8') as f:
            w = csv.writer(f)
            if not file_exists:
                w.writerow(['timestamp', 'provider', 'region', 'datacenter', 'latency_ms'])

            table = driver.find_element(By.XPATH, "//table")
            current_provider = None
            saved = 0

            for row in table.find_elements(By.TAG_NAME, "tr"):
                cells = row.find_elements(By.TAG_NAME, "td")
                texts = [c.text.strip() for c in cells]

                # Proveedor
                if len(cells) == 1 and texts[0]:
                    current_provider = texts[0].replace('™', '').strip()
                    continue

                # Datos
                if len(cells) == 2:
                    region_raw, lat_raw = texts
                    if lat_raw in ['pinging', 'connecting', 'unavailable', 'timeout', '']:
                        continue

                    region_code, datacenter = parse_region(region_raw)
                    latency = extract_latency_value(lat_raw)

                    if latency and float(latency) > 0:
                        w.writerow([
                            timestamp,
                            f"cloudping.info {current_provider}",
                            region_code,
                            datacenter,
                            latency
                        ])
                        print(f"{current_provider} → {datacenter} ({region_code}): {latency}ms")
                        saved += 1

        print(f"\n¡LISTO! {saved} filas guardadas en cloudpinginfo_latency.csv")
        return True

    except Exception as e:
        print(f"ERROR: {e}")
        traceback.print_exc()
        if driver:
            driver.save_screenshot("cloudping_ERROR.png")
        return False

    finally:
        if driver:
            driver.quit()

if __name__ == "__main__":
    print("=== CLOUDPING.INFO SCRAPER LIMPIO ===\n")
    scrape_cloudping_info()