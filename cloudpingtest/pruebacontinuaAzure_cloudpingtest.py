#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AZURE CLOUDPINGTEST 24/7 - FORMATO UNIFICADO
→ timestamp | provider | region | datacenter | latency_ms
→ 100% compatible con GCP, AWS cloudping.net y cloudpingtest AWS
"""
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import csv
import datetime
import time
import os
import random
import logging
import traceback
import re
import signal
import sys
import subprocess

# ===================== CONFIG =====================
URL = "https://cloudpingtest.com/azure"
INTERVALO_MINUTOS = 10
MAX_REINTENTOS = 3
OUTPUT_CSV = "azure_cloudpingtest_latency_longterm.csv" 
LOG_FILE = "azure_cloudpingtest.log"
SCREENSHOT_DIR = "azure_errores"
HEADLESS = True

os.makedirs(SCREENSHOT_DIR, exist_ok=True)

# Logging épico
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[logging.FileHandler(LOG_FILE, encoding='utf-8'), logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger()

USER_AGENTS = [
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
]

# ===================== LIMPIEZA =====================
def matar_chrome():
    try:
        subprocess.run(['pkill', '-f', 'chrome'], capture_output=True, check=False)
        subprocess.run(['pkill', '-f', 'chromedriver'], capture_output=True, check=False)
        time.sleep(2)
    except: pass

def signal_handler(sig, frame):
    logger.info("SCRAPER AZURE DETENIDO")
    sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ===================== DRIVER =====================
def setup_driver():
    matar_chrome()
    chrome_options = Options()
    if HEADLESS: chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-images')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument('--disable-web-security')
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument(f'--user-agent={random.choice(USER_AGENTS)}')
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(120)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def guardar_screenshot(driver, prefijo):
    path = os.path.join(SCREENSHOT_DIR, f"{prefijo}_{int(time.time())}.png")
    driver.save_screenshot(path)
    logger.info(f"ERROR: {path}")

# ===================== UTILIDADES =====================
def extract_ms(text):
    m = re.search(r'(\d+(?:\.\d+)?)\s*ms', text)
    return m.group(1) if m else None

# Mapeo código → nombre bonito (como en AWS)
AZURE_DC_MAP = {
    'australiacentral': 'Australia Central',
    'australiaeast': 'Australia East',
    'australiasoutheast': 'Australia Southeast',
    'centralindia': 'Central India',
    'eastasia': 'East Asia',
    'indonesiacentral': 'Indonesia Central',
    'japaneast': 'Japan East',
    'japanwest': 'Japan West',
    'koreacentral': 'Korea Central',
    'koreasouth': 'Korea South',
    'malaysiawest': 'Malaysia West',
    'newzealandnorth': 'New Zealand North',
    'southindia': 'South India',
    'southeastasia': 'Southeast Asia',
    'westindia': 'West India',
    'austriaeast': 'Austria East',
    'francecentral': 'France Central',
    'germanywestcentral': 'Germany West Central',
    'italynorth': 'Italy North',
    'northeurope': 'North Europe',
    'norwayeast': 'Norway East',
    'polandcentral': 'Poland Central',
    'spaincentral': 'Spain Central',
    'swedencentral': 'Sweden Central',
    'switzerlandnorth': 'Switzerland North',
    'uksouth': 'UK South',
    'ukwest': 'UK West',
    'westeurope': 'West Europe',
    'centralus': 'Central US',
    'eastus': 'East US',
    'eastus2': 'East US 2',
    'northcentralus': 'North Central US',
    'southcentralus': 'South Central US',
    'westcentralus': 'West Central US',
    'westus': 'West US',
    'westus2': 'West US 2',
    'westus3': 'West US 3',
    'israelcentral': 'Israel Central',
    'qatarcentral': 'Qatar Central',
    'uaenorth': 'UAE North',
    'brazilsouth': 'Brazil South',
    'chilecentral': 'Chile Central',
    'canadacentral': 'Canada Central',
    'canadaeast': 'Canada East',
    'mexicocentral': 'Mexico Central',
    'southafricanorth': 'South Africa North',
}

# ===================== CLICK START =====================
def click_start(driver, wait):
    for xpath in ["//button[contains(text(),'Start')]", "//button[contains(text(),'start')]"]:
        try:
            btn = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
            driver.execute_script("arguments[0].click();", btn)
            logger.info("START CLICKEADO")
            return True
        except: pass
    return False

# ===================== ESPERA DATOS =====================
def esperar_datos(driver, max_wait=300):
    start = time.time()
    logger.info("ESPERANDO DATOS AZURE...")
    while time.time() - start < max_wait:
        try:
            ms = len(driver.find_elements(By.XPATH, "//td[contains(text(),'ms')]"))
            if ms >= 50:
                logger.info(f"{ms} latencias → OK")
                time.sleep(15)
                return True
        except: pass
        time.sleep(5)
    return True

# ===================== GUARDAR DATOS (FORMATO UNIFICADO) =====================
def guardar_datos(driver, timestamp):
    rows = 0
    file_exists = os.path.exists(OUTPUT_CSV)
    with open(OUTPUT_CSV, 'a', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow(['timestamp', 'provider', 'region', 'datacenter', 'latency_ms'])
        
        for row in driver.find_elements(By.XPATH, "//table//tr")[1:]:
            cells = row.find_elements(By.TAG_NAME, "td")
            if len(cells) < 4: continue
            region_name = cells[1].text.strip()
            region_code = cells[2].text.strip().lower()
            mean_raw = cells[3].text.strip()
            mean = extract_ms(mean_raw)
            if not (region_name and region_code and mean): continue
            if not (0 < float(mean) < 2000): continue
            
            datacenter = AZURE_DC_MAP.get(region_code, region_name)
            w.writerow([timestamp, 'cloudpingtest.com Azure', region_code, datacenter, mean])
            logger.info(f"{datacenter} ({region_code}): {mean}ms")
            rows += 1
    return rows

# ===================== UNA CAPTURA =====================
def capturar_una_vez():
    driver = None
    try:
        driver = setup_driver()
        wait = WebDriverWait(driver, 60)
        logger.info("CARGANDO AZURE...")

        for _ in range(5):
            try:
                driver.get(URL)
                wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
                time.sleep(5)
                break
            except: time.sleep(10)

        click_start(driver, wait) or logger.info("AUTO-START")
        esperar_datos(driver)

        ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        filas = guardar_datos(driver, ts)
        logger.info(f"{filas} REGIONES AZURE GUARDADAS")
        return filas > 0
    except Exception as e:
        logger.error(f"ERROR: {e}")
        traceback.print_exc()
        if driver: guardar_screenshot(driver, "AZURE_FAIL")
        return False
    finally:
        if driver:
            try: driver.quit()
            except: pass
        matar_chrome()

# ===================== BUCLE 24/7 =====================
def main():
    logger.info("AZURE SCRAPER UNIFICADO INICIADO")
    logger.info(f"Cada {INTERVALO_MINUTOS} min → {OUTPUT_CSV}")
    ciclo = 0
    while True:
        ciclo += 1
        logger.info(f"\nITERACIÓN {ciclo} - {datetime.datetime.now():%H:%M}")
        for intento in range(1, MAX_REINTENTOS + 1):
            if capturar_una_vez(): break
            logger.warning(f"Intento {intento} falló")
            time.sleep(60)
        time.sleep(INTERVALO_MINUTOS * 60)

if __name__ == "__main__":
    main()