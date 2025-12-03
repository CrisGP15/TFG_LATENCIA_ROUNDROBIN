#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SCRAPER CONTINUO CLOUDPINGTEST.COM - GCP GLOBAL 24/7
- Cada 10 min → Start → Solo MEAN latency
- CSV LIMPIO: timestamp | provider | region | datacenter | latency_ms
- Limpia procesos + UA rotativos + logs + screenshots SOLO error
- Cierre limpio con Ctrl+C
- datacenter: extrae ubicación de (parentesis) en region_name
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
URL = "https://cloudpingtest.com/gcp"
INTERVALO_MINUTOS = 10
MAX_REINTENTOS = 3
OUTPUT_CSV = "gcp_cloudpingtest_latency_longterm.csv"
LOG_FILE = "gcp_cloudpingtest.log"
SCREENSHOT_DIR = "gcp_errores"
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
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
]

# ===================== LIMPIEZA PROCESOS =====================
def matar_chrome():
    try:
        subprocess.run(['pkill', '-f', 'chrome'], capture_output=True, check=False)
        subprocess.run(['pkill', '-f', 'chromedriver'], capture_output=True, check=False)
        time.sleep(2)
        logger.info("Zombie Chrome processes killed")
    except: pass

# ===================== CIERRE LIMPIO =====================
def signal_handler(sig, frame):
    logger.info("SCRAPER GCP DETENIDO POR USUARIO")
    sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ===================== DRIVER =====================
def setup_driver():
    matar_chrome()
    chrome_options = Options()
    if HEADLESS:
        chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('--disable-images')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument('--disable-web-security')
    chrome_options.add_argument('--ignore-certificate-errors')
    ua = random.choice(USER_AGENTS)
    chrome_options.add_argument(f'--user-agent={ua}')
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(60)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def guardar_screenshot(driver, prefijo):
    path = os.path.join(SCREENSHOT_DIR, f"{prefijo}_{int(time.time())}.png")
    driver.save_screenshot(path)
    logger.info(f"ERROR CAPTURADO: {path}")

# ===================== UTILIDADES =====================
def extract_mean(text):
    if not text: return None
    m = re.search(r'(\d+(?:\.\d+)?)\s*ms', text)
    return m.group(1) if m else None

def get_datacenter(region_name):
    """Extrae ubicación de (parentesis), ej: 'South Africa (Johannesburg)' → 'Johannesburg'"""
    if '(' in region_name and ')' in region_name:
        return region_name.split('(')[-1].rstrip(')')
    return region_name

# ===================== CLIC START =====================
def click_start(driver, wait):
    start_xpaths = [
        "//button[contains(text(), 'Start')]",
        "//button[contains(text(), 'start')]",
        "//*[contains(@class, 'start')]",
        "//button[contains(@onclick, 'start')]"
    ]
    for xpath in start_xpaths:
        try:
            elements = driver.find_elements(By.XPATH, xpath)
            if elements:
                btn = elements[0]
                driver.execute_script("arguments[0].scrollIntoView(true);", btn)
                time.sleep(1)
                driver.execute_script("arguments[0].click();", btn)
                logger.info("START CLICKEADO")
                return True
        except Exception as e:
            logger.debug(f"Start XPath falló: {xpath} - {e}")
    return False

# ===================== ESPERA DATOS =====================
def esperar_datos(driver, max_wait=300):
    start = time.time()
    logger.info("ESPERANDO >50 'ms' ELEMENTOS (máx 300s)...")
    while time.time() - start < max_wait:
        try:
            ms_elements = driver.find_elements(By.XPATH, "//td[contains(text(), 'ms')]")
            count = len(ms_elements)
            logger.info(f"{int(time.time()-start)}s → {count} elementos 'ms'")
            if count >= 50:
                logger.info("¡DATOS SUFICIENTES! Esperando estabilización...")
                time.sleep(15)
                return True
        except: pass
        time.sleep(5)
    logger.warning("TIMEOUT: Guardando lo disponible")
    return True

# ===================== GUARDAR DATOS =====================
def guardar_datos(driver, timestamp):
    rows = 0
    file_exists = os.path.exists(OUTPUT_CSV)
    with open(OUTPUT_CSV, 'a', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow(['timestamp', 'provider', 'region', 'datacenter', 'latency_ms'])
        
        try:
            tables = driver.find_elements(By.TAG_NAME, "table")
            logger.info(f"{len(tables)} tablas encontradas")
            for table_idx, table in enumerate(tables):
                data_rows = table.find_elements(By.TAG_NAME, "tr")[1:]  # Skip header
                logger.info(f"Tabla {table_idx+1}: {len(data_rows)} filas")
                for row in data_rows:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) < 4: continue
                    row_num = cells[0].text.strip()
                    region_name = cells[1].text.strip()
                    region_code = cells[2].text.strip().lower()
                    mean_raw = cells[3].text.strip()
                    mean = extract_mean(mean_raw)
                    
                    if not (re.match(r'^\d+$', row_num) and region_name and region_code and mean):
                        continue
                    if not (0 < float(mean) < 2000):
                        continue
                    
                    datacenter = get_datacenter(region_name)
                    w.writerow([timestamp, 'cloudpingtest GCP', region_code, datacenter, mean])
                    logger.info(f"{datacenter} ({region_code}): {mean}ms")
                    rows += 1
        except Exception as e:
            logger.error(f"ERROR AL EXTRAER: {e}")
            traceback.print_exc()
    return rows

# ===================== UNA CAPTURA =====================
def capturar_una_vez():
    driver = None
    try:
        driver = setup_driver()
        wait = WebDriverWait(driver, 60)
        logger.info("CARGANDO CLOUDPINGTEST.COM/GCP...")

        # Carga con reintentos
        for _ in range(5):
            try:
                driver.get(URL)
                wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
                time.sleep(5)
                break
            except:
                time.sleep(10)
        else:
            raise Exception("NO CARGA LA PÁGINA")

        # Click Start o auto
        if not click_start(driver, wait):
            logger.info("SIN BOTÓN → Esperando auto-start 30s")
            time.sleep(30)

        # Espera datos
        esperar_datos(driver)

        # Guardar
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        filas = guardar_datos(driver, timestamp)
        logger.info(f"¡{filas} REGIONES GCP GUARDADAS!")
        return filas > 0

    except Exception as e:
        logger.error(f"ERROR TOTAL: {e}")
        traceback.print_exc()
        if driver:
            guardar_screenshot(driver, "FALLO_GCP")
        return False
    finally:
        if driver:
            try: driver.quit()
            except: pass
        matar_chrome()

# ===================== BUCLE 24/7 =====================
def main():
    logger.info("SCRAPER CLOUDPINGTEST GCP INICIADO")
    logger.info(f"Cada {INTERVALO_MINUTOS} min → {OUTPUT_CSV}")
    ciclo = 0
    while True:
        ciclo += 1
        logger.info(f"\nITERACIÓN {ciclo} - {datetime.datetime.now().strftime('%H:%M')}")
        exito = False
        for intento in range(1, MAX_REINTENTOS + 1):
            if capturar_una_vez():
                exito = True
                break
            logger.warning(f"Intento {intento}/{MAX_REINTENTOS} falló → 60s")
            time.sleep(60)
        if not exito:
            logger.error("CICLO FALLIDO")
        logger.info(f"Durmiendo {INTERVALO_MINUTOS} min...")
        time.sleep(INTERVALO_MINUTOS * 60)

if __name__ == "__main__":
    main()