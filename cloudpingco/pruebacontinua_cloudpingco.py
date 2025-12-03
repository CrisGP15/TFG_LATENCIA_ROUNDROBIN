#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SCRAPER CONTINUO CLOUDPING.CO - MATRIZ AWS
- Cada 10 min → latencias entre TODAS las regiones AWS
- CSV limpio: from → to → ms
- Robusto: reintentos, logs, screenshots solo error
- Headless + UA rotativos + cierre limpio
"""
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
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
import gc

# ===================== CONFIG =====================
URL = "https://www.cloudping.co/"
INTERVALO_MINUTOS = 10
MAX_REINTENTOS = 3
OUTPUT_CSV = "cloudpingco_latency_longterm.csv"
LOG_FILE = "cloudping_co.log"
SCREENSHOT_DIR = "cloudping_errores"
HEADLESS = True
DISABLE_IMAGES = True                   
DISABLE_JS_IF_POSSIBLE = False
os.makedirs(SCREENSHOT_DIR, exist_ok=True)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[logging.FileHandler(LOG_FILE, encoding='utf-8'), logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger()

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
]

# ===================== CIERRE LIMPIO =====================
def signal_handler(sig, frame):
    logger.info("Cerrando scraper limpiamente...")
    sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ===================== DRIVER =====================
def setup_driver():
    chrome_options = Options()
    
    # HEADLESS + MÁXIMA OPTIMIZACIÓN DE RAM
    chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')        # ¡CRUCIAL!
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('--disable-plugins')
    chrome_options.add_argument('--disable-images')               # ← Quita imágenes = -300 MB
    chrome_options.add_argument('--disable-javascript')           # ← Cámbialo a True si funciona sin JS
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_argument('--disable-features=VizDisplayCompositor')
    
    # Evita que Chrome se coma RAM en segundo plano
    chrome_options.add_argument('--disable-background-timer-throttling')
    chrome_options.add_argument('--disable-renderer-backgrounding')
    chrome_options.add_argument('--disable-backgrounding-occluded-windows')
    chrome_options.add_argument('--disable-features=ImprovedCookieControls,LazyFrameLoading,GlobalMediaControls,DestroyProfileOnBrowserClose')
    chrome_options.add_argument('--memory-pressure-off')
    chrome_options.add_argument('--max_old_space_size=4096')

    # Anti-detección
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    # User-agent rotativo
    ua = random.choice(USER_AGENTS)
    chrome_options.add_argument(f'--user-agent={ua}')

    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(180)
    
    # Anti-detección extra
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            Object.defineProperty(navigator, 'languages', {get: () => ['es-ES', 'es']});
        """
    })
    
    return driver

def guardar_screenshot(driver, prefijo):
    path = os.path.join(SCREENSHOT_DIR, f"{prefijo}_{int(time.time())}.png")
    driver.save_screenshot(path)
    logger.info(f"{path}")

# ===================== EXTRACCIÓN =====================
def extract_latency_value(text):
    if not text: return None
    m = re.search(r'(\d+\.?\d*)\s*ms', text)
    return m.group(1) if m else None

def guardar_matriz(driver, timestamp):
    rows = 0
    file_exists = os.path.exists(OUTPUT_CSV)
    with open(OUTPUT_CSV, 'a', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow(['timestamp', 'provider', 'from_region', 'to_region', 'latency_ms'])

        try:
            wait = WebDriverWait(driver, 30)
            table = wait.until(EC.presence_of_element_located((By.XPATH, "//table")))
            header_row = table.find_element(By.XPATH, ".//tr[1]")
            headers = header_row.find_elements(By.TAG_NAME, "th")
            if not headers:
                headers = header_row.find_elements(By.TAG_NAME, "td")
            to_regions = [h.text.strip() for h in headers[1:]]
            if not to_regions:
                logger.warning("No se encontraron cabeceras 'to_region'")
                return 0

            data_rows = table.find_elements(By.XPATH, ".//tr")[1:]
            logger.info(f"Procesando {len(data_rows)} filas × {len(to_regions)} columnas")

            for row in data_rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) < 2: continue
                from_region = cells[0].text.strip()
                latencies = cells[1:]

                for j, cell in enumerate(latencies):
                    if j >= len(to_regions): break
                    lat_text = cell.text.strip()
                    if not lat_text or lat_text in ['-', 'N/A', '']: continue
                    lat = extract_latency_value(lat_text)
                    if lat and float(lat) > 0:
                        to_region = to_regions[j]
                        w.writerow([timestamp, 'cloudping.co AWS', from_region, to_region, lat])
                        logger.info(f"{from_region} → {to_region}: {lat}ms")
                        rows += 1

        except Exception as e:
            logger.error(f"Error extrayendo tabla: {e}")
            traceback.print_exc()
    return rows

# ===================== UNA CAPTURA =====================
def capturar_una_vez():
    driver = None
    try:
        driver = setup_driver()
        wait = WebDriverWait(driver, 60)
        logger.info("Cargando cloudping.co...")
        
        for intento in range(5):
            try:
                driver.get(URL)
                break
            except:
                logger.warning(f"Carga fallida, intento {intento+1}/5")
                time.sleep(15)
        else:
            raise Exception("No se pudo cargar la página")

        # Esperar tabla
        table = wait.until(EC.presence_of_element_located((By.XPATH, "//table")))
        logger.info("Tabla detectada")

        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        filas = guardar_matriz(driver, timestamp)
        
        logger.info(f"Guardadas {filas} latencias")
        return filas > 0

    except Exception as e:
        logger.error(f"Error en captura: {e}")
        traceback.print_exc()
        if driver:
            guardar_screenshot(driver, "error_captura")
        return False
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass
            
            gc.collect()

# ===================== BUCLE 24/7 =====================
def main():
    logger.info("SCRAPER CLOUDPING.CO INICIADO")
    logger.info(f"Cada {INTERVALO_MINUTOS} min → {OUTPUT_CSV}")
    ciclo = 0
    while True:
        ciclo += 1
        logger.info(f"\nITERACIÓN {ciclo}")
        exito = False
        for intento in range(1, MAX_REINTENTOS + 1):
            if capturar_una_vez():
                exito = True
                break
            logger.warning(f"Intento {intento} falló → 60s")
            time.sleep(60)
        if not exito:
            logger.error("Ciclo fallido completamente")
        logger.info(f"Durmiendo {INTERVALO_MINUTOS} min...")
        time.sleep(INTERVALO_MINUTOS * 60)

if __name__ == "__main__":
    main()