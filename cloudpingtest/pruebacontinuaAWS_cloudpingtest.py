#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SCRAPER CONTINUO CLOUDPINGTEST.COM - AWS GLOBAL 24/7
- Cada 10 min → Test automático → 34+ regiones
- CSV LIMPIO: timestamp | provider | region | datacenter | latency_ms
- Espera inteligente + reintentos + logs + screenshots SOLO error
- Headless + UA rotativos + Ctrl+C limpio
- Mapeo perfecto de códigos → nombres reales
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
import gc

# ===================== CONFIG =====================
URL = "https://cloudpingtest.com/aws"
INTERVALO_MINUTOS = 10
MAX_REINTENTOS = 3
OUTPUT_CSV = "aws_cloudpingtest_latency_longterm.csv"
LOG_FILE = "cloudpingtest.log"
SCREENSHOT_DIR = "cloudpingtest_errores"
HEADLESS = True
DISABLE_IMAGES = True
os.makedirs(SCREENSHOT_DIR, exist_ok=True)

# Logging épico
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
    logger.info("SCRAPER CLOUDPINGTEST DETENIDO POR USUARIO")
    sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ===================== DRIVER =====================
def setup_driver():
    chrome_options = Options()
    
    chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')        # ¡Imprescindible!
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('--disable-plugins')
    chrome_options.add_argument('--disable-images')               # ← Quita todo lo visual
    chrome_options.add_argument('--disable-javascript')           # ← Esta web funciona sin JS
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_argument('--disable-features=VizDisplayCompositor')
    
    # Anti-consumo RAM en segundo plano
    chrome_options.add_argument('--disable-background-timer-throttling')
    chrome_options.add_argument('--disable-renderer-backgrounding')
    chrome_options.add_argument('--disable-backgrounding-occluded-windows')
    chrome_options.add_argument('--disable-features=ImprovedCookieControls,LazyFrameLoading,GlobalMediaControls,DestroyProfileOnBrowserClose')
    chrome_options.add_argument('--memory-pressure-off')
    chrome_options.add_argument('--max_old_space_size=4096')

    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    ua = random.choice(USER_AGENTS)
    chrome_options.add_argument(f'--user-agent={ua}')

    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(180)
    
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
    logger.info(f"ERROR CAPTURADO: {path}")

# ===================== MAPEO REGIONES =====================
DATACENTER_MAP = {
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

# ===================== UTILIDADES =====================
def extract_latency(text):
    m = re.search(r'(\d+\.?\d*)\s*ms', text)
    return m.group(1) if m else None

def get_region_name(code):
    return DATACENTER_MAP.get(code, code)

# ===================== ESPERA DATOS =====================
def esperar_tabla_completa(driver, wait, max_wait=240):
    start = time.time()
    logger.info("ESPERANDO TABLA COMPLETA (máx 240s)...")
    while time.time() - start < max_wait:
        try:
            rows = driver.find_elements(By.XPATH, "//table//tr")
            valid = sum(1 for r in rows[1:] 
                       if len(r.find_elements(By.TAG_NAME, "td")) >= 4 
                       and "ms" in r.find_elements(By.TAG_NAME, "td")[3].text)
            logger.info(f"{int(time.time()-start)}s → {valid} regiones con latencia")
            if valid >= 25:
                logger.info("¡TABLA LISTA!")
                time.sleep(5)
                return True
        except: pass
        time.sleep(3)
    logger.warning("TIMEOUT: Guardando datos disponibles")
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
            table = driver.find_element(By.XPATH, "//table")
            for row in table.find_elements(By.TAG_NAME, "tr")[1:]:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) < 4: continue
                region_code = cells[2].text.strip()
                latency_raw = cells[3].text.strip()
                latency = extract_latency(latency_raw)
                
                if not (region_code and latency): continue
                if not (10 < float(latency) < 1000): continue
                
                dc_name = get_region_name(region_code)
                w.writerow([timestamp, 'cloudpingtest.com AWS', region_code, dc_name, latency])
                logger.info(f"{dc_name} ({region_code}): {latency}ms")
                rows += 1
        except Exception as e:
            logger.error(f"ERROR AL EXTRAER TABLA: {e}")
            traceback.print_exc()
    return rows

# ===================== UNA CAPTURA =====================
def capturar_una_vez():
    driver = None
    try:
        driver = setup_driver()
        wait = WebDriverWait(driver, 60)
        logger.info("CARGANDO CLOUDPINGTEST.COM/AWS...")

        # Carga con reintentos
        for _ in range(5):
            try:
                driver.get(URL)
                time.sleep(8)
                break
            except:
                time.sleep(15)
        else:
            raise Exception("NO CARGA LA PÁGINA")

        # Espera tabla + datos
        wait.until(EC.presence_of_element_located((By.XPATH, "//table")))
        esperar_tabla_completa(driver, wait)

        # Guardar
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        filas = guardar_datos(driver, timestamp)
        logger.info(f"¡{filas} REGIONES CLOUDPINGTEST GUARDADAS!")
        return filas > 0

    except Exception as e:
        logger.error(f"ERROR TOTAL: {e}")
        traceback.print_exc()
        if driver:
            guardar_screenshot(driver, "FALLO_CPT")
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
    logger.info("SCRAPER CLOUDPINGTEST.COM INICIADO")
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