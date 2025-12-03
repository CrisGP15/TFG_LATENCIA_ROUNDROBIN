#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SCRAPER CONTINUO CLOUDPING.INFO - MULTI-PROVEEDOR 24/7
- Cada 10 min → HTTP Ping → +100 latencias (AWS, GCP, Azure, Oracle, Huawei...)
- CSV LIMPIO: timestamp | provider | region | datacenter | latency_ms
- Espera inteligente + retry automático del ping
- Headless + UA rotativos + logs + screenshots SOLO error
- Cierre limpio con Ctrl+C
"""
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
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
URL = "https://www.cloudping.info/"
INTERVALO_MINUTOS = 10
MAX_REINTENTOS = 3
OUTPUT_CSV = "cloudpinginfo_latency_longterm.csv"
LOG_FILE = "cloudping_info.log"
SCREENSHOT_DIR = "cloudping_info_errores"
HEADLESS = True
DISABLE_IMAGES = True                 
DISABLE_JS_IF_POSSIBLE = False
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
    logger.info("SCRAPER DETENIDO POR USUARIO")
    sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ===================== DRIVER =====================
def setup_driver():
    chrome_options = Options()
    
    # HEADLESS + OPTIMIZACIÓN MÁXIMA DE RAM
    chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')        # ¡¡¡CRUCIAL EN ESTE SITIO!!!
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('--disable-plugins')
    chrome_options.add_argument('--disable-images')               # ← Quita todas las imágenes
    chrome_options.add_argument('--disable-javascript')           # ← Prueba True si funciona (ahorra 500+ MB)
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
    logger.info(f"ERROR CAPTURADO: {path}")

# ===================== UTILIDADES =====================
def extract_latency_value(text):
    if not text: return None
    m = re.search(r'(\d+\.?\d*)\s*ms', text, re.I)
    return m.group(1) if m else None

def parse_region(text):
    if not text:
        return text.strip(), text.strip()

    # 1. Eliminamos TODOS los textos políticos que empiecen por "Doing business with"
    cleaned = re.sub(r'\s*\(?Doing business with[^()]*\)?', '', text, flags=re.IGNORECASE)

    # 2. Eliminamos paréntesis vacíos que puedan quedar () o (   )
    cleaned = re.sub(r'\(\s*\)', '', cleaned)
    cleaned = re.sub(r'\s{2,}', ' ', cleaned).strip()

    # 3. Ahora separamos región código + nombre real entre paréntesis
    # Ejemplos:
    #   us-east-1 → "us-east-1"
    #   eu-west-1 (Ireland) → region="eu-west-1", dc="Ireland"
    #   il-central-1 (Tel Aviv) → region="il-central-1", dc="Tel Aviv"
    match = re.match(r'^([^\(]+?)\s*(?:\(([^()]+)\))?\s*$', cleaned.strip())

    if not match:
        region = cleaned.strip()
        return region, region

    region_code = match.group(1).strip()
    location = match.group(2)
    if location:
        location = location.strip()
        # Casos especiales donde el nombre real está fuera pero queremos usarlo como datacenter
        if location.lower() in ['global', 'global external https load balancer']:
            return region_code, location
        return region_code, location
    else:
        return region_code, region_code

# ===================== CLIC + ESPERA MÁGICA =====================
def click_http_ping(driver, wait):
    try:
        btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'HTTP Ping')]")))
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)
        time.sleep(1.5)
        btn.click()
        logger.info("HTTP PING LANZADO")
        return True
    except Exception as e:
        logger.error(f"NO SE PUDO CLICAR HTTP PING: {e}")
        return False

def esperar_datos_magicos(driver, wait, max_wait=500):
    start = time.time()
    retry_count = 0
    logger.info("ESPERANDO DATOS ESTABLES (máx 500s)...")

    while time.time() - start < max_wait:
        try:
            lat_cells = driver.find_elements(By.XPATH, "//td[contains(text(), 'ms')]")
            pend_cells = driver.find_elements(By.XPATH, "//td[contains(text(), 'pinging') or contains(text(), 'connecting')]")
            lat = len(lat_cells)
            pend = len(pend_cells)

            elapsed = int(time.time() - start)
            logger.info(f"{elapsed}s → {lat} latencias | {pend} pendientes")

            if lat >= 100 and pend < 10:
                logger.info("¡DATOS ESTABLES! Extrayendo...")
                time.sleep(6)
                return True

            # Reintento si está atascado
            if elapsed > 120 and pend > 15 and retry_count < 3:
                logger.warning(f"ATASCADO → REINTENTANDO PING ({retry_count + 1}/3)")
                if click_http_ping(driver, wait):
                    retry_count += 1
                    time.sleep(35)

            time.sleep(3)
        except:
            time.sleep(2)

    logger.warning("TIMEOUT: Guardando lo que haya")
    return False

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
            current_provider = None

            for row in table.find_elements(By.TAG_NAME, "tr"):
                cells = row.find_elements(By.TAG_NAME, "td")
                texts = [c.text.strip() for c in cells]

                # Proveedor
                if len(cells) == 1 and texts[0]:
                    current_provider = texts[0].replace('™', '').strip()
                    logger.info(f"PROVEEDOR: {current_provider}")
                    continue

                # Datos
                if len(cells) == 2:
                    region_text, lat_text = texts
                    if lat_text in ['pinging', 'connecting', 'unavailable', 'timeout', '']:
                        continue

                    region, dc = parse_region(region_text)
                    lat = extract_latency_value(lat_text)
                    if lat and float(lat) > 0:
                        w.writerow([timestamp, f"cloudping.info {current_provider}", region, dc, lat])
                        logger.info(f"{current_provider} → {dc}: {lat}ms")
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
        logger.info("CARGANDO CLOUDPING.INFO...")

        # Carga con reintentos
        for _ in range(5):
            try:
                driver.get(URL)
                break
            except:
                time.sleep(15)
        else:
            raise Exception("NO CARGA LA PÁGINA")

        # Consentimiento
        try:
            btn = driver.find_element(By.XPATH, "//button[contains(text(), 'Accept') or contains(text(), 'Agree')]")
            btn.click()
            time.sleep(2)
        except: pass

        # Clic HTTP Ping
        if not click_http_ping(driver, wait):
            raise Exception("FALLO EL PING")

        # Espera mágica
        esperar_datos_magicos(driver, wait)

        # Guardar
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        filas = guardar_datos(driver, timestamp)
        logger.info(f"¡{filas} FILAS GUARDADAS!")
        return filas > 0

    except Exception as e:
        logger.error(f"ERROR TOTAL: {e}")
        traceback.print_exc()
        if driver:
            guardar_screenshot(driver, "FALLO_TOTAL")
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
    logger.info("SCRAPER CLOUDPING.INFO INICIADO")
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