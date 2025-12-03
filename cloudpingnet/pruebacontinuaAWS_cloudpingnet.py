#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SCRAPER CONTINUO CLOUDPING.NET - AWS GLOBAL 24/7
- Cada 10 min → AWS Ping → ~34 regiones
- CSV LIMPIO: timestamp | provider | region | datacenter | latency_ms
- Clic automático + espera inteligente
- Headless + UA rotativos + logs + screenshots SOLO error
- Cierre limpio con Ctrl+C
"""
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
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
URL = "https://cloudping.net/"
INTERVALO_MINUTOS = 10
MAX_REINTENTOS = 3
OUTPUT_CSV = "aws_cloudpingnet_latency_longterm.csv"
LOG_FILE = "cloudping_net.log"
SCREENSHOT_DIR = "cloudping_net_errores"
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
    
    chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')        # ¡Imprescindible!
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('--disable-plugins')
    chrome_options.add_argument('--disable-images')               # ← Quita el mapa pesado
    chrome_options.add_argument('--disable-javascript')           # ← Prueba True si la tabla carga igual
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_argument('--disable-features=VizDisplayCompositor')
    
    # Evita consumo RAM en segundo plano
    chrome_options.add_argument('--disable-background-timer-throttling')
    chrome_options.add_argument('--disable-renderer-backgrounding')
    chrome_options.add_argument('--disable-backgrounding-occluded-windows')
    chrome_options.add_argument('--disable-features=ImprovedCookieControls,LazyFrameLoading,GlobalMediaControls,DestroyProfileOnBrowserClose')
    chrome_options.add_argument('--memory-pressure-off')
    chrome_options.add_argument('--max_old_space_size=4096')

    # Anti-detección
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

# ===================== UTILIDADES =====================
def extract_latency_value(text):
    if not text: return None
    m = re.search(r'(\d+\.?\d*)\s*ms', text, re.I)
    return m.group(1) if m else None

def parse_region_line(text):
    # Formato: "us-east-1 (N. Virginia) 138.60 ms"
    match = re.match(r'([a-z]+-[a-z]+-\d+)\s*\((.*?)\)\s*(\d+\.\d+\s*ms)', text, re.I)
    if match:
        return match.group(1), match.group(2), match.group(3)
    # Alternativo: "us-east-1 - N. Virginia: 138.60ms"
    match = re.match(r'([a-z]+-[a-z]+-\d+)\s*[-:]\s*(.*?)\s*[:\-]?\s*(\d+\.\d+\s*ms)', text, re.I)
    if match:
        return match.group(1), match.group(2), match.group(3)
    return None, None, None

# ===================== CLIC + ESPERA =====================
def click_aws_ping(driver, wait):
    try:
        btn = wait.until(EC.element_to_be_clickable(
            (By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'aws ping')]")
        ))
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)
        time.sleep(1)
        ActionChains(driver).move_to_element(btn).click().perform()
        logger.info("AWS PING LANZADO")
        return True
    except Exception as e:
        logger.error(f"NO SE PUDO CLICAR AWS PING: {e}")
        return False

def esperar_datos(driver, wait, max_wait=240):
    start = time.time()
    logger.info("ESPERANDO ~34 REGIONES (máx 240s)...")
    while time.time() - start < max_wait:
        try:
            cells = driver.find_elements(By.XPATH, "//*[contains(text(), 'ms')]")
            count = len([c for c in cells if 'ms' in c.text.lower()])
            logger.info(f"{int(time.time()-start)}s → {count} latencias")
            if count >= 30:
                logger.info("¡DATOS COMPLETOS!")
                time.sleep(5)
                return True
        except: pass
        time.sleep(3)
    logger.warning("TIMEOUT: Guardando lo disponible")
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
            # Buscar todos los bloques de región
            regions = driver.find_elements(By.XPATH, "//div[contains(@class, 'region') or contains(@class, 'aws')]//parent::*")
            if not regions:
                regions = driver.find_elements(By.XPATH, "//*[contains(text(), 'ms')]//ancestor::*[contains(@class, 'region') or contains(text(), '(')]")

            for elem in regions:
                text = elem.text.strip()
                if not text or 'ms' not in text.lower(): continue

                region, dc, lat_raw = parse_region_line(text)
                if not region: continue

                lat = extract_latency_value(lat_raw)
                if lat and float(lat) > 10:
                    w.writerow([timestamp, 'cloudping.net AWS', region, dc, lat])
                    logger.info(f"{dc}: {lat}ms")
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
        logger.info("CARGANDO CLOUDPING.NET...")

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

        # Clic AWS Ping
        if not click_aws_ping(driver, wait):
            raise Exception("FALLO EL PING")

        # Espera datos
        esperar_datos(driver, wait)

        # Guardar
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        filas = guardar_datos(driver, timestamp)
        logger.info(f"¡{filas} REGIONES GUARDADAS!")
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
    logger.info("SCRAPER CLOUDPING.NET INICIADO")
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