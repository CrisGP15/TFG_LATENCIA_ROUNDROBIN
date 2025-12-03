#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SCRAPER CONTINUO CLOUDPING.NET - GCP GLOBAL 24/7
- Cada 10 min → GCP Ping → ~31 regiones
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
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
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
OUTPUT_CSV = "gcp_cloudpingnet_latency_longterm.csv"
LOG_FILE = "gcp_cloudping_net.log"
SCREENSHOT_DIR = "gcp_errores"
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
    logger.info("SCRAPER GCP DETENIDO POR USUARIO")
    sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ===================== DRIVER =====================
def setup_driver():
    chrome_options = Options()
    
    chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')        # ¡CRUCIAL!
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('--disable-plugins')
    chrome_options.add_argument('--disable-images')               # ← Quita mapa e iconos
    chrome_options.add_argument('--disable-javascript')           # ← Prueba True si funciona
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

def parse_gcp_line(text):
    # Ejemplos:
    # "Europe West 3  38.60 ms"
    # "US Central 2  116.60 ms"
    # "Asia South 1  Failed"
    # "Europe West 3 Run 2/3: 38.60 ms"
    text = text.strip()
    if 'Failed' in text:
        region = text.replace('Failed', '').strip()
        return region, region, 'Failed'
    
    match = re.match(r'^(.*?)(?:\s*Run \d/\d:\s*)?(\d+\.\d+)\s*ms$', text)
    if match:
        region = match.group(1).strip()
        latency = match.group(2)
        return region, region, latency
    return None, None, None

# ===================== CLIC + ESPERA =====================
def click_gcp_ping(driver, wait):
    try:
        btn = wait.until(EC.element_to_be_clickable(
            (By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'gcp ping')]")
        ))
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)
        time.sleep(1)
        ActionChains(driver).move_to_element(btn).click().perform()
        logger.info("GCP PING LANZADO")
        return True
    except Exception as e:
        logger.error(f"NO SE PUDO CLICAR GCP PING: {e}")
        return False

def esperar_datos(driver, wait, max_wait=300):
    start = time.time()
    logger.info("ESPERANDO ~31 REGIONES (máx 300s)...")
    while time.time() - start < max_wait:
        try:
            cells = driver.find_elements(By.XPATH, "//*[contains(text(), 'ms') or contains(text(), 'Failed')]")
            count = len([c for c in cells if any(x in c.text for x in ['ms', 'Failed'])])
            logger.info(f"{int(time.time()-start)}s → {count} latencias")
            if count >= 25:
                logger.info("¡DATOS SUFICIENTES!")
                time.sleep(6)
                return True
        except: pass
        time.sleep(3)
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
            elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'ms') or contains(text(), 'Failed')]")
            for elem in elements:
                line = elem.text.strip()
                if not line or any(h in line for h in ['North America', 'South America', 'Europe', 'Asia', 'Australia', 'Tested', 'Fastest', 'Slowest']):
                    continue
                try:
                    parent = elem.find_element(By.XPATH, "./..")
                    text = parent.text.strip()
                except:
                    text = line
                
                region, dc, lat = parse_gcp_line(text)
                if not region:
                    continue
                
                if lat == 'Failed':
                    w.writerow([timestamp, 'cloudping.net GCP', region, dc, 'Failed'])
                    logger.info(f"{dc}: Failed")
                    rows += 1
                else:
                    lat_num = float(lat)
                    if 1 <= lat_num <= 2000:
                        w.writerow([timestamp, 'cloudping.net GCP', region, dc, lat])
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
                time.sleep(5)
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

        # Seleccionar pestaña GCP
        try:
            gcp_tab = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'GCP')]")))
            driver.execute_script("arguments[0].click();", gcp_tab)
            logger.info("PESTAÑA GCP SELECCIONADA")
            time.sleep(10)
        except Exception as e:
            logger.error(f"NO SE PUDO SELECCIONAR GCP: {e}")
            raise

        # Clic GCP Ping
        if not click_gcp_ping(driver, wait):
            raise Exception("FALLO EL PING")

        # Espera datos
        esperar_datos(driver, wait)

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
            try:
                driver.quit()
            except:
                pass
            gc.collect()
# ===================== BUCLE 24/7 =====================
def main():
    logger.info("SCRAPER CLOUDPING.NET GCP INICIADO")
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