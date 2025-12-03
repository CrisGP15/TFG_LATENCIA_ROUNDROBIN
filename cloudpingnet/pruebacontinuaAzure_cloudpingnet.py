#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SCRAPER CONTINUO CLOUDPING.NET - AZURE GLOBAL 24/7
- Cada 10 min → Azure Ping → ~41 regiones
- CSV LIMPIO: timestamp | provider | region | datacenter | latency_ms
- Clic automático + espera inteligente (300s máx)
- Headless + UA rotativos + logs + screenshots SOLO error
- Cierre limpio con Ctrl+C | Reintentos automáticos
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import *
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
OUTPUT_CSV = "azure_cloudpingnet_latency_longterm.csv"
LOG_FILE = "azure_cloudping_net.log"
SCREENSHOT_DIR = "azure_cloudping_errores"
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
    logger.info("SCRAPER AZURE DETENIDO POR USUARIO")
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
def extract_latency_value(latency_text):
    if not latency_text:
        return None
    match = re.search(r'(\d+\.?\d*)\s*ms', latency_text, re.I)
    return match.group(1) if match else None

def parse_azure_line(text):
    text = text.strip()
    if text.endswith('Failed'):
        region_name = text.replace('Failed', '').strip()
        return region_name, region_name, 'Failed'
    match = re.match(r'(.+?)\s+(\d+\.\d+\s*ms)$', text)
    if match:
        return match.group(1).strip(), match.group(1).strip(), match.group(2)
    match = re.match(r'(.+?)(?:\s*(?:Run \d/\d:)?\s*)(\d+\.\d+\s*ms)$', text)
    if match:
        return match.group(1).strip(), match.group(1).strip(), match.group(2)
    match = re.match(r'(.+?)(\d+\.\d+ms)$', text)
    if match:
        return match.group(1).strip(), match.group(1).strip(), match.group(2) + ' ms'
    return None, None, None

# ===================== NAVEGACIÓN AZURE =====================
def click_azure_tab(driver, wait):
    max_retries = 5
    for attempt in range(max_retries):
        try:
            azure_tab = wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'azure')]")
            ))
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", azure_tab)
            time.sleep(1)
            ActionChains(driver).move_to_element(azure_tab).click().perform()
            logger.info("PESTAÑA AZURE SELECCIONADA")
            time.sleep(8)
            return True
        except:
            logger.warning(f"Reintento pestaña Azure {attempt+1}/{max_retries}")
            time.sleep(3)
    return False

def click_azure_ping(driver, wait):
    try:
        btn = wait.until(EC.element_to_be_clickable(
            (By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'azure ping')]")
        ))
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)
        time.sleep(1)
        ActionChains(driver).move_to_element(btn).click().perform()
        logger.info("AZURE PING LANZADO")
        time.sleep(3)
        return True
    except Exception as e:
        logger.error(f"NO SE PUDO CLICAR AZURE PING: {e}")
        return False

def esperar_datos_azure(driver, max_wait=300):
    start = time.time()
    logger.info("ESPERANDO ~41 REGIONES AZURE...")
    while time.time() - start < max_wait:
        try:
            elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'ms') or contains(text(), 'Failed')]")
            count = 0
            for elem in elements:
                t = elem.text.strip()
                if len(t) > 5 and ('ms' in t or 'Failed' in t):
                    count += 1
            elapsed = int(time.time() - start)
            logger.info(f"{elapsed}s → {count}/41 regiones")
            if count >= 35:
                logger.info("¡DATOS COMPLETOS!")
                time.sleep(5)
                return True
        except:
            pass
        time.sleep(5)
    logger.warning("TIMEOUT 300s → Guardando lo disponible")
    return True

# ===================== GUARDAR DATOS =====================
def guardar_datos_azure(driver, timestamp):
    rows = 0
    file_exists = os.path.exists(OUTPUT_CSV)
    with open(OUTPUT_CSV, 'a', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow(['timestamp', 'provider', 'region', 'datacenter', 'latency_ms'])

        elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'ms') or contains(text(), 'Failed')]")
        seen_regions = set()

        for elem in elements:
            try:
                parent = elem.find_element(By.XPATH, "./..")
                full_text = parent.text.strip()

                # FILTRAR BASURA
                blacklist = ['Progress', 'fastest', 'slowest', 'CloudPing', 'Tested', 'regions tested',
                            'North America', 'South America', 'Europe', 'Asia', 'Australia', 'Africa', 'Middle East',
                            'Run ', 'runs:', 'ms)', 'AWS', 'GCP', 'DNS']
                if any(bad in full_text for bad in blacklist) or len(full_text) > 200:
                    continue

                region, dc, lat_raw = parse_azure_line(full_text)
                if not region or region in seen_regions:
                    continue

                lat_clean = 'Failed' if lat_raw == 'Failed' else extract_latency_value(lat_raw)
                if not lat_clean:
                    continue

                w.writerow([timestamp, 'cloudping.net Azure', region.strip(), dc.strip(), lat_clean])
                status = "FAILED" if lat_clean == 'Failed' else f"{lat_clean}ms"
                logger.info(f"{region[:25]:25} → {status}")
                seen_regions.add(region)
                rows += 1

            except:
                continue

    logger.info(f"GUARDADAS {rows} REGIONES LIMPIAS")
    return rows

# ===================== UNA CAPTURA COMPLETA =====================
def capturar_azure_una_vez():
    driver = None
    try:
        driver = setup_driver()
        wait = WebDriverWait(driver, 60)
        logger.info("CARGANDO CLOUDPING.NET...")

        for _ in range(5):
            try:
                driver.get(URL)
                time.sleep(8)
                break
            except:
                time.sleep(10)
        else:
            raise Exception("NO CARGA CLOUDPING.NET")

        # Consentimiento
        try:
            driver.find_element(By.XPATH, "//button[contains(text(), 'Accept') or contains(text(), 'Agree')]").click()
            time.sleep(2)
        except:
            pass

        if 'captcha' in driver.page_source.lower():
            logger.warning("CAPTCHA DETECTADO → PAUSA MANUAL 120s")
            time.sleep(120)

        if not click_azure_tab(driver, wait):
            raise Exception("FALLÓ SELECCIÓN AZURE")
        if not click_azure_ping(driver, wait):
            raise Exception("FALLÓ AZURE PING")

        esperar_datos_azure(driver)
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        filas = guardar_datos_azure(driver, timestamp)
        logger.info(f"¡{filas} REGIONES AZURE GUARDADAS!")
        return filas > 0

    except Exception as e:
        logger.error(f"ERROR TOTAL AZURE: {e}")
        traceback.print_exc()
        if driver:
            guardar_screenshot(driver, "FALLO_TOTAL_AZURE")
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
    logger.info("=== SCRAPER AZURE CLOUDPING.NET 24/7 INICIADO ===")
    logger.info(f"Cada {INTERVALO_MINUTOS}min → {OUTPUT_CSV}")
    ciclo = 0
    while True:
        ciclo += 1
        logger.info(f"\nITERACIÓN {ciclo:04d} → {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        exito = False
        for intento in range(1, MAX_REINTENTOS + 1):
            logger.info(f" Intento {intento}/{MAX_REINTENTOS}")
            if capturar_azure_una_vez():
                exito = True
                break
            logger.warning(f" Intento {intento} falló → Esperando 90s...")
            time.sleep(90)
        logger.info("CICLO EXITOSO" if exito else "CICLO FALLIDO")
        logger.info(f"Durmiendo {INTERVALO_MINUTOS} minutos...")
        time.sleep(INTERVALO_MINUTOS * 60)

if __name__ == "__main__":
    main()