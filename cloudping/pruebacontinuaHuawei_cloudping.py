#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SCRAPER CONTINUO HUAWEI CLOUDPING - VERSIÓN 24/7
- Cada 10 min → 2 HTTP Ping (ambos botones)
- ~30 regiones → ~60 latencias por ciclo
- Robusto: reintentos, logs, screenshots solo errores
- Headless + UA rotativos + cierre limpio
"""
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
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
import requests
import gc

# ===================== CONFIG =====================
URL = "https://www.cloudping.cloud/huawei"
INTERVALO_MINUTOS = 10
MAX_REINTENTOS = 3
OUTPUT_CSV = "huawei_cloudping_latency_longterm.csv"
LOG_FILE = "huawei_scraper_continuo.log"
SCREENSHOT_DIR = "huawei_errores"
HEADLESS = True
DISABLE_IMAGES = True 
DISABLE_JS_IF_POSSIBLE = False
os.makedirs(SCREENSHOT_DIR, exist_ok=True)

# Logging bonito
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

# ===================== UTILIDADES =====================
def check_website_accessibility(url):
    try:
        r = requests.get(url, timeout=8)
        return r.status_code == 200
    except:
        return False

def setup_driver():
    chrome_options = Options()
    
    # HEADLESS obligatorio
    chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')        # Crucial en Linux/WSL
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('--disable-plugins')
    chrome_options.add_argument('--disable-images')               # ← Ahorra 200+ MB
    chrome_options.add_argument('--disable-javascript')           # ← Cambia a True si la web funciona sin JS
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_argument('--disable-features=VizDisplayCompositor')
    
    # Anti-detección (ya lo tenías)
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    # User-agent rotativo
    ua = random.choice(USER_AGENTS)
    chrome_options.add_argument(f'--user-agent={ua}')

    # CLAVE PARA 10+ PROCESOS EN PARALELO (evita que Chrome se coma la RAM)
    chrome_options.add_argument('--disable-background-timer-throttling')
    chrome_options.add_argument('--disable-renderer-backgrounding')
    chrome_options.add_argument('--disable-backgrounding-occluded-windows')
    chrome_options.add_argument('--disable-features=ImprovedCookieControls,LazyFrameLoading,GlobalMediaControls,DestroyProfileOnBrowserClose')
    chrome_options.add_argument('--memory-pressure-off')
    chrome_options.add_argument('--max_old_space_size=4096')

    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(60)
    
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
    ts = int(time.time())
    path = os.path.join(SCREENSHOT_DIR, f"{prefijo}_{ts}.png")
    driver.save_screenshot(path)
    logger.info(f"{path}")

def extract_region_code(region_text):
    patterns = [r'([a-z]{2}-[a-z]+-\d+)', r'([a-z]{2}-[a-z]+\d-\d+)']
    for p in patterns:
        m = re.search(p, region_text, re.I)
        if m: return m.group(1)
    return None

def extract_latency_value(txt):
    if not txt: return None
    m = re.search(r'(\d+\.?\d*)', txt)
    return m.group(1) if m else None

def get_datacenter_name(region):
    if not region: return "Unknown"
    mapa = {
        'ap-southeast-1': 'Hong Kong, China',
        'ap-southeast-2': 'Bangkok, Thailand',
        'ap-southeast-3': 'Singapore',
        'na-mexico-1': 'Mexico City 1, Mexico',
        'la-north-2': 'Mexico City 2, Mexico',
        'la-south-2': 'Santiago, Chile',
        'sa-brazil-1': 'Sao Paulo, Brazil',
        'af-south-1': 'Johannesburg, South Africa',
        'cn-north-1': 'Beijing 1',
        'cn-north-4': 'Beijing 4',
        'cn-north-9': 'Wulanchabu',
        'cn-south-1': 'Guangzhou',
        'cn-southwest-2': 'Guiyang 1',
        'cn-east-3': 'Shanghai 1',
        'cn-east-2': 'Shanghai 2'
    }
    return mapa.get(region.lower(), region)

# ===================== TU LÓGICA ORIGINAL =====================
def find_http_ping_buttons(driver):
    selectors = [
        "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'http ping')]",
        "//button[contains(text(), 'HTTP Ping')]",
        "//button[contains(text(), 'HTTP') and contains(text(), 'Ping')]",
    ]
    buttons = []
    for sel in selectors:
        try:
            buttons.extend(driver.find_elements(By.XPATH, sel))
        except: pass
    # únicos por posición
    uniq = []
    seen = set()
    for b in buttons:
        try:
            if b.is_displayed() and b.is_enabled():
                loc = (b.location['x'], b.location['y'])
                if loc not in seen:
                    seen.add(loc)
                    uniq.append(b)
        except: pass
    logger.info(f"Encontrados {len(uniq)} botones HTTP Ping")
    return uniq

def click_and_wait(driver, wait, btn_idx, buttons, ping_name):
    if btn_idx >= len(buttons):
        logger.error(f"No existe botón {btn_idx+1}")
        return 0
    btn = buttons[btn_idx]
    logger.info(f"Clic en {ping_name}...")
    try:
        driver.execute_script("arguments[0].scrollIntoView(true);", btn)
        time.sleep(0.8)
        ActionChains(driver).move_to_element(btn).click().perform()
        logger.info(f"{ping_name} clicado")
        time.sleep(1.5)
    except Exception as e:
        logger.error(f"Error clic {ping_name}: {e}")
        return 0

    # espera datos
    start = time.time()
    while time.time() - start < 50:
        cells = driver.find_elements(By.XPATH, "//td[contains(text(), '.') or contains(text(), 'ms')]")
        if len(cells) >= 8:
            logger.info(f"Datos {ping_name}: {len(cells)} celdas")
            return len(cells)
        time.sleep(1)
    logger.warning(f"Timeout datos {ping_name}")
    return 0

def extraer_y_guardar(driver, ping_type, timestamp):
    rows = 0
    file_exists = os.path.exists(OUTPUT_CSV)
    with open(OUTPUT_CSV, 'a', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow(['timestamp', 'provider', 'ping_type', 'region', 'datacenter', 'latency_ms'])
        tables = driver.find_elements(By.TAG_NAME, "table")
        for tbl in tables:
            for row in tbl.find_elements(By.TAG_NAME, "tr")[1:]:
                try:
                    cells = row.find_elements(By.XPATH, ".//td")
                    if len(cells) < 2: continue
                    region_txt = cells[0].text.strip()
                    lat_cell = cells[1]
                    lat_txt = lat_cell.text.strip()

                    # busca en spans o atributos
                    if not any(c.isdigit() for c in lat_txt):
                        for sp in lat_cell.find_elements(By.TAG_NAME, "span"):
                            if any(c.isdigit() for c in sp.text):
                                lat_txt = sp.text.strip(); break
                        else:
                            lat_txt = (lat_cell.get_attribute('data-value') or
                                     lat_cell.get_attribute('data-latency') or
                                     lat_txt)

                    region_code = extract_region_code(region_txt)
                    dc_name = get_datacenter_name(region_code) if region_code else region_txt
                    lat = extract_latency_value(lat_txt)
                    if lat and any(k in region_txt.lower() for k in ['ap-','na-','la-','sa-','af-','cn-']):
                        w.writerow([timestamp, 'cloudping Huawei', ping_type,
                                   region_code or region_txt, dc_name, lat])
                        logger.info(f"{ping_type} → {dc_name}: {lat}ms")
                        rows += 1
                except: continue
    return rows

# ===================== CAPTURA UNA VEZ =====================
def capturar_una_vez():
    driver = None
    try:
        if not check_website_accessibility(URL):
            logger.error("Sitio no accesible")
            return False

        driver = setup_driver()
        wait = WebDriverWait(driver, 15)
        driver.get(URL)
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logger.info("Página cargada")

        buttons = find_http_ping_buttons(driver)
        if len(buttons) < 2:
            logger.error(f"Solo {len(buttons)} botones → refresh")
            driver.refresh()
            time.sleep(5)
            buttons = find_http_ping_buttons(driver)
            if len(buttons) < 2:
                raise Exception("No hay 2 botones")

        total_rows = 0
        for idx, name in enumerate(["HTTP_Ping_1", "HTTP_Ping_2"]):
            logger.info(f"\n--- {name} ---")
            click_and_wait(driver, wait, idx, buttons, name)
            rows = extraer_y_guardar(driver, name, timestamp)
            total_rows += rows
            if idx == 0:
                time.sleep(3)  # pausa entre pings

        logger.info(f"Guardadas {total_rows} filas")
        return total_rows > 0

    except Exception as e:
        logger.error(f"Error captura: {e}")
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
            # LIBERAR MEMORIA RÁPIDO → CRUCIAL EN BUCLES LARGOS
            gc.collect()

# ===================== BUCLE 24/7 =====================
def main():
    logger.info("SCRAPER CONTINUO HUAWEI INICIADO")
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
            logger.warning(f"Intento {intento}/{MAX_REINTENTOS} falló → 60s")
            time.sleep(60)
        if not exito:
            logger.error("Falló todo el ciclo")
        logger.info(f"Durmiendo {INTERVALO_MINUTOS} min...")
        time.sleep(INTERVALO_MINUTOS * 60)

if __name__ == "__main__":
    main()