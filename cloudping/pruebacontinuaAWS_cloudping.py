#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SCRAPER CONTINUO AWS CLOUDPING - VERSIÓN FIJA
✅ Usa EXACTO tu código original de extracción
- Cada 10 min, 10-20s ping → ~30 regiones
- Robusto: reintentos, logs, screenshots solo errores
- Headless + user-agents rotativos
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
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

# ===================== CONFIG =====================
URL = "https://www.cloudping.cloud/aws"
INTERVALO_MINUTOS = 10
MAX_REINTENTOS = 3
OUTPUT_CSV = "aws_cloudping_latency_longterm.csv"
LOG_FILE = "scraper_continuo.log"
SCREENSHOT_DIR = "errores_screenshots"
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

def signal_handler(sig, frame):
    logger.info("🛑 Cerrando limpiamente...")
    sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ===================== TU CÓDIGO ORIGINAL PORTADO =====================
def setup_driver():
    """Driver ultra-ligero para ejecución 24/7 en paralelo"""
    chrome_options = Options()
    
    # HEADLESS OBLIGATORIO
    chrome_options.add_argument('--headless=new')  # Chrome 109+
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')      # Crucial en Linux/WSL
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('--disable-plugins')
    chrome_options.add_argument('--disable-images')             # ← Ahorra 100-300 MB
    chrome_options.add_argument('--disable-javascript')         # ← Solo si la web funciona sin JS (prueba primero False)
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_argument('--disable-features=VizDisplayCompositor')
    chrome_options.add_argument('--disable-ipc-flooding-protection')
    chrome_options.add_argument('--memory-pressure-off')
    chrome_options.add_argument('--max_old_space_size=4096')
    
    # Anti-detección (ya lo tenías, lo dejamos)
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    # User-agent rotativo (bien!)
    ua = random.choice(USER_AGENTS)
    chrome_options.add_argument(f'--user-agent={ua}')

    # ← ESTO ES ORO PURO PARA 10+ PROCESOS EN PARALELO
    chrome_options.add_argument('--disable-background-timer-throttling')
    chrome_options.add_argument('--disable-renderer-backgrounding')
    chrome_options.add_argument('--disable-backgrounding-occluded-windows')
    chrome_options.add_argument('--disable-features=ImprovedCookieControls,LazyFrameLoading,GlobalMediaControls,DestroyProfileOnBrowserClose,MediaRouter')

    driver = webdriver.Chrome(options=chrome_options)
    
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

def click_http_ping_button(driver, wait):
    """🔥 Tu función original: busca y clica HTTP Ping"""
    logger.info("🔍 Buscando botón 'HTTP Ping'...")
    button_selectors = [
        "//button[contains(text(), 'HTTP Ping') or contains(text(), 'http ping')]",
        "//button[contains(@class, 'ping') or contains(@class, 'Ping')]",
        "//*[contains(text(), 'HTTP') and contains(text(), 'Ping')]",
        "//button[contains(@id, 'ping') or contains(@id, 'Ping')]",
        "//div[contains(text(), 'HTTP Ping')]//button",
        "button:contains('HTTP Ping'), button:contains('http ping')"
    ]
    button = None
    for selector in button_selectors:
        try:
            if selector.startswith("//"):
                button = wait.until(EC.element_to_be_clickable((By.XPATH, selector)))
            else:
                button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector)))
            logger.info(f"✅ Botón con selector: {selector}")
            break
        except TimeoutException:
            continue
    if not button:
        buttons = driver.find_elements(By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'ping')]")
        for btn in buttons:
            if 'http' in btn.text.lower():
                button = btn
                logger.info(f"✅ Botón por texto: {btn.text}")
                break
    if button:
        driver.execute_script("arguments[0].scrollIntoView(true);", button)
        time.sleep(1)
        button.click()
        logger.info("🖱️ CLIC en HTTP Ping!")
        time.sleep(3)
        return True
    logger.error("❌ NO botón HTTP Ping")
    return False

def wait_for_latency_data(driver, wait, max_wait=90, min_cells=20):
    """⏱️ Tu espera inteligente hasta suficientes datos"""
    logger.info(f"⏳ Esperando datos (máx {max_wait}s, min {min_cells} celdas)...")
    start_time = time.time()
    while time.time() - start_time < max_wait:
        latency_cells = driver.find_elements(By.XPATH, """
            //td[contains(text(), 'ms') or contains(text(), '.') or contains(@data-value, '.') or contains(@class, 'latency')]
            | //span[contains(text(), 'ms') or contains(text(), '.') or contains(@data-value, '.')]
        """)
        if len(latency_cells) >= min_cells:
            logger.info(f"✅ ¡Datos listos! {len(latency_cells)} celdas")
            return True
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        logger.info(f"⏳ {int(time.time() - start_time)}s - Celdas: {len(latency_cells)}")
    logger.warning("⚠️ Timeout datos")
    return False

def extract_region_code(region_text):
    """Tu regex para códigos (us-east-1)"""
    patterns = [
        r'([a-z]{2}-[a-z]+-\d+)',
        r'([a-z]{2}-[a-z]+\d-\d+)'
    ]
    for pattern in patterns:
        match = re.search(pattern, region_text, re.IGNORECASE)
        if match:
            return match.group(1)
    return None

def extract_latency_value(latency_text):
    """Tu extracción numérica"""
    if not latency_text:
        return None
    match = re.search(r'(\d+\.?\d*)', latency_text)
    return match.group(1) if match else None

def get_datacenter_name(region):
    """Tu mapa completo nombres bonitos"""
    if not region:
        return "Unknown"
    datacenter_map = {
        'eu-south-2': 'Spain (Milán)', 'eu-central-2': 'Zürich', 'eu-south-1': 'Milan',
        'eu-west-2': 'London', 'eu-central-1': 'Frankfurt', 'eu-west-1': 'Ireland',
        'eu-north-1': 'Stockholm', 'il-central-1': 'Israel', 'me-central-1': 'UAE',
        'us-east-2': 'Ohio', 'me-south-1': 'Bahrain', 'mx-central-1': 'Mexico',
        'ca-west-1': 'Calgary', 'af-south-1': 'Cape Town', 'us-east-1': 'N. Virginia',
        'eu-west-3': 'Paris', 'ca-central-1': 'Canada Central', 'ap-south-2': 'Hyderabad',
        'sa-east-1': 'São Paulo', 'ap-south-1': 'Mumbai', 'us-west-1': 'N. California',
        'us-west-2': 'Oregon', 'ap-northeast-3': 'Osaka', 'ap-southeast-4': 'Melbourne',
        'ap-northeast-1': 'Tokyo', 'ap-east-2': 'Taipei', 'ap-east-1': 'Hong Kong',
        'ap-northeast-2': 'Seoul', 'ap-southeast-7': 'Thailand', 'ap-southeast-3': 'Jakarta',
        'ap-southeast-1': 'Singapore', 'ap-southeast-5': 'Malaysia', 'ap-southeast-2': 'Sydney'
    }
    return datacenter_map.get(region.lower(), region)

def guardar_screenshot(driver, nombre):
    path = os.path.join(SCREENSHOT_DIR, f"{nombre}_{int(time.time())}.png")
    driver.save_screenshot(path)
    logger.info(f"📸 {path}")

# ===================== CAPTURA UNA VEZ (TU LÓGICA ORIGINAL) =====================
def capturar_datos_una_vez():
    driver = None
    try:
        driver = setup_driver()
        wait = WebDriverWait(driver, 20)
        logger.info("🚀 Iniciando captura AWS...")
        driver.get(URL)
        time.sleep(3)

        # 🔥 PASO 1: Clic HTTP Ping (reintenta 3x)
        max_click_attempts = 3
        data_loaded = False
        for attempt in range(1, max_click_attempts + 1):
            if click_http_ping_button(driver, wait):
                # ⏱️ PASO 2: Espera datos
                if wait_for_latency_data(driver, wait, max_wait=90, min_cells=20):
                    data_loaded = True
                    break
                else:
                    logger.warning(f"⚠️ Intento {attempt}: No datos, refresh...")
                    driver.refresh()
                    time.sleep(5)
            else:
                logger.warning(f"❌ Intento {attempt}: No botón")
        if not data_loaded:
            raise Exception("Falló carga datos")

        # 💾 EXTRAER Y GUARDAR (TU CÓDIGO EXACTO)
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        file_exists = os.path.exists(OUTPUT_CSV)
        rows_found = 0
        with open(OUTPUT_CSV, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(['timestamp', 'provider', 'region', 'datacenter', 'latency_ms'])
            tables = driver.find_elements(By.TAG_NAME, "table")
            logger.info(f"📊 {len(tables)} tablas")
            for table_idx, table in enumerate(tables):
                rows = table.find_elements(By.TAG_NAME, "tr")
                for i, row in enumerate(rows[1:], 1):  # Skip header
                    try:
                        cells = row.find_elements(By.XPATH, ".//td | .//th")
                        if len(cells) >= 2:
                            region_text = cells[0].text.strip()
                            latency_cell = cells[1]
                            latency_text = latency_cell.text.strip()
                            # Dig spans/data-*
                            if not any(c.isdigit() for c in latency_text):
                                spans = latency_cell.find_elements(By.TAG_NAME, "span")
                                for span in spans:
                                    st = span.text.strip()
                                    if any(c.isdigit() for c in st):
                                        latency_text = st
                                        break
                            if not any(c.isdigit() for c in latency_text):
                                data_val = latency_cell.get_attribute('data-value') or latency_cell.get_attribute('data-latency')
                                if data_val and any(c.isdigit() for c in data_val):
                                    latency_text = data_val
                            # Filtro regiones válidas
                            if (region_text and region_text not in ['Region', ''] and
                                any(keyword in region_text.lower() for keyword in ['us-', 'eu-', 'ap-', 'ca-', 'me-', 'af-', 'sa-'])):
                                region_code = extract_region_code(region_text)
                                datacenter_name = get_datacenter_name(region_code) or region_text
                                latency_clean = extract_latency_value(latency_text)
                                if latency_clean:
                                    writer.writerow([timestamp, 'cloudping AWS', region_code or region_text, datacenter_name, latency_clean])
                                    logger.info(f"✓ {datacenter_name}: {latency_clean}ms")
                                    rows_found += 1
                    except Exception as e:
                        logger.debug(f"Error fila {i} tabla {table_idx}: {e}")
                        continue
        logger.info(f"🎉 ¡{rows_found} filas guardadas en {OUTPUT_CSV}!")
        return rows_found > 0

    except Exception as e:
        logger.error(f"💥 Error: {e}")
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
            # Forzar limpieza de memoria (muy útil en bucles largos)
            import gc
            gc.collect()

# ===================== BUCLE 24/7 =====================
def main():
    logger.info("🎯 SCRAPER CONTINUO FIJO INICIADO")
    logger.info(f"📈 Cada {INTERVALO_MINUTOS} min → {OUTPUT_CSV}")
    iteracion = 0
    while True:
        iteracion += 1
        logger.info(f"\n🔄 --- ITERACIÓN {iteracion} ---")
        exito = False
        for intento in range(MAX_REINTENTOS):
            if capturar_datos_una_vez():
                exito = True
                break
            logger.warning(f"⚠️ Intento {intento+1}/{MAX_REINTENTOS} falló → 60s...")
            time.sleep(60)
        if not exito:
            logger.error("❌ Falló todo → Siguiente ciclo")
        logger.info(f"💤 Esperando {INTERVALO_MINUTOS} min...")
        time.sleep(INTERVALO_MINUTOS * 60)

if __name__ == "__main__":
    main()