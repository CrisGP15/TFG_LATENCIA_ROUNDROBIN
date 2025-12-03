from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import csv
import datetime
import os
import time
import re

def setup_driver():
    """Configurar el driver de Chrome con evasión de detección"""
    chrome_options = Options()
    # chrome_options.add_argument('--headless')  # Descomenta para modo invisible
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    driver = webdriver.Chrome(options=chrome_options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def click_http_ping_button(driver, wait):
    """🔥 Busca y hace clic en el botón HTTP Ping"""
    print("🔍 Buscando botón 'HTTP Ping'...")
    button_selectors = [
        "//button[contains(text(), 'HTTP Ping') or contains(text(), 'http ping')]",
        "//button[contains(@class, 'ping') or contains(@class, 'Ping')]",
        "//*[contains(text(), 'HTTP') and contains(text(), 'Ping')]",
        "//button[contains(@id, 'ping') or contains(@id, 'Ping')]",
        "//div[contains(text(), 'HTTP Ping')]//button",
        "button:contains('HTTP Ping'), button:contains('http ping')",
    ]
    
    button = None
    for selector in button_selectors:
        try:
            if selector.startswith("//"):
                button = wait.until(EC.element_to_be_clickable((By.XPATH, selector)))
            else:
                button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector)))
            print(f"✅ Botón encontrado con selector: {selector}")
            break
        except TimeoutException:
            continue
    
    if not button:
        buttons = driver.find_elements(By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'ping')]")
        for btn in buttons:
            if 'http' in btn.text.lower():
                button = btn
                print(f"✅ Botón encontrado por texto: {btn.text}")
                break
    
    if button:
        driver.execute_script("arguments[0].scrollIntoView(true);", button)
        time.sleep(1)
        driver.save_screenshot('before_click.png')
        print("📸 Screenshot ANTES del clic guardado")
        button.click()
        print("🖱️ ¡CLIC AUTOMÁTICO en HTTP Ping!")
        time.sleep(3)
        driver.save_screenshot('after_click.png')
        print("📸 Screenshot DESPUÉS del clic guardado")
        return True
    else:
        print("❌ NO se encontró el botón HTTP Ping")
        return False

def wait_for_latency_data(driver, wait, max_wait=180, min_cells=10):
    """⏱️ Espera inteligente hasta que aparezcan suficientes datos de latencia"""
    print(f"⏳ Esperando datos de latencia (máx {max_wait}s)...")
    start_time = time.time()
    
    while time.time() - start_time < max_wait:
        # Múltiples estrategias para detectar celdas de latencia
        latency_cells = driver.find_elements(By.XPATH, """
            //td[contains(text(), 'ms') or contains(text(), '.') or contains(@data-value, '.') or contains(@class, 'latency')]
            | //span[contains(text(), 'ms') or contains(text(), '.') or contains(@data-value, '.')]
        """)
        
        if len(latency_cells) >= min_cells:
            print(f"✅ ¡Datos listos! Encontradas {len(latency_cells)} celdas de latencia")
            return True
        
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        print(f"⏳ Esperando... ({int(time.time() - start_time)}s) - Celdas: {len(latency_cells)}")
    
    print("⚠️ Timeout esperando datos")
    return False

def scrape_cloudping_selenium():
    url = "https://www.cloudping.cloud/aws"
    driver = None
    try:
        print("🚀 === SCRAPER CLOUDPING 100% AUTOMÁTICO ===")
        driver = setup_driver()
        print(f"🌐 Cargando: {url}")
        driver.get(url)
        
        wait = WebDriverWait(driver, 20)
        
        # 🔥 PASO 1: Clic en HTTP Ping (reintentar hasta 3 veces)
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            if click_http_ping_button(driver, wait):
                # ⏱️ PASO 2: Espera datos
                if wait_for_latency_data(driver, wait):
                    break
                else:
                    print(f"⚠️ Intento {attempt}/{max_attempts}: No se cargaron datos, reintentando...")
                    driver.refresh()
                    time.sleep(5)
            else:
                print(f"❌ Intento {attempt}/{max_attempts}: Falló clic automático")
                if attempt == max_attempts:
                    print("❌ Falló tras todos los intentos")
                    return False
        
        # 📸 Screenshot final
        driver.save_screenshot('data_loaded.png')
        print("✅ Screenshot FINAL guardado")
        
        # 💾 Extraer y guardar datos
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        output_file = 'aws_cloudping_latency.csv'
        file_exists = os.path.exists(output_file)
        
        rows_found = 0
        with open(output_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(['timestamp', 'provider', 'region', 'datacenter', 'latency_ms'])
            
            tables = driver.find_elements(By.TAG_NAME, "table")
            print(f"📊 Encontradas {len(tables)} tablas")
            
            for table_idx, table in enumerate(tables):
                rows = table.find_elements(By.TAG_NAME, "tr")
                for i, row in enumerate(rows[1:], 1):  # Saltar header
                    try:
                        cells = row.find_elements(By.XPATH, ".//td | .//th")
                        if len(cells) >= 2:
                            region_text = cells[0].text.strip()
                            latency_cell = cells[1]
                            latency_text = latency_cell.text.strip()
                            
                            # Extraer latencia de spans o data-*
                            if not any(c.isdigit() for c in latency_text):
                                spans = latency_cell.find_elements(By.TAG_NAME, "span")
                                for span in spans:
                                    st = span.text.strip()
                                    if any(c.isdigit() for c in st):
                                        latency_text = st
                                        break
                            
                            if not any(c.isdigit() for c in latency_text):
                                data_val = latency_cell.get_attribute('data-value') or \
                                         latency_cell.get_attribute('data-latency')
                                if data_val and any(c.isdigit() for c in data_val):
                                    latency_text = data_val
                            
                            # Filtrar regiones válidas
                            if (region_text and 
                                region_text not in ['Region', ''] and
                                any(keyword in region_text.lower() for keyword in ['us-', 'eu-', 'ap-', 'ca-', 'me-', 'af-', 'sa-'])):
                                
                                region_code = extract_region_code(region_text)
                                datacenter_name = get_datacenter_name(region_code) or region_text
                                latency_clean = extract_latency_value(latency_text)
                                
                                if latency_clean:
                                    writer.writerow([timestamp, 'cloudping AWS', region_code or region_text, datacenter_name, latency_clean])
                                    print(f"✓ {datacenter_name}: {latency_clean}ms")
                                    rows_found += 1
                    except Exception as e:
                        print(f"⚠️ Error en fila {i} de tabla {table_idx + 1}: {e}")
                        continue
        
        print(f"🎉 ¡ÉXITO! Guardadas {rows_found} filas en {output_file}")
        
        # Guardar HTML para debug
        with open('cloudping_final.html', 'w', encoding='utf-8') as f:
            f.write(driver.page_source)
        
        return rows_found > 0
        
    except Exception as e:
        print(f"💥 Error general: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if driver:
            driver.quit()
            print("🔒 Navegador cerrado")

def extract_region_code(region_text):
    """Extrae código de región (us-east-1, etc.)"""
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
    """Extrae número de latencia"""
    if not latency_text:
        return None
    match = re.search(r'(\d+\.?\d*)', latency_text)
    return match.group(1) if match else None

def get_datacenter_name(region):
    """Mapea código a nombre legible"""
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

if __name__ == "__main__":
    success = scrape_cloudping_selenium()
    if success:
        print("\n🎊 ¡TODO LISTO! Revisa 'aws_cloudping_latency.csv'")
    else:
        print("\n❌ Falló. Revisa screenshots: before_click.png, after_click.png, data_loaded.png")
        print("💡 Revisa también 'cloudping_final.html' para el estado de la página")