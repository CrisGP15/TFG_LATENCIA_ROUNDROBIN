# archivo: lanzar_todos_en_roundrobin.py
import subprocess
import time
from datetime import datetime
import os
import signal
import sys

# ------------------------------------------------------------------
# 1. Lista automáticamente todos los pruebacontinua_*.py de todas las subcarpetas
# ------------------------------------------------------------------
def buscar_scripts_pruebacontinua():
    scripts = []
    carpetas = ["cloudping", "cloudpingco", "cloudpinginfo", "cloudpingnet", "cloudpingtest"]
    
    for carpeta in carpetas:
        ruta_carpeta = os.path.join(os.path.dirname(__file__), carpeta)
        if not os.path.isdir(ruta_carpeta):
            continue
        for archivo in os.listdir(ruta_carpeta):
            if archivo.startswith("pruebacontinua") and archivo.endswith(".py"):
                ruta_completa = os.path.join(ruta_carpeta, archivo)
                scripts.append(ruta_completa)
    
    print(f"Encontrados {len(scripts)} scripts pruebacontinua*.py")
    for s in scripts:
        print("  •", os.path.basename(s))
    
    return scripts

# ------------------------------------------------------------------
# 2. Función que ejecuta un script individual con manejo de señales
# ------------------------------------------------------------------
def ejecutar_script_roundrobin(script_path, tiempo_ejecucion=300):  # 5 minutos por defecto
    nombre = os.path.basename(script_path)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Ejecutando → {nombre}")
    
    try:
        # Ejecutar el script
        proceso = subprocess.Popen(
            ["python", script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Esperar el tiempo asignado o hasta que termine
        try:
            stdout, stderr = proceso.communicate(timeout=tiempo_ejecucion)
            
            if proceso.returncode == 0:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] {nombre} → Completado (terminó naturalmente)")
                if stdout.strip():
                    print(f"   Salida: {stdout.strip()}")
            else:
                print(f"ERROR en {nombre}: código {proceso.returncode}")
                if stderr.strip():
                    print(f"   Error: {stderr.strip()}")
                    
        except subprocess.TimeoutExpired:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {nombre} → Tiempo agotado ({tiempo_ejecucion}s), terminando...")
            proceso.terminate()
            try:
                proceso.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proceso.kill()
                proceso.wait()
            
    except Exception as e:
        print(f"Excepción inesperada en {nombre}: {e}")

# ------------------------------------------------------------------
# 3. Función de limpieza de procesos Selenium
# ------------------------------------------------------------------
def limpiar_procesos_selenium():
    """Limpia procesos Chrome/Chromedriver entre ejecuciones"""
    print("Limpiando procesos Selenium...")
    try:
        subprocess.run(["pkill", "-f", "chromedriver"], capture_output=True)
        subprocess.run(["pkill", "-f", "chrome"], capture_output=True)
        time.sleep(2)  # Esperar a que los procesos terminen
    except Exception as e:
        print(f"Error en limpieza: {e}")

# ------------------------------------------------------------------
# 4. MAIN - Ejecución Round-Robin
# ------------------------------------------------------------------
def main():
    scripts = buscar_scripts_pruebacontinua()
    
    if not scripts:
        print("No se encontraron scripts pruebacontinua*.py")
        return
    
    # Configuración
    TIEMPO_POR_SCRIPT = 150  
   # CICLOS_COMPLETOS = 288   # 24 horas (288 ciclos de 5 minutos)
    DIAS_EJECUCION = 10
    MINUTOS_POR_DIA = 1440
    SEGUNDOS_POR_CICLO = 800  # Aproximadamente
    CICLOS_COMPLETOS = int((DIAS_EJECUCION * MINUTOS_POR_DIA * 60) / SEGUNDOS_POR_CICLO)
    
    print(f"\nConfiguración Round-Robin:")
    print(f"  • Tiempo por script: {TIEMPO_POR_SCRIPT} segundos ({TIEMPO_POR_SCRIPT//60} minutos)")
    print(f"  • Scripts por ciclo: {len(scripts)}")
    print(f"  • Duración total: {CICLOS_COMPLETOS} ciclos (~24 horas)")
    print(f"  • Intervalo entre scripts: 10 segundos")
    
    print("\nIniciando ejecución Round-Robin...")
    print("=" * 60)
    
    # Variable para controlar la parada graceful
    ejecutando = True
    
    def signal_handler(sig, frame):
        nonlocal ejecutando
        print(f"\n\nSeñal de interrupción recibida. Finalizando ejecución...")
        ejecutando = False
    
    # Registrar manejador de señales
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    ciclo_actual = 0
    
    try:
        while ejecutando and ciclo_actual < CICLOS_COMPLETOS:
            ciclo_actual += 1
            print(f"\n🎯 CICLO {ciclo_actual}/{CICLOS_COMPLETOS} - Iniciando a las {datetime.now().strftime('%H:%M:%S')}")
            print("-" * 50)
            
            for i, script in enumerate(scripts):
                if not ejecutando:
                    break
                    
                # Limpiar procesos antes de cada ejecución
                limpiar_procesos_selenium()
                
                # Ejecutar el script actual
                ejecutar_script_roundrobin(script, TIEMPO_POR_SCRIPT)
                
                # Pequeña pausa entre scripts (excepto después del último)
                if i < len(scripts) - 1 and ejecutando:
                    print(f"Esperando 10 segundos antes del próximo script...")
                    for segundos_restantes in range(10, 0, -1):
                        if not ejecutando:
                            break
                        time.sleep(1)
            
            if ejecutando and ciclo_actual < CICLOS_COMPLETOS:
                tiempo_restante = 10  # 10 segundos entre ciclos
                print(f"\n🔄 Ciclo {ciclo_actual} completado. Siguiente ciclo en {tiempo_restante} segundos...")
                for segundos_restantes in range(tiempo_restante, 0, -1):
                    if not ejecutando:
                        break
                    print(f"   Próximo ciclo en {segundos_restantes} segundos...", end='\r')
                    time.sleep(1)
                print("")
    
    except KeyboardInterrupt:
        print("\n\nInterrupción por teclado detectada.")
    
    finally:
        # Limpieza final
        print("\n" + "=" * 60)
        print("Finalizando ejecución Round-Robin...")
        limpiar_procesos_selenium()
        print(f"Ejecución completada. Total de ciclos: {ciclo_actual}")
        print("Todos los procesos finalizados correctamente.")

# ------------------------------------------------------------------
# 5. Ejecución con parámetros configurables
# ------------------------------------------------------------------
if __name__ == "__main__":
    # Puedes modificar estos parámetros según tus necesidades
    if len(sys.argv) > 1:
        try:
            tiempo_script = int(sys.argv[1])
            print(f"Tiempo por script configurado a: {tiempo_script} segundos")
        except ValueError:
            print("Parámetro inválido. Usando valores por defecto.")
    
    main()