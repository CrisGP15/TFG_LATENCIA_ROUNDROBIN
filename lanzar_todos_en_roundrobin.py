# archivo: lanzar_todos_en_roundrobin.py
import subprocess
import time
from datetime import datetime, timedelta
import os
import signal
import sys
import threading
import queue

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
# 2. Función que ejecuta un script con timeout controlado
# ------------------------------------------------------------------
def ejecutar_script_con_timeout(script_path, timeout_segundos=150):
    """Ejecuta un script con timeout, capturando output en tiempo real"""
    nombre = os.path.basename(script_path)
    inicio = datetime.now()
    
    print(f"\n[{inicio.strftime('%H:%M:%S')}] 🚀 INICIANDO: {nombre} (max: {timeout_segundos}s)")
    
    try:
        # Crear proceso
        proceso = subprocess.Popen(
            ["python", script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        # Variables para capturar output
        salida_completa = []
        error_completo = []
        
        # Función para leer output en tiempo real
        def leer_salida(pipe, lista_salida, tipo):
            for linea in iter(pipe.readline, ''):
                if linea:
                    lista_salida.append(linea.strip())
                    # Mostrar solo algunas líneas importantes
                    if "ERROR" in linea.upper() or "EXCEPTION" in linea.upper():
                        print(f"   🔴 {nombre}: {linea.strip()[:80]}")
                    elif "COMPLET" in linea.upper() or "FINALIZ" in linea.upper():
                        print(f"   ✅ {nombre}: {linea.strip()[:80]}")
        
        # Hilos para leer stdout y stderr
        hilo_stdout = threading.Thread(target=leer_salida, args=(proceso.stdout, salida_completa, "stdout"))
        hilo_stderr = threading.Thread(target=leer_salida, args=(proceso.stderr, error_completo, "stderr"))
        hilo_stdout.daemon = True
        hilo_stderr.daemon = True
        hilo_stdout.start()
        hilo_stderr.start()
        
        # Esperar con timeout
        tiempo_inicio = time.time()
        while True:
            # Verificar si el proceso terminó
            retcode = proceso.poll()
            if retcode is not None:
                # Proceso terminó
                break
            
            # Verificar timeout
            if time.time() - tiempo_inicio > timeout_segundos:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] ⏱️  {nombre} → TIMEOUT ({timeout_segundos}s)")
                proceso.terminate()
                try:
                    proceso.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proceso.kill()
                break
            
            # Esperar un poco
            time.sleep(0.5)
        
        # Esperar a que los hilos terminen de leer
        hilo_stdout.join(timeout=2)
        hilo_stderr.join(timeout=2)
        
        # Obtener código de salida final
        retcode_final = proceso.poll()
        if retcode_final is None:
            proceso.kill()
            retcode_final = -9
        
        fin = datetime.now()
        duracion = fin - inicio
        
        # Mostrar resumen
        if retcode_final == 0:
            print(f"[{fin.strftime('%H:%M:%S')}] ✅ {nombre} → EXITOSO ({duracion.seconds}s)")
        elif retcode_final == -9:
            print(f"[{fin.strftime('%H:%M:%S')}] ⏱️  {nombre} → TERMINADO por timeout ({duracion.seconds}s)")
        else:
            print(f"[{fin.strftime('%H:%M:%S')}] ❌ {nombre} → ERROR código {retcode_final} ({duracion.seconds}s)")
        
        return duracion.seconds
        
    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ⚠️  {nombre} → EXCEPCIÓN: {str(e)[:80]}")
        return 0

# ------------------------------------------------------------------
# 3. Función de limpieza de procesos Selenium
# ------------------------------------------------------------------
def limpiar_procesos_selenium():
    """Limpia procesos Chrome/Chromedriver entre ejecuciones"""
    try:
        # Intentar terminar graceful primero
        subprocess.run(["pkill", "-f", "chromedriver"], 
                      capture_output=True, timeout=2)
        subprocess.run(["pkill", "-f", "chrome"], 
                      capture_output=True, timeout=2)
        time.sleep(1)
        
        # Forzar terminación si aún existen
        subprocess.run(["pkill", "-9", "-f", "chromedriver"], 
                      capture_output=True, timeout=2)
        subprocess.run(["pkill", "-9", "-f", "chrome"], 
                      capture_output=True, timeout=2)
        time.sleep(1)
    except:
        pass  # Ignorar errores en limpieza

# ------------------------------------------------------------------
# 4. MAIN - Ejecución Round-Robin robusta
# ------------------------------------------------------------------
def main(dias_solicitados=10):
    scripts = buscar_scripts_pruebacontinua()
    
    if not scripts:
        print("❌ No se encontraron scripts pruebacontinua*.py")
        return
    
    # ------------------------------------------------------------------
    # CONFIGURACIÓN
    # ------------------------------------------------------------------
    TIMEOUT_POR_SCRIPT = 150  # 150 segundos máximo por script
    INTERVALO_ENTRE_SCRIPTS = 3  # 3 segundos entre scripts (reducido)
    INTERVALO_ENTRE_CICLOS = 10  # 10 segundos entre ciclos
    
    # Configurar fecha de finalización EXACTA
    fecha_inicio = datetime.now()
    fecha_fin_exacta = fecha_inicio + timedelta(days=dias_solicitados)
    
    print(f"\n{'='*70}")
    print(f"🚀 ROUND-ROBIN CONTROLADO - {dias_solicitados} DÍAS")
    print(f"{'='*70}")
    print(f"📅 Inicio:        {fecha_inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📅 Fin exacto:    {fecha_fin_exacta.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⏱️  Timeout:       {TIMEOUT_POR_SCRIPT} segundos por script")
    print(f"📊 Total scripts: {len(scripts)}")
    print(f"🔄 Intervalos:    {INTERVALO_ENTRE_SCRIPTS}s entre scripts, {INTERVALO_ENTRE_CICLOS}s entre ciclos")
    print(f"{'='*70}")
    
    # Variables de control
    ejecutando = True
    ciclo_actual = 0
    script_actual = 0
    estadisticas = {
        'exitosos': 0,
        'timeouts': 0,
        'errores': 0,
        'total_tiempo': 0
    }
    
    def signal_handler(sig, frame):
        nonlocal ejecutando
        print(f"\n\n⚠️  Señal de interrupción recibida. Finalizando ciclo actual...")
        ejecutando = False
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # BUCLE PRINCIPAL - Ejecuta hasta fecha exacta
        while ejecutando and datetime.now() < fecha_fin_exacta:
            ciclo_actual += 1
            ahora = datetime.now()
            
            # Calcular tiempos
            tiempo_transcurrido = ahora - fecha_inicio
            tiempo_restante = fecha_fin_exacta - ahora
            
            # Mostrar encabezado del ciclo
            print(f"\n{'='*60}")
            print(f"🔄 CICLO {ciclo_actual} - {ahora.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"⏳ Transcurrido: {tiempo_transcurrido.days}d {tiempo_transcurrido.seconds//3600:02d}h")
            print(f"⏰ Restante:     {tiempo_restante.days}d {tiempo_restante.seconds//3600:02d}h")
            print(f"📊 Estadísticas: ✅{estadisticas['exitosos']} ⏱️{estadisticas['timeouts']} ❌{estadisticas['errores']}")
            print(f"{'='*60}")
            
            # Ejecutar cada script en el ciclo
            for i, script in enumerate(scripts):
                script_actual += 1
                
                # Verificar si debemos detenernos
                if not ejecutando or datetime.now() >= fecha_fin_exacta:
                    print("⏹️  Límite de tiempo alcanzado")
                    ejecutando = False
                    break
                
                # Limpieza entre scripts
                limpiar_procesos_selenium()
                
                # Ejecutar script con timeout
                tiempo_ejecucion = ejecutar_script_con_timeout(script, TIMEOUT_POR_SCRIPT)
                
                # Actualizar estadísticas
                estadisticas['total_tiempo'] += tiempo_ejecucion
                
                # Pequeña pausa entre scripts (excepto el último)
                if i < len(scripts) - 1 and ejecutando and datetime.now() < fecha_fin_exacta:
                    print(f"⏸️  Pausa de {INTERVALO_ENTRE_SCRIPTS} segundos...")
                    for seg in range(INTERVALO_ENTRE_SCRIPTS, 0, -1):
                        if not ejecutando or datetime.now() >= fecha_fin_exacta:
                            ejecutando = False
                            break
                        print(f"   Próximo script en {seg}s...", end='\r')
                        time.sleep(1)
                    print(" " * 40, end='\r')
            
            # Pausa entre ciclos (solo si aún no llegamos al límite)
            if ejecutando and datetime.now() < fecha_fin_exacta:
                print(f"\n✅ Ciclo {ciclo_actual} completado")
                print(f"🔄 Próximo ciclo en {INTERVALO_ENTRE_CICLOS} segundos...")
                
                for seg in range(INTERVALO_ENTRE_CICLOS, 0, -1):
                    if not ejecutando or datetime.now() >= fecha_fin_exacta:
                        ejecutando = False
                        break
                    
                    # Mostrar tiempo restante actualizado
                    ahora_temp = datetime.now()
                    tiempo_r_temp = fecha_fin_exacta - ahora_temp
                    horas_r = tiempo_r_temp.seconds // 3600
                    minutos_r = (tiempo_r_temp.seconds % 3600) // 60
                    
                    print(f"   ⏰ Ciclo {ciclo_actual+1} en {seg:2d}s | Restante: {tiempo_r_temp.days}d {horas_r:02d}h {minutos_r:02d}m", end='\r')
                    time.sleep(1)
                
                print(" " * 80, end='\r')
    
    except KeyboardInterrupt:
        print("\n\n🛑 Interrupción por teclado detectada.")
    except Exception as e:
        print(f"\n\n⚠️  Error inesperado: {e}")
    
    finally:
        # LIMPIEZA FINAL Y ESTADÍSTICAS
        print("\n" + "="*70)
        print("🧹 Finalizando ejecución...")
        
        fin_ejecucion = datetime.now()
        duracion_total = fin_ejecucion - fecha_inicio
        
        # Limpieza final
        limpiar_procesos_selenium()
        
        # Calcular estadísticas
        total_ejecuciones = ciclo_actual * len(scripts)
        tiempo_promedio = estadisticas['total_tiempo'] / total_ejecuciones if total_ejecuciones > 0 else 0
        
        print(f"\n📊 ESTADÍSTICAS FINALES:")
        print(f"{'='*70}")
        print(f"   📅 Solicitado:      {dias_solicitados} días")
        print(f"   🕒 Ejecutado:       {duracion_total.days}d {duracion_total.seconds//3600:02d}h")
        print(f"   🔄 Ciclos:          {ciclo_actual}")
        print(f"   🚀 Ejecuciones:     {total_ejecuciones}")
        print(f"   ✅ Exitosos:        {estadisticas['exitosos']}")
        print(f"   ⏱️  Timeouts:        {estadisticas['timeouts']}")
        print(f"   ❌ Errores:         {estadisticas['errores']}")
        print(f"   📈 Tiempo promedio: {tiempo_promedio:.1f}s/script")
        print(f"   ⏰ Inicio:          {fecha_inicio.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   ⏰ Fin:             {fin_ejecucion.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Verificar cumplimiento
        cumplimiento = "✅ COMPLETO" if duracion_total >= timedelta(days=dias_solicitados) else "❌ INCOMPLETO"
        print(f"   🎯 Cumplimiento:    {cumplimiento}")
        
        if duracion_total < timedelta(days=dias_solicitados):
            tiempo_faltante = timedelta(days=dias_solicitados) - duracion_total
            print(f"   ⏰ Faltaron:        {tiempo_faltante.days}d {tiempo_faltante.seconds//3600:02d}h")
        
        print(f"\n🎯 Procesos finalizados correctamente.")
        print(f"{'='*70}")

# ------------------------------------------------------------------
# 5. Ejecución desde línea de comandos
# ------------------------------------------------------------------
if __name__ == "__main__":
    # Valor por defecto
    dias_a_ejecutar = 10
    
    # Procesar argumentos
    if len(sys.argv) > 1:
        try:
            dias_a_ejecutar = int(sys.argv[1])
            if dias_a_ejecutar <= 0:
                print("❌ ERROR: El número de días debe ser mayor a 0")
                print("📖 Uso: python lanzar_todos_en_roundrobin.py [días]")
                print("💡 Ejemplo: python lanzar_todos_en_roundrobin.py 7")
                sys.exit(1)
                
            print(f"🎯 Configuración: {dias_a_ejecutar} días")
            
        except ValueError:
            print("❌ ERROR: El parámetro debe ser un número entero")
            print("📖 Uso: python lanzar_todos_en_roundrobin.py [días]")
            print("💡 Ejemplo: python lanzar_todos_en_roundrobin.py 7")
            sys.exit(1)
    else:
        print(f"ℹ️  Usando {dias_a_ejecutar} días por defecto")
    
    # Confirmación
    fecha_fin = datetime.now() + timedelta(days=dias_a_ejecutar)
    print(f"\n⚠️  CONFIRMACIÓN REQUERIDA")
    print(f"   📅 Ejecutará por: {dias_a_ejecutar} días completos")
    print(f"   ⏰ Hora inicio:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   ⏰ Hora fin:      {fecha_fin.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   ⏱️  Timeout:      150 segundos por script")
    
    respuesta = input("\n¿Continuar? (s/n): ").strip().lower()
    if respuesta != 's':
        print("🛑 Cancelado por el usuario")
        sys.exit(0)
    
    # Ejecutar
    print(f"\n{'='*70}")
    print("🚀 INICIANDO EJECUCIÓN...")
    print(f"{'='*70}")
    
    main(dias_a_ejecutar)