# archivo: lanzar_todos_en_paralelo.py
# Pon este archivo en la carpeta padre (al mismo nivel que cloudping, cloudpingco, etc.)

import multiprocessing
import subprocess
import time
from datetime import datetime
import os
import sys
import pandas as pd
import numpy as np
import glob

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
    return scripts

# ------------------------------------------------------------------
# 2. Función que ejecuta un script individual (uno de tus pruebacontinua)
# ------------------------------------------------------------------
def ejecutar_monitor(script_path):
    nombre = os.path.basename(script_path)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Iniciando → {nombre}")
    
    try:
        # Esto ejecuta tu script como si lo lanzaras desde la terminal
        resultado = subprocess.run(
            ["python", script_path],
            check=True,
            capture_output=True,
            text=True
        )
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {nombre} → Finalizado correctamente")
    except subprocess.CalledProcessError as e:
        print(f"ERROR en {nombre}: código {e.returncode}")
        print(e.stderr)
    except Exception as e:
        print(f"Excepción inesperada en {nombre}: {e}")

# ------------------------------------------------------------------
# 3. Función para calcular medianas de todos los CSV
# ------------------------------------------------------------------
def calcular_medianas_csv():
    print("\n" + "="*80)
    print("CALCULANDO MEDIANAS DE LATENCIAS")
    print("="*80)
    
    resultados = []
    
    # Definir las carpetas donde buscar los CSV
    carpetas = ["cloudping", "cloudpingco", "cloudpinginfo", "cloudpingnet", "cloudpingtest"]
    
    for carpeta in carpetas:
        ruta_carpeta = os.path.join(os.path.dirname(__file__), carpeta)
        if not os.path.isdir(ruta_carpeta):
            print(f"  ⚠ Carpeta no encontrada: {carpeta}")
            continue
        
        # Buscar archivos CSV en la carpeta
        csv_files = glob.glob(os.path.join(ruta_carpeta, "*.csv"))
        
        if not csv_files:
            print(f"  📁 {carpeta}: No se encontraron archivos CSV")
            continue
        
        print(f"\n  📂 Carpeta: {carpeta}")
        print("  " + "-" * 70)
        
        for csv_file in csv_files:
            try:
                # Leer el archivo CSV
                df = pd.read_csv(csv_file)
                
                # Verificar si tiene columna de latencia (podría llamarse 'latency', 'latencia', 'time_ms', etc.)
                columnas_latencia = [col for col in df.columns if any(x in col.lower() for x in ['latency', 'latencia', 'time', 'ms', 'delay'])]
                
                if not columnas_latencia:
                    print(f"    ⚠ {os.path.basename(csv_file)}: No se encontró columna de latencia")
                    continue
                
                # Usar la primera columna de latencia encontrada
                columna_latencia = columnas_latencia[0]
                
                # Filtrar valores no numéricos y eliminar NaN
                latencias = pd.to_numeric(df[columna_latencia], errors='coerce')
                latencias = latencias.dropna()
                
                if len(latencias) == 0:
                    print(f"    ⚠ {os.path.basename(csv_file)}: Sin datos de latencia válidos")
                    continue
                
                # Calcular estadísticas
                mediana = np.median(latencias)
                promedio = np.mean(latencias)
                minimo = np.min(latencias)
                maximo = np.max(latencias)
                percentil_95 = np.percentile(latencias, 95)
                
                resultados.append({
                    'carpeta': carpeta,
                    'archivo': os.path.basename(csv_file),
                    'mediana_ms': mediana,
                    'promedio_ms': promedio,
                    'minimo_ms': minimo,
                    'maximo_ms': maximo,
                    'p95_ms': percentil_95,
                    'muestras': len(latencias)
                })
                
                print(f"    ✅ {os.path.basename(csv_file):40} | Mediana: {mediana:7.2f} ms | Promedio: {promedio:7.2f} ms | Muestras: {len(latencias):5}")
                print(f"         Mín: {minimo:6.1f} ms | Máx: {maximo:6.1f} ms | P95: {percentil_95:6.1f} ms")
                
            except Exception as e:
                print(f"    ❌ {os.path.basename(csv_file)}: Error al procesar - {str(e)}")
    
    # Mostrar resumen por carpeta
    if resultados:
        print("\n" + "="*80)
        print("RESUMEN POR CARPETA")
        print("="*80)
        
        for carpeta in carpetas:
            resultados_carpeta = [r for r in resultados if r['carpeta'] == carpeta]
            
            if resultados_carpeta:
                # Calcular estadísticas agregadas por carpeta
                medianas = [r['mediana_ms'] for r in resultados_carpeta]
                promedios = [r['promedio_ms'] for r in resultados_carpeta]
                
                mediana_global = np.mean(medianas) if medianas else 0
                promedio_global = np.mean(promedios) if promedios else 0
                
                print(f"\n  📊 {carpeta}:")
                print(f"     • Archivos procesados: {len(resultados_carpeta)}")
                print(f"     • Mediana global de medianas: {mediana_global:.2f} ms")
                print(f"     • Promedio global de promedios: {promedio_global:.2f} ms")
                
                # Mostrar mejores y peores medianas
                if len(resultados_carpeta) > 1:
                    mejor = min(resultados_carpeta, key=lambda x: x['mediana_ms'])
                    peor = max(resultados_carpeta, key=lambda x: x['mediana_ms'])
                    
                    print(f"     • Mejor mediana: {mejor['archivo']} ({mejor['mediana_ms']:.2f} ms)")
                    print(f"     • Peor mediana: {peor['archivo']} ({peor['mediana_ms']:.2f} ms)")
        
        # Mostrar tabla completa
        print("\n" + "="*80)
        print("TABLA COMPLETA DE RESULTADOS")
        print("="*80)
        
        df_resultados = pd.DataFrame(resultados)
        df_resultados = df_resultados.sort_values(['carpeta', 'mediana_ms'])
        
        # Formatear la tabla
        pd.set_option('display.max_rows', None)
        pd.set_option('display.width', None)
        
        # Columnas a mostrar
        columnas_display = ['carpeta', 'archivo', 'mediana_ms', 'promedio_ms', 'minimo_ms', 'maximo_ms', 'muestras']
        
        print("\n" + df_resultados[columnas_display].to_string(index=False))
        
        # Guardar resultados en un archivo
        fecha_actual = datetime.now().strftime("%Y%m%d_%H%M%S")
        archivo_resultados = f"resultados_medianas_{fecha_actual}.csv"
        df_resultados.to_csv(archivo_resultados, index=False)
        print(f"\n📄 Resultados guardados en: {archivo_resultados}")
        
        # Mostrar las 5 mejores medianas globalmente
        print("\n" + "="*80)
        print("TOP 5 MEJORES MEDIANAS (menor latencia)")
        print("="*80)
        
        top_5 = df_resultados.nsmallest(5, 'mediana_ms')[['carpeta', 'archivo', 'mediana_ms', 'promedio_ms', 'muestras']]
        print("\n" + top_5.to_string(index=False))
        
    else:
        print("\n⚠ No se encontraron archivos CSV con datos de latencia válidos")
    
    return len(resultados)

# ------------------------------------------------------------------
# 4. MAIN - Lanzar todos en paralelo
# ------------------------------------------------------------------
if __name__ == "__main__":
    # Obtener número de días desde la línea de comandos
    dias_ejecucion = 10  # Valor por defecto
    
    if len(sys.argv) > 1:
        try:
            dias_ejecucion = int(sys.argv[1])
            print(f"Se ejecutará por {dias_ejecucion} días")
        except ValueError:
            print(f"Error: '{sys.argv[1]}' no es un número válido")
            print("Usando valor por defecto: 10 días")
    else:
        print(f"No se especificó número de días. Usando valor por defecto: {dias_ejecucion} días")
    
    print("Para especificar días: python lanzar_todos_en_paralelo.py [número_de_días]")
    print(f"Ejemplo: python lanzar_todos_en_paralelo.py 7")
    print("-" * 60)
    
    scripts = buscar_scripts_pruebacontinua()
    
    if not scripts:
        print("No se encontraron scripts pruebacontinua*.py")
        print("Verifica que las carpetas existan y contengan los scripts")
        sys.exit(1)
    
    print(f"\nEncontrados {len(scripts)} scripts pruebacontinua*.py")
    for s in scripts:
        print("  •", os.path.basename(s))
    
    print(f"\nLanzando todos en paralelo por {dias_ejecucion} días...")
    print("="*60)
    
    procesos = []
    for script in scripts:
        p = multiprocessing.Process(target=ejecutar_monitor, args=(script,))
        p.start()
        procesos.append(p)
        
        # Pequeña pausa entre lanzamientos para no saturar al inicio
        time.sleep(5)
    
    print(f"\nTodos los {len(procesos)} monitores están corriendo en paralelo.")
    print(f"Duración programada: {dias_ejecucion} días")
    print("Puedes cerrar esta terminal y seguirán funcionando (o usar 'nohup' si estás en Linux).")
    print("Presiona Ctrl+C para parar todo cuando quieras.\n")
    
    try:
        # Calcular tiempo de ejecución en segundos
        segundos_ejecucion = dias_ejecucion * 24 * 60 * 60 + 3600  # días + 1 hora de margen
        
        # Mostrar información del tiempo de ejecución
        horas_totales = dias_ejecucion * 24
        print(f"Tiempo total de ejecución: {dias_ejecucion} días ({horas_totales} horas)")
        print(f"Finalizará aproximadamente: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Esperar el tiempo especificado
        time.sleep(segundos_ejecucion)
        
        print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
        print(f"Finalizado el tiempo programado de {dias_ejecucion} días.")
        
    except KeyboardInterrupt:
        print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
        print("Detención manual solicitada (Ctrl+C)...")
    
    # Detener todos los procesos
    print("\nDeteniendo todos los procesos...")
    for i, p in enumerate(procesos):
        print(f"Deteniendo proceso {i+1}/{len(procesos)}...")
        p.terminate()
        p.join(timeout=10)
    
    print("\n✅ Todo detenido correctamente.")
    print(f"Finalizado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Calcular medianas de los CSV generados
    print("\n" + "="*80)
    print("PROCESANDO RESULTADOS...")
    print("="*80)
    
    archivos_procesados = calcular_medianas_csv()
    
    print(f"\n🎯 EJECUCIÓN COMPLETADA")
    print(f"   • Días de ejecución: {dias_ejecucion}")
    print(f"   • Scripts ejecutados: {len(scripts)}")
    print(f"   • Archivos CSV procesados: {archivos_procesados}")
    print(f"   • Hora de finalización: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)