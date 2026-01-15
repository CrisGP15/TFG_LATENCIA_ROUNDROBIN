import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from datetime import datetime
import sys
import os
import re

def crear_carpeta_resultados():
    """Crea una carpeta para guardar los resultados con timestamp"""
    timestamp = datetime.now().strftime("%d_%m_%Y_%H_%M")
    nombre_carpeta = f"CORRELACION_{timestamp}"
    
    if not os.path.exists(nombre_carpeta):
        os.makedirs(nombre_carpeta)
        print(f"📁 Carpeta creada: {nombre_carpeta}")
    
    return nombre_carpeta

def extraer_nombre_plataforma(nombre_archivo):
    """Extrae el nombre de la plataforma del nombre del archivo"""
    nombre_base = os.path.basename(nombre_archivo).lower()
    
    if 'cloudpingnet' in nombre_base:
        return 'CloudPing.net'
    elif 'cloudpingtest' in nombre_base:
        return 'CloudPingTest'
    elif 'cloudpinginfo' in nombre_base:
        return 'CloudPing.info'
    elif 'cloudping' in nombre_base:
        return 'CloudPing'
    
    return nombre_base.split('_')[0].title() if '_' in nombre_base else 'Plataforma'

def extraer_nombre_proveedor(nombre_archivo):
    """Extrae el nombre del proveedor del nombre del archivo"""
    nombre_base = os.path.basename(nombre_archivo).lower()
    
    if 'aws' in nombre_base or 'amazon' in nombre_base:
        return 'AWS'
    elif 'azure' in nombre_base or 'microsoft' in nombre_base:
        return 'Azure'
    elif 'gcp' in nombre_base or 'google' in nombre_base:
        return 'GCP'
    elif 'huawei' in nombre_base:
        return 'Huawei'
    elif 'oracle' in nombre_base:
        return 'Oracle'
    elif 'ibm' in nombre_base:
        return 'IBM'
    elif 'alibaba' in nombre_base:
        return 'Alibaba'
    elif 'digitalocean' in nombre_base:
        return 'DigitalOcean'
    elif 'linode' in nombre_base:
        return 'Linode'
    
    return nombre_base.split('_')[0].upper() if '_' in nombre_base else nombre_base.upper()

def cargar_y_preparar_datos(archivos):
    """Carga los archivos CSV y prepara los datos para análisis de correlación"""
    dataframes = []
    
    print("📊 CARGANDO DATOS...")
    print("-" * 50)
    
    for archivo in archivos:
        if not os.path.exists(archivo):
            print(f"⚠️  Advertencia: El archivo '{archivo}' no existe. Se omitirá.")
            continue
        
        try:
            # Cargar el archivo CSV
            df = pd.read_csv(archivo)
            
            # Verificar columnas necesarias
            columnas_requeridas = ['timestamp', 'provider', 'region', 'datacenter', 'latency_ms']
            if not all(col in df.columns for col in columnas_requeridas):
                print(f"⚠️  El archivo '{archivo}' no tiene la estructura esperada. Se omitirá.")
                continue
            
            # Extraer nombres de plataforma y proveedor
            nombre_plataforma = extraer_nombre_plataforma(archivo)
            nombre_proveedor = extraer_nombre_proveedor(archivo)
            
            # Añadir columnas identificativas
            df['plataforma'] = nombre_plataforma
            df['proveedor'] = nombre_proveedor
            df['archivo_origen'] = os.path.basename(archivo)
            
            # Convertir timestamp a datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Ordenar por timestamp
            df = df.sort_values('timestamp')
            
            dataframes.append(df)
            
            print(f"✅ {nombre_proveedor}: {len(df):,} registros")
            
        except Exception as e:
            print(f"❌ Error al cargar '{archivo}': {e}")
    
    if not dataframes:
        print("\n❌ No se pudieron cargar datos válidos de ningún archivo.")
        return None, None
    
    # Combinar todos los DataFrames
    df_combinado = pd.concat(dataframes, ignore_index=True)
    
    # Obtener plataforma (debería ser la misma para todos)
    plataforma = df_combinado['plataforma'].iloc[0]
    proveedores = df_combinado['proveedor'].unique()
    
    print(f"\n📈 RESUMEN:")
    print(f"   • Archivos: {len(dataframes)}")
    print(f"   • Registros: {len(df_combinado):,}")
    print(f"   • Plataforma: {plataforma}")
    print(f"   • Proveedores: {', '.join(proveedores)}")
    
    return df_combinado, plataforma, proveedores

def preparar_datos_para_correlacion(df, proveedores):
    """Prepara los datos para análisis de correlación por pares"""
    print("\n⚙️  PREPARANDO DATOS PARA CORRELACIÓN...")
    
    if len(proveedores) < 2:
        print(f"❌ Se necesitan al menos 2 proveedores para análisis de correlación.")
        return None
    
    # Crear DataFrame para correlación
    df_correlacion = pd.DataFrame()
    
    for proveedor in proveedores:
        df_proveedor = df[df['proveedor'] == proveedor].copy()
        
        # Agrupar por hora para tener puntos comparables
        df_proveedor['timestamp_hour'] = df_proveedor['timestamp'].dt.floor('H')
        df_agrupado = df_proveedor.groupby('timestamp_hour').agg({
            'latency_ms': 'mean'
        }).reset_index()
        
        # Renombrar columna para este proveedor
        df_agrupado = df_agrupado.rename(columns={'latency_ms': proveedor})
        
        # Si es el primer proveedor, establecer el DataFrame base
        if df_correlacion.empty:
            df_correlacion = df_agrupado[['timestamp_hour', proveedor]]
        else:
            # Unir por timestamp_hour
            df_correlacion = pd.merge(df_correlacion, df_agrupado[['timestamp_hour', proveedor]],
                                     on='timestamp_hour', how='inner')
    
    # Eliminar filas con NaN
    df_correlacion = df_correlacion.dropna()
    
    print(f"   • Puntos de datos: {len(df_correlacion):,}")
    print(f"   • Período: {df_correlacion['timestamp_hour'].min()} a {df_correlacion['timestamp_hour'].max()}")
    
    return df_correlacion

def calcular_correlaciones_por_pares(df_correlacion, proveedores):
    """Calcula las correlaciones entre todos los pares de proveedores"""
    print("\n📊 CALCULANDO CORRELACIONES...")
    
    resultados = []
    
    # Calcular todas las combinaciones de pares
    for i in range(len(proveedores)):
        for j in range(i + 1, len(proveedores)):
            prov1 = proveedores[i]
            prov2 = proveedores[j]
            
            # Obtener datos
            x = df_correlacion[prov1].values
            y = df_correlacion[prov2].values
            
            # Calcular correlación de Pearson
            corr, p_value = stats.pearsonr(x, y)
            r2 = corr ** 2  # R cuadrado
            
            # Calcular línea de regresión
            slope, intercept = np.polyfit(x, y, 1)
            
            resultados.append({
                'proveedor1': prov1,
                'proveedor2': prov2,
                'correlacion': corr,
                'p_value': p_value,
                'r2': r2,
                'slope': slope,
                'intercept': intercept,
                'n': len(x)
            })
            
            # Mostrar resultados en consola
            signo = "✅" if p_value < 0.05 else "⚠️ "
            print(f"{signo} {prov1} vs {prov2}: r = {corr:.3f}, p = {p_value:.4f}, R² = {r2:.3f}, n = {len(x):,}")
    
    return resultados

def interpretar_correlacion(r):
    """Interpreta el valor del coeficiente de correlación"""
    r_abs = abs(r)
    
    if r_abs >= 0.9:
        return "Correlación muy fuerte"
    elif r_abs >= 0.7:
        return "Correlación fuerte"
    elif r_abs >= 0.5:
        return "Correlación moderada"
    elif r_abs >= 0.3:
        return "Correlación débil"
    else:
        return "Correlación muy débil o nula"

def crear_grafica_correlacion_por_pares(df_correlacion, resultados_correlacion, plataforma, carpeta_resultados):
    """Crea una gráfica de correlación por pares"""
    print("\n📈 CREANDO GRÁFICA DE CORRELACIÓN POR PARES...")
    
    # Determinar el layout de subplots
    n_pares = len(resultados_correlacion)
    
    if n_pares == 0:
        print("❌ No hay pares para graficar")
        return None
    
    # Configurar tamaño de la figura
    if n_pares <= 2:
        fig, axes = plt.subplots(1, n_pares, figsize=(6*n_pares, 5))
    elif n_pares <= 4:
        fig, axes = plt.subplots(2, 2, figsize=(12, 10))
    else:
        # Para más de 4 pares, usar grid flexible
        n_cols = min(3, n_pares)
        n_rows = (n_pares + n_cols - 1) // n_cols
        fig, axes = plt.subplots(n_rows, n_cols, figsize=(5*n_cols, 4*n_rows))
    
    # Aplanar axes si es necesario
    if n_pares == 1:
        axes = [axes]
    elif isinstance(axes, np.ndarray):
        axes = axes.flatten()
    
    # Colores para los puntos
    colores = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b']
    
    # Crear un scatter plot por cada par
    for idx, resultado in enumerate(resultados_correlacion):
        if idx >= len(axes):
            break
            
        ax = axes[idx]
        prov1 = resultado['proveedor1']
        prov2 = resultado['proveedor2']
        
        # Obtener datos
        x = df_correlacion[prov1].values
        y = df_correlacion[prov2].values
        
        # Crear scatter plot
        scatter = ax.scatter(x, y, alpha=0.6, s=20, color=colores[idx % len(colores)])
        
        # Añadir línea de regresión
        x_line = np.linspace(min(x), max(x), 100)
        y_line = resultado['slope'] * x_line + resultado['intercept']
        ax.plot(x_line, y_line, 'r-', linewidth=2, alpha=0.8)
        
        # Configurar título y etiquetas
        ax.set_title(f'{prov1} vs {prov2}', fontsize=12, fontweight='bold', pad=10)
        ax.set_xlabel(f'Latencia {prov1} (ms)', fontsize=10)
        ax.set_ylabel(f'Latencia {prov2} (ms)', fontsize=10)
        
        # Añadir texto con estadísticas
        stats_text = f'r = {resultado["correlacion"]:.3f}\n'
        stats_text += f'p = {resultado["p_value"]:.4f}\n'
        stats_text += f'R² = {resultado["r2"]:.3f}\n'
        stats_text += f'n = {resultado["n"]:,}'
        
        # Posicionar el texto en la esquina superior izquierda
        ax.text(0.05, 0.95, stats_text, transform=ax.transAxes,
                fontsize=9, fontweight='bold', verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
        
        # Añadir grid
        ax.grid(True, alpha=0.3, linestyle='--')
        
        # Ajustar límites para mejor visualización
        ax.set_xlim([min(x) * 0.9, max(x) * 1.1])
        ax.set_ylim([min(y) * 0.9, max(y) * 1.1])
    
    # Ocultar ejes vacíos si los hay
    for idx in range(len(resultados_correlacion), len(axes)):
        axes[idx].axis('off')
    
    # Título general
    plt.suptitle(f'Correlación de Latencia - {plataforma}', fontsize=14, fontweight='bold', y=1.02)
    
    # Ajustar layout
    plt.tight_layout()
    
    # Guardar gráfico
    if carpeta_resultados:
        plataforma_limpia = plataforma.lower().replace(".", "_").replace(" ", "_")
        nombre_archivo = f'correlacion_pares_{plataforma_limpia}.png'
        ruta_completa = os.path.join(carpeta_resultados, nombre_archivo)
        plt.savefig(ruta_completa, dpi=300, bbox_inches='tight', facecolor='white')
        print(f"💾 Gráfica guardada como: {nombre_archivo}")
        plt.close()
    
    return nombre_archivo if carpeta_resultados else None

def generar_informe_correlacion(resultados_correlacion, plataforma, carpeta_resultados, archivos_procesados):
    """Genera un informe simple de correlación"""
    ruta_informe = os.path.join(carpeta_resultados, 'informe_correlacion.txt')
    
    with open(ruta_informe, 'w', encoding='utf-8') as f:
        f.write("="*70 + "\n")
        f.write(f"INFORME - CORRELACIÓN DE LATENCIA\n")
        f.write("="*70 + "\n\n")
        
        f.write(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Plataforma: {plataforma}\n")
        f.write(f"Carpeta: {carpeta_resultados}\n\n")
        
        f.write("ARCHIVOS:\n")
        f.write("-"*35 + "\n")
        for archivo in archivos_procesados:
            proveedor = extraer_nombre_proveedor(archivo)
            f.write(f"• {proveedor}: {os.path.basename(archivo)}\n")
        
        f.write("\nRESULTADOS DE CORRELACIÓN:\n")
        f.write("-"*35 + "\n")
        
        for resultado in resultados_correlacion:
            f.write(f"\n{resultado['proveedor1']} vs {resultado['proveedor2']}:\n")
            f.write(f"  Correlación (r): {resultado['correlacion']:.3f}\n")
            f.write(f"  Valor p: {resultado['p_value']:.4f}\n")
            f.write(f"  Significativo (p<0.05): {'SÍ' if resultado['p_value'] < 0.05 else 'NO'}\n")
            f.write(f"  R²: {resultado['r2']:.3f}\n")
            f.write(f"  Interpretación: {interpretar_correlacion(resultado['correlacion'])}\n")
            f.write(f"  Puntos: {resultado['n']:,}\n")
            f.write(f"  Ecuación: y = {resultado['slope']:.3f}x + {resultado['intercept']:.3f}\n")
        
        f.write("\nCONCLUSIONES:\n")
        f.write("-"*35 + "\n")
        
        if resultados_correlacion:
            # Encontrar correlación más fuerte
            mejor = max(resultados_correlacion, key=lambda x: abs(x['correlacion']))
            f.write(f"Correlación más fuerte: {mejor['proveedor1']} vs {mejor['proveedor2']}\n")
            f.write(f"• Valor: {mejor['correlacion']:.3f}\n")
            f.write(f"• Tipo: {interpretar_correlacion(mejor['correlacion'])}\n")
            f.write(f"• Significativa: {'SÍ' if mejor['p_value'] < 0.05 else 'NO'}\n")
            
            # Contar correlaciones significativas
            sig = sum(1 for r in resultados_correlacion if r['p_value'] < 0.05)
            f.write(f"\nCorrelaciones significativas: {sig}/{len(resultados_correlacion)}\n")
    
    print(f"📄 Informe generado: informe_correlacion.txt")

def main():
    """Función principal"""
    print("🔗 CORRELACIÓN ENTRE PROVEEDORES")
    print("="*50)
    
    # Verificar argumentos
    if len(sys.argv) < 2:
        print("Uso: python3 CORRELACION.py <archivo1.csv> [archivo2.csv ...]")
        print("\nEjemplo:")
        print("  python3 CORRELACION.py aws_cloudpingnet.csv azure_cloudpingnet.csv gcp_cloudpingnet.csv")
        return
    
    archivos = sys.argv[1:]
    print(f"📁 Archivos a procesar: {len(archivos)}")
    
    if len(archivos) < 2:
        print("❌ Error: Se necesitan al menos 2 archivos")
        return
    
    # Crear carpeta de resultados
    carpeta_resultados = crear_carpeta_resultados()
    print(f"📁 Carpeta de resultados: {carpeta_resultados}\n")
    
    # Cargar y preparar datos
    df, plataforma, proveedores = cargar_y_preparar_datos(archivos)
    if df is None:
        return
    
    # Preparar datos para correlación
    df_correlacion = preparar_datos_para_correlacion(df, proveedores)
    if df_correlacion is None:
        return
    
    # Calcular correlaciones
    resultados = calcular_correlaciones_por_pares(df_correlacion, proveedores)
    
    if not resultados:
        print("❌ No se pudieron calcular correlaciones")
        return
    
    print("\n" + "="*60)
    print("GENERANDO GRÁFICA DE CORRELACIÓN")
    print("="*60)
    
    # Crear gráfica
    crear_grafica_correlacion_por_pares(df_correlacion, resultados, plataforma, carpeta_resultados)
    
    # Generar informe
    generar_informe_correlacion(resultados, plataforma, carpeta_resultados, archivos)
    
    print("\n" + "="*60)
    print("✅ ANÁLISIS COMPLETADO")
    print("="*60)
    
    # Mostrar resumen
    print(f"\n📊 RESUMEN:")
    print(f"   • Plataforma: {plataforma}")
    print(f"   • Proveedores: {len(proveedores)}")
    print(f"   • Pares analizados: {len(resultados)}")
    
    # Mejor correlación
    if resultados:
        mejor = max(resultados, key=lambda x: abs(x['correlacion']))
        print(f"\n🔗 MEJOR CORRELACIÓN:")
        print(f"   • {mejor['proveedor1']} vs {mejor['proveedor2']}")
        print(f"   • r = {mejor['correlacion']:.3f}")
        print(f"   • {interpretar_correlacion(mejor['correlacion'])}")
    
    print(f"\n📁 ARCHIVOS GENERADOS:")
    plataforma_limpia = plataforma.lower().replace(".", "_").replace(" ", "_")
    print(f"   1. correlacion_pares_{plataforma_limpia}.png")
    print(f"   2. informe_correlacion.txt")
    
    print(f"\n📍 Carpeta: {os.path.abspath(carpeta_resultados)}")

if __name__ == "__main__":
    main()