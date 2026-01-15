import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import sys
import os
import re

def crear_carpeta_resultados():
    """Crea una carpeta para guardar los resultados con timestamp"""
    timestamp = datetime.now().strftime("%d_%m_%Y_%H_%M")
    nombre_carpeta = f"GRAFICAS_ANALISIS_{timestamp}"
    
    if not os.path.exists(nombre_carpeta):
        os.makedirs(nombre_carpeta)
        print(f"📁 Carpeta creada: {nombre_carpeta}")
    
    return nombre_carpeta

def extraer_nombre_plataforma(nombre_archivo):
    """Extrae el nombre de la plataforma del nombre del archivo"""
    nombre_base = os.path.basename(nombre_archivo).lower()
    
    # Buscar patrones comunes de plataformas
    if 'cloudpingnet' in nombre_base:
        return 'CloudPing.net'
    elif 'cloudpingtest' in nombre_base:
        return 'CloudPingTest'
    elif 'cloudpinginfo' in nombre_base:
        return 'CloudPing.info'
    elif 'cloudping' in nombre_base:
        return 'CloudPing'
    
    # Si no encuentra patrones conocidos
    return nombre_base.split('_')[0].title() if '_' in nombre_base else 'Plataforma'

def extraer_nombre_proveedor(nombre_archivo):
    """Extrae el nombre del proveedor del nombre del archivo"""
    nombre_base = os.path.basename(nombre_archivo).lower()
    
    # Diccionario de mapeo de proveedores
    if 'aws' in nombre_base or 'amazon' in nombre_base:
        return 'AWS'
    elif 'azure' in nombre_base or 'microsoft' in nombre_base:
        return 'Azure'
    elif 'gcp' in nombre_base or 'google' in nombre_base:
        return 'GCP'
    elif 'huawei' in nombre_base:
        return 'Huawei Cloud'
    elif 'oracle' in nombre_base:
        return 'Oracle Cloud'
    elif 'ibm' in nombre_base:
        return 'IBM Cloud'
    elif 'alibaba' in nombre_base:
        return 'Alibaba Cloud'
    elif 'digitalocean' in nombre_base:
        return 'DigitalOcean'
    elif 'linode' in nombre_base:
        return 'Linode'
    
    # Si no encuentra, usar la primera parte
    return nombre_base.split('_')[0].upper() if '_' in nombre_base else nombre_base.upper()

def cargar_y_preparar_datos(archivos):
    """Carga los archivos CSV y prepara los datos para el gráfico temporal"""
    dataframes = []
    plataformas_encontradas = set()
    
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
            plataformas_encontradas.add(nombre_plataforma)
            
            print(f"✅ {nombre_proveedor}: {len(df):,} registros")
            
        except Exception as e:
            print(f"❌ Error al cargar '{archivo}': {e}")
    
    if not dataframes:
        print("\n❌ No se pudieron cargar datos válidos de ningún archivo.")
        return None, None
    
    # Combinar todos los DataFrames
    df_combinado = pd.concat(dataframes, ignore_index=True)
    
    print(f"\n📈 RESUMEN DE DATOS CARGADOS:")
    print(f"   • Total de archivos: {len(dataframes)}")
    print(f"   • Total de registros: {len(df_combinado):,}")
    print(f"   • Plataforma: {', '.join(plataformas_encontradas)}")
    print(f"   • Proveedores: {', '.join(df_combinado['proveedor'].unique())}")
    
    return df_combinado, plataformas_encontradas

def crear_grafico_temporal_comparativo(df, carpeta_resultados=None):
    """Crea un gráfico temporal comparativo simple"""
    plt.figure(figsize=(16, 9))
    
    # Configurar estilo
    plt.style.use('default')
    sns.set_style("whitegrid")
    
    # Obtener proveedores únicos y sus colores
    proveedores = df['proveedor'].unique()
    colores = plt.cm.Set2(np.linspace(0, 1, len(proveedores)))
    
    # Crear el gráfico principal
    ax = plt.gca()
    
    # Para cada proveedor, crear su línea temporal
    for i, proveedor in enumerate(proveedores):
        df_proveedor = df[df['proveedor'] == proveedor].copy()
        
        # Agrupar por hora para suavizar la visualización
        df_proveedor['timestamp_hour'] = df_proveedor['timestamp'].dt.floor('H')
        df_agrupado = df_proveedor.groupby('timestamp_hour').agg({
            'latency_ms': 'mean'
        }).reset_index()
        
        # Graficar la línea temporal
        plt.plot(df_agrupado['timestamp_hour'], df_agrupado['latency_ms'],
                label=proveedor,
                color=colores[i],
                linewidth=2.5,
                alpha=0.8,
                marker='o',
                markersize=4,
                markevery=len(df_agrupado)//20 if len(df_agrupado) > 20 else 1)
    
    # Configurar título y etiquetas
    plataforma = df['plataforma'].iloc[0] if 'plataforma' in df.columns else "Plataforma"
    plt.title(f'Comparación Temporal de Latencia - {plataforma}',
              fontsize=20, fontweight='bold', pad=20)
    plt.xlabel('Fecha y Hora', fontsize=14, fontweight='bold', labelpad=10)
    plt.ylabel('Latencia (ms)', fontsize=14, fontweight='bold', labelpad=10)
    
    # Configurar leyenda
    plt.legend(title='Proveedores',
               title_fontsize=12,
               fontsize=11,
               loc='best',
               framealpha=0.9,
               shadow=True)
    
    # Configurar grid
    plt.grid(True, alpha=0.3, linestyle='--')
    ax.yaxis.grid(True, alpha=0.2, linestyle=':')
    
    # Configurar ejes
    plt.xticks(rotation=45, ha='right', fontsize=11)
    plt.yticks(fontsize=11)
    
    # Añadir cuadro de estadísticas
    stats_text = "Estadísticas por Proveedor:\n"
    for proveedor in proveedores:
        df_proveedor = df[df['proveedor'] == proveedor]
        media = df_proveedor['latency_ms'].mean()
        count = len(df_proveedor)
        stats_text += f"{proveedor}: {media:.1f} ms (n={count:,})\n"
    
    plt.text(0.02, 0.98, stats_text,
             transform=ax.transAxes,
             fontsize=10,
             verticalalignment='top',
             bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
    
    # Ajustar márgenes
    plt.tight_layout()
    
    # Guardar gráfico
    if carpeta_resultados:
        # Crear nombre de archivo basado en la plataforma
        plataforma_limpia = plataforma.lower().replace(".", "_").replace(" ", "_")
        nombre_archivo = f'comparacion_temporal_{plataforma_limpia}.png'
        ruta_completa = os.path.join(carpeta_resultados, nombre_archivo)
        plt.savefig(ruta_completa, dpi=300, bbox_inches='tight', facecolor='white')
        print(f"\n💾 Gráfico guardado como: {nombre_archivo}")
        plt.close()
    
    return nombre_archivo if carpeta_resultados else None

def generar_informe_simple(df, carpeta_resultados, archivos_procesados):
    """Genera un informe simple con los resultados"""
    ruta_informe = os.path.join(carpeta_resultados, 'informe_comparacion.txt')
    
    plataforma = df['plataforma'].iloc[0] if 'plataforma' in df.columns else "Desconocida"
    
    with open(ruta_informe, 'w', encoding='utf-8') as f:
        f.write("="*70 + "\n")
        f.write(f"INFORME - COMPARACIÓN TEMPORAL DE LATENCIA\n")
        f.write("="*70 + "\n\n")
        
        f.write(f"Fecha de análisis: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Plataforma: {plataforma}\n")
        f.write(f"Carpeta: {carpeta_resultados}\n\n")
        
        f.write("ARCHIVOS PROCESADOS:\n")
        f.write("-"*35 + "\n")
        for archivo in archivos_procesados:
            proveedor = extraer_nombre_proveedor(archivo)
            f.write(f"• {proveedor}: {os.path.basename(archivo)}\n")
        
        f.write("\nRESUMEN GENERAL:\n")
        f.write("-"*35 + "\n")
        f.write(f"Total registros: {len(df):,}\n")
        f.write(f"Periodo: {df['timestamp'].min()} a {df['timestamp'].max()}\n")
        f.write(f"Duración: {(df['timestamp'].max() - df['timestamp'].min()).days} días\n")
        f.write(f"Proveedores: {df['proveedor'].nunique()}\n")
        
        f.write("\nESTADÍSTICAS POR PROVEEDOR:\n")
        f.write("-"*35 + "\n")
        
        stats = df.groupby('proveedor')['latency_ms'].agg([
            'count', 'mean', 'std', 'min', 'max'
        ]).round(2)
        
        for proveedor, row in stats.iterrows():
            f.write(f"\n{proveedor}:\n")
            f.write(f"  Registros: {row['count']:,}\n")
            f.write(f"  Media: {row['mean']:.2f} ms\n")
            f.write(f"  Desviación: {row['std']:.2f} ms\n")
            f.write(f"  Mínimo: {row['min']:.2f} ms\n")
            f.write(f"  Máximo: {row['max']:.2f} ms\n")
            f.write(f"  Coef. variación: {(row['std']/row['mean']*100):.1f}%\n")
        
        f.write("\nCOMPARATIVA:\n")
        f.write("-"*35 + "\n")
        
        # Identificar mejor y peor
        mejor = stats['mean'].idxmin()
        peor = stats['mean'].idxmax()
        mejor_valor = stats.loc[mejor, 'mean']
        peor_valor = stats.loc[peor, 'mean']
        
        f.write(f"Mejor latencia: {mejor} ({mejor_valor:.1f} ms)\n")
        f.write(f"Peor latencia: {peor} ({peor_valor:.1f} ms)\n")
        f.write(f"Diferencia: {peor_valor - mejor_valor:.1f} ms\n")
        f.write(f"Variación: {(peor_valor - mejor_valor)/mejor_valor*100:.1f}%\n")
        
        f.write("\nGRÁFICO GENERADO:\n")
        f.write("-"*35 + "\n")
        plataforma_limpia = plataforma.lower().replace(".", "_").replace(" ", "_")
        f.write(f"comparacion_temporal_{plataforma_limpia}.png\n")
    
    print(f"📄 Informe generado: informe_comparacion.txt")

def main():
    """Función principal"""
    print("📈 COMPARACIÓN TEMPORAL DE LATENCIA")
    print("="*50)
    
    # Verificar argumentos
    if len(sys.argv) < 2:
        print("Uso: python3 GRAFICADOR.py <archivo1.csv> [archivo2.csv ...]")
        print("\nEjemplo:")
        print("  python3 GRAFICADOR.py aws_cloudpingnet.csv azure_cloudpingnet.csv gcp_cloudpingnet.csv")
        return
    
    archivos = sys.argv[1:]
    print(f"📁 Archivos a procesar: {len(archivos)}")
    print(f"📋 Lista: {', '.join([os.path.basename(f) for f in archivos])}")
    
    # Crear carpeta de resultados
    carpeta_resultados = crear_carpeta_resultados()
    print(f"📁 Carpeta de resultados: {carpeta_resultados}\n")
    
    # Cargar y preparar datos
    df, plataformas = cargar_y_preparar_datos(archivos)
    if df is None:
        return
    
    # Mostrar advertencia si hay múltiples plataformas
    if len(plataformas) > 1:
        print(f"\n⚠️  ADVERTENCIA: Se detectaron {len(plataformas)} plataformas diferentes")
        print(f"   {', '.join(plataformas)}")
        print("   La comparación puede no ser totalmente equitativa.\n")
    
    print("\n" + "="*60)
    print("GENERANDO GRÁFICO DE COMPARACIÓN TEMPORAL")
    print("="*60)
    
    # Crear el gráfico temporal
    crear_grafico_temporal_comparativo(df, carpeta_resultados)
    
    # Generar informe simple
    generar_informe_simple(df, carpeta_resultados, archivos)
    
    print("\n" + "="*60)
    print("✅ ANÁLISIS COMPLETADO")
    print("="*60)
    
    # Mostrar información final
    plataforma = df['plataforma'].iloc[0]
    plataforma_limpia = plataforma.lower().replace(".", "_").replace(" ", "_")
    
    print(f"📊 RESULTADOS:")
    print(f"   • Gráfico: comparacion_temporal_{plataforma_limpia}.png")
    print(f"   • Informe: informe_comparacion.txt")
    print(f"   • Carpeta: {carpeta_resultados}")
    
    # Mostrar ranking de proveedores
    print(f"\n🏆 RANKING POR LATENCIA PROMEDIO:")
    stats = df.groupby('proveedor')['latency_ms'].mean().sort_values()
    for i, (proveedor, latencia) in enumerate(stats.items(), 1):
        print(f"   {i}. {proveedor}: {latencia:.1f} ms")
    
    # Ruta absoluta
    ruta_absoluta = os.path.abspath(carpeta_resultados)
    print(f"\n📍 Ruta absoluta: {ruta_absoluta}")

if __name__ == "__main__":
    main()