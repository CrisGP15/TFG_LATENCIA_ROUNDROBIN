"""
TFG - ANOVA CON LATENCIA NORMALIZADA
Fórmula: Latencia normalizada = Latencia (ms) / Distancia (km)
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
import statsmodels.api as sm
from statsmodels.formula.api import ols
from statsmodels.stats.multicomp import pairwise_tukeyhsd
import os
import warnings
warnings.filterwarnings('ignore')

# ============================================
# CONFIGURACIÓN
# ============================================
CARPETA = "ANOVA_Resultados"
os.makedirs(CARPETA, exist_ok=True)

COLORES = {
    'AWS': '#FF9900',
    'Azure': '#0078D4',
    'GCP': '#4285F4',
    'Huawei': '#FF0000'
}

# Coordenadas UAM Madrid
UAM_MADRID = (40.5449, -3.6969)

# Coordenadas aproximadas de regiones
COORDENADAS = {
    'AWS': {
        'eu-south-2': (40.4168, -3.7038),      # Madrid
        'eu-west-3': (48.8566, 2.3522),        # París
        'eu-west-1': (53.3498, -6.2603),       # Dublín
        'us-east-1': (39.0438, -77.4874),      # Virginia
        'ap-southeast-1': (1.3521, 103.8198),  # Singapur
    },
    'Azure': {
        'francecentral': (48.8566, 2.3522),    # París
        'eastus': (39.0438, -77.4874),         # Virginia
        'southeastasia': (1.3521, 103.8198),   # Singapur
    },
    'GCP': {
        'europe-southwest1': (40.4168, -3.7038),  # Madrid
        'europe-west1': (50.8503, 4.3517),     # Bélgica
        'us-central1': (41.8781, -93.0977),    # Iowa
        'asia-southeast1': (1.3521, 103.8198), # Singapur
    },
    'Huawei': {
        'eu-west-0': (48.8566, 2.3522),        # París
        'ap-southeast-1': (1.3521, 103.8198),  # Singapur
        'cn-north-1': (39.9042, 116.4074),     # Beijing
    }
}

# ============================================
# FUNCIONES DE CÁLCULO
# ============================================
def calcular_distancia(coord1, coord2):
    """Calcula distancia en km usando Haversine"""
    from math import radians, sin, cos, sqrt, atan2
    R = 6371.0
    lat1, lon1 = map(radians, coord1)
    lat2, lon2 = map(radians, coord2)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    return R * c

def cargar_y_normalizar():
    """Carga datos y calcula latencia normalizada"""
    print("📊 CARGANDO Y NORMALIZANDO DATOS")
    print("="*50)
    
    archivos = {
        'AWS': 'aws_cloudping_latency_longterm.csv',
        'Huawei': 'huawei_cloudping_latency_longterm.csv',
        'Azure': 'azure_cloudpingnet_latency_longterm.csv',
        'GCP': 'gcp_cloudpingnet_latency_longterm.csv'
    }
    
    datos_todos = []
    
    for proveedor, archivo in archivos.items():
        try:
            if not os.path.exists(archivo):
                print(f"⚠️  {archivo} no encontrado")
                continue
            
            print(f"📂 {proveedor}...")
            df = pd.read_csv(archivo)
            df.columns = df.columns.str.lower().str.strip()
            
            # Buscar columna de latencia
            col_latencia = None
            for col in df.columns:
                if 'latency' in col or 'ping' in col or 'ms' in col:
                    col_latencia = col
                    break
            
            if col_latencia is None:
                continue
            
            # Limpiar datos
            df_limpio = df[[col_latencia]].copy()
            df_limpio = df_limpio.dropna()
            df_limpio = df_limpio[df_limpio[col_latencia] > 0]
            
            # Buscar región
            col_region = None
            for col in df.columns:
                if 'region' in col or 'location' in col:
                    col_region = col
                    break
            
            if col_region:
                df_limpio['region'] = df[col_region]
            else:
                df_limpio['region'] = 'default'
            
            df_limpio['latency_ms'] = df_limpio[col_latencia]
            df_limpio['provider'] = proveedor
            
            datos_todos.append(df_limpio[['provider', 'region', 'latency_ms']])
            print(f"   ✅ {len(df_limpio):,} registros")
            
        except Exception as e:
            print(f"   ❌ Error: {str(e)}")
    
    if not datos_todos:
        raise ValueError("No se cargaron datos")
    
    df_completo = pd.concat(datos_todos, ignore_index=True)
    
    # Calcular latencia normalizada
    print(f"\n🧮 CALCULANDO LATENCIA NORMALIZADA...")
    resultados = []
    
    for idx, row in df_completo.iterrows():
        proveedor = row['provider']
        region = row['region']
        
        # Buscar coordenadas para calcular distancia
        distancia_km = 1000  # Valor por defecto
        
        if proveedor in COORDENADAS:
            for reg_pattern, coords in COORDENADAS[proveedor].items():
                if reg_pattern.lower() in str(region).lower():
                    distancia_km = calcular_distancia(UAM_MADRID, coords)
                    break
        
        # Aplicar fórmula: Latencia normalizada = Latencia / Distancia
        if distancia_km > 0:
            latencia_norm = row['latency_ms'] / distancia_km
        else:
            latencia_norm = row['latency_ms']
        
        resultados.append({
            'provider': proveedor,
            'region': region,
            'latency_ms': row['latency_ms'],
            'distancia_km': distancia_km,
            'latencia_norm': latencia_norm
        })
    
    df_resultados = pd.DataFrame(resultados)
    
    # Guardar
    ruta = os.path.join(CARPETA, 'latencia_normalizada.csv')
    df_resultados.to_csv(ruta, index=False)
    print(f"💾 Datos guardados: {ruta}")
    
    return df_resultados

# ============================================
# GRÁFICAS DE LATENCIA NORMALIZADA
# ============================================
def crear_graficas(df):
    """Crea gráficas solo de latencia normalizada"""
    print(f"\n🎨 CREANDO GRÁFICAS DE LATENCIA NORMALIZADA")
    print("="*50)
    
    # 1. Boxplot de latencia normalizada
    plt.figure(figsize=(12, 8))
    
    orden = df.groupby('provider')['latencia_norm'].median().sort_values().index
    
    sns.boxplot(x='provider', y='latencia_norm', data=df,
                order=orden, palette=COLORES)
    
    plt.title('Distribución de Latencia Normalizada por Proveedor', 
              fontsize=16, fontweight='bold')
    plt.xlabel('Proveedor Cloud', fontsize=12)
    plt.ylabel('Latencia Normalizada (ms/km)', fontsize=12)
    plt.grid(True, alpha=0.3)
    
    # Añadir medianas
    for i, provider in enumerate(orden):
        median_val = df[df['provider'] == provider]['latencia_norm'].median()
        plt.text(i, median_val, f'{median_val:.4f}', 
                ha='center', va='bottom', fontweight='bold')
    
    plt.tight_layout()
    ruta1 = os.path.join(CARPETA, 'boxplot_normalizado.png')
    plt.savefig(ruta1, dpi=300, bbox_inches='tight')
    print(f"✅ Boxplot: {ruta1}")
    plt.close()
    
    # 2. Violin plot
    plt.figure(figsize=(12, 8))
    
    sns.violinplot(x='provider', y='latencia_norm', data=df,
                   order=orden, palette=COLORES, cut=0, inner='quartile')
    
    plt.title('Distribución Detallada - Latencia Normalizada', 
              fontsize=16, fontweight='bold')
    plt.xlabel('Proveedor Cloud', fontsize=12)
    plt.ylabel('Latencia Normalizada (ms/km)', fontsize=12)
    plt.grid(True, alpha=0.3)
    
    plt.tight_layout()
    ruta2 = os.path.join(CARPETA, 'violinplot_normalizado.png')
    plt.savefig(ruta2, dpi=300, bbox_inches='tight')
    print(f"✅ Violin plot: {ruta2}")
    plt.close()
    
    # 3. Gráfico de barras (medias)
    plt.figure(figsize=(10, 6))
    
    medias = df.groupby('provider')['latencia_norm'].mean().sort_values()
    errores = df.groupby('provider')['latencia_norm'].std() / np.sqrt(df.groupby('provider').size())
    
    colores = [COLORES[p] for p in medias.index]
    bars = plt.bar(medias.index, medias.values, yerr=errores.values,
                   capsize=10, alpha=0.8, color=colores, edgecolor='black')
    
    plt.title('Latencia Normalizada Media por Proveedor', 
              fontsize=14, fontweight='bold')
    plt.xlabel('Proveedor', fontsize=12)
    plt.ylabel('Latencia Normalizada (ms/km)', fontsize=12)
    plt.grid(True, alpha=0.3, axis='y')
    
    # Añadir valores
    for bar, valor in zip(bars, medias.values):
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + 0.0001,
                f'{valor:.6f}', ha='center', va='bottom', fontsize=9, fontweight='bold')
    
    plt.tight_layout()
    ruta3 = os.path.join(CARPETA, 'barras_medias_normalizada.png')
    plt.savefig(ruta3, dpi=300, bbox_inches='tight')
    print(f"✅ Barras medias: {ruta3}")
    plt.close()

# ============================================
# ANÁLISIS ANOVA
# ============================================
def analisis_anova(df):
    """Realiza análisis ANOVA con latencia normalizada"""
    print(f"\n📈 ANÁLISIS ANOVA - LATENCIA NORMALIZADA")
    print("="*50)
    
    # Estadísticas
    print("\n📊 ESTADÍSTICAS:")
    stats_df = df.groupby('provider')['latencia_norm'].describe().round(6)
    print(stats_df)
    
    # Guardar estadísticas
    ruta_stats = os.path.join(CARPETA, 'estadisticas.csv')
    stats_df.to_csv(ruta_stats)
    
    # ANOVA
    model = ols('latencia_norm ~ C(provider)', data=df).fit()
    anova_table = sm.stats.anova_lm(model, typ=2)
    
    print(f"\n📋 TABLA ANOVA:")
    print("-" * 40)
    print(anova_table.round(6))
    print("-" * 40)
    
    p_value = anova_table['PR(>F)']['C(provider)']
    f_value = anova_table['F']['C(provider)']
    
    # Guardar ANOVA
    ruta_anova = os.path.join(CARPETA, 'anova.csv')
    anova_table.to_csv(ruta_anova)
    
    print(f"\n📊 RESULTADO:")
    print(f"• F-valor: {f_value:.4f}")
    print(f"• p-valor: {p_value:.6f}")
    
    if p_value < 0.05:
        print("• ✅ SIGNIFICATIVO (p < 0.05)")
        print("  → Hay diferencias entre proveedores")
        
        # Tukey HSD
        print(f"\n🔍 TEST TUKEY HSD:")
        tukey = pairwise_tukeyhsd(df['latencia_norm'], df['provider'], alpha=0.05)
        print(tukey.summary())
        
        # Guardar Tukey
        tukey_df = pd.DataFrame(data=tukey.summary().data[1:], 
                              columns=tukey.summary().data[0])
        ruta_tukey = os.path.join(CARPETA, 'tukey.csv')
        tukey_df.to_csv(ruta_tukey, index=False)
        
    else:
        print("• ❌ NO SIGNIFICATIVO (p ≥ 0.05)")
        print("  → No hay diferencias significativas")
    
    return p_value, f_value

# ============================================
# FUNCIÓN PRINCIPAL
# ============================================
def main():
    """Ejecuta análisis completo"""
    print("="*60)
    print("ANÁLISIS ANOVA - LATENCIA NORMALIZADA")
    print("="*60)
    print("Fórmula: Latencia Normalizada = Latencia (ms) / Distancia (km)")
    print(f"Resultados en: {CARPETA}/")
    print("="*60)
    
    try:
        # 1. Cargar y normalizar
        df = cargar_y_normalizar()
        
        # 2. Crear gráficas
        crear_graficas(df)
        
        # 3. ANOVA
        p_value, f_value = analisis_anova(df)
        
        # Resumen
        print(f"\n{'='*60}")
        print("✅ ANÁLISIS COMPLETADO")
        print(f"{'='*60}")
        print(f"📁 Resultados en: {CARPETA}/")
        print(f"📊 ANOVA: {'SIGNIFICATIVO' if p_value < 0.05 else 'NO SIGNIFICATIVO'}")
        
        # Mostrar archivos
        archivos = os.listdir(CARPETA)
        print(f"\n📋 Archivos generados:")
        for archivo in sorted(archivos):
            print(f"  • {archivo}")
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")

# ============================================
# EJECUTAR
# ============================================
if __name__ == "__main__":
    # Instalar dependencias si faltan
    try:
        import statsmodels
    except ImportError:
        print("Instalando dependencias...")
        import subprocess
        subprocess.check_call(['pip', 'install', 'statsmodels', 'seaborn', 'scipy'])
    
    main()