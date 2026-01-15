import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from scipy.stats import f_oneway
import sys
import os
from datetime import datetime
import re

def crear_carpeta_resultados(provider_buscado):
    """Crea una carpeta para guardar los resultados con timestamp"""
    timestamp = datetime.now().strftime("%d_%m_%Y_%H_%M")
    # Limpiar nombre del provider para carpeta
    provider_limpio = re.sub(r'[^a-zA-Z0-9]', '_', provider_buscado)
    nombre_carpeta = f"ANOVA_RESULTADOS_{provider_limpio}_{timestamp}"
    
    if not os.path.exists(nombre_carpeta):
        os.makedirs(nombre_carpeta)
        print(f"📁 Carpeta creada: {nombre_carpeta}")
    
    ruta_graficos = os.path.join(nombre_carpeta, 'graficos')
    if not os.path.exists(ruta_graficos):
        os.makedirs(ruta_graficos)
    
    return nombre_carpeta

def cargar_csvs_individualmente(archivos, provider_buscado):
    """Carga cada archivo CSV individualmente y filtra por provider"""
    dataframes_individuales = {}
    total_registros = 0
    
    # Obtener patrones de búsqueda para este provider
    patrones = obtener_patrones_busqueda(provider_buscado)
    print(f"🔍 Patrones de búsqueda para '{provider_buscado}': {', '.join(patrones)}")
    
    for archivo in archivos:
        if not os.path.exists(archivo):
            print(f"⚠️  Advertencia: El archivo '{archivo}' no existe. Se omitirá.")
            continue
            
        try:
            df = pd.read_csv(archivo)
            total_registros += len(df)
            
            # Verificar que tenga las columnas necesarias
            columnas_requeridas = ['timestamp', 'provider', 'region', 'datacenter', 'latency_ms']
            if not all(col in df.columns for col in columnas_requeridas):
                print(f"⚠️  Advertencia: El archivo '{archivo}' no tiene la estructura esperada. Se omitirá.")
                continue
            
            # Crear máscara combinada para todos los patrones
            mascara_combinada = pd.Series([False] * len(df))
            
            for patron in patrones:
                mascara = df['provider'].str.contains(patron, case=False, na=False)
                mascara_combinada = mascara_combinada | mascara
            
            df_filtrado = df[mascara_combinada].copy()
            
            if len(df_filtrado) > 0:
                # Crear nombre amigable para el archivo
                nombre_base = os.path.basename(archivo).replace('.csv', '')
                nombre_amigable = nombre_base.replace(f"{provider_buscado.lower()}_", "").replace("_latency_longterm", "")
                nombre_amigable = nombre_amigable.replace("_", " ").title()
                
                # Si es cloudping.info, usar un nombre más específico
                if "cloudping.info" in nombre_base:
                    nombre_amigable = "CloudPing.info"
                
                df_filtrado['fuente_datos'] = nombre_amigable
                df_filtrado['archivo_origen'] = os.path.basename(archivo)
                df_filtrado['provider_original'] = df_filtrado['provider']
                df_filtrado['provider_normalizado'] = provider_buscado
                
                dataframes_individuales[nombre_amigable] = df_filtrado
                
                # Mostrar estadísticas de este archivo
                nombres_unicos = df_filtrado['provider_original'].unique()
                print(f"✅ Archivo '{archivo}':")
                print(f"   • Fuente: {nombre_amigable}")
                print(f"   • Registros de {provider_buscado}: {len(df_filtrado):,} (de {len(df):,} total)")
                if len(nombres_unicos) <= 3:
                    for nombre in nombres_unicos:
                        count = len(df_filtrado[df_filtrado['provider_original'] == nombre])
                        print(f"   •   '{nombre}': {count:,} registros")
            else:
                print(f"ℹ️  Archivo '{archivo}': 0 registros de {provider_buscado}")
            
        except Exception as e:
            print(f"❌ Error al cargar '{archivo}': {e}")
    
    if not dataframes_individuales:
        print(f"\n❌ No se encontraron registros del provider '{provider_buscado}' en ningún archivo.")
        return None, None
    
    # Combinar todos los DataFrames para análisis general
    df_combinado = pd.concat(list(dataframes_individuales.values()), ignore_index=True)
    
    print(f"\n📊 RESUMEN DE CARGA:")
    print(f"   Total de registros en archivos: {total_registros:,}")
    print(f"   Registros de {provider_buscado}: {len(df_combinado):,}")
    print(f"   Porcentaje: {len(df_combinado)/total_registros*100:.1f}%")
    print(f"   Fuentes de datos encontradas: {len(dataframes_individuales)}")
    
    # Mostrar distribución por fuente de datos
    print(f"\n📋 DISTRIBUCIÓN POR FUENTE DE DATOS:")
    for fuente, df_fuente in dataframes_individuales.items():
        porcentaje = len(df_fuente)/len(df_combinado)*100
        print(f"   • {fuente}: {len(df_fuente):,} registros ({porcentaje:.1f}%)")
    
    return df_combinado, dataframes_individuales

def obtener_patrones_busqueda(provider_buscado):
    """Devuelve los patrones de búsqueda para diferentes proveedores de nube"""
    patrones = {
        # AWS - Amazon Web Services
        'AWS': ['aws', 'amazon web services', 'amazon aws', 'cloudping aws'],
        'AMAZON': ['amazon', 'amazon web services', 'aws'],
        'AMAZON WEB SERVICES': ['amazon web services', 'aws', 'amazon'],
        
        # Google Cloud Platform
        'GCP': ['gcp', 'google cloud', 'google cloud platform'],
        'GOOGLE': ['google', 'google cloud', 'google cloud platform', 'gcp'],
        'GOOGLE CLOUD': ['google cloud', 'google cloud platform', 'gcp'],
        'GOOGLE CLOUD PLATFORM': ['google cloud platform', 'gcp', 'google cloud'],
        
        # Microsoft Azure
        'AZURE': ['azure', 'microsoft azure', 'azure cloud'],
        'MICROSOFT AZURE': ['microsoft azure', 'azure'],
        'MICROSOFT': ['microsoft', 'microsoft azure', 'azure'],
        
        # Huawei Cloud
        'HUAWEI': ['huawei', 'huawei cloud', 'huawei cloud services'],
        'HUAWEI CLOUD': ['huawei cloud', 'huawei'],
        
        # Oracle Cloud
        'ORACLE': ['oracle', 'oracle cloud', 'oracle cloud infrastructure'],
        'ORACLE CLOUD': ['oracle cloud', 'oracle'],
        
        # IBM Cloud
        'IBM': ['ibm', 'ibm cloud'],
        'IBM CLOUD': ['ibm cloud', 'ibm'],
        
        # Alibaba Cloud
        'ALIBABA': ['alibaba', 'alibaba cloud', 'alibaba cloud services'],
        'ALIBABA CLOUD': ['alibaba cloud', 'alibaba'],
        
        # DigitalOcean
        'DIGITALOCEAN': ['digitalocean', 'digital ocean'],
        'DIGITAL OCEAN': ['digital ocean', 'digitalocean'],
        
        # Linode / Akamai
        'LINODE': ['linode', 'akamai linode'],
        'AKAMAI': ['akamai', 'linode'],
    }
    
    # Convertir provider buscado a mayúsculas para buscar en el diccionario
    provider_key = provider_buscado.upper().strip()
    
    if provider_key in patrones:
        return patrones[provider_key]
    else:
        # Si no está en el diccionario, usar el texto original como patrón
        return [provider_buscado.lower()]

def analisis_anova_por_fuente(df_combinado, provider_buscado):
    """Realiza análisis ANOVA comparando las diferentes fuentes de datos"""
    print(f"\n{'='*60}")
    print(f"ANÁLISIS ANOVA: Comparando fuentes de datos de {provider_buscado}")
    print(f"{'='*60}")
    
    # Estadísticas descriptivas por fuente de datos
    print("\n📈 ESTADÍSTICAS DESCRIPTIVAS POR FUENTE DE DATOS:")
    stats_por_fuente = df_combinado.groupby('fuente_datos')['latency_ms'].agg([
        'count', 'mean', 'std', 'min', 'median', 'max'
    ]).round(2)
    print(stats_por_fuente)
    
    # Preparar datos para ANOVA
    grupos = [grupo['latency_ms'].values for nombre, grupo in df_combinado.groupby('fuente_datos')]
    nombres_grupos = [nombre for nombre, _ in df_combinado.groupby('fuente_datos')]
    
    # Prueba de homogeneidad de varianzas (Levene)
    print("\n📊 PRUEBA DE HOMOGENEIDAD DE VARIANZAS (Levene):")
    if len(grupos) >= 2:
        stat_levene, p_levene = stats.levene(*grupos)
        print(f"Estadístico={stat_levene:.4f}, p-valor={p_levene:.4f}")
        if p_levene > 0.05:
            print("✓ Las varianzas son homogéneas (p > 0.05)")
        else:
            print("⚠️  Las varianzas NO son homogéneas (p < 0.05)")
    else:
        print("⚠️  Se necesita al menos 2 grupos para la prueba de Levene")
    
    # Realizar ANOVA
    print("\n🔬 RESULTADOS ANOVA:")
    if len(grupos) >= 2:
        f_stat, p_valor = f_oneway(*grupos)
        print(f"F-estadístico = {f_stat:.4f}")
        print(f"p-valor = {p_valor:.4f}")
        
        if p_valor < 0.05:
            print("\n✅ RESULTADO: Existen diferencias significativas entre las fuentes de datos (p < 0.05)")
            
            # Prueba post-hoc si hay diferencias significativas
            print("\n🔍 PRUEBA POST-HOC (Tukey HSD):")
            from statsmodels.stats.multicomp import pairwise_tukeyhsd
            tukey = pairwise_tukeyhsd(df_combinado['latency_ms'], df_combinado['fuente_datos'], alpha=0.05)
            print(tukey.summary())
            
            # Mostrar comparaciones significativas
            print("\n🎯 COMPARACIONES SIGNIFICATIVAS (p < 0.05):")
            tukey_df = pd.DataFrame(data=tukey.summary().data[1:], columns=tukey.summary().data[0])
            significativas = tukey_df[tukey_df['p-adj'] < 0.05]
            if not significativas.empty:
                for _, row in significativas.iterrows():
                    print(f"  {row['group1']} vs {row['group2']}: diferencia={row['meandiff']:.2f}, p={row['p-adj']:.4f}")
            else:
                print("  No hay comparaciones significativas según Tukey HSD")
        else:
            print("\n❌ RESULTADO: NO existen diferencias significativas entre las fuentes de datos (p ≥ 0.05)")
    else:
        print("⚠️  Se necesita al menos 2 grupos para ANOVA")
        f_stat, p_valor = None, None
    
    return f_stat, p_valor, stats_por_fuente

def crear_violinplot_por_fuente(df_combinado, provider_buscado, carpeta_resultados=None):
    """Crea un violin plot por cada fuente de datos del provider"""
    plt.figure(figsize=(14, 10))
    
    # Configurar estilo
    plt.style.use('default')
    sns.set_style("whitegrid")
    
    # Ordenar fuentes por mediana para mejor visualización
    orden_fuentes = df_combinado.groupby('fuente_datos')['latency_ms'].median().sort_values().index
    
    # Crear violin plot por fuente
    ax = sns.violinplot(data=df_combinado, x='fuente_datos', y='latency_ms', 
                        order=orden_fuentes, palette='Set2', cut=0, inner='quartile')
    
    # Título y etiquetas
    plt.title(f'Distribución de Latencia de {provider_buscado} por Fuente de Datos', 
              fontsize=18, fontweight='bold', pad=20)
    plt.xlabel('Fuente de Datos', fontsize=14, fontweight='bold')
    plt.ylabel('Latencia (ms)', fontsize=14, fontweight='bold')
    plt.xticks(rotation=45, ha='right', fontsize=12)
    plt.yticks(fontsize=12)
    
    # Añadir puntos individuales (muestra reducida para claridad)
    if len(df_combinado) > 2000:
        muestra = df_combinado.sample(n=2000, random_state=42)
        sns.stripplot(data=muestra, x='fuente_datos', y='latency_ms', 
                      order=orden_fuentes, color='black', alpha=0.2, 
                      size=2, jitter=0.3)
    
    # Añadir mediana a cada violín
    medianas = df_combinado.groupby('fuente_datos')['latency_ms'].median().loc[orden_fuentes]
    for i, (fuente, mediana) in enumerate(zip(orden_fuentes, medianas)):
        ax.scatter(i, mediana, color='red', s=100, zorder=5, 
                   label='Mediana' if i == 0 else "", marker='D')
    
    # Añadir leyenda para la mediana
    from matplotlib.lines import Line2D
    legend_elements = [Line2D([0], [0], marker='D', color='w', 
                             markerfacecolor='red', markersize=10, label='Mediana')]
    ax.legend(handles=legend_elements, loc='upper right', fontsize=11)
    
    # Añadir estadísticas generales
    stats_text = f'''Estadísticas Globales de {provider_buscado}:
Total de registros: {len(df_combinado):,}
Media global: {df_combinado['latency_ms'].mean():.1f} ms
Mediana global: {df_combinado['latency_ms'].median():.1f} ms
Número de fuentes: {len(df_combinado['fuente_datos'].unique())}'''
    
    plt.text(0.02, 0.98, stats_text, transform=ax.transAxes,
             fontsize=11, verticalalignment='top',
             bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.8))
    
    # Ajustar layout
    plt.tight_layout()
    
    # Guardar gráfico
    if carpeta_resultados:
        provider_limpio = re.sub(r'[^a-zA-Z0-9]', '_', provider_buscado)
        nombre_archivo = f'violinplot_fuentes_{provider_limpio}.png'
        ruta_completa = os.path.join(carpeta_resultados, 'graficos', nombre_archivo)
        plt.savefig(ruta_completa, dpi=300, bbox_inches='tight', facecolor='white')
        print(f"💾 Violin plot por fuente guardado como: {ruta_completa}")
        plt.close()
    
    return nombre_archivo if carpeta_resultados else None

def crear_boxplot_varianza_por_fuente(df_combinado, provider_buscado, carpeta_resultados=None):
    """Crea un box plot para mostrar varianza por fuente de datos"""
    plt.figure(figsize=(14, 10))
    
    # Configurar estilo
    sns.set_style("whitegrid")
    
    # Ordenar fuentes por mediana
    orden_fuentes = df_combinado.groupby('fuente_datos')['latency_ms'].median().sort_values().index
    
    # Crear box plot por fuente
    ax = sns.boxplot(data=df_combinado, x='fuente_datos', y='latency_ms', 
                     order=orden_fuentes, palette='Set3', showfliers=True,
                     flierprops=dict(marker='o', markersize=4, alpha=0.3))
    
    # Título y etiquetas
    plt.title(f'Varianza de Latencia de {provider_buscado} por Fuente de Datos', 
              fontsize=18, fontweight='bold', pad=20)
    plt.xlabel('Fuente de Datos', fontsize=14, fontweight='bold')
    plt.ylabel('Latencia (ms)', fontsize=14, fontweight='bold')
    plt.xticks(rotation=45, ha='right', fontsize=12)
    plt.yticks(fontsize=12)
    
    # Calcular y mostrar estadísticas de varianza por fuente
    varianzas = df_combinado.groupby('fuente_datos')['latency_ms'].var()
    iqrs = df_combinado.groupby('fuente_datos')['latency_ms'].apply(
        lambda x: x.quantile(0.75) - x.quantile(0.25)
    )
    
    # Añadir anotaciones de varianza para cada fuente
    for i, (fuente, varianza, iqr) in enumerate(zip(orden_fuentes, 
                                                    varianzas.loc[orden_fuentes], 
                                                    iqrs.loc[orden_fuentes])):
        # Mostrar varianza e IQR
        ax.text(i, ax.get_ylim()[1] * 0.95, 
                f'Var: {varianza:,.0f}\nIQR: {iqr:.0f}', 
                ha='center', va='top', fontsize=9,
                bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.7))
    
    # Añadir cuadrícula
    plt.grid(True, alpha=0.3, axis='y')
    
    # Ajustar layout
    plt.tight_layout()
    
    # Guardar gráfico
    if carpeta_resultados:
        provider_limpio = re.sub(r'[^a-zA-Z0-9]', '_', provider_buscado)
        nombre_archivo = f'boxplot_varianza_fuentes_{provider_limpio}.png'
        ruta_completa = os.path.join(carpeta_resultados, 'graficos', nombre_archivo)
        plt.savefig(ruta_completa, dpi=300, bbox_inches='tight', facecolor='white')
        print(f"💾 Box plot de varianza por fuente guardado como: {ruta_completa}")
        plt.close()
    
    return nombre_archivo if carpeta_resultados else None

def crear_barras_medias_por_fuente(df_combinado, provider_buscado, carpeta_resultados=None):
    """Crea un gráfico de barras con medias por fuente de datos"""
    plt.figure(figsize=(16, 12))
    
    # Configurar estilo
    sns.set_style("whitegrid")
    
    # Calcular estadísticas por fuente
    stats_por_fuente = df_combinado.groupby('fuente_datos')['latency_ms'].agg(['mean', 'std', 'count']).round(2)
    stats_por_fuente['error_estandar'] = stats_por_fuente['std'] / np.sqrt(stats_por_fuente['count'])
    
    # Ordenar por media
    stats_por_fuente = stats_por_fuente.sort_values('mean')
    
    # Crear gráfico de barras
    x_pos = range(len(stats_por_fuente))
    colors = plt.cm.viridis(np.linspace(0.2, 0.9, len(stats_por_fuente)))
    
    barras = plt.bar(x_pos, stats_por_fuente['mean'].values, 
                     yerr=stats_por_fuente['error_estandar'].values,
                     capsize=12, color=colors, 
                     edgecolor='black', linewidth=2, 
                     alpha=0.85, error_kw={'elinewidth': 2.5, 'ecolor': 'darkred', 'capthick': 2})
    
    # Título y etiquetas
    plt.title(f'Comparativa de Latencia Media: {provider_buscado} por Fuente de Datos', 
              fontsize=20, fontweight='bold', pad=25)
    plt.xlabel('Fuente de Datos', fontsize=16, fontweight='bold')
    plt.ylabel('Latencia Media (ms)', fontsize=16, fontweight='bold')
    
    # Etiquetas del eje X
    etiquetas = [str(fuente) for fuente in stats_por_fuente.index]
    plt.xticks(x_pos, etiquetas, rotation=45, ha='right', fontsize=12)
    plt.yticks(fontsize=13)
    
    # Añadir valores en las barras
    for i, (v, err, count) in enumerate(zip(stats_por_fuente['mean'].values, 
                                            stats_por_fuente['error_estandar'].values,
                                            stats_por_fuente['count'].values)):
        plt.text(i, v + err + stats_por_fuente['mean'].max()*0.02, 
                f'{v:.1f} ± {err:.1f}\nn={count:,}', 
                ha='center', va='bottom', 
                fontsize=11, fontweight='bold',
                bbox=dict(boxstyle='round,pad=0.4', facecolor='white', alpha=0.9))
    
    # Añadir línea de media global
    media_global = df_combinado['latency_ms'].mean()
    plt.axhline(y=media_global, color='green', linestyle='--', linewidth=3, 
                label=f'Media global {provider_buscado}: {media_global:.1f} ms', alpha=0.8)
    
    # Añadir intervalo de confianza 95% para cada barra
    for i, (v, err) in enumerate(zip(stats_por_fuente['mean'].values, 
                                     stats_por_fuente['error_estandar'].values)):
        # Línea vertical principal para IC 95%
        plt.plot([i, i], [v - 1.96*err, v + 1.96*err], color='darkred', linewidth=2.5, alpha=0.7)
        # Líneas horizontales en los extremos
        plt.plot([i-0.15, i+0.15], [v - 1.96*err, v - 1.96*err], color='darkred', linewidth=2.5, alpha=0.7)
        plt.plot([i-0.15, i+0.15], [v + 1.96*err, v + 1.96*err], color='darkred', linewidth=2.5, alpha=0.7)
    
    # Añadir leyenda
    plt.legend(loc='upper right', fontsize=12)
    
    # Añadir cuadrícula
    plt.grid(True, alpha=0.3, axis='y')
    
    # Ajustar layout
    plt.tight_layout()
    
    # Guardar gráfico
    if carpeta_resultados:
        provider_limpio = re.sub(r'[^a-zA-Z0-9]', '_', provider_buscado)
        nombre_archivo = f'barras_medias_fuentes_{provider_limpio}.png'
        ruta_completa = os.path.join(carpeta_resultados, 'graficos', nombre_archivo)
        plt.savefig(ruta_completa, dpi=300, bbox_inches='tight', facecolor='white')
        print(f"💾 Gráfico de barras por fuente guardado como: {ruta_completa}")
        plt.close()
    
    return nombre_archivo if carpeta_resultados else None

def generar_informe_completo(df_combinado, provider_buscado, dataframes_individuales, 
                            resultados_anova, carpeta_resultados, archivos_procesados):
    """Genera un informe completo con los resultados"""
    ruta_informe = os.path.join(carpeta_resultados, f'informe_{provider_buscado}.txt')
    
    f_stat, p_valor, stats_por_fuente = resultados_anova
    
    with open(ruta_informe, 'w', encoding='utf-8') as f:
        f.write("="*80 + "\n")
        f.write(f"INFORME - ANÁLISIS COMPARATIVO DE {provider_buscado} POR FUENTE DE DATOS\n")
        f.write("="*80 + "\n\n")
        
        f.write(f"Fecha de análisis: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Proveedor analizado: {provider_buscado}\n")
        f.write(f"Carpeta de resultados: {carpeta_resultados}\n")
        f.write(f"Número de fuentes comparadas: {len(dataframes_individuales)}\n\n")
        
        f.write("ARCHIVOS PROCESADOS:\n")
        f.write("-"*40 + "\n")
        for archivo in archivos_procesados:
            f.write(f"  • {archivo}\n")
        
        f.write("\nRESUMEN GENERAL:\n")
        f.write("-"*40 + "\n")
        f.write(f"Total de registros: {len(df_combinado):,}\n")
        if 'timestamp' in df_combinado.columns:
            f.write(f"Periodo cubierto: {df_combinado['timestamp'].min()} a {df_combinado['timestamp'].max()}\n")
        
        f.write("\nFUENTES DE DATOS ANALIZADAS:\n")
        f.write("-"*40 + "\n")
        for fuente, df_fuente in dataframes_individuales.items():
            porcentaje = len(df_fuente)/len(df_combinado)*100
            f.write(f"\n{fuente}:\n")
            f.write(f"  Registros: {len(df_fuente):,} ({porcentaje:.1f}%)\n")
            f.write(f"  Media: {df_fuente['latency_ms'].mean():.2f} ms\n")
            f.write(f"  Mediana: {df_fuente['latency_ms'].median():.2f} ms\n")
        
        f.write("\nESTADÍSTICAS DETALLADAS POR FUENTE:\n")
        f.write("-"*40 + "\n")
        f.write(stats_por_fuente.to_string())
        
        f.write("\n\n" + "="*80 + "\n")
        f.write("RESULTADOS ANOVA:\n")
        f.write("="*80 + "\n\n")
        
        if f_stat is not None and p_valor is not None:
            f.write(f"F-estadístico: {f_stat:.4f}\n")
            f.write(f"p-valor: {p_valor:.4f}\n")
            
            if p_valor < 0.05:
                f.write("\nCONCLUSIÓN: Existen diferencias SIGNIFICATIVAS entre las fuentes de datos\n")
            else:
                f.write("\nCONCLUSIÓN: NO existen diferencias significativas entre las fuentes de datos\n")
        else:
            f.write("⚠️  No se pudo realizar ANOVA (se necesitan al menos 2 fuentes de datos)\n")
        
        # Identificar mejor y peor fuente
        if len(dataframes_individuales) > 1:
            mejor_fuente = min(dataframes_individuales.keys(), 
                             key=lambda x: dataframes_individuales[x]['latency_ms'].mean())
            peor_fuente = max(dataframes_individuales.keys(), 
                            key=lambda x: dataframes_individuales[x]['latency_ms'].mean())
            
            mejor_valor = dataframes_individuales[mejor_fuente]['latency_ms'].mean()
            peor_valor = dataframes_individuales[peor_fuente]['latency_ms'].mean()
            
            f.write("\n" + "="*80 + "\n")
            f.write("COMPARATIVA DE FUENTES:\n")
            f.write("="*80 + "\n\n")
            f.write(f"Mejor latencia promedio: {mejor_fuente} ({mejor_valor:.1f} ms)\n")
            f.write(f"Peor latencia promedio: {peor_fuente} ({peor_valor:.1f} ms)\n")
            f.write(f"Diferencia: {peor_valor - mejor_valor:.1f} ms\n")
            f.write(f"Variación porcentual: {(peor_valor - mejor_valor)/mejor_valor*100:.1f}%\n")
        
        f.write("\n" + "="*80 + "\n")
        f.write("GRÁFICOS GENERADOS:\n")
        f.write("="*80 + "\n\n")
        provider_limpio = re.sub(r'[^a-zA-Z0-9]', '_', provider_buscado)
        f.write(f"1. violinplot_fuentes_{provider_limpio}.png - Distribución por fuente\n")
        f.write(f"2. boxplot_varianza_fuentes_{provider_limpio}.png - Varianza por fuente\n")
        f.write(f"3. barras_medias_fuentes_{provider_limpio}.png - Medias comparativas\n")
    
    print(f"📄 Informe generado: {ruta_informe}")

def main():
    """Función principal"""
    print("🔍 ANÁLISIS COMPARATIVO DE LATENCIA POR FUENTE DE DATOS")
    print("="*60)
    
    # Verificar argumentos
    if len(sys.argv) < 3:
        print("Uso: python3 ANOVA.py <provider> <archivo1.csv> [archivo2.csv ...]")
        print("\nEjemplos:")
        print("  python3 ANOVA.py AWS aws_cloudping.csv aws_cloudpingnet.csv")
        print("  python3 ANOVA.py AWS *.csv")
        print("  python3 ANOVA.py GCP datos_google.csv")
        print("\nNOTA: Cada archivo se tratará como una fuente de datos separada")
        return
    
    # Obtener provider y archivos
    provider_buscado = sys.argv[1]
    archivos = sys.argv[2:]
    
    print(f"🔎 Proveedor a analizar: '{provider_buscado}'")
    print(f"📁 Archivos a procesar: {len(archivos)} archivo(s)")
    print(f"📋 Cada archivo será tratado como fuente de datos independiente\n")
    
    # Crear carpeta de resultados
    carpeta_resultados = crear_carpeta_resultados(provider_buscado)
    print(f"📁 Carpeta de resultados: {carpeta_resultados}\n")
    
    # Cargar datos individualmente por archivo
    print("📊 CARGANDO DATOS POR ARCHIVO...")
    df_combinado, dataframes_individuales = cargar_csvs_individualmente(archivos, provider_buscado)
    
    if df_combinado is None:
        return
    
    # Mostrar información básica
    print(f"\n📊 DATOS COMBINADOS DE {provider_buscado}:")
    print(f"Total de registros: {len(df_combinado):,}")
    print(f"Número de fuentes: {len(dataframes_individuales)}")
    
    # Análisis ANOVA por fuente de datos
    resultados_anova = analisis_anova_por_fuente(df_combinado, provider_buscado)
    
    print("\n" + "="*80)
    print(f"GENERANDO GRÁFICOS COMPARATIVOS")
    print("="*80)
    
    # Generar los 3 gráficos comparativos
    print(f"\n📊 Generando violin plot por fuente...")
    crear_violinplot_por_fuente(df_combinado, provider_buscado, carpeta_resultados)
    
    print(f"\n📊 Generando box plot de varianza por fuente...")
    crear_boxplot_varianza_por_fuente(df_combinado, provider_buscado, carpeta_resultados)
    
    print(f"\n📊 Generando gráfico de barras comparativo...")
    crear_barras_medias_por_fuente(df_combinado, provider_buscado, carpeta_resultados)
    
    # Generar informe completo
    generar_informe_completo(df_combinado, provider_buscado, dataframes_individuales,
                            resultados_anova, carpeta_resultados, archivos)
    
    print("\n" + "="*80)
    print("✅ ANÁLISIS COMPARATIVO COMPLETADO")
    print("="*80)
    provider_limpio = re.sub(r'[^a-zA-Z0-9]', '_', provider_buscado)
    print(f"📁 Todos los resultados guardados en: {carpeta_resultados}")
    print(f"📈 Gráficos generados:")
    print(f"   1. violinplot_fuentes_{provider_limpio}.png - Distribución por fuente")
    print(f"   2. boxplot_varianza_fuentes_{provider_limpio}.png - Varianza por fuente")
    print(f"   3. barras_medias_fuentes_{provider_limpio}.png - Comparativa de medias")
    print(f"📄 Informe: informe_{provider_buscado}.txt")
    
    # Mostrar ruta absoluta
    ruta_absoluta = os.path.abspath(carpeta_resultados)
    print(f"\n📍 Ruta absoluta: {ruta_absoluta}")

if __name__ == "__main__":
    main()