#!/usr/bin/env python3
"""
Análisis de latencia para determinar rentabilidad de sistemas multiservidor vs monoservidor
Procesa archivos específicos de diferentes proveedores y herramientas de ping
Autor: Analista de Infraestructura
Fecha: 2024
"""

import pandas as pd
import numpy as np
import glob
import os
import warnings
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
import json
from pathlib import Path
import gc
import re

# Configuración
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

# Configuración de estilo para gráficos
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

class AnalizadorLatenciaMultiProveedor:
    def __init__(self, ruta_datos, umbral_latencia=100, umbral_disponibilidad=0.95):
        """
        Inicializa el analizador de latencia para múltiples proveedores
        
        Args:
            ruta_datos: Ruta a los archivos CSV
            umbral_latencia: Latencia máxima aceptable en ms
            umbral_disponibilidad: Disponibilidad mínima aceptable
        """
        self.ruta_datos = ruta_datos
        self.umbral_latencia = umbral_latencia
        self.umbral_disponibilidad = umbral_disponibilidad
        self.df_completo = None
        self.metricas_cache = {}
        
        # Lista de archivos esperados
        self.archivos_esperados = [
            'aws_cloudping_latency_longterm.csv',
            'aws_cloudpingnet_latency_longterm.csv',
            'aws_cloudpingtest_latency_longterm.csv',
            'azure_cloudpingnet_latency_longterm.csv',
            'azure_cloudpingtest_latency_longterm.csv',
            'cloudpingco_latency_longterm.csv',
            'cloudpinginfo_latency_longterm.csv',
            'gcp_cloudpingnet_latency_longterm.csv',
            'gcp_cloudpingtest_latency_longterm.csv',
            'huawei_cloudping_latency_longterm.csv'
        ]
        
        # Mapeo para extraer proveedor real del nombre del archivo
        self.mapeo_proveedores = {
            'aws_': 'aws',
            'azure_': 'azure',
            'gcp_': 'gcp',
            'huawei_': 'huawei',
            'cloudpingco_': 'multi',  # Mide entre regiones
            'cloudpinginfo_': 'multi'  # Mide múltiples proveedores
        }
        
    def cargar_y_unificar_datos(self, muestra_porcentaje=100):
        """
        Carga y unifica todos los archivos CSV adaptándose a sus formatos específicos
        """
        print("=" * 80)
        print("CARGANDO Y PROCESANDO ARCHIVOS DE DATOS")
        print("=" * 80)
        
        dfs = []
        archivos_procesados = []
        archivos_no_encontrados = []
        
        print(f"📁 Buscando archivos en: {os.path.abspath(self.ruta_datos)}")
        print(f"📋 Archivos en el directorio: {len(os.listdir(self.ruta_datos)) if os.path.exists(self.ruta_datos) else 'Directorio no existe'} archivos")
        
        for archivo_nombre in self.archivos_esperados:
            archivo_path = os.path.join(self.ruta_datos, archivo_nombre)
            
            if not os.path.exists(archivo_path):
                archivos_no_encontrados.append(archivo_nombre)
                continue
                
            print(f"\n✅ Procesando: {archivo_nombre}")
            
            try:
                # Leer primero las primeras líneas para inspeccionar
                with open(archivo_path, 'r') as f:
                    primeras_lineas = [next(f) for _ in range(3)]
                
                print(f"  📄 Muestra de datos (3 primeras líneas):")
                for i, linea in enumerate(primeras_lineas):
                    print(f"    Línea {i+1}: {linea.strip()}")
                
                # Determinar el formato del archivo
                if archivo_nombre == 'cloudpingco_latency_longterm.csv':
                    # Formato especial: from_region, to_region
                    try:
                        df = pd.read_csv(archivo_path, 
                                       names=['timestamp', 'provider', 'from_region', 'to_region', 'latency_ms'],
                                       parse_dates=['timestamp'])
                    except Exception as e:
                        print(f"  ⚠️  Error leyendo con parse_dates, intentando sin: {e}")
                        df = pd.read_csv(archivo_path, 
                                       names=['timestamp', 'provider', 'from_region', 'to_region', 'latency_ms'])
                        # Intentar convertir timestamp manualmente
                        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
                    
                    # Extraer proveedor del nombre del archivo
                    proveedor_real = None
                    for key, value in self.mapeo_proveedores.items():
                        if archivo_nombre.startswith(key):
                            proveedor_real = value
                            break
                    
                    # Para cloudping.co, usar el proveedor de destino
                    df['provider'] = proveedor_real
                    df['region'] = df['to_region']
                    df['datacenter'] = df['to_region']
                    
                else:
                    # Formato estándar: timestamp,provider,region,datacenter,latency_ms
                    try:
                        df = pd.read_csv(archivo_path, 
                                       names=['timestamp', 'provider', 'region', 'datacenter', 'latency_ms'],
                                       parse_dates=['timestamp'])
                    except Exception as e:
                        print(f"  ⚠️  Error leyendo con parse_dates, intentando sin: {e}")
                        df = pd.read_csv(archivo_path, 
                                       names=['timestamp', 'provider', 'region', 'datacenter', 'latency_ms'])
                        # Intentar convertir timestamp manualmente
                        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
                
                # Verificar que timestamp sea datetime
                if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
                    print(f"  ⚠️  timestamp no es datetime, intentando convertir...")
                    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
                
                # Verificar si hay valores nulos en timestamp
                nulos_timestamp = df['timestamp'].isnull().sum()
                if nulos_timestamp > 0:
                    print(f"  ⚠️  {nulos_timestamp} timestamps nulos encontrados, eliminando...")
                    df = df.dropna(subset=['timestamp'])
                
                # Añadir información del archivo
                df['archivo_fuente'] = archivo_nombre
                df['proveedor_real'] = self._extraer_proveedor_real(archivo_nombre)
                df['herramienta'] = self._extraer_herramienta(archivo_nombre)
                
                # Limpieza específica por archivo
                df = self._limpiar_datos_especificos(df, archivo_nombre)
                
                if muestra_porcentaje < 100:
                    df = df.sample(frac=muestra_porcentaje/100, random_state=42)
                
                dfs.append(df)
                archivos_procesados.append(archivo_nombre)
                
                print(f"  ✅ {len(df):,} registros cargados")
                print(f"  📅 Período: {df['timestamp'].min()} a {df['timestamp'].max()}")
                print(f"  🏢 Proveedor detectado: {df['proveedor_real'].iloc[0]}")
                print(f"  🛠️  Herramienta: {df['herramienta'].iloc[0]}")
                
                # Liberar memoria
                gc.collect()
                
            except Exception as e:
                print(f"  ❌ Error procesando {archivo_nombre}: {str(e)}")
                import traceback
                traceback.print_exc()
                continue
        
        # Reporte de archivos
        print(f"\n{'='*60}")
        print("RESUMEN DE CARGA DE ARCHIVOS")
        print(f"{'='*60}")
        print(f"✅ Archivos procesados: {len(archivos_procesados)}/{len(self.archivos_esperados)}")
        
        if archivos_no_encontrados:
            print(f"\n⚠️  Archivos no encontrados:")
            for archivo in archivos_no_encontrados[:5]:
                print(f"  - {archivo}")
        
        if archivos_procesados:
            print(f"\n🔗 Unificando datos de {len(dfs)} dataframes...")
            self.df_completo = pd.concat(dfs, ignore_index=True, sort=False)
            
            print(f"  📊 Total antes de limpieza: {len(self.df_completo):,} registros")
            
            # Limpieza final
            self.df_completo = self._limpiar_datos_generales(self.df_completo)
            
            print(f"\n{'='*60}")
            print(f"📊 DATOS UNIFICADOS EXITOSAMENTE")
            print(f"{'='*60}")
            print(f"✅ Total de registros: {len(self.df_completo):,}")
            print(f"📅 Periodo cubierto: {self.df_completo['timestamp'].min()} a {self.df_completo['timestamp'].max()}")
            print(f"🏢 Proveedores únicos: {self.df_completo['proveedor_real'].nunique()}")
            print(f"🛠️  Herramientas únicas: {self.df_completo['herramienta'].nunique()}")
            print(f"🌍 Regiones únicas: {self.df_completo['region'].nunique()}")
            print(f"🏭 Datacenters únicos: {self.df_completo['datacenter'].nunique()}")
            print(f"{'='*60}\n")
            
            # Mostrar distribución por proveedor
            print("📈 DISTRIBUCIÓN POR PROVEEDOR:")
            print("-" * 40)
            distribucion = self.df_completo['proveedor_real'].value_counts()
            for proveedor, count in distribucion.items():
                porcentaje = count/len(self.df_completo)*100
                print(f"{proveedor:15s}: {count:10,} registros ({porcentaje:5.1f}%)")
            
            print(f"\n🔧 DISTRIBUCIÓN POR HERRAMIENTA:")
            print("-" * 40)
            distribucion_herramienta = self.df_completo['herramienta'].value_counts()
            for herramienta, count in distribucion_herramienta.items():
                porcentaje = count/len(self.df_completo)*100
                print(f"{herramienta:15s}: {count:10,} registros ({porcentaje:5.1f}%)")
            
            # Mostrar estadísticas básicas
            print(f"\n📊 ESTADÍSTICAS BÁSICAS DE LATENCIA:")
            print("-" * 40)
            print(f"Media: {self.df_completo['latency_ms'].mean():.1f} ms")
            print(f"Mediana: {self.df_completo['latency_ms'].median():.1f} ms")
            print(f"Mínimo: {self.df_completo['latency_ms'].min():.1f} ms")
            print(f"Máximo: {self.df_completo['latency_ms'].max():.1f} ms")
            print(f"Std Dev: {self.df_completo['latency_ms'].std():.1f} ms")
            
        else:
            raise ValueError("No se pudieron cargar datos de ningún archivo")
        
        return self.df_completo
    
    def _extraer_proveedor_real(self, archivo_nombre):
        """Extrae el proveedor real del nombre del archivo"""
        for key, value in self.mapeo_proveedores.items():
            if archivo_nombre.startswith(key):
                return value
        return 'desconocido'
    
    def _extraer_herramienta(self, archivo_nombre):
        """Extrae la herramienta del nombre del archivo"""
        # Eliminar el prefijo del proveedor
        nombre_sin_prefijo = archivo_nombre
        for prefijo in ['aws_', 'azure_', 'gcp_', 'huawei_']:
            if archivo_nombre.startswith(prefijo):
                nombre_sin_prefijo = archivo_nombre[len(prefijo):]
                break
        
        # Extraer la herramienta
        herramienta = nombre_sin_prefijo.split('_')[0]
        return herramienta
    
    def _limpiar_datos_especificos(self, df, archivo_nombre):
        """Limpieza específica por tipo de archivo"""
        
        print(f"  🔧 Limpiando datos de {archivo_nombre}...")
        
        # Eliminar duplicados
        antes = len(df)
        df = df.drop_duplicates()
        despues = len(df)
        if antes != despues:
            print(f"    ✅ Eliminados {antes - despues} duplicados")
        
        # Limpiar valores de latencia
        df['latency_ms'] = pd.to_numeric(df['latency_ms'], errors='coerce')
        
        # Contar valores inválidos
        invalidos = df['latency_ms'].isnull().sum()
        if invalidos > 0:
            print(f"    ⚠️  {invalidos} valores de latencia inválidos encontrados")
        
        # Filtrar latencias razonables
        antes_filtro = len(df)
        df = df[(df['latency_ms'] > 0) & (df['latency_ms'] < 10000)]
        despues_filtro = len(df)
        if antes_filtro != despues_filtro:
            print(f"    ✅ Eliminados {antes_filtro - despues_filtro} registros con latencia fuera de rango")
        
        # Limpiar cadenas de texto
        if 'provider' in df.columns:
            df['provider'] = df['provider'].astype(str).str.strip()
        
        if 'region' in df.columns:
            df['region'] = df['region'].astype(str).str.strip().str.upper()
        
        if 'datacenter' in df.columns:
            df['datacenter'] = df['datacenter'].astype(str).str.strip()
        
        # Limpieza específica por archivo
        if archivo_nombre == 'cloudpingco_latency_longterm.csv':
            # Para cloudping.co, extraer proveedor de to_region
            df['region'] = df['region'].astype(str).str.strip().str.upper()
        
        elif 'cloudpingtest' in archivo_nombre:
            # cloudpingtest.com tiene latencias más altas
            print(f"    ℹ️  Archivo cloudpingtest detectado (latencias altas esperadas)")
        
        print(f"    ✅ Limpieza completada: {len(df):,} registros válidos")
        
        return df
    
    def _limpiar_datos_generales(self, df):
        """Limpieza general después de unificar todos los datos"""
        
        print("\n🔧 APLICANDO LIMPIEZA GENERAL A DATOS UNIFICADOS...")
        
        # Verificar y asegurar que timestamp es datetime
        if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            print("  ⚠️  timestamp no es datetime, convirtiendo...")
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        
        # Eliminar registros con timestamp nulo
        antes = len(df)
        df = df.dropna(subset=['timestamp'])
        despues = len(df)
        if antes != despues:
            print(f"  ✅ Eliminados {antes - despues} registros con timestamp nulo")
        
        # Crear identificador único de datacenter
        df['datacenter_id'] = df['proveedor_real'] + '_' + df['region']
        
        # Crear columnas derivadas solo si timestamp es datetime
        try:
            df['fecha'] = df['timestamp'].dt.date
            df['hora'] = df['timestamp'].dt.hour
            df['dia_semana'] = df['timestamp'].dt.day_name()
            df['mes'] = df['timestamp'].dt.month
            df['dia_mes'] = df['timestamp'].dt.day
            
            # Crear identificador de hora completa
            df['hora_completa'] = df['timestamp'].dt.floor('H')
            
            print("  ✅ Columnas temporales creadas exitosamente")
        except Exception as e:
            print(f"  ❌ Error creando columnas temporales: {e}")
            print(f"    Tipo de timestamp: {df['timestamp'].dtype}")
            print(f"    Primeros valores: {df['timestamp'].head()}")
            raise
        
        # Clasificar latencia
        df['latencia_aceptable'] = df['latency_ms'] <= self.umbral_latencia
        
        # Categorizar latencia
        try:
            df['latencia_categoria'] = pd.cut(df['latency_ms'],
                                             bins=[0, 50, 100, 200, 500, float('inf')],
                                             labels=['Excelente (<50ms)', 'Buena (<100ms)', 
                                                    'Aceptable (<200ms)', 'Alta (<500ms)', 
                                                    'Muy Alta (>=500ms)'])
        except Exception as e:
            print(f"  ⚠️  Error categorizando latencia: {e}")
            df['latencia_categoria'] = 'No categorizada'
        
        # Ordenar por timestamp
        df = df.sort_values('timestamp').reset_index(drop=True)
        
        print(f"  ✅ Limpieza general completada: {len(df):,} registros finales")
        
        return df
    
    def calcular_metricas_generales(self):
        """
        Calcula métricas generales de rendimiento
        """
        print("\n" + "="*80)
        print("📊 CALCULANDO MÉTRICAS GENERALES DE RENDIMIENTO")
        print("="*80)
        
        metricas = {}
        
        # 1. Métricas por proveedor real
        print("\n1. 📊 MÉTRICAS POR PROVEEDOR:")
        print("-" * 60)
        
        metricas_proveedor = self.df_completo.groupby('proveedor_real').agg({
            'latency_ms': ['count', 'mean', 'median', 'std', 
                          lambda x: np.percentile(x, 95),
                          lambda x: np.percentile(x, 99)],
            'latencia_aceptable': 'mean'
        }).round(2)
        
        metricas_proveedor.columns = ['registros', 'media', 'mediana', 'std', 
                                     'p95', 'p99', 'disponibilidad']
        metricas['por_proveedor'] = metricas_proveedor
        
        print(metricas_proveedor.to_string())
        
        # 2. Métricas por herramienta
        print("\n2. 🛠️ MÉTRICAS POR HERRAMIENTA:")
        print("-" * 60)
        
        metricas_herramienta = self.df_completo.groupby('herramienta').agg({
            'latency_ms': ['mean', 'std', lambda x: np.percentile(x, 95)],
            'latencia_aceptable': 'mean',
            'proveedor_real': 'nunique'
        }).round(2)
        
        metricas_herramienta.columns = ['latencia_media', 'latencia_std', 'latencia_p95', 
                                       'disponibilidad', 'proveedores_medidos']
        metricas['por_herramienta'] = metricas_herramienta
        
        print(metricas_herramienta.to_string())
        
        # 3. Métricas por hora del día
        print("\n3. ⏰ MÉTRICAS POR HORA DEL DÍA:")
        print("-" * 60)
        
        metricas_hora = self.df_completo.groupby('hora').agg({
            'latency_ms': 'mean',
            'latencia_aceptable': 'mean'
        }).round(2)
        
        metricas['por_hora'] = metricas_hora
        
        print("Hora | Latencia Media | Disponibilidad")
        print("-" * 60)
        for hora, fila in metricas_hora.iterrows():
            print(f"{hora:4d} | {fila['latency_ms']:13.1f} ms | {fila['latencia_aceptable']:.2%}")
        
        # 4. Métricas por datacenter
        print("\n4. 🏭 TOP 10 DATACENTERS POR LATENCIA:")
        print("-" * 60)
        
        # Filtrar datacenters con suficiente datos
        conteo_datacenters = self.df_completo['datacenter_id'].value_counts()
        datacenters_suficientes = conteo_datacenters[conteo_datacenters > 100].index
        
        if len(datacenters_suficientes) > 0:
            df_filtrado = self.df_completo[self.df_completo['datacenter_id'].isin(datacenters_suficientes)]
            
            top_datacenters = df_filtrado.groupby('datacenter_id').agg({
                'latency_ms': ['mean', 'std', lambda x: np.percentile(x, 95)],
                'latencia_aceptable': 'mean',
                'proveedor_real': 'first'
            }).round(2)
            
            top_datacenters.columns = ['latencia_media', 'latencia_std', 'latencia_p95', 
                                      'disponibilidad', 'proveedor']
            
            # Ordenar por latencia media
            top_datacenters = top_datacenters.sort_values('latencia_media')
            metricas['top_datacenters'] = top_datacenters.head(10)
            
            print("\n🏆 Mejores datacenters (menor latencia):")
            for idx, (datacenter, fila) in enumerate(metricas['top_datacenters'].iterrows(), 1):
                print(f"\n{idx:2d}. {datacenter}")
                print(f"    Proveedor: {fila['proveedor']}")
                print(f"    Latencia: {fila['latencia_media']} ms (P95: {fila['latencia_p95']} ms)")
                print(f"    Disponibilidad: {fila['disponibilidad']:.2%}")
                print(f"    Std Dev: {fila['latencia_std']} ms")
        else:
            print("⚠️  No hay datacenters con suficientes datos (>100 registros)")
        
        self.metricas_cache['generales'] = metricas
        return metricas
    
    def analizar_comparacion_proveedores(self):
        """
        Compara proveedores en las mismas regiones
        """
        print("\n" + "="*80)
        print("🔄 COMPARANDO PROVEEDORES EN MISMAS REGIONES")
        print("="*80)
        
        # Identificar regiones con múltiples proveedores
        regiones_comunes = []
        
        # Agrupar por región y ver qué proveedores tienen datos
        region_proveedores = self.df_completo.groupby('region')['proveedor_real'].unique()
        
        for region, proveedores in region_proveedores.items():
            if len(proveedores) > 1:
                # Excluir 'multi' que son herramientas de medición
                proveedores_reales = [p for p in proveedores if p not in ['multi', 'desconocido']]
                if len(proveedores_reales) > 1:
                    regiones_comunes.append({
                        'region': region,
                        'proveedores': proveedores_reales,
                        'count': len(proveedores_reales)
                    })
        
        # Ordenar por número de proveedores
        regiones_comunes.sort(key=lambda x: x['count'], reverse=True)
        
        print(f"\n🌍 Regiones con múltiples proveedores: {len(regiones_comunes)}")
        
        if len(regiones_comunes) == 0:
            print("⚠️  No se encontraron regiones con múltiples proveedores reales")
            return {}
        
        resultados_comparacion = {}
        
        # Analizar las top 5 regiones con más proveedores
        for i, region_info in enumerate(regiones_comunes[:5]):
            region = region_info['region']
            proveedores = region_info['proveedores']
            
            print(f"\n{'='*60}")
            print(f"📊 ANÁLISIS REGIÓN: {region}")
            print(f"🏢 Proveedores disponibles: {', '.join(proveedores)}")
            print(f"{'='*60}")
            
            resultados_region = {}
            
            for proveedor in proveedores:
                datos_proveedor = self.df_completo[
                    (self.df_completo['region'] == region) & 
                    (self.df_completo['proveedor_real'] == proveedor)
                ]
                
                if len(datos_proveedor) > 0:
                    latencia_media = datos_proveedor['latency_ms'].mean()
                    disponibilidad = datos_proveedor['latencia_aceptable'].mean()
                    latencia_p95 = np.percentile(datos_proveedor['latency_ms'].values, 95)
                    
                    resultados_region[proveedor] = {
                        'latencia_media': latencia_media,
                        'disponibilidad': disponibilidad,
                        'latencia_p95': latencia_p95,
                        'muestras': len(datos_proveedor)
                    }
                    
                    print(f"\n{proveedor.upper():10s}:")
                    print(f"  📊 Latencia media: {latencia_media:.1f} ms")
                    print(f"  📈 Latencia P95:   {latencia_p95:.1f} ms")
                    print(f"  ✅ Disponibilidad: {disponibilidad:.2%}")
                    print(f"  🔢 Muestras:       {len(datos_proveedor):,}")
            
            # Determinar el mejor proveedor para esta región
            if resultados_region:
                # Mejor por latencia media
                mejor_latencia = min(resultados_region.items(), 
                                   key=lambda x: x[1]['latencia_media'])
                
                # Mejor por disponibilidad
                mejor_disponibilidad = max(resultados_region.items(), 
                                         key=lambda x: x[1]['disponibilidad'])
                
                print(f"\n🎯 CONCLUSIÓN PARA {region}:")
                print(f"  • 🏆 Mejor latencia: {mejor_latencia[0]} ({mejor_latencia[1]['latencia_media']:.1f} ms)")
                print(f"  • 🥇 Mejor disponibilidad: {mejor_disponibilidad[0]} ({mejor_disponibilidad[1]['disponibilidad']:.2%})")
                
                # Calcular mejora potencial con multiservidor
                if len(resultados_region) > 1:
                    latencias = [v['latencia_media'] for v in resultados_region.values()]
                    mejor_latencia_valor = min(latencias)
                    latencia_promedio = np.mean(latencias)
                    
                    mejora_potencial = ((latencia_promedio - mejor_latencia_valor) / 
                                      latencia_promedio * 100) if latencia_promedio > 0 else 0
                    
                    print(f"  • 📈 Mejora potencial con balanceador: {mejora_potencial:.1f}%")
                    
                    # Recomendación
                    if mejora_potencial > 20:
                        recomendacion = "✅ FUERTEMENTE RECOMENDADO multiservidor"
                    elif mejora_potencial > 10:
                        recomendacion = "🟡 CONSIDERAR multiservidor"
                    else:
                        recomendacion = "🔴 MONOSERVIDOR suficiente"
                    
                    print(f"  • 💡 Recomendación: {recomendacion}")
                
                resultados_comparacion[region] = {
                    'proveedores': resultados_region,
                    'mejor_latencia': mejor_latencia,
                    'mejor_disponibilidad': mejor_disponibilidad,
                    'mejora_potencial': mejora_potencial if len(resultados_region) > 1 else 0
                }
        
        self.metricas_cache['comparacion_proveedores'] = resultados_comparacion
        return resultados_comparacion
    
    def analizar_tendencias_temporales(self):
        """
        Analiza tendencias de latencia a lo largo del tiempo
        """
        print("\n" + "="*80)
        print("📈 ANALIZANDO TENDENCIAS TEMPORALES")
        print("="*80)
        
        # Configurar timestamp como índice para resample
        df_temp = self.df_completo.set_index('timestamp')
        
        tendencias = {}
        
        # Analizar tendencias por proveedor
        proveedores_a_analizar = [p for p in self.df_completo['proveedor_real'].unique() 
                                 if p not in ['multi', 'desconocido']]
        
        for proveedor in proveedores_a_analizar:
            print(f"\n📊 Analizando tendencias de {proveedor.upper()}...")
            
            df_proveedor = df_temp[df_temp['proveedor_real'] == proveedor]
            
            if len(df_proveedor) < 100:  # Muy pocos datos
                print(f"  ⚠️  Muy pocos datos ({len(df_proveedor):,} registros), saltando...")
                continue
            
            # Latencia diaria
            latencia_diaria = df_proveedor['latency_ms'].resample('D').agg(['mean', 'std', 'count'])
            
            if len(latencia_diaria) < 3:  # Necesitamos al menos 3 días
                print(f"  ⚠️  Menos de 3 días de datos, saltando...")
                continue
            
            latencia_diaria['rolling_3d'] = latencia_diaria['mean'].rolling(window=3).mean()
            
            # Disponibilidad diaria
            disponibilidad_diaria = df_proveedor['latencia_aceptable'].resample('D').mean()
            
            # Calcular tendencia (pendiente de regresión lineal)
            x = np.arange(len(latencia_diaria))
            y = latencia_diaria['mean'].values
            mask = ~np.isnan(y)
            
            if np.sum(mask) > 2:
                slope, intercept, r_value, p_value, std_err = stats.linregress(
                    x[mask], y[mask]
                )
                
                # Clasificar tendencia
                cambio_porcentual = ((y[mask][-1] - y[mask][0]) / y[mask][0] * 100) if y[mask][0] != 0 else 0
                
                if slope < -1:
                    tendencia = "📉 MEJORANDO RÁPIDAMENTE"
                elif slope < -0.1:
                    tendencia = "📉 Mejorando"
                elif abs(slope) <= 0.1:
                    tendencia = "➡️ ESTABLE"
                elif slope <= 1:
                    tendencia = "📈 Empeorando"
                else:
                    tendencia = "📈 EMPEORANDO RÁPIDAMENTE"
                
                tendencias[proveedor] = {
                    'tendencia': tendencia,
                    'pendiente': float(slope),
                    'r_cuadrado': float(r_value**2),
                    'cambio_porcentual': float(cambio_porcentual),
                    'latencia_inicial': float(y[mask][0]),
                    'latencia_final': float(y[mask][-1]),
                    'dias_analizados': int(len(latencia_diaria)),
                    'disponibilidad_promedio': float(disponibilidad_diaria.mean())
                }
                
                print(f"  📊 Tendencia: {tendencia}")
                print(f"  🔄 Cambio: {cambio_porcentual:+.1f}%")
                print(f"  ⏱️  Latencia inicial/final: {y[mask][0]:.1f}ms → {y[mask][-1]:.1f}ms")
                print(f"  ✅ Disponibilidad: {disponibilidad_diaria.mean():.2%}")
                print(f"  📐 R²: {r_value**2:.3f}")
        
        # Resetear índice
        self.df_completo = df_temp.reset_index()
        
        self.metricas_cache['tendencias'] = tendencias
        return tendencias
    
    def analizar_rentabilidad_multiservidor(self):
        """
        Analiza la rentabilidad de implementar multiservidor vs monoservidor
        """
        print("\n" + "="*80)
        print("💰 ANÁLISIS DE RENTABILIDAD: MULTISERVIDOR vs MONOSERVIDOR")
        print("="*80)
        
        resultados = {}
        
        # Identificar regiones con múltiples proveedores
        regiones_comunes = []
        region_proveedores = self.df_completo.groupby('region')['proveedor_real'].unique()
        
        for region, proveedores in region_proveedores.items():
            if len(proveedores) > 1:
                # Solo considerar regiones con al menos 2 proveedores reales (excluyendo 'multi')
                proveedores_reales = [p for p in proveedores if p not in ['multi', 'desconocido']]
                if len(proveedores_reales) > 1:
                    regiones_comunes.append({
                        'region': region,
                        'proveedores': proveedores_reales
                    })
        
        print(f"\n🌍 Regiones analizables con múltiples proveedores: {len(regiones_comunes)}")
        
        if len(regiones_comunes) == 0:
            print("⚠️  No se encontraron regiones con múltiples proveedores para análisis")
            return {}
        
        for i, region_info in enumerate(regiones_comunes[:15]):  # Limitar a 15 regiones
            region = region_info['region']
            proveedores = region_info['proveedores']
            
            print(f"\n{'='*60}")
            print(f"📍 REGIÓN: {region}")
            print(f"🏢 Proveedores disponibles: {', '.join(proveedores)}")
            print(f"{'='*60}")
            
            # Recopilar datos por proveedor
            datos_proveedores = {}
            timestamps_comunes = None
            
            for proveedor in proveedores:
                datos = self.df_completo[
                    (self.df_completo['region'] == region) & 
                    (self.df_completo['proveedor_real'] == proveedor)
                ]
                
                if not datos.empty and len(datos) > 10:  # Al menos 10 registros
                    # Agrupar por hora para análisis de disponibilidad simultánea
                    datos_hora = datos.groupby('hora_completa').agg({
                        'latency_ms': 'mean',
                        'latencia_aceptable': 'any'
                    })
                    
                    datos_proveedores[proveedor] = datos_hora
                    
                    # Encontrar timestamps comunes
                    if timestamps_comunes is None:
                        timestamps_comunes = set(datos_hora.index)
                    else:
                        timestamps_comunes = timestamps_comunes.intersection(set(datos_hora.index))
            
            # Solo continuar si hay timestamps comunes suficientes
            if timestamps_comunes and len(timestamps_comunes) > 10:
                # Calcular métricas de multiservidor
                disponibilidad_simultanea = []
                latencia_minima_por_hora = []
                latencia_media_por_hora = []
                
                # Usar una muestra de timestamps para no sobrecargar
                timestamps_muestra = list(timestamps_comunes)
                if len(timestamps_muestra) > 100:
                    timestamps_muestra = np.random.choice(timestamps_muestra, 100, replace=False)
                
                for timestamp in timestamps_muestra:
                    latencias = []
                    disponible = True
                    
                    for proveedor, datos in datos_proveedores.items():
                        if timestamp in datos.index:
                            latencias.append(datos.loc[timestamp, 'latency_ms'])
                            if not datos.loc[timestamp, 'latencia_aceptable']:
                                disponible = False
                    
                    if latencias and len(latencias) > 1:  # Solo si hay datos de múltiples proveedores
                        disponibilidad_simultanea.append(disponible)
                        latencia_minima_por_hora.append(min(latencias))
                        latencia_media_por_hora.append(np.mean(latencias))
                
                if disponibilidad_simultanea and latencia_minima_por_hora:
                    disponibilidad_simultanea_pct = np.mean(disponibilidad_simultanea)
                    latencia_media_agregada = np.mean(latencia_media_por_hora)
                    latencia_minima_agregada = np.mean(latencia_minima_por_hora)
                    
                    mejora_potencial = ((latencia_media_agregada - latencia_minima_agregada) / 
                                      latencia_media_agregada * 100) if latencia_media_agregada > 0 else 0
                    
                    # Calcular costo-beneficio
                    # Supuestos simplificados:
                    # - Costo monoservidor: 100 unidades
                    # - Costo multiservidor: 100 + 60 por proveedor adicional
                    costo_mono = 100
                    costo_multi = 100 + (len(proveedores) - 1) * 60
                    
                    # Beneficio estimado (mejora de latencia + mejora disponibilidad)
                    beneficio_latencia = mejora_potencial * 2  # Ponderación
                    beneficio_disponibilidad = (disponibilidad_simultanea_pct - 0.95) * 1000 if disponibilidad_simultanea_pct > 0.95 else 0
                    beneficio_total = beneficio_latencia + beneficio_disponibilidad
                    
                    costo_adicional = costo_multi - costo_mono
                    roi = (beneficio_total - costo_adicional) / costo_adicional * 100 if costo_adicional > 0 else 0
                    
                    # Determinar recomendación
                    if disponibilidad_simultanea_pct > 0.99 and mejora_potencial > 20:
                        recomendacion = "✅ MULTISERVIDOR ALTAMENTE RECOMENDADO"
                        razon = f"Excelente disponibilidad ({disponibilidad_simultanea_pct:.1%}) + alta mejora ({mejora_potencial:.1f}%)"
                    elif roi > 100:
                        recomendacion = "✅ MULTISERVIDOR RENTABLE"
                        razon = f"ROI muy positivo ({roi:.0f}%)"
                    elif roi > 50:
                        recomendacion = "🟡 CONSIDERAR MULTISERVIDOR"
                        razon = f"ROI positivo ({roi:.0f}%)"
                    elif disponibilidad_simultanea_pct > 0.98:
                        recomendacion = "🟡 MULTISERVIDOR PARA RESILIENCIA"
                        razon = f"Buena disponibilidad simultánea ({disponibilidad_simultanea_pct:.1%})"
                    elif mejora_potencial > 30:
                        recomendacion = "🟡 MULTISERVIDOR PARA PERFORMANCE"
                        razon = f"Mejora de performance muy alta ({mejora_potencial:.1f}%)"
                    else:
                        recomendacion = "🔴 MONOSERVIDOR SUFICIENTE"
                        razon = f"Baja mejora ({mejora_potencial:.1f}%) y disponibilidad ({disponibilidad_simultanea_pct:.1%})"
                    
                    resultados[region] = {
                        'recomendacion': recomendacion,
                        'razon': razon,
                        'proveedores': proveedores,
                        'disponibilidad_simultanea': disponibilidad_simultanea_pct,
                        'mejora_potencial_latencia': mejora_potencial,
                        'costo_monoservidor': costo_mono,
                        'costo_multiservidor': costo_multi,
                        'costo_adicional': costo_adicional,
                        'roi_estimado': roi,
                        'muestras_analizadas': len(disponibilidad_simultanea)
                    }
                    
                    print(f"\n💰 ANÁLISIS DE RENTABILIDAD:")
                    print(f"  • 📋 Recomendación: {recomendacion}")
                    print(f"  • 📝 Razón: {razon}")
                    print(f"  • ✅ Disponibilidad simultánea: {disponibilidad_simultanea_pct:.2%}")
                    print(f"  • 📈 Mejora potencial de latencia: {mejora_potencial:.1f}%")
                    print(f"  • 💰 Costo adicional multiservidor: {costo_adicional:.0f} unidades")
                    print(f"  • 📊 ROI estimado: {roi:.0f}%")
                    print(f"  • 🔢 Muestras analizadas: {len(disponibilidad_simultanea):,}")
            else:
                print(f"  ⚠️  No hay suficientes datos coincidentes para análisis")
        
        self.metricas_cache['rentabilidad'] = resultados
        return resultados
    
    def generar_reporte_completo(self):
        """
        Genera un reporte completo del análisis
        """
        print("\n" + "="*80)
        print("📝 GENERANDO REPORTE COMPLETO")
        print("="*80)
        
        # Crear directorio para reportes
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dir_reportes = f"reporte_latencia_{timestamp}"
        os.makedirs(dir_reportes, exist_ok=True)
        
        try:
            # 1. Reporte ejecutivo
            self._generar_reporte_ejecutivo(dir_reportes)
            
            # 2. Reporte detallado
            self._generar_reporte_detallado(dir_reportes)
            
            # 3. Gráficos
            self._generar_graficos_comparativos(dir_reportes)
            
            # 4. Datos procesados
            self._guardar_datos_procesados(dir_reportes)
            
            print(f"\n{'✅'*30}")
            print(f"🎉 REPORTES GENERADOS EXITOSAMENTE!")
            print(f"{'✅'*30}")
            print(f"\n📁 Los reportes se han guardado en: {dir_reportes}/")
            print("\n📄 Archivos generados:")
            print("  • 📋 reporte_ejecutivo.json - Resumen JSON del análisis")
            print("  • 📄 resumen_ejecutivo.txt - Resumen en texto plano")
            print("  • 📊 reporte_detallado_por_region.csv - Recomendaciones por región")
            print("  • 🌐 reporte_detallado.html - Reporte HTML interactivo")
            print("  • 📈 graficos_comparativos.png - Gráficos comparativos")
            print("  • 📊 metricas_completas.json - Todas las métricas calculadas")
            print("  • 📊 datos_procesados_muestra.csv - Muestra de datos procesados")
            
            # Mostrar resumen en pantalla
            self._mostrar_resumen_ejecutivo()
            
        except Exception as e:
            print(f"  ❌ Error generando reportes: {e}")
            import traceback
            traceback.print_exc()
        
        return dir_reportes
    
    def _generar_reporte_ejecutivo(self, directorio):
        """Genera reporte ejecutivo con conclusiones principales"""
        
        try:
            resumen = {
                "fecha_analisis": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "datos_analizados": {
                    "total_registros": int(len(self.df_completo)),
                    "periodo": {
                        "inicio": self.df_completo['timestamp'].min().strftime("%Y-%m-%d %H:%M:%S"),
                        "fin": self.df_completo['timestamp'].max().strftime("%Y-%m-%d %H:%M:%S")
                    },
                    "duracion_dias": (self.df_completo['timestamp'].max() - self.df_completo['timestamp'].min()).days,
                    "proveedores": int(self.df_completo['proveedor_real'].nunique()),
                    "regiones": int(self.df_completo['region'].nunique()),
                    "herramientas": int(self.df_completo['herramienta'].nunique())
                },
                "umbrales": {
                    "latencia_aceptable_ms": self.umbral_latencia,
                    "disponibilidad_minima": self.umbral_disponibilidad
                }
            }
            
            # Agregar métricas generales
            disponibilidad_global = self.df_completo['latencia_aceptable'].mean()
            latencia_global_media = self.df_completo['latency_ms'].mean()
            latencia_global_p95 = np.percentile(self.df_completo['latency_ms'].dropna().values, 95)
            
            resumen["metricas_globales"] = {
                "disponibilidad": float(disponibilidad_global),
                "latencia_media_ms": float(latencia_global_media),
                "latencia_p95_ms": float(latencia_global_p95)
            }
            
            # Agregar recomendaciones de rentabilidad
            if 'rentabilidad' in self.metricas_cache:
                rentabilidad = self.metricas_cache['rentabilidad']
                
                conteo_recomendaciones = {
                    "MULTISERVIDOR_RECOMENDADO": 0,
                    "MULTISERVIDOR_CONSIDERAR": 0,
                    "MONOSERVIDOR": 0
                }
                
                for region, datos in rentabilidad.items():
                    rec = datos['recomendacion']
                    if "ALTAMENTE RECOMENDADO" in rec or "RENTABLE" in rec:
                        conteo_recomendaciones["MULTISERVIDOR_RECOMENDADO"] += 1
                    elif "CONSIDERAR" in rec or "PARA" in rec:
                        conteo_recomendaciones["MULTISERVIDOR_CONSIDERAR"] += 1
                    else:
                        conteo_recomendaciones["MONOSERVIDOR"] += 1
                
                resumen["recomendaciones"] = conteo_recomendaciones
                
                # Recomendación general basada en mayoría
                total = sum(conteo_recomendaciones.values())
                if total > 0:
                    if conteo_recomendaciones["MULTISERVIDOR_RECOMENDADO"] / total > 0.3:
                        resumen["recomendacion_general"] = "IMPLEMENTAR MULTISERVIDOR EN VARIAS REGIONES"
                    elif (conteo_recomendaciones["MULTISERVIDOR_RECOMENDADO"] + conteo_recomendaciones["MULTISERVIDOR_CONSIDERAR"]) / total > 0.5:
                        resumen["recomendacion_general"] = "CONSIDERAR MULTISERVIDOR SELECTIVAMENTE"
                    else:
                        resumen["recomendacion_general"] = "MANTENER MONOSERVIDOR EN LA MAYORÍA DE REGIONES"
            
            # Guardar como JSON
            with open(os.path.join(directorio, 'reporte_ejecutivo.json'), 'w', encoding='utf-8') as f:
                json.dump(resumen, f, indent=2, ensure_ascii=False, default=str)
            
            # Crear versión texto
            with open(os.path.join(directorio, 'resumen_ejecutivo.txt'), 'w', encoding='utf-8') as f:
                f.write("=" * 80 + "\n")
                f.write("REPORTE EJECUTIVO - ANÁLISIS MULTISERVIDOR vs MONOSERVIDOR\n")
                f.write("=" * 80 + "\n\n")
                
                f.write("RESUMEN DE DATOS ANALIZADOS:\n")
                f.write("-" * 40 + "\n")
                f.write(f"• Período: {resumen['datos_analizados']['periodo']['inicio']} a {resumen['datos_analizados']['periodo']['fin']}\n")
                f.write(f"• Duración: {resumen['datos_analizados']['duracion_dias']} días\n")
                f.write(f"• Total registros: {resumen['datos_analizados']['total_registros']:,}\n")
                f.write(f"• Proveedores analizados: {resumen['datos_analizados']['proveedores']}\n")
                f.write(f"• Regiones analizadas: {resumen['datos_analizados']['regiones']}\n")
                f.write(f"• Umbral latencia: {resumen['umbrales']['latencia_aceptable_ms']} ms\n\n")
                
                if 'metricas_globales' in resumen:
                    f.write("MÉTRICAS GLOBALES:\n")
                    f.write("-" * 40 + "\n")
                    f.write(f"• Disponibilidad global: {resumen['metricas_globales']['disponibilidad']:.2%}\n")
                    f.write(f"• Latencia media: {resumen['metricas_globales']['latencia_media_ms']:.1f} ms\n")
                    f.write(f"• Latencia P95: {resumen['metricas_globales']['latencia_p95_ms']:.1f} ms\n\n")
                
                if 'recomendaciones' in resumen:
                    f.write("DISTRIBUCIÓN DE RECOMENDACIONES:\n")
                    f.write("-" * 40 + "\n")
                    for tipo, cantidad in resumen['recomendaciones'].items():
                        porcentaje = (cantidad / sum(resumen['recomendaciones'].values()) * 100) if sum(resumen['recomendaciones'].values()) > 0 else 0
                        tipo_bonito = tipo.replace('_', ' ').title()
                        f.write(f"• {tipo_bonito}: {cantidad} regiones ({porcentaje:.1f}%)\n")
                    f.write(f"\nRECOMENDACIÓN GENERAL:\n{resumen.get('recomendacion_general', 'No disponible')}\n")
            
            print("✅ Reporte ejecutivo generado")
            
        except Exception as e:
            print(f"  ❌ Error generando reporte ejecutivo: {e}")
    
    def _generar_reporte_detallado(self, directorio):
        """Genera reporte detallado por región"""
        
        try:
            if 'rentabilidad' not in self.metricas_cache or not self.metricas_cache['rentabilidad']:
                print("  ⚠️  No hay datos de rentabilidad para reporte detallado")
                return
            
            # Crear DataFrame detallado
            filas = []
            for region, datos in self.metricas_cache['rentabilidad'].items():
                fila = {
                    'region': region,
                    'recomendacion': datos['recomendacion'],
                    'proveedores': ', '.join(datos['proveedores']),
                    'disponibilidad_simultanea': f"{datos['disponibilidad_simultanea']:.2%}",
                    'mejora_potencial': f"{datos['mejora_potencial_latencia']:.1f}%",
                    'costo_adicional': f"{datos['costo_adicional']:.0f}",
                    'roi_estimado': f"{datos['roi_estimado']:.0f}%",
                    'muestras': datos['muestras_analizadas']
                }
                filas.append(fila)
            
            df_detalle = pd.DataFrame(filas)
            
            # Guardar en diferentes formatos
            df_detalle.to_csv(os.path.join(directorio, 'reporte_detallado_por_region.csv'), 
                             index=False, encoding='utf-8')
            
            # Crear versión HTML con mejor formato
            html = """
            <html>
            <head>
                <title>Reporte Detallado por Región</title>
                <style>
                    body { font-family: Arial, sans-serif; margin: 20px; }
                    h1 { color: #333; }
                    table { border-collapse: collapse; width: 100%; margin-top: 20px; }
                    th { background-color: #4CAF50; color: white; padding: 12px; text-align: left; }
                    td { padding: 10px; border-bottom: 1px solid #ddd; }
                    tr:hover { background-color: #f5f5f5; }
                    .multiservidor-recomendado { background-color: #d4edda; }
                    .multiservidor-considerar { background-color: #fff3cd; }
                    .monoservidor { background-color: #f8d7da; }
                </style>
            </head>
            <body>
                <h1>Reporte Detallado - Análisis por Región</h1>
                <table>
                    <tr>
                        <th>Región</th>
                        <th>Recomendación</th>
                        <th>Proveedores</th>
                        <th>Disponibilidad</th>
                        <th>Mejora Potencial</th>
                        <th>Costo Adicional</th>
                        <th>ROI Estimado</th>
                        <th>Muestras</th>
                    </tr>
            """
            
            for _, fila in df_detalle.iterrows():
                clase = ""
                if "ALTAMENTE RECOMENDADO" in fila['recomendacion'] or "RENTABLE" in fila['recomendacion']:
                    clase = "multiservidor-recomendado"
                elif "CONSIDERAR" in fila['recomendacion'] or "PARA" in fila['recomendacion']:
                    clase = "multiservidor-considerar"
                elif "MONOSERVIDOR" in fila['recomendacion']:
                    clase = "monoservidor"
                
                html += f"""
                    <tr class="{clase}">
                        <td>{fila['region']}</td>
                        <td><strong>{fila['recomendacion']}</strong></td>
                        <td>{fila['proveedores']}</td>
                        <td>{fila['disponibilidad_simultanea']}</td>
                        <td>{fila['mejora_potencial']}</td>
                        <td>{fila['costo_adicional']}</td>
                        <td>{fila['roi_estimado']}</td>
                        <td>{fila['muestras']}</td>
                    </tr>
                """
            
            html += """
                </table>
            </body>
            </html>
            """
            
            with open(os.path.join(directorio, 'reporte_detallado.html'), 'w', encoding='utf-8') as f:
                f.write(html)
            
            print("✅ Reporte detallado generado")
            
        except Exception as e:
            print(f"  ❌ Error generando reporte detallado: {e}")
    
    def _generar_graficos_comparativos(self, directorio):
        """Genera gráficos comparativos"""
        
        try:
            # 1. Comparación de proveedores
            fig, axes = plt.subplots(2, 2, figsize=(16, 12))
            
            # Boxplot por proveedor (excluir 'multi')
            df_proveedores_reales = self.df_completo[~self.df_completo['proveedor_real'].isin(['multi', 'desconocido'])]
            
            if len(df_proveedores_reales) > 0:
                proveedores_orden = df_proveedores_reales.groupby('proveedor_real')['latency_ms'].median().sort_values().index
                
                sns.boxplot(data=df_proveedores_reales, x='proveedor_real', y='latency_ms', 
                           order=proveedores_orden, ax=axes[0,0])
                axes[0,0].set_title('Distribución de Latencia por Proveedor')
                axes[0,0].set_xlabel('Proveedor')
                axes[0,0].set_ylabel('Latencia (ms)')
                axes[0,0].axhline(y=self.umbral_latencia, color='r', linestyle='--', alpha=0.7, label=f'Umbral ({self.umbral_latencia}ms)')
                axes[0,0].legend()
                axes[0,0].tick_params(axis='x', rotation=45)
            
            # Disponibilidad por proveedor
            disponibilidad = df_proveedores_reales.groupby('proveedor_real')['latencia_aceptable'].mean().sort_values(ascending=False)
            
            axes[0,1].bar(disponibilidad.index, disponibilidad.values * 100)
            axes[0,1].set_title('Disponibilidad por Proveedor')
            axes[0,1].set_xlabel('Proveedor')
            axes[0,1].set_ylabel('Disponibilidad (%)')
            axes[0,1].axhline(y=self.umbral_disponibilidad * 100, color='r', linestyle='--', alpha=0.7, label=f'Umbral ({self.umbral_disponibilidad*100:.0f}%)')
            axes[0,1].legend()
            axes[0,1].tick_params(axis='x', rotation=45)
            
            # Latencia por hora del día
            latencia_hora = self.df_completo.groupby('hora')['latency_ms'].mean()
            
            axes[1,0].plot(latencia_hora.index, latencia_hora.values, marker='o', linewidth=2)
            axes[1,0].set_title('Latencia Media por Hora del Día')
            axes[1,0].set_xlabel('Hora del Día')
            axes[1,0].set_ylabel('Latencia Media (ms)')
            axes[1,0].grid(True, alpha=0.3)
            axes[1,0].fill_between(latencia_hora.index, latencia_hora.values, 
                                  alpha=0.3, color='skyblue')
            
            # Distribución de recomendaciones (si existe)
            if 'rentabilidad' in self.metricas_cache and self.metricas_cache['rentabilidad']:
                rentabilidad = self.metricas_cache['rentabilidad']
                
                conteo = {'Recomendado': 0, 'Considerar': 0, 'Monoservidor': 0}
                for region, datos in rentabilidad.items():
                    rec = datos['recomendacion']
                    if "ALTAMENTE RECOMENDADO" in rec or "RENTABLE" in rec:
                        conteo['Recomendado'] += 1
                    elif "CONSIDERAR" in rec or "PARA" in rec:
                        conteo['Considerar'] += 1
                    else:
                        conteo['Monoservidor'] += 1
                
                colors = ['#2ecc71', '#f1c40f', '#e74c3c']
                axes[1,1].pie(conteo.values(), labels=conteo.keys(), autopct='%1.1f%%',
                            colors=colors, startangle=90, explode=(0.1, 0, 0))
                axes[1,1].set_title('Distribución de Recomendaciones por Región')
            
            plt.tight_layout()
            plt.savefig(os.path.join(directorio, 'graficos_comparativos.png'), 
                       dpi=300, bbox_inches='tight')
            plt.close()
            
            # 2. Gráfico de tendencias (si existe)
            if 'tendencias' in self.metricas_cache and self.metricas_cache['tendencias']:
                fig, ax = plt.subplots(figsize=(12, 6))
                
                tendencias = self.metricas_cache['tendencias']
                proveedores = list(tendencias.keys())
                cambios = [t['cambio_porcentual'] for t in tendencias.values()]
                
                colors = ['green' if c < 0 else 'red' for c in cambios]
                
                bars = ax.bar(proveedores, cambios, color=colors)
                ax.set_title('Tendencia de Latencia por Proveedor (% cambio)')
                ax.set_xlabel('Proveedor')
                ax.set_ylabel('Cambio Porcentual (%)')
                ax.axhline(y=0, color='black', linewidth=0.8)
                ax.tick_params(axis='x', rotation=45)
                
                # Añadir etiquetas de valor
                for bar, cambio in zip(bars, cambios):
                    height = bar.get_height()
                    ax.text(bar.get_x() + bar.get_width()/2., height,
                           f'{cambio:+.1f}%',
                           ha='center', va='bottom' if height > 0 else 'top')
                
                plt.tight_layout()
                plt.savefig(os.path.join(directorio, 'tendencias_proveedores.png'),
                           dpi=300, bbox_inches='tight')
                plt.close()
            
            print("✅ Gráficos generados")
            
        except Exception as e:
            print(f"  ❌ Error generando gráficos: {e}")
    
    def _guardar_datos_procesados(self, directorio):
        """Guarda los datos procesados para análisis posterior"""
        
        try:
            # Guardar una muestra de los datos procesados
            muestra = self.df_completo.sample(min(5000, len(self.df_completo)), random_state=42)
            muestra.to_csv(os.path.join(directorio, 'datos_procesados_muestra.csv'), 
                          index=False, encoding='utf-8')
            
            # Guardar métricas en JSON
            with open(os.path.join(directorio, 'metricas_completas.json'), 'w', 
                     encoding='utf-8') as f:
                json.dump(self.metricas_cache, f, indent=2, 
                         ensure_ascii=False, default=str)
            
            print("✅ Datos procesados guardados")
            
        except Exception as e:
            print(f"  ❌ Error guardando datos procesados: {e}")
    
    def _mostrar_resumen_ejecutivo(self):
        """Muestra un resumen ejecutivo en pantalla"""
        
        print("\n" + "="*80)
        print("🎯 RESUMEN EJECUTIVO - CONCLUSIONES PRINCIPALES")
        print("="*80)
        
        # Métricas globales
        disponibilidad_global = self.df_completo['latencia_aceptable'].mean()
        latencia_media = self.df_completo['latency_ms'].mean()
        latencia_p95 = np.percentile(self.df_completo['latency_ms'].dropna().values, 95)
        
        print(f"\n📊 MÉTRICAS GLOBALES:")
        print(f"   • ✅ Disponibilidad: {disponibilidad_global:.2%}")
        print(f"   • ⏱️  Latencia media: {latencia_media:.1f} ms")
        print(f"   • 📈 Latencia P95: {latencia_p95:.1f} ms")
        
        # Comparación de proveedores
        if 'generales' in self.metricas_cache:
            metricas = self.metricas_cache['generales']['por_proveedor']
            # Excluir 'multi' y 'desconocido'
            metricas_filtradas = metricas[~metricas.index.isin(['multi', 'desconocido'])]
            
            if len(metricas_filtradas) > 0:
                mejor_proveedor = metricas_filtradas['disponibilidad'].idxmax()
                mejor_disponibilidad = metricas_filtradas['disponibilidad'].max()
                
                peor_proveedor = metricas_filtradas['disponibilidad'].idxmin()
                peor_disponibilidad = metricas_filtradas['disponibilidad'].min()
                
                print(f"\n🏆 COMPARATIVA DE PROVEEDORES:")
                print(f"   • 🥇 Mejor disponibilidad: {mejor_proveedor.upper()} ({mejor_disponibilidad:.2%})")
                print(f"   • ⚠️  Peor disponibilidad: {peor_proveedor.upper()} ({peor_disponibilidad:.2%})")
        
        # Tendencias
        if 'tendencias' in self.metricas_cache and self.metricas_cache['tendencias']:
            print(f"\n📈 TENDENCIAS DESTACADAS:")
            for proveedor, datos in self.metricas_cache['tendencias'].items():
                print(f"   • {proveedor.upper():10s}: {datos['tendencia']} ({datos['cambio_porcentual']:+.1f}%)")
        
        # Recomendaciones finales
        if 'rentabilidad' in self.metricas_cache and self.metricas_cache['rentabilidad']:
            rentabilidad = self.metricas_cache['rentabilidad']
            
            if rentabilidad:
                regiones_multiservidor = [r for r, d in rentabilidad.items() 
                                        if "ALTAMENTE RECOMENDADO" in d['recomendacion'] or "RENTABLE" in d['recomendacion']]
                
                regiones_monoservidor = [r for r, d in rentabilidad.items() 
                                       if "MONOSERVIDOR" in d['recomendacion']]
                
                print(f"\n🎯 RECOMENDACIONES ESTRATÉGICAS:")
                
                if regiones_multiservidor:
                    print(f"\n   ✅ IMPLEMENTAR MULTISERVIDOR EN ({len(regiones_multiservidor)} regiones):")
                    for i, region in enumerate(regiones_multiservidor[:5], 1):
                        datos = rentabilidad[region]
                        print(f"      {i}. {region}")
                        print(f"         • ROI: {datos['roi_estimado']:.0f}%")
                        print(f"         • Mejora: {datos['mejora_potencial_latencia']:.1f}%")
                        print(f"         • Disponibilidad: {datos['disponibilidad_simultanea']:.1%}")
                
                if regiones_monoservidor:
                    print(f"\n   🔴 MANTENER MONOSERVIDOR EN ({len(regiones_monoservidor)} regiones):")
                    for i, region in enumerate(regiones_monoservidor[:5], 1):
                        datos = rentabilidad[region]
                        print(f"      {i}. {region}")
                        print(f"         • Mejora insuficiente: {datos['mejora_potencial_latencia']:.1f}%")
        
        print(f"\n{'='*80}")
        print("💡 CONSEJOS DE IMPLEMENTACIÓN:")
        print(f"{'='*80}")
        print("1. 🎯 Comience con regiones críticas para el negocio")
        print("2. 📊 Implemente gradualmente, monitorizando resultados")
        print("3. 💰 Considere costos de balanceo de carga adicionales")
        print("4. 🤝 Evalúe proveedores complementarios (ej: AWS + Azure)")
        print("5. 📑 Revise SLA de cada proveedor para redundancia")


# Función principal para ejecutar el análisis
def main():
    """
    Función principal para ejecutar el análisis completo
    """
    print("=" * 80)
    print("ANÁLISIS DE RENTABILIDAD: MULTISERVIDOR vs MONOSERVIDOR")
    print("=" * 80)
    
    # Configuración
    RUTA_DATOS = "."  # Directorio actual
    
    UMBRAL_LATENCIA = 100  # ms
    UMBRAL_DISPONIBILIDAD = 0.95  # 95%
    
    try:
        # 1. Inicializar analizador
        print("\n🔧 INICIALIZANDO ANALIZADOR...")
        analizador = AnalizadorLatenciaMultiProveedor(
            ruta_datos=RUTA_DATOS,
            umbral_latencia=UMBRAL_LATENCIA,
            umbral_disponibilidad=UMBRAL_DISPONIBILIDAD
        )
        
        # 2. Cargar y procesar datos
        print("\n📂 CARGANDO DATOS...")
        df = analizador.cargar_y_unificar_datos(muestra_porcentaje=100)  # 100% de los datos
        
        # 3. Calcular métricas generales
        print("\n📊 CALCULANDO MÉTRICAS GENERALES...")
        analizador.calcular_metricas_generales()
        
        # 4. Comparar proveedores
        print("\n🔄 COMPARANDO PROVEEDORES...")
        analizador.analizar_comparacion_proveedores()
        
        # 5. Analizar tendencias
        print("\n📈 ANALIZANDO TENDENCIAS TEMPORALES...")
        analizador.analizar_tendencias_temporales()
        
        # 6. Analizar rentabilidad
        print("\n💰 ANALIZANDO RENTABILIDAD...")
        analizador.analizar_rentabilidad_multiservidor()
        
        # 7. Generar reporte completo
        print("\n📝 GENERANDO REPORTE COMPLETO...")
        dir_reportes = analizador.generar_reporte_completo()
        
        print(f"\n{'🎉' * 30}")
        print("🎊 ANÁLISIS COMPLETADO EXITOSAMENTE!")
        print(f"{'🎉' * 30}")
        print(f"\n📁 Los reportes se han guardado en: {dir_reportes}/")
        
    except Exception as e:
        print(f"\n{'❌' * 30}")
        print(f"❌ ERROR DURANTE EL ANÁLISIS: {str(e)}")
        print(f"{'❌' * 30}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # Ejecutar análisis completo
    main()