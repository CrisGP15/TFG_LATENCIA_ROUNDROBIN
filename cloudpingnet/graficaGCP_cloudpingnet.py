import pandas as pd
import matplotlib.pyplot as plt
import os
import numpy as np

# === 1. CREAR CARPETA SI NO EXISTE ===
output_dir = "graficas_latencias"
os.makedirs(output_dir, exist_ok=True)

# === 2. CARGAR DATOS ===
csv_file = "gcp_cloudping_net_data.csv"
if not os.path.exists(csv_file):
    raise FileNotFoundError(f"No se encontró el archivo: {csv_file}")

df = pd.read_csv(csv_file)

# Excluir 'Failed' y convertir a numérico
df = df[df['latency_ms'] != 'Failed']
df['latency_ms'] = pd.to_numeric(df['latency_ms'], errors='coerce')
df = df.dropna(subset=['latency_ms'])

# === 3. MAPEO DE REGIONES A CONTINENTES (GCP) ===
continent_mapping = {
    # América del Norte
    'North America Northeast 1': 'North America',
    'North America Northeast 2': 'North America',
    'US Central 1': 'North America',
    'US Central 2': 'North America',
    'US East 1': 'North America',
    'US East 4': 'North America',
    'US West': 'North America',
    'US West 2': 'North America',
    'US West 3': 'North America',
    'US West 4': 'North America',

    # América del Sur
    'South America East': 'South America',
    'South America West': 'South America',

    # Europa
    'Europe Central 2': 'Europe',
    'Europe North 1': 'Europe',
    'Europe West 1': 'Europe',
    'Europe West 2': 'Europe',
    'Europe West 3': 'Europe',
    'Europe West 4': 'Europe',
    'Europe West 5': 'Europe',
    'Europe West 6': 'Europe',

    # Asia
    'Asia East 1': 'Asia',
    'Asia Northeast 1': 'Asia',
    'Asia Northeast 2': 'Asia',
    'Asia Northeast 3': 'Asia',
    'Asia South 1': 'Asia',
    'Asia South 2': 'Asia',
    'Asia Southeast 1': 'Asia',
    'Asia Southeast 2': 'Asia',

    # Oceanía
    'Australia Southeast 1': 'Oceania',
    'Australia Southeast 2': 'Oceania',

    # Medio Oriente / África
    'Middle East West 1': 'Middle East',
    'Middle East Central 1': 'Middle East',
    'Africa South 1': 'Africa',
}

# Aplicar mapeo usando 'region' → 'datacenter'
df['continent'] = df['region'].map(continent_mapping)
df = df.dropna(subset=['continent'])

# === 4. CALCULAR MEDIA POR CdD ===
df_mean = df.groupby(['datacenter', 'continent'])['latency_ms'].mean().reset_index()
df_mean = df_mean.sort_values(['continent', 'latency_ms'])

# === 5. SELECCIONAR 2 REPRESENTATIVOS POR CONTINENTE ===
selected_datacenters = []
continents = df_mean['continent'].unique()

for continent in continents:
    subset = df_mean[df_mean['continent'] == continent]
    if len(subset) >= 2:
        fastest = subset.nsmallest(1, 'latency_ms')
        middle = subset.nsmallest(3, 'latency_ms').iloc[-1:]  # Tercero más rápido
        selected = pd.concat([fastest, middle])
    else:
        selected = subset
    selected_datacenters.append(selected)

df_plot = pd.concat(selected_datacenters).drop_duplicates()

# === 6. ORDENAR POR LATENCIA GLOBAL ===
df_plot = df_plot.sort_values('latency_ms').reset_index(drop=True)

# === 7. PALETA DE COLORES POR CONTINENTE (IGUAL QUE AWS) ===
continent_colors = {
    'Europe': '#1f77b4',        # Azul
    'North America': '#ff7f0e', # Naranja
    'South America': '#2ca02c', # Verde
    'Asia': '#d62728',          # Rojo
    'Oceania': '#9467bd',       # Púrpura
    'Africa': '#8c564b',        # Marrón
    'Middle East': '#e377c2',   # Rosa
}

# Asignar color
df_plot['color'] = df_plot['continent'].map(continent_colors)

# === 8. GRÁFICO ===
plt.figure(figsize=(10, 7))
bars = plt.barh(
    y=df_plot['datacenter'],
    width=df_plot['latency_ms'],
    color=df_plot['color'],
    edgecolor='black',
    height=0.6
)

# Añadir valores
for i, bar in enumerate(bars):
    width = bar.get_width()
    plt.text(
        width + 5, bar.get_y() + bar.get_height()/2,
        f'{width:.1f} ms',
        va='center', ha='left',
        fontsize=10, fontweight='bold', color='black'
    )

# === 9. ESTÉTICA ===
plt.title("Latencia desde UAM a centros de datos GCP\n(Medias por CdD - Validación por continente)",
          fontsize=14, pad=20, fontweight='bold')
plt.xlabel("Latencia promedio (ms)", fontsize=12)
plt.ylabel("Centro de Datos (GCP)", fontsize=12)
plt.xlim(0, df_plot['latency_ms'].max() + 100)
plt.grid(axis='x', alpha=0.3, linestyle='--')

# === 10. LEYENDA POR CONTINENTE ===
from matplotlib.patches import Patch
legend_elements = [
    Patch(facecolor=continent_colors.get(cont, 'gray'), label=cont)
    for cont in continents
]
plt.legend(handles=legend_elements, title="Continente", loc='lower right', fontsize=9)

plt.tight_layout()

# === 11. GUARDAR ===
output_path = os.path.join(output_dir, "latencias_gcp_validacion_medias_coloreado.png")
plt.savefig(output_path, dpi=300, bbox_inches='tight')
print(f"Gráfica GCP (coloreada por continente) guardada en: {output_path}")
plt.show()