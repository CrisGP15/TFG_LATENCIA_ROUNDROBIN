import pandas as pd
import matplotlib.pyplot as plt
import os

# === 1. CREAR CARPETA SI NO EXISTE ===
output_dir = "graficas_latencias"
os.makedirs(output_dir, exist_ok=True)

# === 2. CARGAR DATOS ===
csv_file = "azure_cloudping_net_data.csv"
if not os.path.exists(csv_file):
    raise FileNotFoundError(f"No se encontró el archivo: {csv_file}")

df = pd.read_csv(csv_file)
df['latency_ms'] = pd.to_numeric(df['latency_ms'], errors='coerce')
df = df.dropna(subset=['latency_ms'])

# === 3. MAPEO DE CdD A CONTINENTES (Azure) ===
continent_mapping = {
    # América del Norte
    'Canada East': 'North America',
    'Canada Central': 'North America',
    'North Central US': 'North America',
    'West US 3': 'North America',
    'Mexico Central': 'North America',
    # Europa
    'West Europe': 'Europe',
    'Germany West Central': 'Europe',
    'Norway East': 'Europe',
    'Sweden Central': 'Europe',
    'Poland Central': 'Europe',
    'Italy North': 'Europe',
    'Switzerland North': 'Europe',
    # Asia
    'Southeast Asia': 'Asia',
    'East Asia': 'Asia',
    'Jio India West': 'Asia',
    # Oceanía
    'Australia Central': 'Oceania',
    # Medio Oriente
    'Israel Central': 'Middle East',
    'Qatar Central': 'Middle East',
    # África (si aplica en futuros datos)
    # 'South Africa North': 'Africa',
}

df['continent'] = df['datacenter'].map(continent_mapping)
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
df_plot = df_plot.sort_values('latency_ms').reset_index(drop=True)

# === 6. PALETA DE COLORES (COHERENTE CON TODOS) ===
continent_colors = {
    'Europe': '#1f77b4',  # Azul (base AWS)
    'North America': '#ff7f0e',  # Naranja
    'South America': '#2ca02c',  # Verde
    'Asia': '#d62728',  # Rojo
    'Oceania': '#9467bd',  # Púrpura
    'Africa': '#8c564b',  # Marrón
    'Middle East': '#e377c2',  # Rosa
}

df_plot['color'] = df_plot['continent'].map(continent_colors)

# === 7. GRÁFICO ===
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

# === 8. ESTÉTICA ===
plt.title("Latencia desde UAM a centros de datos Azure\n(Medias por CdD - Validación por continente)",
          fontsize=14, pad=20, fontweight='bold')
plt.xlabel("Latencia promedio (ms)", fontsize=12)
plt.ylabel("Centro de Datos (Azure)", fontsize=12)
plt.xlim(0, df_plot['latency_ms'].max() + 100)
plt.grid(axis='x', alpha=0.3, linestyle='--')

# === 9. LEYENDA POR CONTINENTE ===
from matplotlib.patches import Patch
legend_elements = [
    Patch(facecolor=continent_colors.get(cont, 'gray'), label=cont)
    for cont in continents
]
plt.legend(handles=legend_elements, title="Continente", loc='lower right', fontsize=9)
plt.tight_layout()

# === 10. GUARDAR ===
output_path = os.path.join(output_dir, "latencias_azure_validacion_medias_coloreado.png")
plt.savefig(output_path, dpi=300, bbox_inches='tight')
print(f"Gráfica Azure (azure_cloudping_net_data.csv) guardada en: {output_path}")
plt.show()