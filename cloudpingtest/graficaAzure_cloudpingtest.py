import pandas as pd
import matplotlib.pyplot as plt
import os

# === 1. CREAR CARPETA SI NO EXISTE ===
output_dir = "graficas_latencias"
os.makedirs(output_dir, exist_ok=True)

# === 2. CARGAR DATOS ===
csv_file = "azure_mean_latency.csv"
if not os.path.exists(csv_file):
    raise FileNotFoundError(f"No se encontró el archivo: {csv_file}")

df = pd.read_csv(csv_file)
df['mean_ms'] = pd.to_numeric(df['mean_ms'], errors='coerce')
df = df.dropna(subset=['mean_ms'])

# === 3. MAPEO DE REGION_NAME A CONTINENTES (Azure) ===
# Usamos region_name para el mapeo (es más legible)
continent_mapping = {
    # Europa
    'Austria (Austria East)': 'Europe',
    'France (France Central)': 'Europe',
    'Germany (Germany West Central)': 'Europe',
    'Italy (Italy North)': 'Europe',
    'Norway (Norway East)': 'Europe',
    'Poland (Poland Central)': 'Europe',
    'Spain (Spain Central)': 'Europe',
    'Sweden (Sweden Central)': 'Europe',
    'Switzerland (Switzerland North)': 'Europe',
    'United Kingdom (UK South)': 'Europe',
    'United Kingdom (UK West)': 'Europe',
    'Europe (North Europe)': 'Europe',
    'Europe (West Europe)': 'Europe',

    # América del Norte
    'Canada (Canada Central)': 'North America',
    'Canada (Canada East)': 'North America',
    'Mexico (Mexico Central)': 'North America',
    'United States (Central US)': 'North America',
    'United States (East US)': 'North America',
    'United States (East US 2)': 'North America',
    'United States (North Central US)': 'North America',
    'United States (South Central US)': 'North America',
    'United States (West Central US)': 'North America',
    'United States (West US)': 'North America',
    'United States (West US 2)': 'North America',
    'United States (West US 3)': 'North America',

    # América del Sur
    'Brazil (Brazil South)': 'South America',
    'Chile (Chile Central)': 'South America',

    # África
    'Africa (South Africa North)': 'Africa',

    # Asia
    'India (Central India)': 'Asia',
    'India (South India)': 'Asia',
    'India (West India)': 'Asia',
    'Asia Pacific (East Asia)': 'Asia',
    'Indonesia (Indonesia Central)': 'Asia',
    'Japan (Japan East)': 'Asia',
    'Japan (Japan West)': 'Asia',
    'Korea (Korea Central)': 'Asia',
    'Korea (Korea South)': 'Asia',
    'Malaysia (Malaysia West)': 'Asia',
    'Asia Pacific (Southeast Asia)': 'Asia',

    # Oceanía
    'Australia (Australia Central)': 'Oceania',
    'Australia (Australia East)': 'Oceania',
    'Australia (Australia Southeast)': 'Oceania',
    'New Zealand (New Zealand North)': 'Oceania',

    # Medio Oriente
    'Israel (Israel Central)': 'Middle East',
    'Qatar (Qatar Central)': 'Middle East',
    'United Arab Emirates (UAE North)': 'Middle East',
}

df['continent'] = df['region_name'].map(continent_mapping)
df = df.dropna(subset=['continent'])

# === 4. CALCULAR MEDIA POR CdD (usamos region_name como "datacenter") ===
df_mean = df.groupby(['region_name', 'continent'])['mean_ms'].mean().reset_index()
df_mean = df_mean.rename(columns={'region_name': 'datacenter'})
df_mean = df_mean.sort_values(['continent', 'mean_ms'])

# === 5. SELECCIONAR 2 REPRESENTATIVOS POR CONTINENTE ===
selected_datacenters = []
continents = df_mean['continent'].unique()

for continent in continents:
    subset = df_mean[df_mean['continent'] == continent]
    if len(subset) >= 2:
        fastest = subset.nsmallest(1, 'mean_ms')
        middle = subset.nsmallest(3, 'mean_ms').iloc[-1:]  # Tercero más rápido
        selected = pd.concat([fastest, middle])
    else:
        selected = subset
    selected_datacenters.append(selected)

df_plot = pd.concat(selected_datacenters).drop_duplicates()
df_plot = df_plot.sort_values('mean_ms').reset_index(drop=True)

# === 6. PALETA DE COLORES (IDÉNTICA A TODAS) ===
continent_colors = {
    'Europe': '#1f77b4',        # Azul
    'North America': '#ff7f0e', # Naranja
    'South America': '#2ca02c', # Verde
    'Asia': '#d62728',          # Rojo
    'Oceania': '#9467bd',       # Púrpura
    'Africa': '#8c564b',        # Marrón
    'Middle East': '#e377c2',   # Rosa
}

df_plot['color'] = df_plot['continent'].map(continent_colors)

# === 7. GRÁFICO ===
plt.figure(figsize=(10, 7))
bars = plt.barh(
    y=df_plot['datacenter'],
    width=df_plot['mean_ms'],
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

# === 8. ESTÉTICA (EXACTAMENTE COMO LAS DEMÁS) ===
plt.title("Latencia desde UAM a centros de datos Azure\n(Medias por CdD - cloudpingtest.com)",
          fontsize=14, pad=20, fontweight='bold')
plt.xlabel("Latencia promedio (ms)", fontsize=12)
plt.ylabel("Centro de Datos (Azure)", fontsize=12)
plt.xlim(0, df_plot['mean_ms'].max() + 100)
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
output_path = os.path.join(output_dir, "latencias_azure_medias_coloreado.png")
plt.savefig(output_path, dpi=300, bbox_inches='tight')
print(f"Gráfica Azure guardada en: {output_path}")
plt.show()