import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import os

# === 1. CONFIGURACIÓN ===
csv_file = "cloudping_co_data.csv"
output_dir = "graficas_latencias"
os.makedirs(output_dir, exist_ok=True)

# === 2. CARGAR DATOS ===
if not os.path.exists(csv_file):
    raise FileNotFoundError(f"No se encontró el archivo: {csv_file}")

df = pd.read_csv(csv_file)
df['latency_ms'] = pd.to_numeric(df['latency_ms'], errors='coerce')
df = df.dropna(subset=['latency_ms'])

print(f"Datos cargados: {len(df)} filas válidas.")

# === 3. MAPEO DE REGIONES A NOMBRES LEGIBLES ===
region_name_mapping = {
    'af-south-1': 'Cape Town (África)',
    'ap-east-1': 'Hong Kong',
    'ap-east-2': 'Hong Kong (East 2)',
    'ap-northeast-1': 'Tokio',
    'ap-northeast-2': 'Seúl',
    'ap-northeast-3': 'Osaka',
    'ap-south-1': 'Mumbai',
    'ap-south-2': 'Hyderabad',
    'ap-southeast-1': 'Singapur',
    'ap-southeast-2': 'Sídney',
    'ap-southeast-3': 'Yakarta',
    'ap-southeast-4': 'Melbourne',
    'ap-southeast-5': 'Malasia',
    'ap-southeast-6': 'Auckland',
    'ap-southeast-7': 'Bangkok',
    'ca-central-1': 'Montreal',
    'ca-west-1': 'Calgary',
    'eu-central-1': 'Fráncfort',
    'eu-central-2': 'Zúrich',
    'eu-north-1': 'Estocolmo',
    'eu-south-1': 'Milán',
    'eu-south-2': 'España',
    'eu-west-1': 'Irlanda',
    'eu-west-2': 'Londres',
    'eu-west-3': 'París',
    'il-central-1': 'Tel Aviv',
    'me-central-1': 'Emiratos Árabes',
    'me-south-1': 'Baréin',
    'mx-central-1': 'Querétaro',
    'sa-east-1': 'São Paulo',
    'us-east-1': 'N. Virginia',
    'us-east-2': 'Ohio',
    'us-west-1': 'N. California',
    'us-west-2': 'Oregón',
}

# === 4. FUNCIÓN PARA CONTINENTE ===
def get_continent(region):
    if region.startswith('af-'): return 'África'
    if region.startswith('ap-'):
        if any(x in region for x in ['southeast-2', 'southeast-4', 'southeast-6']): return 'Oceanía'
        return 'Asia'
    if region.startswith('eu-'): return 'Europa'
    if region.startswith('us-') or region.startswith('ca-') or region == 'mx-central-1': return 'Norteamérica'
    if region.startswith('sa-'): return 'Sudamérica'
    if region.startswith('me-') or region == 'il-central-1': return 'Oriente Medio'
    return 'Desconocido'

# === 5. COLORES POR CONTINENTE ===
continent_colors = {
    'Europa': '#1f77b4',
    'Norteamérica': '#ff7f0e',
    'Sudamérica': '#2ca02c',
    'Asia': '#d62728',
    'Oceanía': '#9467bd',
    'África': '#8c564b',
    'Oriente Medio': '#e377c2',
    'Desconocido': '#7f7f7f'
}

# === 6. ELIGE TU REGIÓN DE ORIGEN (puedes cambiarla) ===
from_region = 'eu-west-3'  # París

# === 7. FILTRAR Y PREPARAR DATOS ===
df_from = df[df['from_region'] == from_region].copy()
if df_from.empty:
    raise ValueError(f"No hay datos para la región de origen: {from_region}")

# Excluir latencia a sí mismo
df_from = df_from[df_from['to_region'] != from_region]

# Añadir nombres y continentes
df_from['datacenter'] = df_from['to_region'].map(region_name_mapping).fillna(df_from['to_region'])
df_from['continent'] = df_from['to_region'].apply(get_continent)

# Calcular latencia promedio por destino
df_mean = df_from.groupby(['to_region', 'datacenter', 'continent'])['latency_ms'].mean().reset_index()
df_mean = df_mean.sort_values('latency_ms')

# === 8. SELECCIONAR 2 POR CONTINENTE: el más rápido + uno intermedio ===
selected = []
for continent in df_mean['continent'].unique():
    subset = df_mean[df_mean['continent'] == continent]
    if len(subset) == 0:
        continue
    # Más rápido
    fastest = subset.nsmallest(1, 'latency_ms')
    # Intermedio (si hay al menos 3, el del medio; si no, el segundo)
    if len(subset) >= 3:
        middle = subset.nsmallest(3, 'latency_ms').iloc[[1]]  # segundo más rápido
    elif len(subset) == 2:
        middle = subset.nlargest(1, 'latency_ms')  # el más lento de los 2
    else:
        middle = pd.DataFrame()
    
    selected.append(fastest)
    if not middle.empty:
        selected.append(middle)

df_plot = pd.concat(selected).drop_duplicates().sort_values('latency_ms')
df_plot['color'] = df_plot['continent'].map(continent_colors)

# === 9. GRÁFICO ===
plt.figure(figsize=(11, 8))
bars = plt.barh(
    y=df_plot['datacenter'],
    width=df_plot['latency_ms'],
    color=df_plot['color'],
    edgecolor='black',
    height=0.7
)

# Etiquetas de valores
for bar in bars:
    width = bar.get_width()
    plt.text(
        width + 3,
        bar.get_y() + bar.get_height()/2,
        f'{width:.1f} ms',
        va='center', ha='left', fontsize=10, fontweight='bold'
    )

# Títulos y etiquetas
origin_name = region_name_mapping.get(from_region, from_region)
plt.title(f'Latencia promedio desde {origin_name} (AWS)\n2 centros representativos por continente', 
          fontsize=16, pad=20, fontweight='bold')
plt.xlabel('Latencia (ms)', fontsize=12)
plt.ylabel('Centro de datos destino', fontsize=12)
plt.xlim(0, df_plot['latency_ms'].max() * 1.15)
plt.grid(axis='x', alpha=0.3, linestyle='--')

# Leyenda por continente
legend_elements = [Patch(facecolor=continent_colors[c], label=c) for c in df_plot['continent'].unique()]
plt.legend(handles=legend_elements, title='Continente', loc='lower right', fontsize=10)

plt.tight_layout()

# === 10. GUARDAR ===
output_path = os.path.join(output_dir, f"latencia_desde_{from_region}_representativos.png")
plt.savefig(output_path, dpi=300, bbox_inches='tight')
print(f"\nGRÁFICA GENERADA: {output_path}")
plt.close()