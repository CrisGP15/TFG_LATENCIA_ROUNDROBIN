# generar_graficas_cloudping_info.py

import pandas as pd
import matplotlib.pyplot as plt
import os
from matplotlib.patches import Patch

# === 1. CONFIGURACIÓN ===
output_dir = "graficas_latencias"
os.makedirs(output_dir, exist_ok=True)

csv_file = "cloudping_info_data.csv"
if not os.path.exists(csv_file):
    raise FileNotFoundError(f"No se encontró el archivo: {csv_file}")

df = pd.read_csv(csv_file)
df['latency_ms'] = pd.to_numeric(df['latency_ms'], errors='coerce')
df = df.dropna(subset=['latency_ms'])

# === 2. LIMPIAR NOMBRES DE PROVEEDORES ===
df['provider'] = df['provider'].str.replace('cloudpinginfo ', '', regex=False).str.strip()

# === 3. MAPEO DE datacenter A CONTINENTES (GENERAL) ===
continent_mapping = {
    # Europa
    'Spain': 'Europe', 'Madrid': 'Europe', 'Paris': 'Europe', 'France': 'Europe', 'London': 'Europe',
    'Frankfurt': 'Europe', 'Germany': 'Europe', 'Amsterdam': 'Europe', 'Netherlands': 'Europe',
    'Milan': 'Europe', 'Italy': 'Europe', 'Warsaw': 'Europe', 'Poland': 'Europe',
    'Stockholm': 'Europe', 'Sweden': 'Europe', 'Finland': 'Europe', 'Hamina': 'Europe',
    'Zurich': 'Europe', 'Switzerland': 'Europe', 'Belgium': 'Europe', 'St. Ghislain': 'Europe',
    'Ireland': 'Europe', 'Manchester': 'Europe', 'Gravelines': 'Europe', 'Roubaix': 'Europe',
    'Strasbourg': 'Europe', 'Nuremberg': 'Europe', 'Falkenstein': 'Europe', 'Turin': 'Europe',
    'Berlin': 'Europe',

    # América del Norte
    'Virginia': 'North America', 'Ohio': 'North America', 'California': 'North America',
    'Oregon': 'North America', 'Canada Central': 'North America', 'Canada West': 'North America',
    'Toronto': 'North America', 'Montreal': 'North America', 'Montréal': 'North America',
    'New York': 'North America', 'New Jersey': 'North America', 'Chicago': 'North America',
    'Dallas': 'North America', 'Atlanta': 'North America', 'Washington DC': 'North America',
    'Las Vegas': 'North America', 'San Francisco': 'North America', 'Silicon Valley': 'North America',
    'Seattle': 'North America', 'Salt Lake City': 'North America', 'Los Angeles': 'North America',
    'Mexico': 'North America', 'Mexico City': 'North America', 'Quebec': 'North America',
    'Beauharnois': 'North America', 'Hillsboro': 'North America', 'Ashburn': 'North America',
    'Columbus': 'North America', 'Iowa': 'North America', 'South Carolina': 'North America',
    'North Virginia': 'North America',

    # América del Sur
    'São Paulo': 'South America', 'Sao Paulo': 'South America', 'Santiago': 'South America',

    # África
    'Cape Town': 'Africa', 'Johannesburg': 'Africa',

    # Asia
    'Singapore': 'Asia', 'Mumbai': 'Asia', 'Delhi': 'Asia', 'Delhi NCR': 'Asia', 'Bangalore': 'Asia',
    'Hyderabad': 'Asia', 'Tokyo': 'Asia', 'Osaka': 'Asia', 'Seoul': 'Asia', 'Hong Kong': 'Asia',
    'Taipei': 'Asia', 'Jakarta': 'Asia', 'Taiwan': 'Asia', 'Beijing': 'Asia', 'Ningxia': 'Asia',

    # Oceanía
    'Sydney': 'Oceania', 'Melbourne': 'Oceania',

    # Medio Oriente
    'Doha': 'Middle East', 'Israel': 'Middle East', 'Tel Aviv': 'Middle East',
    'Bahrain': 'Middle East', 'UAE': 'Middle East',
}

# === 4. PALETA DE COLORES (IDÉNTICA A TODAS) ===
continent_colors = {
    'Europe': '#1f77b4',        # Azul
    'North America': '#ff7f0e', # Naranja
    'South America': '#2ca02c', # Verde
    'Asia': '#d62728',          # Rojo
    'Oceania': '#9467bd',       # Púrpura
    'Africa': '#8c564b',        # Marrón
    'Middle East': '#e377c2',   # Rosa
}

# === 5. FUNCIÓN PARA GENERAR GRÁFICA POR PROVEEDOR ===
def generar_grafica_proveedor(provider_df, provider_name):
    df_p = provider_df.copy()
    df_p['continent'] = df_p['datacenter'].map(continent_mapping)
    df_p = df_p.dropna(subset=['continent'])

    if df_p.empty:
        print(f"Proveedor {provider_name}: No hay datos con continente conocido.")
        return

    df_mean = df_p.groupby(['datacenter', 'continent'])['latency_ms'].mean().reset_index()
    df_mean = df_mean.sort_values(['continent', 'latency_ms'])

    selected = []
    for continent in df_mean['continent'].unique():
        subset = df_mean[df_mean['continent'] == continent]
        if len(subset) >= 2:
            fastest = subset.nsmallest(1, 'latency_ms')
            middle = subset.nsmallest(3, 'latency_ms').iloc[-1:]
            selected.append(pd.concat([fastest, middle]))
        else:
            selected.append(subset)
    df_plot = pd.concat(selected).drop_duplicates().sort_values('latency_ms').reset_index(drop=True)
    df_plot['color'] = df_plot['continent'].map(continent_colors)

    plt.figure(figsize=(10, 7))
    bars = plt.barh(y=df_plot['datacenter'], width=df_plot['latency_ms'],
                    color=df_plot['color'], edgecolor='black', height=0.6)

    for bar in bars:
        width = bar.get_width()
        plt.text(width + 5, bar.get_y() + bar.get_height()/2,
                 f'{width:.1f} ms', va='center', ha='left',
                 fontsize=10, fontweight='bold', color='black')

    plt.title(f"Latencia desde UAM a centros de datos {provider_name}\n(Medias por CdD - cloudping.info)",
              fontsize=14, pad=20, fontweight='bold')
    plt.xlabel("Latencia promedio (ms)", fontsize=12)
    plt.ylabel("Centro de Datos", fontsize=12)
    plt.xlim(0, df_plot['latency_ms'].max() + 100)
    plt.grid(axis='x', alpha=0.3, linestyle='--')

    legend_elements = [Patch(facecolor=continent_colors.get(c, 'gray'), label=c)
                       for c in df_plot['continent'].unique()]
    plt.legend(handles=legend_elements, title="Continente", loc='lower right', fontsize=9)
    plt.tight_layout()

    safe_name = provider_name.replace(' ', '_').replace('/', '_')
    output_path = os.path.join(output_dir, f"latencias_{safe_name}_cloudpinginfo_medias_coloreado.png")
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Gráfica guardada: {output_path}")
    plt.close()

# === 6. GENERAR GRÁFICA POR PROVEEDOR ===
proveedores = df['provider'].unique()
for prov in proveedores:
    print(f"\nGenerando gráfica para: {prov}")
    prov_df = df[df['provider'] == prov]
    generar_grafica_proveedor(prov_df, prov)

print("\nTodas las gráficas generadas en la carpeta 'graficas_latencias/'")