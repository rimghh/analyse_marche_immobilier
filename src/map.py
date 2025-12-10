import folium
import pandas as pd

# Charger le fichier CSV
df = pd.read_csv("../data/locamoi_tous_types_clean.csv")

# Vérifier que les colonnes GPS existent
if "lat" not in df.columns or "lon" not in df.columns:
    raise ValueError("Il manque les colonnes 'lat' et 'lon'. Géocode d'abord les adresses.")

# Nettoyage : supprimer lignes sans GPS
df = df.dropna(subset=["lat", "lon"])

# -------------------------------------------------------------------
# 1. CALCUL DU PRIX AU M2 MOYEN PAR VILLE
# -------------------------------------------------------------------
prix_m2_moyen_par_ville = df.groupby("ville")["prix_m2"].mean()

# -------------------------------------------------------------------
# 2. CRÉATION DE LA CARTE FOLIUM CENTRÉE SUR LA MOYENNE DES POINTS
# -------------------------------------------------------------------
center_lat = df["lat"].mean()
center_lon = df["lon"].mean()

carte = folium.Map(location=[center_lat, center_lon], zoom_start=11)

# -------------------------------------------------------------------
# 3. AJOUT DES POINTS SUR LA CARTE
# -------------------------------------------------------------------
for _, row in df.iterrows():

    ville = row["ville"]
    prix_m2_moyen = prix_m2_moyen_par_ville[ville]

    popup_html = f"""
    <b>{row['titre']}</b><br>
    <b>Catégorie :</b> {row['type_bien']}<br><br>
    <b>Loyer :</b> {row['loyer_mensuel_eur']} € / mois<br>
    <b>Surface :</b> {row['surface_m2']} m²<br>
    <b>Prix/m² :</b> {row['prix_m2']:.2f} €<br>
    <b>Prix/m² moyen ({ville}) :</b> {prix_m2_moyen:.2f} €
    """

    folium.CircleMarker(
        location=[row["lat"], row["lon"]],
        radius=6,
        fill=True,
        fill_color="blue",
        color="darkblue",
        popup=folium.Popup(popup_html, max_width=300),
        tooltip=f"{row['type_bien']} - {row['loyer_mensuel_eur']}€ / {row['prix_m2']}€/m²"
    ).add_to(carte)

# -------------------------------------------------------------------
# 4. SAUVEGARDE DE LA CARTE
# -------------------------------------------------------------------
output_path = "../data/carte_biens_locamoi.html"
carte.save(output_path)

print(f"✅ Carte interactive créée : {output_path}")
