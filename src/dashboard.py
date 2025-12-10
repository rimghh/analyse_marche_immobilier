import os
from pathlib import Path

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from analyses import (
    compute_global_price_stats,
    compute_price_stats_by_region,
    compute_surface_price_correlation,
    plot_price_distribution,
    plot_price_boxplot_by_region,
    plot_price_by_region_bar,
    plot_surface_vs_price,          # maintenant : graphique interactif Plotly
    plot_mean_price_m2_by_city,     # NOUVEAU : prix moyen au m² par ville (Plotly)
)


# ----------------- Chargement des données -----------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
CSV_PATH = DATA_DIR / "locamoi_tous_types_clean.csv"
MAP_HTML_PATH = DATA_DIR / "carte_biens_locamoi.html"   # adapte si besoin


@st.cache_data
def load_data(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)

    # Sécurité : conversion numérique
    for col in ["surface_m2", "nb_pieces", "loyer_mensuel_eur", "prix_m2"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# ----------------- Filtres -----------------

def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.header("Filtres")

    # Région
    regions = ["Toutes"] + sorted(df["region"].dropna().unique().tolist())
    region_choice = st.sidebar.selectbox("Région", regions)

    # Ville (filtrée par région)
    if region_choice != "Toutes":
        df_region = df[df["region"] == region_choice]
    else:
        df_region = df

    cities = ["Toutes"] + sorted(df_region["ville"].dropna().unique().tolist())
    city_choice = st.sidebar.selectbox("Ville", cities)

    # Type de bien
    types = ["Tous"] + sorted(df["type_bien"].dropna().unique().tolist())
    type_choice = st.sidebar.selectbox("Type de bien", types)

    # Sliders numériques
    # Surface
    surface_min = float(df["surface_m2"].min())
    surface_max = float(df["surface_m2"].max())
    surface_range = st.sidebar.slider(
        "Surface (m²)",
        min_value=float(surface_min),
        max_value=float(surface_max),
        value=(float(surface_min), float(surface_max)),
    )

    # Loyer
    rent_min = float(df["loyer_mensuel_eur"].min())
    rent_max = float(df["loyer_mensuel_eur"].max())
    rent_range = st.sidebar.slider(
        "Loyer mensuel (€)",
        min_value=float(rent_min),
        max_value=float(rent_max),
        value=(float(rent_min), float(rent_max)),
    )

    # Nombre de pièces
    pieces_min = int(df["nb_pieces"].min())
    pieces_max = int(df["nb_pieces"].max())
    pieces_range = st.sidebar.slider(
        "Nombre de pièces",
        min_value=pieces_min,
        max_value=pieces_max,
        value=(pieces_min, pieces_max),
    )

    # --- Application des filtres ---
    filtered = df.copy()

    if region_choice != "Toutes":
        filtered = filtered[filtered["region"] == region_choice]

    if city_choice != "Toutes":
        filtered = filtered[filtered["ville"] == city_choice]

    if type_choice != "Tous":
        filtered = filtered[filtered["type_bien"] == type_choice]

    filtered = filtered[
        (filtered["surface_m2"].between(surface_range[0], surface_range[1]))
        & (filtered["loyer_mensuel_eur"].between(rent_range[0], rent_range[1]))
        & (filtered["nb_pieces"].between(pieces_range[0], pieces_range[1]))
    ]

    return filtered


# ----------------- App Streamlit -----------------

def main():
    st.set_page_config(page_title="Locamoi - Tableau de bord", layout="wide")

    st.title("Tableau de bord Locamoi")

    # Chargement
    if not CSV_PATH.exists():
        st.error(f"CSV introuvable : {CSV_PATH}")
        st.stop()

    df = load_data(CSV_PATH)

    # Filtres
    df_filtered = apply_filters(df)

    if df_filtered.empty:
        st.warning("Aucun bien ne correspond aux filtres sélectionnés.")
        return

    st.markdown(f"**Nombre de biens après filtres :** {len(df_filtered)}")

    # Tabs pour structurer l'app
    tab_overview, tab_stats, tab_plots, tab_map = st.tabs(
        ["Vue d'ensemble", "Stats prix au m²", "Graphiques", "Carte interactive"]
    )

    # ---- Vue d'ensemble ----
    with tab_overview:
        st.subheader("Aperçu des biens filtrés")
        st.dataframe(
            df_filtered[
                [
                    "ville",
                    "region",
                    "type_bien",
                    "surface_m2",
                    "nb_pieces",
                    "loyer_mensuel_eur",
                    "prix_m2",
                ]
            ].reset_index(drop=True)
        )

    # ---- Stats prix au m² ----
    with tab_stats:
        st.subheader("Statistiques sur le prix au m²")

        global_stats = compute_global_price_stats(df_filtered)
        col1, col2 = st.columns(2)
        col1.metric("Prix moyen au m²", f"{global_stats['mean_price_m2']:.2f} €")
        col2.metric("Prix médian au m²", f"{global_stats['median_price_m2']:.2f} €")

        st.markdown("### Par région")
        stats_region = compute_price_stats_by_region(df_filtered)
        st.dataframe(stats_region.rename(columns={"mean": "moyenne", "median": "médiane"}))

        corr = compute_surface_price_correlation(df_filtered)
        st.markdown(f"**Corrélation surface / loyer mensuel :** `{corr:.3f}`")

    # ---- Graphiques ----
    with tab_plots:
        st.subheader("Histogramme du prix au m²")
        fig_hist = plot_price_distribution(df_filtered)
        st.pyplot(fig_hist)

        st.subheader("Boxplot du prix au m² par région")
        fig_box = plot_price_boxplot_by_region(df_filtered, top_n=10)
        st.pyplot(fig_box)

        st.subheader("Prix moyen au m² par région")
        fig_bar = plot_price_by_region_bar(df_filtered)
        st.pyplot(fig_bar)

        st.subheader("Surface vs Loyer mensuel (graphique interactif)")
        fig_scatter = plot_surface_vs_price(df_filtered)
        st.plotly_chart(fig_scatter, use_container_width=True)

        st.subheader("Prix moyen au m² par ville (graphique interactif)")
        top_n_city = st.slider(
            "Nombre de villes à afficher (classées par prix moyen au m² décroissant)",
            min_value=5,
            max_value=50,
            value=20,
            step=1,
        )
        fig_city = plot_mean_price_m2_by_city(df_filtered, top_n=top_n_city)
        st.plotly_chart(fig_city, use_container_width=True)

    # ---- Carte interactive ----
    with tab_map:
        st.subheader("Carte interactive (Folium)")

        if MAP_HTML_PATH.exists():
            with open(MAP_HTML_PATH, "r", encoding="utf-8") as f:
                map_html = f.read()
            # On insère le HTML dans Streamlit
            components.html(map_html, height=600, scrolling=True)
        else:
            st.info(
                f"Fichier de carte HTML non trouvé : {MAP_HTML_PATH}\n"
                "Lance d'abord map.py pour générer la carte."
            )


if __name__ == "__main__":
    main()
