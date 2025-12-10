import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px

sns.set(style="whitegrid")


# ---------- Statistiques simples ----------

def compute_global_price_stats(df: pd.DataFrame) -> pd.Series:
    """
    Retourne moyenne et médiane du prix au m² (colonne 'prix_m2').
    """
    s = df["prix_m2"].dropna()
    return pd.Series(
        {
            "mean_price_m2": s.mean(),
            "median_price_m2": s.median(),
        }
    )


def compute_price_stats_by_region(df: pd.DataFrame) -> pd.DataFrame:
    """
    Moyenne et médiane du prix au m² par région.
    """
    grp = df.dropna(subset=["prix_m2"]).groupby("region")["prix_m2"]
    return grp.agg(["mean", "median"]).sort_values("mean", ascending=False)


def compute_surface_price_correlation(df: pd.DataFrame) -> float:
    """
    Corrélation (Pearson) entre surface (m²) et loyer mensuel.
    """
    sub = df[["surface_m2", "loyer_mensuel_eur"]].dropna()
    if sub.empty:
        return float("nan")
    return sub["surface_m2"].corr(sub["loyer_mensuel_eur"])


# ---------- Figures / visualisations matplotlib ----------

def plot_price_distribution(df: pd.DataFrame):
    """
    Histogramme de la distribution du prix au m².
    """
    s = df["prix_m2"].dropna()

    fig, ax = plt.subplots(figsize=(8, 4))
    sns.histplot(s, bins=30, kde=True, ax=ax)
    ax.set_title("Distribution du prix au m²")
    ax.set_xlabel("Prix au m² (€)")
    ax.set_ylabel("Nombre de biens")

    return fig


def plot_price_boxplot_by_region(df: pd.DataFrame, top_n: int = 10):
    """
    Boxplot du prix au m² par région (limité aux top_n régions les plus représentées).
    """
    sub = df.dropna(subset=["prix_m2"]).copy()

    # On garde les régions les plus présentes
    top_regions = (
        sub["region"]
        .value_counts()
        .head(top_n)
        .index
    )
    sub = sub[sub["region"].isin(top_regions)]

    fig, ax = plt.subplots(figsize=(10, 5))
    sns.boxplot(data=sub, x="region", y="prix_m2", ax=ax)
    ax.set_title(f"Boxplot du prix au m² par région (top {top_n} régions)")
    ax.set_xlabel("Région")
    ax.set_ylabel("Prix au m² (€)")
    ax.tick_params(axis="x", rotation=45)

    return fig


def plot_price_by_region_bar(df: pd.DataFrame):
    """
    Barplot du prix moyen au m² par région (matplotlib).
    """
    stats_reg = compute_price_stats_by_region(df).reset_index()

    fig, ax = plt.subplots(figsize=(10, 5))
    sns.barplot(
        data=stats_reg,
        x="region",
        y="mean",
        ax=ax,
    )
    ax.set_title("Prix moyen au m² par région")
    ax.set_xlabel("Région")
    ax.set_ylabel("Prix moyen au m² (€)")
    ax.tick_params(axis="x", rotation=45)

    return fig


# ---------- Figures / visualisations dynamiques (Plotly) ----------

def plot_surface_vs_price(df: pd.DataFrame):
    """
    Nuage de points interactif (Plotly) : surface (m²) vs loyer mensuel.

    - Couleurs par type de bien
    - Infobulle : ville, type de bien, prix au m²
    - Titre indiquant la corrélation surface / loyer
    """
    cols = ["surface_m2", "loyer_mensuel_eur", "ville", "type_bien"]
    if "prix_m2" in df.columns:
        cols.append("prix_m2")

    sub = df[cols].dropna(subset=["surface_m2", "loyer_mensuel_eur"]).copy()

    corr = compute_surface_price_correlation(df)

    hover_cols = ["ville", "type_bien"]
    if "prix_m2" in sub.columns:
        hover_cols.append("prix_m2")

    fig = px.scatter(
        sub,
        x="surface_m2",
        y="loyer_mensuel_eur",
        color="type_bien",
        hover_data=hover_cols,
        labels={
            "surface_m2": "Surface (m²)",
            "loyer_mensuel_eur": "Loyer mensuel (€)",
            "type_bien": "Type de bien",
            "prix_m2": "Prix au m² (€)",
        },
        title=f"Surface vs Loyer mensuel (corrélation = {corr:.2f})",
    )

    fig.update_traces(marker=dict(opacity=0.7, size=9))
    fig.update_layout(margin=dict(l=20, r=20, t=60, b=40))

    return fig


def plot_mean_price_m2_by_city(df: pd.DataFrame, top_n: int = 30):
    """
    Graphique interactif (Plotly) du prix moyen au m² par ville.

    - df est déjà filtré dans Streamlit (région / ville / type / etc.)
    - On agrège par ville, puis on affiche les top_n villes les plus chères.
    """
    if "prix_m2" not in df.columns:
        raise KeyError("La colonne 'prix_m2' est manquante dans le DataFrame.")

    sub = df.dropna(subset=["prix_m2", "ville"]).copy()

    if sub.empty:
        # On renvoie une figure vide pour que Streamlit ne plante pas
        fig = px.bar(title="Aucune donnée disponible pour ce filtre")
        return fig

    stats = (
        sub.groupby("ville")
        .agg(
            mean_price_m2=("prix_m2", "mean"),
            nb_biens=("prix_m2", "size"),
        )
        .reset_index()
        .sort_values("mean_price_m2", ascending=False)
        .head(top_n)
    )

    fig = px.bar(
        stats,
        x="ville",
        y="mean_price_m2",
        hover_data=["nb_biens"],
        labels={
            "ville": "Ville",
            "mean_price_m2": "Prix moyen au m² (€)",
            "nb_biens": "Nombre de biens",
        },
        title=f"Prix moyen au m² par ville (top {top_n})",
    )

    fig.update_layout(
        xaxis_tickangle=-45,
        margin=dict(l=20, r=20, t=60, b=80),
    )

    return fig
