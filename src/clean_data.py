import pandas as pd
import logging
import os
import time
import requests
from tqdm import tqdm
from typing import Optional, Tuple


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# dossier du script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# chemin du CSV
csv_path = os.path.join(BASE_DIR, "..", "data", "locamoi_tous_types.csv")
csv_path = os.path.abspath(csv_path)

print("Chemin du CSV :", csv_path)  # juste pour vérifier

# Chargement en DataFrame
df = pd.read_csv(csv_path, encoding="utf-8-sig")

#normalisation des noms de villes
df["ville"] = (
    df["ville"]
    .str.lower()
    .str.normalize("NFKD")  # supprime accents
    .str.encode("ascii", errors="ignore")
    .str.decode("utf-8")
    .str.strip()
)

def remove_exact_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Supprime les doublons exacts d'annonces, définis comme ayant
    simultanément :
      - même type_bien
      - même titre
      - même ville
      - même surface_m2
      - même nb_pieces
      - même loyer_mensuel_eur

    Retourne un nouveau DataFrame sans ces doublons.
    """
    df = df.copy()

    if df.empty:
        return df

    subset_cols = [
        "titre",
        "ville",
        "surface_m2",
        "nb_pieces",
        "loyer_mensuel_eur",
    ]

    # On garde une copie pour ne pas modifier l'original en place
    df_clean = df.drop_duplicates(subset=subset_cols, keep="first").reset_index(drop=True)

    nb_removed = len(df) - len(df_clean)
    logger.info(
        f"[CLEAN] Doublons supprimés (sur critères type/titre/ville/surface/pièces/loyer) : {nb_removed}"
    )

    return df_clean

df = remove_exact_duplicates(df)

def drop_rows_with_missing_core_fields(
    df: pd.DataFrame) -> pd.DataFrame:
    """
    Supprime les lignes du DataFrame si :
      - la colonne prix au m² est vide (NaN)
      OU
      - la colonne surface est vide (NaN)
      OU
      - la colonne nombre de pièces est vide (NaN)

    Retourne un nouveau DataFrame filtré.
    """
    price_col = "loyer_mensuel_eur"
    surface_col = "surface_m2"
    rooms_col = "nb_pieces"

    df = df.copy()
    
    # On garde uniquement les lignes où les trois colonnes sont NON nulles
    mask = (
        df[price_col].notna()
        & df[surface_col].notna()
        & df[rooms_col].notna()
    )
    return df[mask].copy()

df = drop_rows_with_missing_core_fields(df)

def add_price_per_m2_column(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Ajoute une colonne prix au m² :
        prix_m2 = loyer_mensuel_eur / surface_m2

    - Ignore les lignes où surface = 0 ou None.
    - Retourne un DF modifié (copie) avec la nouvelle colonne.
    """
    rent_col = "loyer_mensuel_eur"
    surface_col = "surface_m2"
    output_col = "prix_m2"

    df = df.copy()

    # Conversion numérique au cas où
    df[rent_col] = pd.to_numeric(df[rent_col], errors="coerce")
    df[surface_col] = pd.to_numeric(df[surface_col], errors="coerce")

    # Calcul : division sécurisée
    df[output_col] = round((df[rent_col] / df[surface_col]),1)

    # Supprime les divisions impossibles (surface = 0 → inf)
    df[output_col].replace([float("inf"), -float("inf")], pd.NA, inplace=True)

    return df

df = add_price_per_m2_column(df)


import os
import time
from typing import Optional, Tuple, Dict

import requests
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed


def geocode_address_positionstack(
    address: str,
    api_key: Optional[str] = None,
    country: Optional[str] = "FR",
    timeout: int = 10,
) -> Tuple[Optional[float], Optional[float]]:
    """
    Géocode une adresse avec PositionStack et retourne (lat, lon).

    - address : chaîne d'adresse (ex: '10 Rue du Cloître Notre-Dame, 75004 Paris, France')
    - api_key : clé API PositionStack (sinon lue dans l'env POSITIONSTACK_API_KEY)
    - country : code pays ISO2 pour restreindre les résultats (ex: 'FR')
    - timeout : timeout en secondes pour la requête HTTP

    Retour :
        (lat, lon) ou (None, None) en cas d'échec.
    """
    if not address or pd.isna(address):
        return (None, None)

    if api_key is None:
        # Tu peux garder ta clé en dur ou utiliser l'env :
        # api_key = os.getenv("POSITIONSTACK_API_KEY")
        api_key = "10e36aa9300d1d59f96a167abd426897"

    if not api_key:
        raise ValueError(
            "Clé API PositionStack manquante. "
            "Définis POSITIONSTACK_API_KEY dans les variables d'environnement "
            "ou passe api_key en argument."
        )

    url = "http://api.positionstack.com/v1/forward"

    params = {
        "access_key": api_key,
        "query": address,
        "limit": 1,
    }

    # Optionnel : restreindre au pays France pour améliorer les résultats
    if country:
        params["country"] = country

    try:
        resp = requests.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()

        results = data.get("data", [])
        if not results:
            return (None, None)

        first = results[0]
        lat = first.get("latitude")
        lon = first.get("longitude")

        # On s’assure de retourner des floats ou None
        try:
            lat = float(lat) if lat is not None else None
            lon = float(lon) if lon is not None else None
        except (TypeError, ValueError):
            return (None, None)

        return (lat, lon)

    except Exception:
        # En prod, tu peux logger l'erreur ici
        return (None, None)


def add_gps_coordinates_positionstack(
    df: pd.DataFrame,
    api_key: Optional[str] = None,
    country: Optional[str] = "FR",
    max_workers: int = 16,
    sleep_between_calls: float = 0.0,
) -> pd.DataFrame:
    """
    Ajoute deux colonnes 'lat' et 'lon' au DataFrame à partir de la colonne 'adresse',
    en utilisant PositionStack.

    Optimisations :
    - ne fait QU'UN APPEL API par adresse unique (déduplication)
    - parallélise les appels avec ThreadPoolExecutor
    - affiche une barre de progression avec tqdm

    Paramètres :
    - df : DataFrame contenant une colonne 'adresse'
    - api_key : clé API PositionStack (sinon lue dans POSITIONSTACK_API_KEY)
    - country : code pays 'FR' par défaut
    - max_workers : nombre max de threads pour le parallélisme
    - sleep_between_calls : pause en secondes entre chaque requête (par thread).
    """
    if "adresse" not in df.columns:
        raise KeyError("La colonne 'adresse' est manquante dans le DataFrame.")

    if api_key is None:
        # Tu peux garder ta clé ou utiliser l'env :
        # api_key = os.getenv("POSITIONSTACK_API_KEY")
        api_key = "10e36aa9300d1d59f96a167abd426897"

    if not api_key:
        raise ValueError(
            "Clé API PositionStack manquante. "
            "Définis POSITIONSTACK_API_KEY dans les variables d'environnement "
            "ou passe api_key en argument."
        )

    # Normalisation légère des adresses (pour déduplication)
    # On garde la version brute pour le DataFrame, mais on utilise une version "clé"
    # pour éviter les appels multiples pour la même adresse.
    addresses_series = df["adresse"].fillna("").astype(str)
    normalized_addresses = addresses_series.str.strip()

    # Ensemble des adresses uniques non vides
    unique_addrs = sorted(set(a for a in normalized_addresses if a))

    # Dictionnaire: adresse_normalisée -> (lat, lon)
    addr_to_coords: Dict[str, Tuple[Optional[float], Optional[float]]] = {}

    # Fonction interne pour un thread
    def _worker(addr: str) -> Tuple[str, Tuple[Optional[float], Optional[float]]]:
        lat, lon = geocode_address_positionstack(
            address=addr,
            api_key=api_key,
            country=country,
        )
        if sleep_between_calls > 0:
            time.sleep(sleep_between_calls)
        return addr, (lat, lon)

    # Parallélisation + barre de progression
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_worker, addr): addr
            for addr in unique_addrs
        }

        for fut in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Geocoding (PositionStack - unique addresses)",
        ):
            try:
                addr, coords = fut.result()
            except Exception:
                addr, coords = futures[fut], (None, None)
            addr_to_coords[addr] = coords

    # Maintenant, on mappe chaque ligne du df sur le dict addr_to_coords
    lats = []
    lons = []
    for addr_norm in normalized_addresses:
        if not addr_norm:
            lats.append(None)
            lons.append(None)
        else:
            lat, lon = addr_to_coords.get(addr_norm, (None, None))
            lats.append(lat)
            lons.append(lon)

    df = df.copy()
    df["lat"] = lats
    df["lon"] = lons

    return df


df = add_gps_coordinates_positionstack(df)

def drop_url_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Supprime la colonne 'url' du DataFrame si elle existe.
    Retourne le DataFrame modifié.
    """
    df = df.copy()
    if "url" in df.columns:
        df = df.drop(columns=["url"])
    return df

df = drop_url_column(df)

def clean_extreme_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Supprime les lignes :
    - où le loyer dépasse 15 000 €/mois
    - où la surface dépasse 1000 m²

    Retourne un nouveau DataFrame nettoyé.
    """

    df_clean = df.copy()

    # Suppression loyers > 15000 €
    df_clean = df_clean[df_clean["loyer_mensuel_eur"] <= 15000]

    # Suppression surfaces > 1000 m²
    df_clean = df_clean[df_clean["surface_m2"] <= 1000]

    # Optionnel : supprimer aussi les NaN
    # df_clean = df_clean.dropna(subset=["loyer_mensuel_eur", "surface_m2"])

    df_clean = df_clean.reset_index(drop=True)

    return df_clean

df = clean_extreme_values(df)

# Dossier data pour la sauvegarde
output_dir = os.path.join(BASE_DIR, "..", "data")
os.makedirs(output_dir, exist_ok=True)  # crée le dossier s'il n'existe pas

# Chemin complet du CSV nettoyé
output_path = os.path.join(output_dir, "locamoi_tous_types_clean.csv")
output_path = os.path.abspath(output_path)

print("Chemin du CSV nettoyé :", output_path)

# Sauvegarde du DataFrame
df.to_csv(output_path, index=False, encoding="utf-8-sig")