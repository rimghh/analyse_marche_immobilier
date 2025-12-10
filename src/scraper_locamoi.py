import logging
import re
from typing import List, Dict, Any, Optional
from urllib.parse import urlencode, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import os
import pandas as pd
from constants import BASE_URL, PROPERTY_TYPES, FRANCE_PREFECTURES

# ---------------------------------------------------------------------------
# Logging (affichage des msg) et faire passer pour un vrai navigateur (évite le blocage)
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)
#permet de savoir quel fichier afffiche quel message
logging.basicConfig(level=logging.INFO)
#configuration des log pour éviter de polluer l'écran avec trop d'info(cache debug car bcp d'info))

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}

# ---------------------------------------------------------------------------
# Patterns pour les titres (nbre, types et surface) et les prix (extraire depuis un texte [count])
# Exemples de titres :
#   "3 chambres maison de 73m²"
#   "1 chambre de 78m²"  <-- SANS type explicite (cas géré [?])
# ---------------------------------------------------------------------------

TITLE_PATTERN = re.compile(
    r"(?P<rooms>\d+)\s+chambre[s]?"                          # recupere le nbre de chambres
    r"(?:\s+(?P<ptype>maison|appartement|studio|chambre))?"  # type (optionnel si pas de type)
    r"\s+de\s+"
    r"(?P<surface>[\d\.,]+)\s*m²", #surface
    re.IGNORECASE, #ne pas tenire compte majuscule et min
)

# Exemple de prix :
#   "1 860 € / mois"
PRICE_PATTERN = re.compile(
    r"(?P<amount>[\d\s\u202f\u00a0]+)\s*€\s*/\s*mois",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Helpers de parsing
# ---------------------------------------------------------------------------

def _parse_price(price_text: str) -> Optional[float]:
    """
    Extrait le loyer mensuel (float) à partir d'une chaîne de type '1 860 € / mois'.
    """
    m = PRICE_PATTERN.search(price_text)
    if not m:
        return None
    raw = m.group("amount")
    # On enlève les espaces (y compris insécables)
    raw = raw.replace("\u202f", "").replace("\u00a0", "").replace(" ", "")
    try:
        return float(raw)
    except ValueError:
        return None


def _parse_surface_from_title(title_text: str) -> Optional[float]:
    """
    Extrait la surface en m² à partir du titre (via TITLE_PATTERN).
    """
    m = TITLE_PATTERN.search(title_text)
    if not m:
        return None
    surface_str = m.group("surface").replace(",", ".").replace(" ", "")
    try:
        return float(surface_str)
    except ValueError:
        return None


def _parse_rooms_from_title(title_text: str) -> Optional[int]:
    """
    Extrait le nombre de pièces/chambres à partir du titre (via TITLE_PATTERN).
    """
    m = TITLE_PATTERN.search(title_text)
    if not m:
        return None
    try:
        return int(m.group("rooms"))
    except (ValueError, KeyError):
        return None


# ---------------------------------------------------------------------------
# Construction & téléchargement des pages
# ---------------------------------------------------------------------------

def build_search_url(city: str, property_type_slug: str, page: int = 1) -> str:
    """
    Construit l'URL de recherche locamoi pour une ville donnée et un type de bien.
    On utilise les paramètres 'location' et 'property_types'.
    Exemple :
      https://locamoi.fr/location?location=paris&property_types=house&page=2
    """
    params = {
        "location": city,
        "property_types": property_type_slug,
    }
    if page > 1:
        params["page"] = page

    # On suppose que BASE_URL = "https://locamoi.fr"
    return f"{BASE_URL}/location?{urlencode(params)}"


def fetch_page_html(url: str) -> Optional[str]:
    """Télécharge une page, renvoie son HTML ou None en cas d'erreur."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            logger.warning(f"[WARN] Status {resp.status_code} pour {url}")
            return None
        return resp.text
    except requests.RequestException as e:
        logger.error(f"[ERROR] Requête échouée pour {url} : {e}")
        return None


# ---------------------------------------------------------------------------
# Extraction des annonces depuis le HTML (pattern sur les titres)
# ---------------------------------------------------------------------------

def extract_listings_from_html(
    html: str,
    city: str,
    region: str,
    property_type_key: str,
    page: int,
) -> List[Dict[str, Any]]:
    """
    Extrait la liste des annonces depuis le HTML en se basant sur des motifs textuels
    (titres / prix).

    On retourne des dicts avec :
      id, type_bien, titre, ville, region,
      surface_m2, nb_pieces, loyer_mensuel_eur, adresse, url

    Règles :
    - Pour les types "classiques" (appartement, maison, studio, chambre) :
        * si le type explicite (maison/appartement/studio/chambre) est présent dans le titre,
          on vérifie qu'il correspond au type du flux
          (ex: 'appartement' pour property_type_key='appartement').
        * si le type N'EST PAS présent (ex: "1 chambre de 78m²"), on l'accepte
          en l'affectant au type du flux courant (ex: 'chambre').
    - Pour 'appartement_etudiant' (logements étudiants) ET 'studio' (catégorie studio) :
        * on garde toutes les annonces dont le titre matche le pattern,
          qu'il y ait un type explicite ou non (maison/appartement/studio/chambre ou None).
    """
    soup = BeautifulSoup(html, "html.parser")

    type_conf = PROPERTY_TYPES[property_type_key]
    property_type_label = type_conf["label"].lower()

    # Cas particuliers : logement étudiant & catégorie studio
    is_student = property_type_key == "appartement_etudiant"
    is_studio_category = property_type_key == "studio"

    listings: List[Dict[str, Any]] = []
    idx = 0

    valid_ptypes = ("maison", "appartement", "studio", "chambre")

    # On parcourt tous les textes qui ressemblent au motif de titre
    for title_node in soup.find_all(string=TITLE_PATTERN):
        title_text = title_node.strip()
        m = TITLE_PATTERN.search(title_text)
        if not m:
            continue

        # ptype peut être None (ex: "1 chambre de 78m²")
        ptype_raw = m.group("ptype")
        ptype = ptype_raw.lower() if ptype_raw else None

        if is_student or is_studio_category:
            # Pour les logements étudiants & la catégorie studio :
            # - si un type est présent, on le garde s'il fait partie des types valides
            if ptype is not None and ptype not in valid_ptypes:
                continue
            # - si ptype est None, on accepte aussi (cas "1 chambre de 78m²")
        else:
            # Pour les autres property_types (appartement, maison, chambre)
            # on veut quand même accepter les titres sans type explicite.
            if ptype is not None:
                # Type présent : il doit être parmi les types attendus
                if ptype not in valid_ptypes:
                    continue
                # et il doit correspondre au type du flux
                if ptype != property_type_label:
                    continue
            else:
                # Pas de type explicite dans le titre :
                # on considère que c'est du type correspondant au flux courant
                ptype = property_type_label

        idx += 1

        # Surface et nb de pièces
        surface = _parse_surface_from_title(title_text)
        rooms = _parse_rooms_from_title(title_text)

        # Adresse : texte immédiatement suivant le titre
        address_node = title_node.find_next(string=True)
        address_text = address_node.strip() if address_node else None

        # Prix : texte du type 'xxx € / mois' suivant
        price_node = title_node.find_next(string=PRICE_PATTERN)
        price_text = price_node.strip() if price_node else ""
        rent = _parse_price(price_text)

        # URL : parent <a> si possible
        url = None
        link_parent = title_node.find_parent("a")
        if link_parent and link_parent.has_attr("href"):
            href = link_parent["href"]
            url = urljoin(BASE_URL, href)

        # ID généré (unique à l'échelle du dataset)
        listing_id = f"{property_type_key}_{city.replace(' ', '_').lower()}_p{page}_{idx}"

        listings.append(
            {
                "id": listing_id,
                "type_bien": property_type_key,   # ex : "chambre", "appartement_etudiant", "studio", etc.
                "titre": title_text,
                "ville": city,
                "region": region,
                "surface_m2": surface,
                "nb_pieces": rooms,
                "loyer_mensuel_eur": rent,
                "adresse": address_text,
                "url": url,
            }
        )

    return listings


# ---------------------------------------------------------------------------
# Scraping multi-pages pour (ville, type_bien)
# ---------------------------------------------------------------------------

def scrape_city_property_type(
    city: str,
    region: str,
    property_type_key: str,
    max_pages: int = 50,
) -> List[Dict[str, Any]]:
    """
    Scrappe toutes les pages de résultats pour une ville + un type de bien
    en utilisant l'URL de recherche locamoi :
      https://locamoi.fr/location?location=<city>&property_types=<slug>&page=N
    """
    assert property_type_key in PROPERTY_TYPES, f"Type de bien inconnu: {property_type_key}"

    type_conf = PROPERTY_TYPES[property_type_key]
    type_slug = type_conf["slug"]
    type_label = type_conf["label"]

    all_listings: List[Dict[str, Any]] = []

    for page in range(1, max_pages + 1):
        url = build_search_url(city, type_slug, page=page)
        logger.info(f"[INFO] {city} ({region}) - {property_type_key} - page {page} -> {url}")

        html = fetch_page_html(url)
        if not html:
            # Problème réseau ou autre : on arrête sur cette ville + type
            break

        listings = extract_listings_from_html(
            html=html,
            city=city,
            region=region,
            property_type_key=property_type_key,
            page=page,
        )

        if not listings:
            # Plus aucune annonce => fin de la pagination
            if page == 1:
                logger.info(f"[INFO] 0 annonce pour {city} / {property_type_key}")
            break

        logger.info(
            f"[INFO] {len(listings)} annonces trouvées pour {city} "
            f"(page {page}, type={type_label})"
        )
        all_listings.extend(listings)

    return all_listings


# ---------------------------------------------------------------------------
# Scraping global (préfectures x types de biens) en parallèle
# ---------------------------------------------------------------------------

def scrape_all_prefectures_parallel(max_workers: int) -> List[Dict[str, Any]]:
    """
    Lance le scrapping pour :
    - toutes les régions
    - toutes les préfectures de chaque région
    - tous les types de biens définis dans PROPERTY_TYPES

    Le tout est parallélisé par (ville, type_bien).
    """
    tasks = []
    for region, cities in FRANCE_PREFECTURES.items():
        for city in cities:
            for property_type_key in PROPERTY_TYPES.keys():
                tasks.append((region, city, property_type_key))

    results: List[Dict[str, Any]] = []

    logger.info(f"[INFO] Nombre total de combinaisons (ville, type_bien) : {len(tasks)}")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                scrape_city_property_type,
                city,
                region,
                property_type_key,
            ): (region, city, property_type_key)
            for (region, city, property_type_key) in tasks
        }

        for future in tqdm(as_completed(futures), total=len(futures), desc="Scraping"):
            region, city, property_type_key = futures[future]
            try:
                listings = future.result()
                results.extend(listings)
                logger.info(
                    f"[INFO] Terminé : {city} ({region}) / {property_type_key} -> "
                    f"{len(listings)} annonces"
                )
            except Exception as e:
                logger.error(
                    f"[ERROR] Échec pour {city} ({region}) / {property_type_key} : {e}"
                )

    logger.info(f"[INFO] Nombre total d'annonces collectées : {len(results)}")
    return results


# ---------------------------------------------------------------------------
# Construction du DataFrame global
# ---------------------------------------------------------------------------

def build_dataset_from_records(records: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    À partir de la liste de dicts (annonces), construit :
    - un DataFrame global (tous types confondus)
    """
    if not records:
        df_all = pd.DataFrame(
            columns=[
                "id",
                "type_bien",
                "type_bien_label",
                "titre",
                "ville",
                "region",
                "surface_m2",
                "nb_pieces",
                "loyer_mensuel_eur",
                "adresse",
                "url",
            ]
        )
    else:
        df_all = pd.DataFrame(records)

    # Nettoyage / typage
    if "loyer_mensuel_eur" in df_all.columns:
        df_all["loyer_mensuel_eur"] = pd.to_numeric(
            df_all["loyer_mensuel_eur"], errors="coerce"
        )
    if "surface_m2" in df_all.columns:
        df_all["surface_m2"] = pd.to_numeric(
            df_all["surface_m2"], errors="coerce"
        )
    if "nb_pieces" in df_all.columns:
        df_all["nb_pieces"] = pd.to_numeric(
            df_all["nb_pieces"], errors="coerce"
        )

    return df_all


# ---------------------------------------------------------------------------
# Sauvegarde sur disque : UN SEUL CSV global
# ---------------------------------------------------------------------------

def save_dataset_to_csv(
    df_all: pd.DataFrame,
    filename: str = "locamoi_tous_types.csv",
) -> None:
    """
    Sauvegarde le jeu de données global dans un CSV unique
    dans le dossier data/ à la racine du projet (parent de src/).
    """
    # Dossier racine du projet = parent de src/
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # Dossier data à la racine
    output_dir = os.path.join(project_root, "data")
    os.makedirs(output_dir, exist_ok=True)

    # Chemin final du CSV
    csv_path = os.path.join(output_dir, filename)

    df_all.to_csv(csv_path, index=False, encoding="utf-8-sig")
    logger.info(f"[INFO] Dataset global sauvegardé : {csv_path}")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    # 1. Scraping parallèle sur toutes les préfectures / tous les types
    records = scrape_all_prefectures_parallel(max_workers=4)  # ajuste si besoin

    # 2. Construction du DataFrame global
    df_all = build_dataset_from_records(records)

    # 3. Sauvegarde sur disque (UN SEUL CSV)
    save_dataset_to_csv(df_all, filename="locamoi_tous_types.csv")


if __name__ == "__main__":
    main()
