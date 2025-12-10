"""
Constantes du projet Locamoi Scraper
- Préfectures de France métropolitaine (sans la Corse), groupées par région
- Types de biens à scraper
"""

import unicodedata


BASE_URL = "https://locamoi.fr"

# ---------------------------------------------------------------------------
# Types de biens
# ---------------------------------------------------------------------------
# Clés (dict) = ce qui apparaîtra dans le dataset dans la colonne "type_bien"
# slug       = segment d'URL utilisé par Locamoi pour filtrer le type de bien
# label      = étiquette lisible
PROPERTY_TYPES = {
    "chambre": {
        "slug": "room",
        "label": "Chambre",
    },
    "maison": {
        "slug": "house",
        "label": "Maison",
    },
    "appartement": {
        "slug": "apartment",
        "label": "Appartement",
    },
    "studio": {
        "slug": "studio",
        "label": "Studio",
    },
    "appartement_etudiant": {
        "slug": "student-apartment",
        "label": "Appartement étudiant",
    },
}

# ---------------------------------------------------------------------------
# Préfectures de France métropolitaine (sans la Corse), groupées par région
# ---------------------------------------------------------------------------
# Pour rester lisible, on ne stocke que les noms de villes ; le slug sera
# construit dynamiquement à partir du nom de la ville.
FRANCE_PREFECTURES = {
    "Ile-de-France": [
        "Paris",                # 75
        "Créteil",              # 94
        "Versailles",           # 78
        "Nanterre",             # 92
        "Bobigny",              # 93
        "Melun",                # 77
        "Évry-Courcouronnes",   # 91
        "Pontoise",             # 95 (Cergy-Pontoise)
    ],
    "Hauts-de-France": [
        "Lille",        # 59
        "Arras",        # 62
        "Amiens",       # 80
        "Beauvais",     # 60
        "Laon",         # 02
    ],
    "Normandie": [
        "Rouen",            # 76
        "Caen",             # 14
        "Évreux",           # 27
        "Saint-Lô",         # 50
        "Alençon",          # 61
    ],
    "Grand Est": [
        "Strasbourg",       # 67
        "Metz",             # 57
        "Nancy",            # 54
        "Châlons-en-Champagne",  # 51
        "Charleville-Mézières",  # 08
        "Chaumont",         # 52
        "Bar-le-Duc",       # 55
        "Colmar",           # 68
        "Mulhouse",         # (sous-préf) – mais on laisse Colmar pour 68
        "Troyes",           # 10
    ],
    "Bretagne": [
        "Rennes",           # 35
        "Quimper",          # 29
        "Vannes",           # 56
        "Saint-Brieuc",     # 22
    ],
    "Pays de la Loire": [
        "Nantes",           # 44
        "Angers",           # 49
        "Le Mans",          # 72
        "Laval",            # 53
        "La Roche-sur-Yon", # 85
    ],
    "Centre-Val de Loire": [
        "Orléans",      # 45
        "Chartres",     # 28
        "Blois",        # 41
        "Tours",        # 37
        "Bourges",      # 18
        "Châteauroux",  # 36
    ],
    "Bourgogne-Franche-Comté": [
        "Dijon",            # 21
        "Auxerre",          # 89
        "Nevers",           # 58
        "Mâcon",            # 71
        "Besançon",         # 25
        "Belfort",          # 90
        "Vesoul",           # 70
        "Lons-le-Saunier",  # 39
    ],
    "Nouvelle-Aquitaine": [
        "Bordeaux",         # 33
        "Limoges",          # 87
        "Poitiers",         # 86
        "Périgueux",        # 24
        "Agen",             # 47
        "Montauban",        # 82 (Occitanie maintenant mais historiquement NA – on simplifie)
        "Pau",              # 64
        "Bayonne",          # 64 (sous-préf)
        "La Rochelle",      # 17
        "Angoulême",        # 16
        "Tulle",            # 19
        "Guéret",           # 23
    ],
    "Occitanie": [
        "Toulouse",         # 31
        "Montpellier",      # 34
        "Nîmes",            # 30
        "Perpignan",        # 66
        "Carcassonne",      # 11
        "Foix",             # 09
        "Rodez",            # 12
        "Albi",             # 81
        "Mende",            # 48
        "Tarbes",           # 65
    ],
    "Auvergne-Rhône-Alpes": [
        "Lyon",             # 69
        "Clermont-Ferrand", # 63
        "Grenoble",         # 38
        "Saint-Étienne",    # 42
        "Annecy",           # 74
        "Chambéry",         # 73
        "Valence",          # 26
        "Le Puy-en-Velay",  # 43
        "Aurillac",         # 15
        "Moulins",          # 03
    ],
    "Provence-Alpes-Côte d'Azur": [
        "Marseille",        # 13
        "Nice",             # 06
        "Toulon",           # 83
        "Avignon",          # 84
        "Gap",              # 05
        "Digne-les-Bains",  # 04
    ]
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def slugify_city(city_name: str) -> str:
    """
    Transforme un nom de ville en slug d'URL compatible avec Locamoi.
    Ex : "Évry-Courcouronnes" -> "evry-courcouronnes"
         "Saint-Brieuc"       -> "saint-brieuc"
    """
    # Normalisation / suppression des accents
    nfkd = unicodedata.normalize("NFKD", city_name)
    ascii_str = nfkd.encode("ascii", "ignore").decode("ascii")
    # Mise en forme slug
    slug = (
        ascii_str.lower()
        .replace("'", "")
        .replace("’", "")
        .replace(",", "")
        .replace(" ", "-")
    )
    # Nettoyage double tirets
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug
