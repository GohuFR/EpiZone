"""
EpiZone — Calculateur de zones reglementees

Calcule le perimetre de X km autour d'un ou plusieurs foyers
et identifie les communes incluses.

Methode : distance euclidienne en Lambert-93 depuis les centroides.
Rapide (~50 ms pour 35k communes) et suffisamment precis pour
le perimetre reglementaire.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import urllib.request
    HAS_URLLIB = True
except ImportError:
    HAS_URLLIB = False

try:
    from pyproj import Transformer
    HAS_PYPROJ = True
except ImportError:
    HAS_PYPROJ = False

try:
    from shapely.geometry import Point, mapping
    from shapely.ops import unary_union
    HAS_SHAPELY = True
except ImportError:
    HAS_SHAPELY = False


CACHE_DIR = Path("cache")
CENTROIDS_CACHE = CACHE_DIR / "communes_centroids.parquet"
CONTOURS_CACHE = CACHE_DIR / "communes_contours.parquet"
API_DELAY = 0.12

# Departements de France metropolitaine
DEPTS_METRO = (
    [f"{i:02d}" for i in range(1, 20)]
    + ["2A", "2B"]
    + [f"{i:02d}" for i in range(21, 96)]
)


# ── Chargement des centroides ────────────────────────────────────────────────

def _wgs84_to_lambert93(lon: np.ndarray, lat: np.ndarray):
    """Convertit WGS84 → Lambert-93 (EPSG:2154)."""
    if HAS_PYPROJ:
        transformer = Transformer.from_crs("EPSG:4326", "EPSG:2154", always_xy=True)
        x, y = transformer.transform(lon, lat)
        return x, y

    # Fallback approximatif (erreur < 1 km sur la France metro)
    # Projection conique conforme simplifiee
    lon0, lat0 = 3.0, 46.5
    lon_rad = np.radians(lon)
    lat_rad = np.radians(lat)
    lon0_rad = np.radians(lon0)
    lat0_rad = np.radians(lat0)
    R = 6371000
    x = R * (lon_rad - lon0_rad) * np.cos(lat0_rad) + 700000
    y = R * (lat_rad - lat0_rad) + 6600000
    return x, y


def load_commune_reference() -> pd.DataFrame:
    """
    Charge le referentiel des communes avec centroides.

    Colonnes : code_insee, nom, dep_code, population, lon, lat, x_l93, y_l93
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    if CENTROIDS_CACHE.exists():
        print("  Referentiel communes : cache")
        return pd.read_parquet(CENTROIDS_CACHE)

    if not HAS_URLLIB:
        raise RuntimeError("urllib requis pour telecharger les communes")

    print("  Referentiel communes : telechargement depuis geo.api.gouv.fr...")
    all_data = []

    for dep in DEPTS_METRO:
        url = (
            f"https://geo.api.gouv.fr/departements/{dep}"
            f"/communes?fields=code,nom,centre,population,codeDepartement"
            f"&format=json"
        )
        try:
            req = urllib.request.Request(url)
            req.add_header("User-Agent", "EpiZone/1.0")
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            print(f"    ⚠ Dept {dep}: {e}")
            continue

        for commune in data:
            centre = commune.get("centre")
            if not centre or not centre.get("coordinates"):
                continue
            coords = centre["coordinates"]
            all_data.append({
                "code_insee": commune["code"],
                "nom": commune["nom"],
                "dep_code": commune.get("codeDepartement", dep),
                "population": commune.get("population", 0) or 0,
                "lon": coords[0],
                "lat": coords[1],
            })

        time.sleep(API_DELAY)

    df = pd.DataFrame(all_data)
    df["code_insee"] = df["code_insee"].apply(
        lambda c: c if c[:2] in ("2A", "2B") else f"{int(c):05d}"
    )

    # Projection Lambert-93
    x, y = _wgs84_to_lambert93(df["lon"].values, df["lat"].values)
    df["x_l93"] = x
    df["y_l93"] = y

    df.to_parquet(CENTROIDS_CACHE, index=False)
    print(f"    → {len(df)} communes chargees et cachees")
    return df


def load_commune_contours():
    """
    Charge les contours communaux pour le mode polygone.
    Reutilise le cache d'une maladie existante, ou telecharge si absent.
    Retourne un GeoDataFrame avec colonnes : code_insee, geometry.
    """
    try:
        import geopandas as gpd
    except ImportError:
        print("  ⚠ geopandas requis pour le mode polygone")
        return None

    # Verifier le cache dedie
    if CONTOURS_CACHE.exists():
        print("  Contours communes : cache")
        return gpd.read_parquet(CONTOURS_CACHE)

    # Sinon, chercher un cache existant d'une maladie (ils couvrent la France)
    for cached in sorted(CACHE_DIR.glob("communes_geo_*.parquet")):
        try:
            gdf = gpd.read_parquet(cached)
            if len(gdf) > 30000:
                print(f"  Contours communes : reutilisation de {cached.name}")
                gdf[["code_insee", "geometry"]].to_parquet(CONTOURS_CACHE, index=False)
                return gdf[["code_insee", "geometry"]]
        except Exception:
            continue

    # Telecharger (prend quelques minutes)
    print("  Contours communes : telechargement (~3 min)...")
    from .geometry import _download_departments, _simplify_geometries
    gdf = _download_departments(DEPTS_METRO)
    if gdf is not None and len(gdf) > 0:
        gdf = _simplify_geometries(gdf)
        gdf = gdf[["code_insee", "geometry"]]
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        gdf.to_parquet(CONTOURS_CACHE, index=False)
        print(f"    → {len(gdf)} contours caches")
        return gdf

    return None


# ── Calcul de zone ───────────────────────────────────────────────────────────

@dataclass
class ZoneResult:
    """Resultat du calcul de zone."""
    communes: pd.DataFrame       # Communes dans la zone
    foyers: pd.DataFrame         # Communes-foyers
    inconnus: list[str]          # Codes non trouves
    rayon_km: float
    buffer_geojson: dict | None  # GeoJSON des cercles de buffer

    @property
    def n_communes(self) -> int:
        return len(self.communes)

    @property
    def n_departements(self) -> int:
        return self.communes["dep_code"].nunique() if not self.communes.empty else 0

    @property
    def population_totale(self) -> int:
        return int(self.communes["population"].sum()) if not self.communes.empty else 0


def calculer_zone(
    codes_foyers: list[str],
    rayon_km: float,
    communes_ref: pd.DataFrame,
    methode: str = "centroide",
    contours: "gpd.GeoDataFrame | None" = None,
) -> ZoneResult:
    """
    Calcule la zone reglementee autour d'un ou plusieurs foyers.

    methode :
      - "centroide" : distance euclidienne L93 depuis les centroides (~50 ms)
      - "polygone"  : intersection geometrique avec les contours (~2-5 s)
    """
    rayon_m = rayon_km * 1000

    codes_ok = [c for c in codes_foyers if c in communes_ref["code_insee"].values]
    codes_ko = [c for c in codes_foyers if c not in communes_ref["code_insee"].values]

    if not codes_ok:
        return ZoneResult(
            communes=pd.DataFrame(), foyers=pd.DataFrame(),
            inconnus=codes_ko, rayon_km=rayon_km, buffer_geojson=None,
        )

    foyers = communes_ref[communes_ref["code_insee"].isin(codes_ok)].copy()

    if methode == "polygone" and contours is not None and HAS_SHAPELY:
        communes_zone = _calcul_polygone(foyers, rayon_m, communes_ref, contours)
    else:
        communes_zone = _calcul_centroide(foyers, rayon_m, communes_ref)

    buffer_geojson = _make_buffer_geojson(foyers, rayon_m)

    return ZoneResult(
        communes=communes_zone, foyers=foyers,
        inconnus=codes_ko, rayon_km=rayon_km, buffer_geojson=buffer_geojson,
    )


def _calcul_centroide(foyers, rayon_m, communes_ref):
    """Distance euclidienne en Lambert-93 depuis les centroides."""
    en_zone = np.zeros(len(communes_ref), dtype=bool)
    for _, foyer in foyers.iterrows():
        dx = communes_ref["x_l93"].values - foyer["x_l93"]
        dy = communes_ref["y_l93"].values - foyer["y_l93"]
        en_zone |= (dx * dx + dy * dy) <= (rayon_m ** 2)
    return communes_ref[en_zone].copy()


def _calcul_polygone(foyers, rayon_m, communes_ref, contours):
    """Intersection geometrique avec les contours communaux."""
    try:
        import geopandas as gpd
    except ImportError:
        return _calcul_centroide(foyers, rayon_m, communes_ref)

    # Construire le buffer en Lambert-93
    foyer_points = gpd.GeoDataFrame(
        foyers,
        geometry=[Point(x, y) for x, y in zip(foyers["x_l93"], foyers["y_l93"])],
        crs="EPSG:2154",
    )
    buffer_union = foyer_points.geometry.buffer(rayon_m).union_all()

    # Projeter les contours en L93 si necessaire
    if contours.crs and contours.crs.to_epsg() != 2154:
        contours_l93 = contours.to_crs(epsg=2154)
    else:
        contours_l93 = contours

    # Intersection
    mask = contours_l93.intersects(buffer_union)
    codes_in_zone = set(contours_l93.loc[mask, "code_insee"])

    return communes_ref[communes_ref["code_insee"].isin(codes_in_zone)].copy()


def _make_buffer_geojson(foyers: pd.DataFrame, rayon_m: float) -> dict | None:
    """Genere le GeoJSON des cercles de buffer autour des foyers."""
    if not HAS_SHAPELY:
        return None

    features = []

    for _, foyer in foyers.iterrows():
        # Creer le cercle en Lambert-93 puis convertir en WGS84
        center_l93 = Point(foyer["x_l93"], foyer["y_l93"])
        circle_l93 = center_l93.buffer(rayon_m, resolution=64)

        # Conversion approchee L93 → WGS84
        if HAS_PYPROJ:
            transformer = Transformer.from_crs(
                "EPSG:2154", "EPSG:4326", always_xy=True
            )
            coords_l93 = list(circle_l93.exterior.coords)
            coords_wgs = [transformer.transform(x, y) for x, y in coords_l93]
            from shapely.geometry import Polygon
            circle_wgs = Polygon(coords_wgs)
        else:
            # Fallback : cercle approximatif en degres
            import math
            lat_r = math.radians(foyer["lat"])
            km_per_deg_lat = 111.32
            km_per_deg_lon = 111.32 * math.cos(lat_r)
            rayon_km_val = rayon_m / 1000
            coords = []
            for i in range(65):
                angle = 2 * math.pi * i / 64
                dlat = (rayon_km_val / km_per_deg_lat) * math.sin(angle)
                dlon = (rayon_km_val / km_per_deg_lon) * math.cos(angle)
                coords.append((foyer["lon"] + dlon, foyer["lat"] + dlat))
            from shapely.geometry import Polygon
            circle_wgs = Polygon(coords)

        features.append({
            "type": "Feature",
            "geometry": mapping(circle_wgs),
            "properties": {
                "code_insee": foyer["code_insee"],
                "commune": foyer["nom"],
                "type": "buffer",
            },
        })

    # Marqueurs des foyers
    for _, foyer in foyers.iterrows():
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [foyer["lon"], foyer["lat"]]},
            "properties": {
                "code_insee": foyer["code_insee"],
                "commune": foyer["nom"],
                "type": "foyer",
            },
        })

    return {"type": "FeatureCollection", "features": features}


def communes_to_geojson(communes: pd.DataFrame) -> dict:
    """Convertit les communes en zone en GeoJSON de points (mode centroide)."""
    features = []
    for _, row in communes.iterrows():
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [row["lon"], row["lat"]],
            },
            "properties": {
                "code_insee": row["code_insee"],
                "commune": row["nom"],
                "dep_code": row["dep_code"],
                "population": int(row.get("population", 0)),
            },
        })
    return {"type": "FeatureCollection", "features": features}


def communes_to_polygon_geojson(communes: pd.DataFrame, contours) -> dict:
    """Convertit les communes en zone en GeoJSON de polygones (mode polygone)."""
    if contours is None:
        return communes_to_geojson(communes)

    codes_in_zone = set(communes["code_insee"])
    geo_in_zone = contours[contours["code_insee"].isin(codes_in_zone)]

    # Joindre les attributs
    attrs = communes[["code_insee", "nom", "dep_code", "population"]].drop_duplicates("code_insee")
    merged = geo_in_zone.merge(attrs, on="code_insee", how="left")

    features = []
    for _, row in merged.iterrows():
        geom = row.geometry
        if geom is None or geom.is_empty:
            continue
        features.append({
            "type": "Feature",
            "geometry": geom.__geo_interface__,
            "properties": {
                "code_insee": row.get("code_insee", ""),
                "commune": row.get("nom", ""),
                "dep_code": row.get("dep_code", ""),
                "population": int(row.get("population", 0) or 0),
            },
        })
    return {"type": "FeatureCollection", "features": features}


# ── Export Excel ─────────────────────────────────────────────────────────────

def export_to_excel(result: ZoneResult) -> bytes:
    """Genere un fichier Excel en memoire avec les resultats."""
    output = BytesIO()

    # Onglet Communes
    base_cols = ["code_insee", "nom", "dep_code", "lon", "lat"]
    chep_cols = ["exploit_lait", "exploit_nourr", "vaches_lait", "vaches_nourr"]
    export_cols = base_cols + [c for c in chep_cols if c in result.communes.columns]

    communes_export = result.communes[
        [c for c in export_cols if c in result.communes.columns]
    ].copy()
    communes_export = communes_export.sort_values(["dep_code", "code_insee"])

    rename_map = {
        "code_insee": "Code INSEE", "nom": "Commune", "dep_code": "Departement",
        "lon": "Longitude", "lat": "Latitude",
        "exploit_lait": "Expl. laitieres", "exploit_nourr": "Expl. allaitantes",
        "vaches_lait": "Vaches laitieres", "vaches_nourr": "Vaches allaitantes",
    }
    communes_export.columns = [rename_map.get(c, c) for c in communes_export.columns]

    # Onglet Parametres
    params_data = [
        ("Foyer(s)", ", ".join(result.foyers["code_insee"].tolist())),
        ("Rayon", f"{result.rayon_km} km"),
        ("Communes en zone", str(result.n_communes)),
        ("Departements", str(result.n_departements)),
    ]
    # Ajouter totaux cheptel si disponibles
    for chep_col, label in [("vaches_lait","Total vaches laitieres"),
                             ("vaches_nourr","Total vaches allaitantes"),
                             ("exploit_lait","Total expl. laitieres"),
                             ("exploit_nourr","Total expl. allaitantes")]:
        if chep_col in result.communes.columns:
            val = result.communes[chep_col].sum()
            if pd.notna(val):
                params_data.append((label, f"{int(val):,}"))

    params = pd.DataFrame(params_data, columns=["Parametre", "Valeur"])

    # Onglet Foyers
    foyers_export = result.foyers[["code_insee", "nom", "dep_code"]].copy()
    foyers_export.columns = ["Code INSEE", "Commune", "Departement"]

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        communes_export.to_excel(writer, sheet_name="Communes", index=False)
        params.to_excel(writer, sheet_name="Parametres", index=False)
        foyers_export.to_excel(writer, sheet_name="Foyers", index=False)

    return output.getvalue()
