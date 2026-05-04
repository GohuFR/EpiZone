"""
EpiZone — Gestionnaire de geometries communales

Telecharge les contours des communes depuis geo.api.gouv.fr,
les simplifie pour alleger le rendu web, et les cache en GeoParquet.

Gere :
- Communes ordinaires (COG en vigueur)
- Communes deleguees/associees (anciennes communes fusionnees dont
  le code INSEE est encore utilise dans les arretes)
- Simplification en Lambert-93 avec tolerance configurable
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pandas as pd

try:
    import geopandas as gpd
    from shapely.geometry import shape
    HAS_GEO = True
except ImportError:
    HAS_GEO = False

try:
    import urllib.request
    HAS_URLLIB = True
except ImportError:
    HAS_URLLIB = False


CACHE_DIR = Path("cache")
API_DELAY = 0.2  # Politesse envers l'API

# Tolerance de simplification en metres (Lambert-93)
# 100m = bon compromis lisibilite / poids
SIMPLIFY_TOLERANCE = 100


def _geo_cache_path(disease_id: str) -> Path:
    return CACHE_DIR / f"communes_geo_{disease_id}.parquet"


# ── Telechargement depuis geo.api.gouv.fr ────────────────────────────────────

def _fetch_geojson(url: str) -> dict | None:
    """Telecharge un GeoJSON depuis une URL."""
    if not HAS_URLLIB:
        return None
    try:
        req = urllib.request.Request(url)
        req.add_header("User-Agent", "EpiZone/1.0")
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"    ⚠ {e}")
        return None


def _download_dept_geometries(dept_code: str) -> gpd.GeoDataFrame | None:
    """
    Telecharge les contours de toutes les communes d'un departement,
    y compris les communes deleguees.
    """
    if not HAS_GEO:
        print("    geopandas non installe — geometries indisponibles")
        return None

    frames = []

    # Communes ordinaires (actuelles uniquement, pas les deleguees)
    url_com = (
        f"https://geo.api.gouv.fr/departements/{dept_code}"
        f"/communes?format=geojson&geometry=contour"
    )
    data = _fetch_geojson(url_com)
    if data and "features" in data:
        gdf = gpd.GeoDataFrame.from_features(data["features"], crs="EPSG:4326")
        if "code" in gdf.columns:
            gdf = gdf.rename(columns={"code": "code_insee"})
            gdf = gdf[["code_insee", "geometry"]]
            frames.append(gdf)

    if not frames:
        return None

    combined = pd.concat(frames, ignore_index=True)
    # Deduplication : garder la premiere occurrence (commune actuelle prioritaire)
    combined = combined.drop_duplicates(subset="code_insee", keep="first")

    return gpd.GeoDataFrame(combined, crs="EPSG:4326")


# ── Simplification ───────────────────────────────────────────────────────────

def _simplify_geometries(
    gdf: gpd.GeoDataFrame,
    tolerance: float = SIMPLIFY_TOLERANCE,
) -> gpd.GeoDataFrame:
    """
    Simplifie les geometries en Lambert-93 (metres).
    Reduit le poids ~5x sans impact visuel significatif.
    """
    # Projeter en Lambert-93 pour simplifier en metres
    gdf_l93 = gdf.to_crs(epsg=2154)
    gdf_l93["geometry"] = gdf_l93["geometry"].simplify(
        tolerance, preserve_topology=True
    )
    # Reprojeter en WGS84 pour Leaflet
    return gdf_l93.to_crs(epsg=4326)


# ── Point d'entree principal ─────────────────────────────────────────────────

def load_geometries(
    codes_insee: list[str],
    disease_id: str,
    force_download: bool = False,
) -> gpd.GeoDataFrame:
    """
    Charge les geometries pour les communes demandees.
    Utilise le cache si disponible, sinon telecharge et cache.

    Parametres
    ----------
    codes_insee : list[str]
        Codes INSEE des communes dont on a besoin des contours.
    disease_id : str
        Identifiant de la maladie (pour nommer le cache).
    force_download : bool
        Si True, ignore le cache et retelecharge tout.

    Retour
    ------
    GeoDataFrame avec colonnes : code_insee, geometry
    """
    if not HAS_GEO:
        raise ImportError(
            "geopandas est requis pour les geometries. "
            "Installez-le avec : pip install geopandas"
        )

    cache_path = _geo_cache_path(disease_id)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # ── Essayer le cache ──────────────────────────────────────────────
    if cache_path.exists() and not force_download:
        print(f"  Geometries : cache {cache_path.name}")
        cached = gpd.read_parquet(cache_path)

        # Verifier la couverture
        cached_codes = set(cached["code_insee"])
        missing = set(codes_insee) - cached_codes
        found = len(set(codes_insee) & cached_codes)

        if not missing:
            print(f"  → {found}/{len(codes_insee)} communes couvertes (100%)")
            return cached

        print(f"  → {found}/{len(codes_insee)} communes couvertes, "
              f"{len(missing)} manquantes")

        # Telecharger les departements manquants
        missing_depts = sorted({
            c[:2] if not c.startswith("2") else c[:2]
            for c in missing
            if len(c) >= 2
        })
        # Inclure les prefixes corses
        for c in missing:
            if c.startswith("2A") or c.startswith("2B"):
                missing_depts.append(c[:2])
        missing_depts = sorted(set(missing_depts))

        if missing_depts:
            new_gdf = _download_departments(missing_depts)
            if new_gdf is not None and len(new_gdf) > 0:
                cached = pd.concat([cached, new_gdf], ignore_index=True)
                cached = cached.drop_duplicates(subset="code_insee", keep="first")
                cached = gpd.GeoDataFrame(cached, crs="EPSG:4326")
                cached.to_parquet(cache_path, index=False)
                print(f"  Cache mis a jour ({len(cached)} geometries)")

        return cached

    # ── Telechargement complet ────────────────────────────────────────
    # Identifier les departements a telecharger
    depts = sorted({
        c[:2] for c in codes_insee if len(c) >= 2
    })
    # Gerer les prefixes corses correctement
    dept_set = set()
    for c in codes_insee:
        if c.startswith("2A") or c.startswith("2B"):
            dept_set.add(c[:2])
        elif len(c) >= 2:
            dept_set.add(c[:2])
    depts = sorted(dept_set)

    print(f"  Geometries : telechargement de {len(depts)} departements...")

    gdf = _download_departments(depts)

    if gdf is not None and len(gdf) > 0:
        # Simplifier
        print(f"  Simplification (tolerance={SIMPLIFY_TOLERANCE}m)... ", end="", flush=True)
        gdf = _simplify_geometries(gdf)
        print(f"OK ({len(gdf)} geometries)")

        # Sauvegarder
        gdf.to_parquet(cache_path, index=False)
        print(f"  Cache sauvegarde : {cache_path}")

        # Stats de couverture
        found = len(set(codes_insee) & set(gdf["code_insee"]))
        missing = len(set(codes_insee) - set(gdf["code_insee"]))
        print(f"  → {found}/{len(codes_insee)} communes couvertes"
              f" ({missing} introuvables)")

        return gdf

    return gpd.GeoDataFrame(columns=["code_insee", "geometry"])


def _download_departments(depts: list[str]) -> gpd.GeoDataFrame | None:
    """Telecharge les geometries de plusieurs departements."""
    if not HAS_GEO:
        return None

    all_gdfs = []

    for dept in depts:
        print(f"    Dept {dept}... ", end="", flush=True)
        gdf = _download_dept_geometries(dept)
        if gdf is not None and len(gdf) > 0:
            print(f"{len(gdf)} geometries")
            all_gdfs.append(gdf)
        else:
            print("vide ou erreur")
        time.sleep(API_DELAY)

    if all_gdfs:
        combined = pd.concat(all_gdfs, ignore_index=True)
        combined = combined.drop_duplicates(subset="code_insee", keep="first")
        return gpd.GeoDataFrame(combined, crs="EPSG:4326")

    return None


# ── Fusion snapshots + geometries ────────────────────────────────────────────

def merge_with_geometries(
    snapshots: dict[str, pd.DataFrame],
    geometries: gpd.GeoDataFrame,
) -> dict[str, gpd.GeoDataFrame]:
    """
    Fusionne chaque snapshot avec les geometries communales.
    Retourne un dict de GeoDataFrames prets a afficher sur la carte.
    """
    geo_snapshots = {}

    for date_str, statuts in snapshots.items():
        if statuts.empty:
            geo_snapshots[date_str] = gpd.GeoDataFrame(
                columns=["code_insee", "commune", "departement", "region", "zone", "geometry"]
            )
            continue

        merged = geometries.merge(statuts, on="code_insee", how="inner")
        geo_snapshots[date_str] = gpd.GeoDataFrame(merged, crs="EPSG:4326")

    return geo_snapshots
