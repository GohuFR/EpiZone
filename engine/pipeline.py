"""
EpiZone — Pipeline de donnees (version PC definitive)

Tout est pre-calcule au demarrage et cache sur disque :
  1. Periodes : cache Parquet (skip relecture Excel au 2e lancement)
  2. Geometries : pool partage, telecharge une seule fois
  3. GeoJSON : tous les snapshots pre-generes en gzip
     → 1er lancement : ~2 min (telechargement geo + generation)
     → 2e lancement : ~5s (tout depuis le cache)
     → Invalidation auto si un Excel change
"""

from __future__ import annotations

import gzip
import hashlib
import json
import time
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from .config import DiseaseConfig, load_all_configs
from .loader import load_disease_data
from .expansion import expand_dept_periods
from .snapshots import compute_key_dates, build_all_snapshots

try:
    import geopandas as gpd
    from .geometry import load_geometries, _download_departments, _simplify_geometries
    HAS_GEO = True
except ImportError:
    HAS_GEO = False


CACHE_DIR = Path("cache")
EXPORT_DIR = Path("data/clean")

DEPTS_METRO = (
    [f"{i:02d}" for i in range(1, 20)] + ["2A", "2B"]
    + [f"{i:02d}" for i in range(21, 96)]
)


# =============================================================================
#  Geometries partagees
# =============================================================================

_shared_geo: "gpd.GeoDataFrame | None" = None
_display_geo: "gpd.GeoDataFrame | None" = None
_dept_geo: "gpd.GeoDataFrame | None" = None  # Polygones departementaux officiels
_geo_per_dept: dict[str, int] = {}  # Nombre de communes par dept dans le cache geo


def _load_dept_polygons() -> "gpd.GeoDataFrame | None":
    """
    Charge les polygones departementaux officiels depuis data/departements_geo.parquet
    (source : data.gouv.fr, IGN Admin Express).
    """
    src = Path("data") / "departements_geo.parquet"
    if not src.exists():
        print("  Departements : fichier data/departements_geo.parquet absent")
        return None

    print("  Departements : chargement... ", end="", flush=True)
    t0 = time.time()

    gdf = gpd.read_parquet(src)
    gdf = gdf.rename(columns={"code": "dept_code", "nom": "dept_nom"})

    # Simplifier pour l'affichage (200m en L93, deja en L93)
    if gdf.crs and gdf.crs.to_epsg() != 2154:
        gdf = gdf.to_crs(epsg=2154)
    gdf["geometry"] = gdf["geometry"].simplify(200, preserve_topology=True)

    # Reprojeter en WGS84 pour Leaflet
    gdf = gdf.to_crs(epsg=4326)

    print(f"{len(gdf)} departements en {time.time()-t0:.1f}s")
    return gdf


def _load_shared_geometries():
    """Charge ou telecharge les geometries. Une seule fois par session."""
    global _shared_geo, _display_geo, _dept_geo, _geo_per_dept

    if _shared_geo is not None:
        return

    if not HAS_GEO:
        print("  ⚠ geopandas non installe — pas de geometries")
        return

    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # ── Communes ──────────────────────────────────────────────────────
    best, best_sz = None, 0
    for f in CACHE_DIR.glob("communes_geo_*.parquet"):
        sz = f.stat().st_size
        if sz > best_sz:
            best, best_sz = f, sz

    if best is not None:
        print(f"  Geometries communes : {best.name} ({best_sz/1e6:.0f} Mo)")
        _shared_geo = gpd.read_parquet(best)
    else:
        print("  Geometries communes : telechargement (~5 min)...")
        gdf = _download_departments(DEPTS_METRO)
        if gdf is None or len(gdf) == 0:
            print("  ⚠ Telechargement echoue")
            return
        gdf = _simplify_geometries(gdf, tolerance=100)
        out = CACHE_DIR / "communes_geo_shared.parquet"
        gdf.to_parquet(out, index=False)
        _shared_geo = gdf
        print(f"  → {len(gdf)} geometries cachees")

    # Version simplifiee pour l'affichage
    disp_path = CACHE_DIR / "communes_display.parquet"
    if disp_path.exists():
        _display_geo = gpd.read_parquet(disp_path)
    else:
        print("  Simplification affichage (300m)... ", end="", flush=True)
        t0 = time.time()
        g = _shared_geo.copy()
        g = g.to_crs(epsg=2154)
        g["geometry"] = g["geometry"].simplify(300, preserve_topology=True)
        g = g.to_crs(epsg=4326)
        _display_geo = g
        g.to_parquet(disp_path, index=False)
        print(f"{time.time()-t0:.1f}s")

    # Compter les communes par departement dans le cache geo
    _geo_per_dept = {}
    for code in _shared_geo["code_insee"]:
        d = code[:2] if code[:2] in ("2A", "2B") else code[:2]
        _geo_per_dept[d] = _geo_per_dept.get(d, 0) + 1

    # ── Departements (polygones officiels depuis data/) ─────────────
    _dept_geo = _load_dept_polygons()
    if _dept_geo is not None:
        print(f"  → {len(_shared_geo)} communes, {len(_dept_geo)} departements")
    else:
        print(f"  → {len(_shared_geo)} communes, 0 departements (agregation desactivee)")


def get_display_geo():
    """Retourne les geometries simplifiees pour l'affichage."""
    if _display_geo is not None:
        return _display_geo
    return _shared_geo


# =============================================================================
#  Cache periodes (Parquet + Excel clean)
# =============================================================================

def _excel_checksum(path: Path) -> str:
    h = hashlib.md5()
    h.update(path.stat().st_size.to_bytes(8, "little"))
    h.update(str(path.stat().st_mtime).encode())
    return h.hexdigest()[:12]


def _load_or_cache_periodes(config: DiseaseConfig, data_dir: Path) -> pd.DataFrame:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = CACHE_DIR / f"periodes_{config.id}.parquet"
    excel_path = data_dir / config.excel_file
    checksum_path = cache_path.with_suffix(".md5")

    # Cache valide ?
    if cache_path.exists() and checksum_path.exists():
        if checksum_path.read_text().strip() == _excel_checksum(excel_path):
            print(f"Chargement de {config.name} (cache)...")
            p = pd.read_parquet(cache_path)
            n = p.loc[p.get("_is_dept", False) != True, "code_insee"].nunique()
            zones = sorted(p["zone"].dropna().unique())
            print(f"  → {len(p)} periodes, {n} communes [{', '.join(zones)}]")
            return p

    # Charger depuis Excel
    p = load_disease_data(config, data_dir)
    if config.dept_expansion.enabled:
        p = expand_dept_periods(p, config)

    # Nettoyer les booleens parasites avant Parquet
    for col in ["commune", "departement", "region", "zone", "code_insee"]:
        if col in p.columns:
            p[col] = p[col].apply(
                lambda v: "" if isinstance(v, bool) else (str(v) if v is not None else ""))

    p.to_parquet(cache_path, index=False)
    checksum_path.write_text(_excel_checksum(excel_path))
    return p


def _compute_dept_zone_periods(
    periodes: pd.DataFrame,
    snapshots: dict,
    threshold: float = 0.99,
) -> pd.DataFrame:
    """
    Calcule les periodes ou chaque departement est entierement dans une zone unique.

    Referentiel : communes connues dans les periodes de la maladie (pas le COG complet).
    Seuil : 97 % des communes du dept dans la meme zone (absorbe les differences COG).
    Retour : dept_code | departement | zone | date_debut | date_fin (NaT = toujours actif)
    """
    ref = periodes[["code_insee", "departement"]].copy()
    ref = ref[ref["code_insee"].notna() & (ref["code_insee"] != "")]
    ref["_dept"] = ref["code_insee"].str[:2]
    ref_by_dept = ref.groupby("_dept")["code_insee"].apply(set).to_dict()
    dept_names = (
        ref.drop_duplicates("_dept").set_index("_dept")["departement"].to_dict()
    )

    all_dates = sorted(snapshots.keys())

    # Pour chaque (dept, date) : zone si couverture >= seuil + une seule zone
    dept_at_date = {}
    for ds in all_dates:
        statuts = snapshots[ds]
        if statuts.empty:
            for dept in ref_by_dept:
                dept_at_date[(dept, ds)] = None
            continue
        snap_codes = set(statuts["code_insee"])
        for dept, ref_codes in ref_by_dept.items():
            in_snap = ref_codes & snap_codes
            coverage = len(in_snap) / len(ref_codes) if ref_codes else 0
            if coverage >= threshold:
                group_zones = statuts.loc[
                    statuts["code_insee"].isin(ref_codes), "zone"
                ].unique()
                dept_at_date[(dept, ds)] = (
                    group_zones[0] if len(group_zones) == 1 else None
                )
            else:
                dept_at_date[(dept, ds)] = None

    # Construire les periodes continues
    periods = []
    for dept in sorted(ref_by_dept.keys()):
        seq = [(ds, dept_at_date.get((dept, ds))) for ds in all_dates]
        run_start = None
        run_zone = None

        for i, (ds, zone) in enumerate(seq):
            if zone is not None and zone == run_zone:
                pass  # Continuation
            elif zone is not None and zone != run_zone:
                if run_start is not None:
                    periods.append({
                        "dept_code": dept,
                        "departement": dept_names.get(dept, dept),
                        "zone": run_zone,
                        "date_debut": pd.Timestamp(run_start),
                        "date_fin": pd.Timestamp(seq[i - 1][0]),
                    })
                run_start = ds
                run_zone = zone
            else:
                if run_start is not None:
                    periods.append({
                        "dept_code": dept,
                        "departement": dept_names.get(dept, dept),
                        "zone": run_zone,
                        "date_debut": pd.Timestamp(run_start),
                        "date_fin": pd.Timestamp(seq[i - 1][0]),
                    })
                    run_start = None
                    run_zone = None

        if run_start is not None:
            periods.append({
                "dept_code": dept,
                "departement": dept_names.get(dept, dept),
                "zone": run_zone,
                "date_debut": pd.Timestamp(run_start),
                "date_fin": pd.NaT,
            })

    if not periods:
        return pd.DataFrame(
            columns=["dept_code", "departement", "zone", "date_debut", "date_fin"]
        )
    df = pd.DataFrame(periods)
    df = df.sort_values(["zone", "dept_code", "date_debut"]).reset_index(drop=True)
    return df


def _save_clean(
    periodes: pd.DataFrame,
    config: DiseaseConfig,
    snapshots: dict | None = None,
):
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    pq = EXPORT_DIR / f"{config.id}_periodes.parquet"
    xl = EXPORT_DIR / f"{config.id}_periodes.xlsx"
    if pq.exists() and xl.exists():
        return

    cols = ["code_insee","commune","departement","region","date_debut","date_fin","zone"]
    df = periodes[[c for c in cols if c in periodes.columns]].copy()
    for c in ["commune","departement","region","zone","code_insee"]:
        if c in df.columns:
            df[c] = df[c].apply(lambda v: "" if isinstance(v, bool) else (str(v) if v is not None else ""))
    if not pq.exists():
        df.to_parquet(pq, index=False)
    if not xl.exists():
        dx = df.copy()
        for c in ["date_debut","date_fin"]:
            if c in dx.columns: dx[c] = pd.to_datetime(dx[c]).dt.date
        with pd.ExcelWriter(xl, engine="openpyxl") as writer:
            dx.to_excel(writer, index=False, sheet_name="Periodes")
            if snapshots:
                dept_df = _compute_dept_zone_periods(periodes, snapshots)
                if not dept_df.empty:
                    dd = dept_df.copy()
                    for c in ["date_debut", "date_fin"]:
                        if c in dd.columns:
                            dd[c] = pd.to_datetime(dd[c]).dt.date
                    dd.to_excel(writer, index=False, sheet_name="Departements_entiers")
                    n_dept = dept_df["dept_code"].nunique()
                    n_per = len(dept_df)
                    print(f"  → Departements entiers : {n_dept} depts, {n_per} periodes")
        print(f"  → Export : {xl.name}")


# =============================================================================
#  Cache GeoJSON complet
# =============================================================================

def _geojson_cache_path(config: DiseaseConfig) -> Path:
    return CACHE_DIR / f"geojson_{config.id}.json.gz"



def _build_geojson(statuts, geo, config):
    """
    GeoJSON optimise avec detection intelligente d'agregation departementale.
    
    Logique : pour chaque departement present dans le snapshot,
    comparer le nombre de communes en zone avec le nombre total de communes
    du departement dans le cache geometrique. Si >= 95% sont dans la meme
    zone → utiliser le polygone departemental officiel.
    
    Le seuil de 95% absorbe les differences de COG entre les Excel
    et le referentiel geometrique sans creer de faux positifs.
    """
    if statuts.empty or geo is None or len(geo) == 0:
        return {"type": "FeatureCollection", "features": []}

    merged = geo.merge(statuts, on="code_insee", how="inner")
    if len(merged) == 0:
        return {"type": "FeatureCollection", "features": []}

    colors = config.zone_colors

    merged["_dept"] = merged["code_insee"].apply(
        lambda c: c[:2] if c[:2] in ("2A", "2B") else c[:2])

    features = []
    depts_done = set()

    # ── Passe 1 : departements complets → polygone officiel ──────────
    if _dept_geo is not None and len(_dept_geo) > 0 and _geo_per_dept:
        dept_groups = merged.groupby("_dept")

        for dept_code, group in dept_groups:
            # Nombre total de communes avec geometrie dans ce departement
            n_total_geo = _geo_per_dept.get(dept_code, 0)
            if n_total_geo < 10:
                continue

            n_in_zone = len(group)
            zones_in_dept = group["zone"].unique()

            # Une seule zone ET le departement est quasi-complet
            # Tolerance de 3 communes max pour les differences de COG
            missing = n_total_geo - n_in_zone
            if len(zones_in_dept) == 1 and missing <= 3:
                zone = zones_in_dept[0]
                dept_row = _dept_geo[_dept_geo["dept_code"] == dept_code]
                if dept_row.empty:
                    continue

                geom = dept_row.geometry.iloc[0]
                if geom is None or geom.is_empty:
                    continue

                dept_name = (dept_row["dept_nom"].iloc[0]
                             if "dept_nom" in dept_row.columns else dept_code)
                features.append({
                    "type": "Feature",
                    "geometry": geom.__geo_interface__,
                    "properties": {
                        "code_insee": dept_code,
                        "commune": f"{dept_name} ({n_in_zone} communes)",
                        "departement": str(dept_name),
                        "region": str(group["region"].iloc[0])
                                  if "region" in group.columns else "",
                        "zone": zone,
                        "color": colors.get(zone, "#888"),
                    },
                })
                depts_done.add(dept_code)

    # ── Passe 2 : communes individuelles ─────────────────────────────
    remaining = merged[~merged["_dept"].isin(depts_done)]
    for _, row in remaining.iterrows():
        geom = row.geometry
        if geom is None or geom.is_empty:
            continue
        zone = row.get("zone", "")
        features.append({
            "type": "Feature",
            "geometry": geom.__geo_interface__,
            "properties": {
                "code_insee": row.get("code_insee", ""),
                "commune": str(row.get("commune", "")),
                "departement": str(row.get("departement", "")),
                "region": str(row.get("region", "")),
                "zone": zone,
                "color": colors.get(zone, "#888"),
            },
        })

    return {"type": "FeatureCollection", "features": features}


def _get_all_geojson(snapshots, config, periodes_checksum):
    """Charge depuis cache ou genere tous les GeoJSON."""
    gz = _geojson_cache_path(config)
    gz_md5 = gz.with_suffix(".md5")

    # Cache valide ?
    if gz.exists() and gz_md5.exists():
        if gz_md5.read_text().strip() == periodes_checksum:
            t0 = time.time()
            with gzip.open(gz, "rt", encoding="utf-8") as f:
                data = json.load(f)
            print(f"  GeoJSON : cache ({len(data)} snapshots, {time.time()-t0:.1f}s)")
            return data

    # Generer
    geo = get_display_geo()
    if geo is None:
        return {}

    print(f"  GeoJSON : generation de {len(snapshots)} snapshots... ", end="", flush=True)
    t0 = time.time()
    data = {}
    for ds, st in snapshots.items():
        data[ds] = _build_geojson(st, geo, config)

    with gzip.open(gz, "wt", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"))
    gz_md5.write_text(periodes_checksum)

    print(f"{time.time()-t0:.1f}s ({gz.stat().st_size/1e6:.1f} Mo)")
    return data


# =============================================================================
#  DiseaseData
# =============================================================================

@dataclass
class DiseaseData:
    config: DiseaseConfig
    periodes: pd.DataFrame
    dates_cles: list[pd.Timestamp]
    snapshots: dict[str, pd.DataFrame]
    geo_snapshots: dict[str, dict] = field(default_factory=dict)
    has_geometries: bool = False

    @property
    def date_strings(self): return [str(d.date()) for d in self.dates_cles]
    @property
    def date_min(self): return self.dates_cles[0] if self.dates_cles else pd.Timestamp.now()
    @property
    def date_max(self): return self.dates_cles[-1] if self.dates_cles else pd.Timestamp.now()

    def _resolve(self, ds):
        if ds in self.snapshots: return ds
        t = pd.Timestamp(ds)
        best = None
        for d in self.dates_cles:
            if d <= t: best = str(d.date())
        return best

    def snapshot_at(self, ds):
        k = self._resolve(ds)
        return self.snapshots.get(k, pd.DataFrame()) if k else pd.DataFrame()

    def geojson_at(self, ds):
        if not self.has_geometries: return None
        k = self._resolve(ds)
        return self.geo_snapshots.get(k) if k else None


# =============================================================================
#  Point d'entree
# =============================================================================

def load_disease(config, data_dir, with_geometries=True):
    # 1. Periodes
    periodes = _load_or_cache_periodes(config, data_dir)
    periodes_com = periodes[periodes["_is_dept"] != True].copy()

    # 2. Snapshots
    dates_cles = compute_key_dates(periodes_com)
    snapshots = build_all_snapshots(periodes_com, config, dates_cles)

    # 3. Export clean (apres snapshots pour calculer les depts entiers)
    _save_clean(periodes_com, config, snapshots)

    # 4. GeoJSON
    geo_snapshots = {}
    has_geo = False
    if with_geometries and HAS_GEO and _shared_geo is not None:
        codes = set(periodes_com["code_insee"].unique())
        geo_codes = set(_shared_geo["code_insee"])
        found = len(codes & geo_codes)
        print(f"  → Geometries : {found}/{len(codes)} communes")
        has_geo = True

        checksum_path = (CACHE_DIR / f"periodes_{config.id}.parquet").with_suffix(".md5")
        pcheck = checksum_path.read_text().strip() if checksum_path.exists() else "none"
        geo_snapshots = _get_all_geojson(snapshots, config, pcheck)

    return DiseaseData(
        config=config, periodes=periodes_com, dates_cles=dates_cles,
        snapshots=snapshots, geo_snapshots=geo_snapshots, has_geometries=has_geo)


def load_all_diseases(config_dir, data_dir, with_geometries=True):
    # Charger les geometries UNE SEULE FOIS avant les maladies
    if with_geometries:
        _load_shared_geometries()

    configs = load_all_configs(Path(config_dir))
    diseases = {}
    for did, cfg in configs.items():
        print(f"\n{'─' * 60}")
        try:
            diseases[did] = load_disease(cfg, Path(data_dir), with_geometries)
        except Exception as e:
            print(f"  ✗ Erreur {did}: {e}")
            import traceback; traceback.print_exc()
    return diseases


# =============================================================================
#  Regions francaises (overlay cartographique)
# =============================================================================

DEPT_TO_REGION = {
    "75":"Ile-de-France","77":"Ile-de-France","78":"Ile-de-France",
    "91":"Ile-de-France","92":"Ile-de-France","93":"Ile-de-France",
    "94":"Ile-de-France","95":"Ile-de-France",
    "18":"Centre-Val de Loire","28":"Centre-Val de Loire","36":"Centre-Val de Loire",
    "37":"Centre-Val de Loire","41":"Centre-Val de Loire","45":"Centre-Val de Loire",
    "21":"Bourgogne-Franche-Comte","25":"Bourgogne-Franche-Comte",
    "39":"Bourgogne-Franche-Comte","58":"Bourgogne-Franche-Comte",
    "70":"Bourgogne-Franche-Comte","71":"Bourgogne-Franche-Comte",
    "89":"Bourgogne-Franche-Comte","90":"Bourgogne-Franche-Comte",
    "14":"Normandie","27":"Normandie","50":"Normandie","61":"Normandie","76":"Normandie",
    "02":"Hauts-de-France","59":"Hauts-de-France","60":"Hauts-de-France",
    "62":"Hauts-de-France","80":"Hauts-de-France",
    "08":"Grand Est","10":"Grand Est","51":"Grand Est","52":"Grand Est",
    "54":"Grand Est","55":"Grand Est","57":"Grand Est","67":"Grand Est",
    "68":"Grand Est","88":"Grand Est",
    "44":"Pays de la Loire","49":"Pays de la Loire","53":"Pays de la Loire",
    "72":"Pays de la Loire","85":"Pays de la Loire",
    "22":"Bretagne","29":"Bretagne","35":"Bretagne","56":"Bretagne",
    "16":"Nouvelle-Aquitaine","17":"Nouvelle-Aquitaine","19":"Nouvelle-Aquitaine",
    "23":"Nouvelle-Aquitaine","24":"Nouvelle-Aquitaine","33":"Nouvelle-Aquitaine",
    "40":"Nouvelle-Aquitaine","47":"Nouvelle-Aquitaine","64":"Nouvelle-Aquitaine",
    "79":"Nouvelle-Aquitaine","86":"Nouvelle-Aquitaine","87":"Nouvelle-Aquitaine",
    "09":"Occitanie","11":"Occitanie","12":"Occitanie","30":"Occitanie",
    "31":"Occitanie","32":"Occitanie","34":"Occitanie","46":"Occitanie",
    "48":"Occitanie","65":"Occitanie","66":"Occitanie","81":"Occitanie","82":"Occitanie",
    "01":"Auvergne-Rhone-Alpes","03":"Auvergne-Rhone-Alpes","07":"Auvergne-Rhone-Alpes",
    "15":"Auvergne-Rhone-Alpes","26":"Auvergne-Rhone-Alpes","38":"Auvergne-Rhone-Alpes",
    "42":"Auvergne-Rhone-Alpes","43":"Auvergne-Rhone-Alpes","63":"Auvergne-Rhone-Alpes",
    "69":"Auvergne-Rhone-Alpes","73":"Auvergne-Rhone-Alpes","74":"Auvergne-Rhone-Alpes",
    "04":"Provence-Alpes-Cote d'Azur","05":"Provence-Alpes-Cote d'Azur",
    "06":"Provence-Alpes-Cote d'Azur","13":"Provence-Alpes-Cote d'Azur",
    "83":"Provence-Alpes-Cote d'Azur","84":"Provence-Alpes-Cote d'Azur",
    "2A":"Corse","2B":"Corse",
}


def build_regions_geojson() -> dict:
    """Construit le GeoJSON des regions en dissolvant les departements."""
    if _dept_geo is None or len(_dept_geo) == 0:
        return {"type": "FeatureCollection", "features": []}

    gdf = _dept_geo.copy()
    gdf["region"] = gdf["dept_code"].map(DEPT_TO_REGION)
    gdf = gdf.dropna(subset=["region"])

    # Dissoudre par region
    dissolved = gdf.dissolve(by="region").reset_index()

    features = []
    for _, row in dissolved.iterrows():
        geom = row.geometry
        if geom is None or geom.is_empty:
            continue
        # Centroide pour le label
        c = geom.centroid
        features.append({
            "type": "Feature",
            "geometry": geom.__geo_interface__,
            "properties": {
                "region": row["region"],
                "lat": c.y,
                "lon": c.x,
            },
        })

    return {"type": "FeatureCollection", "features": features}
