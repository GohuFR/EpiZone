"""
EpiZone — Expansion des entrees departement-entier

Resout les codes departementaux (DEPT_01, 36XXX, etc.) en codes
INSEE individuels de chaque commune du departement, via l'API
geo.api.gouv.fr.

Les resultats sont caches dans un fichier Parquet pour eviter
les appels API repetes.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pandas as pd

try:
    import urllib.request
    HAS_URLLIB = True
except ImportError:
    HAS_URLLIB = False

from .config import DiseaseConfig


CACHE_DIR = Path("cache")
EXPANSION_CACHE = CACHE_DIR / "dept_communes.parquet"

# Delai entre appels API (politesse)
API_DELAY = 0.15


# ── Cache des communes par departement ───────────────────────────────────────

def _load_expansion_cache() -> dict[str, pd.DataFrame]:
    """Charge le cache d'expansion departement -> communes."""
    if EXPANSION_CACHE.exists():
        df = pd.read_parquet(EXPANSION_CACHE)
        return {
            dept: group.reset_index(drop=True)
            for dept, group in df.groupby("dept_code")
        }
    return {}


def _save_expansion_cache(cache: dict[str, pd.DataFrame]) -> None:
    """Sauvegarde le cache d'expansion."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if cache:
        df = pd.concat(cache.values(), ignore_index=True)
        df.to_parquet(EXPANSION_CACHE, index=False)


def _fetch_dept_communes(dept_code: str) -> pd.DataFrame | None:
    """
    Interroge l'API geo.api.gouv.fr pour obtenir la liste des communes
    d'un departement.

    Retourne un DataFrame avec colonnes : code_insee, commune, dept_code
    """
    if not HAS_URLLIB:
        print(f"    urllib non disponible, impossible de telecharger dept {dept_code}")
        return None

    url = (
        f"https://geo.api.gouv.fr/departements/{dept_code}"
        f"/communes?fields=code,nom"
    )

    try:
        req = urllib.request.Request(url)
        req.add_header("User-Agent", "EpiZone/1.0")
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"    ⚠ Erreur API dept {dept_code}: {e}")
        return None

    if not data:
        return None

    df = pd.DataFrame(data)
    df = df.rename(columns={"code": "code_insee", "nom": "commune"})
    df["dept_code"] = dept_code

    # Normaliser les codes INSEE (Corse ou padding)
    df["code_insee"] = df["code_insee"].apply(
        lambda c: c if c[:2] in ("2A", "2B") else f"{int(c):05d}"
    )

    return df


def fetch_all_dept_communes(dept_codes: list[str]) -> dict[str, pd.DataFrame]:
    """
    Telecharge la liste des communes pour tous les departements demandes,
    en utilisant le cache quand disponible.
    """
    cache = _load_expansion_cache()
    missing = [d for d in dept_codes if d not in cache]

    if missing:
        print(f"  Telechargement de {len(missing)} departements depuis geo.api.gouv.fr...")
        for dept in missing:
            print(f"    Dept {dept}... ", end="", flush=True)
            df = _fetch_dept_communes(dept)
            if df is not None and not df.empty:
                cache[dept] = df
                print(f"{len(df)} communes")
            else:
                print("vide ou erreur")
            time.sleep(API_DELAY)

        _save_expansion_cache(cache)
        print(f"  Cache sauvegarde ({len(cache)} departements)")
    else:
        print(f"  Expansion : {len(dept_codes)} departements (cache)")

    return {d: cache[d] for d in dept_codes if d in cache}


# ── Expansion des periodes ───────────────────────────────────────────────────

def expand_dept_periods(
    periodes: pd.DataFrame,
    config: DiseaseConfig,
) -> pd.DataFrame:
    """
    Remplace les entrees departement-entier par des entrees individuelles
    pour chaque commune du departement.

    Les lignes avec _is_dept == True sont expandees.
    Les lignes avec _is_dept == False sont gardees telles quelles.

    Retour : DataFrame normalise (meme schema que l'entree).
    """
    if not config.dept_expansion.enabled:
        return periodes

    dept_rows = periodes[periodes["_is_dept"] == True]
    commune_rows = periodes[periodes["_is_dept"] != True]

    if dept_rows.empty:
        return periodes

    # Collecter les codes departement a expander
    dept_codes = sorted(dept_rows["_dept_code"].dropna().unique().tolist())
    print(f"  {len(dept_rows)} entrees departementales ({len(dept_codes)} departements)")

    # Telecharger / charger depuis le cache
    dept_communes = fetch_all_dept_communes(dept_codes)

    # Expander chaque ligne departementale
    expanded_parts = []

    for _, row in dept_rows.iterrows():
        dept_code = row["_dept_code"]
        if dept_code not in dept_communes:
            continue

        communes_df = dept_communes[dept_code]

        # Creer une ligne par commune pour cette periode
        expanded = pd.DataFrame({
            "code_insee": communes_df["code_insee"].values,
            "commune": communes_df["commune"].values,
            "departement": row["departement"],
            "region": row.get("region"),
            "date_debut": row["date_debut"],
            "date_fin": row["date_fin"],
            "zone": row["zone"],
            "_is_dept": False,
            "_dept_code": None,
        })
        expanded_parts.append(expanded)

    if expanded_parts:
        expanded_all = pd.concat(expanded_parts, ignore_index=True)
        print(f"  → {len(expanded_all)} entrees communes generees")

        # Fusionner avec les entrees communales existantes
        result = pd.concat([commune_rows, expanded_all], ignore_index=True)
    else:
        result = commune_rows.copy()

    result = result.sort_values(["code_insee", "zone", "date_debut"])
    result = result.reset_index(drop=True)

    print(f"  → {result['code_insee'].nunique()} communes distinctes apres expansion")
    return result
