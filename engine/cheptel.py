"""
EpiZone — Donnees cheptel (Recensement Agricole 2020)

Charge les donnees d'elevage a trois niveaux (commune, departement, region)
et fournit une agregation intelligente pour eviter le secret statistique.

Logique d'agregation :
  1. Si une REGION est entierement dans la zone → donnees regionales
  2. Si un DEPARTEMENT est entierement dans la zone → donnees departementales
  3. Sinon → donnees communales (avec NaN pour les valeurs secretes)
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


DATA_DIR = Path("data")

# Colonnes utiles du RA 2020 (par mots-cles dans les en-tetes Agreste)
FIELD_PATTERNS = {
    "exploit_lait": "exploitations avec des vaches laiti",
    "exploit_nourr": "exploitations avec des vaches nourri",
    "vaches_lait": "nombre de vaches laiti",
    "vaches_nourr": "nombre de vaches nourri",
}

# Mapping departement → region (France metro, COG 2020)
DEPT_TO_REGION = {}
_REG = {
    "84": ["01","03","07","15","26","38","42","43","63","69","73","74"],
    "27": ["21","25","39","58","70","71","89","90"],
    "53": ["22","29","35","56"],
    "24": ["18","28","36","37","41","45"],
    "94": ["2A","2B"],
    "44": ["08","10","51","52","54","55","57","67","68","88"],
    "32": ["02","59","60","62","80"],
    "11": ["75","77","78","91","92","93","94","95"],
    "28": ["14","27","50","61","76"],
    "75": ["16","17","19","23","24","33","40","47","64","79","86","87"],
    "76": ["09","11","12","30","31","32","34","46","48","65","66","81","82"],
    "52": ["44","49","53","72","85"],
    "93": ["04","05","06","13","83","84"],
}
for reg_code, depts in _REG.items():
    for d in depts:
        DEPT_TO_REGION[d] = reg_code


def _parse_ra_csv(filepath: Path) -> pd.DataFrame:
    """Parse un CSV Agreste Cartostat (2 lignes d'en-tete, separateur ;)."""
    df = pd.read_csv(filepath, sep=";", skiprows=2, encoding="utf-8-sig")

    # Identifier les colonnes utiles par mots-cles
    result = {"code": df.iloc[:, 0].astype(str).str.strip(),
              "libelle": df.iloc[:, 1].astype(str).str.strip()}

    cols_lower = [c.lower() for c in df.columns]

    for field, pattern in FIELD_PATTERNS.items():
        for i, col in enumerate(cols_lower):
            if pattern in col and "estimation" not in col and "moyen" not in col:
                result[field] = pd.to_numeric(
                    df.iloc[:, i].replace(
                        {"N/A - secret statistique": np.nan,
                         "N/A": np.nan, "": np.nan, " ": np.nan}
                    ), errors="coerce"
                )
                break

    return pd.DataFrame(result)


@dataclass
class CheptelData:
    """Donnees RA 2020 aux trois niveaux."""
    communes: pd.DataFrame     # code = INSEE 5 chars
    departements: pd.DataFrame  # code = 2 chars
    regions: pd.DataFrame       # code = 2 chars

    def is_loaded(self) -> bool:
        return not self.communes.empty


def load_cheptel(data_dir: Path = DATA_DIR) -> CheptelData:
    """Charge les trois niveaux du RA 2020."""
    files = {
        "communes": data_dir / "cheptel_communes.csv",
        "departements": data_dir / "cheptel_departement.csv",
        "regions": data_dir / "cheptel_region.csv",
    }

    dfs = {}
    for level, filepath in files.items():
        if filepath.exists():
            df = _parse_ra_csv(filepath)
            # Normaliser les codes
            if level == "communes":
                df["code"] = df["code"].apply(
                    lambda c: c if c[:2] in ("2A", "2B") else f"{int(c):05d}"
                    if c.replace(" ", "").isdigit() or c[:2] in ("2A", "2B")
                    else c
                )
            elif level == "departements":
                df["code"] = df["code"].apply(lambda c: f"{int(c):02d}" if c.isdigit() else c)
            elif level == "regions":
                df["code"] = df["code"].apply(lambda c: f"{int(c):02d}" if c.isdigit() else c)

            dfs[level] = df
            n_data = df[list(FIELD_PATTERNS.keys())].notna().any(axis=1).sum()
            print(f"    {level:<14} {len(df):>6} entrees, {n_data} avec donnees")
        else:
            dfs[level] = pd.DataFrame()
            print(f"    {level:<14} fichier absent")

    return CheptelData(
        communes=dfs.get("communes", pd.DataFrame()),
        departements=dfs.get("departements", pd.DataFrame()),
        regions=dfs.get("regions", pd.DataFrame()),
    )


# ── Agregation intelligente ─────────────────────────────────────────────────

CHEPTEL_FIELDS = list(FIELD_PATTERNS.keys())


def compute_cheptel_stats(
    communes_in_zone: pd.DataFrame,
    all_communes: pd.DataFrame,
    cheptel: CheptelData,
) -> dict:
    """
    Calcule les statistiques cheptel pour les communes dans la zone,
    en utilisant l'agregation la plus haute possible pour eviter le
    secret statistique.

    Retourne un dict avec :
      - totaux par champ (exploit_lait, vaches_lait, etc.)
      - detail de l'agregation (nb communes/dept/regions utilisees)
    """
    if not cheptel.is_loaded() or communes_in_zone.empty:
        return {}

    codes_in_zone = set(communes_in_zone["code_insee"])
    all_codes = set(all_communes["code_insee"])

    # Ajouter dep_code aux communes en zone si absent
    if "dep_code" not in communes_in_zone.columns:
        communes_in_zone = communes_in_zone.copy()
        communes_in_zone["dep_code"] = communes_in_zone["code_insee"].apply(
            lambda c: c[:2] if not c.startswith("2") else c[:2]
        )

    # ── 1. Identifier les departements entierement dans la zone ──────
    depts_in_zone = communes_in_zone["dep_code"].unique()

    fully_zoned_depts = set()
    for dept in depts_in_zone:
        # Toutes les communes de ce departement dans le referentiel
        all_in_dept = {c for c in all_codes if c.startswith(dept)}
        in_zone_in_dept = codes_in_zone & all_in_dept
        if all_in_dept and in_zone_in_dept == all_in_dept:
            fully_zoned_depts.add(dept)

    # ── 2. Identifier les regions entierement dans la zone ───────────
    fully_zoned_regions = set()
    for reg_code, reg_depts in _REG.items():
        reg_depts_set = set(reg_depts)
        if reg_depts_set and reg_depts_set.issubset(fully_zoned_depts):
            fully_zoned_regions.add(reg_code)

    # ── 3. Agreger ───────────────────────────────────────────────────
    # Departements couverts par une region complete (pas besoin de dept-level)
    depts_covered_by_region = set()
    for reg in fully_zoned_regions:
        depts_covered_by_region.update(_REG.get(reg, []))

    # Departements entiers non couverts par une region
    depts_standalone = fully_zoned_depts - depts_covered_by_region

    # Communes non couvertes par dept ou region
    codes_covered = set()
    for dept in fully_zoned_depts:
        codes_covered.update(c for c in codes_in_zone if c.startswith(dept))
    communes_standalone = codes_in_zone - codes_covered

    # ── 4. Sommer ────────────────────────────────────────────────────
    totals = {f: 0.0 for f in CHEPTEL_FIELDS}
    n_secret = 0

    # Regions
    for reg in fully_zoned_regions:
        row = cheptel.regions[cheptel.regions["code"] == reg]
        if not row.empty:
            for f in CHEPTEL_FIELDS:
                if f in row.columns:
                    val = row.iloc[0].get(f)
                    if pd.notna(val):
                        totals[f] += val

    # Departements
    for dept in depts_standalone:
        row = cheptel.departements[cheptel.departements["code"] == dept]
        if not row.empty:
            for f in CHEPTEL_FIELDS:
                if f in row.columns:
                    val = row.iloc[0].get(f)
                    if pd.notna(val):
                        totals[f] += val

    # Communes
    for code in communes_standalone:
        row = cheptel.communes[cheptel.communes["code"] == code]
        if not row.empty:
            for f in CHEPTEL_FIELDS:
                if f in row.columns:
                    val = row.iloc[0].get(f)
                    if pd.notna(val):
                        totals[f] += val
                    else:
                        n_secret += 1

    return {
        "exploit_lait": int(totals.get("exploit_lait", 0)),
        "exploit_nourr": int(totals.get("exploit_nourr", 0)),
        "vaches_lait": int(totals.get("vaches_lait", 0)),
        "vaches_nourr": int(totals.get("vaches_nourr", 0)),
        "total_vaches": int(totals.get("vaches_lait", 0) + totals.get("vaches_nourr", 0)),
        "total_exploit": int(totals.get("exploit_lait", 0) + totals.get("exploit_nourr", 0)),
        "n_regions": len(fully_zoned_regions),
        "n_depts_complets": len(depts_standalone),
        "n_communes_individuelles": len(communes_standalone),
        "n_secret": n_secret,
    }
