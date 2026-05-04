"""
EpiZone — Parseur de configuration maladie

Charge un fichier YAML decrivant une maladie et ses zones reglementees,
et expose une structure Python typee pour le reste du moteur.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


# ── Structures de donnees ────────────────────────────────────────────────────

@dataclass
class ZoneConfig:
    """Definition d'un type de zone (ex: ZP, ZS, ZR)."""
    id: str
    label: str
    color: str
    priority: int
    description: str = ""


@dataclass
class ColumnMapping:
    """Correspondance entre noms logiques et noms de colonnes Excel."""
    commune: str
    code_insee: str
    date_debut: str
    date_fin: str
    departement: str | None = None
    region: str | None = None
    dept_num: str | None = None


@dataclass
class RowFilter:
    """Filtre de lignes sur un onglet (ex: exclure Raison == 'ZVI')."""
    column: str
    exclude_values: list[str]


@dataclass
class SheetConfig:
    """Configuration d'un onglet Excel."""
    sheet_name: str
    zone_id: str | None          # None si la zone est lue depuis une colonne
    columns: ColumnMapping
    zone_column: str | None = None
    zone_value_map: dict[str, str] | None = None
    row_filter: RowFilter | None = None
    extra_columns: dict[str, str] = field(default_factory=dict)
    skip_columns: list[str] = field(default_factory=list)


@dataclass
class DeptExpansionConfig:
    """Configuration de l'expansion des entrees departement-entier."""
    enabled: bool
    pattern: str | None = None       # Regex pour detecter les codes dept
    dept_code_source: str | None = None

    @property
    def compiled_pattern(self) -> re.Pattern | None:
        if self.pattern:
            return re.compile(self.pattern)
        return None


@dataclass
class DerivedZoneConfig:
    """Zone deduite d'un onglet existant (ex: ZVII depuis ZS)."""
    id: str
    source_sheet: str
    filter: dict[str, str]         # {column, equals}
    date_debut_from: str           # Nom de colonne source pour date_debut
    date_fin_value: Any = None     # Valeur fixe ou None
    date_fin_rule: dict | None = None  # Regle conditionnelle
    copy_columns: dict[str, str] = field(default_factory=dict)


@dataclass
class ComboZoneConfig:
    """Zone combinee (ex: ZR+ZV quand une commune est dans les deux)."""
    id: str
    requires: list[str]


@dataclass
class MapConfig:
    """Parametres de la carte."""
    center: list[float]
    zoom: int


@dataclass
class DiseaseConfig:
    """Configuration complete d'une maladie."""
    id: str
    name: str
    subtitle: str
    description: str
    accent_color: str
    excel_file: str
    zones: list[ZoneConfig]
    sheets: list[SheetConfig]
    dept_expansion: DeptExpansionConfig
    derived_zones: list[DerivedZoneConfig]
    combo_zones: list[ComboZoneConfig]
    map: MapConfig
    regulatory: dict[str, str]

    @property
    def zone_by_id(self) -> dict[str, ZoneConfig]:
        return {z.id: z for z in self.zones}

    @property
    def zone_colors(self) -> dict[str, str]:
        return {z.id: z.color for z in self.zones}

    @property
    def zone_priorities(self) -> dict[str, int]:
        return {z.id: z.priority for z in self.zones}


# ── Parsing ──────────────────────────────────────────────────────────────────

def _parse_columns(raw: dict) -> ColumnMapping:
    return ColumnMapping(
        commune=raw["commune"],
        code_insee=raw["code_insee"],
        date_debut=raw["date_debut"],
        date_fin=raw["date_fin"],
        departement=raw.get("departement"),
        region=raw.get("region"),
        dept_num=raw.get("dept_num"),
    )


def _parse_sheet(raw: dict) -> SheetConfig:
    row_filter = None
    if "row_filter" in raw:
        rf = raw["row_filter"]
        row_filter = RowFilter(
            column=rf["column"],
            exclude_values=rf["exclude_values"],
        )
    return SheetConfig(
        sheet_name=raw["sheet_name"],
        zone_id=raw.get("zone_id"),
        columns=_parse_columns(raw["columns"]),
        zone_column=raw.get("zone_column"),
        zone_value_map=raw.get("zone_value_map"),
        row_filter=row_filter,
        extra_columns=raw.get("extra_columns", {}),
        skip_columns=raw.get("skip_columns", []),
    )


def _parse_derived_zone(raw: dict) -> DerivedZoneConfig:
    return DerivedZoneConfig(
        id=raw["id"],
        source_sheet=raw["source_sheet"],
        filter=raw["filter"],
        date_debut_from=raw["date_debut_from"],
        date_fin_value=raw.get("date_fin_value"),
        date_fin_rule=raw.get("date_fin_rule"),
        copy_columns=raw.get("copy_columns", {}),
    )


def load_disease_config(path: str | Path) -> DiseaseConfig:
    """Charge et valide un fichier YAML de configuration maladie."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config introuvable : {path}")

    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    # Zones
    zones = [
        ZoneConfig(
            id=z["id"],
            label=z["label"],
            color=z["color"],
            priority=z["priority"],
            description=z.get("description", ""),
        )
        for z in raw["zones"]
    ]

    # Sheets
    sheets = [_parse_sheet(s) for s in raw["sheets"]]

    # Dept expansion
    de = raw.get("dept_expansion", {"enabled": False})
    dept_expansion = DeptExpansionConfig(
        enabled=de.get("enabled", False),
        pattern=de.get("pattern"),
        dept_code_source=de.get("dept_code_source"),
    )

    # Derived zones
    derived_zones = [
        _parse_derived_zone(d) for d in raw.get("derived_zones", [])
    ]

    # Combo zones
    combo_zones = [
        ComboZoneConfig(id=c["id"], requires=c["requires"])
        for c in raw.get("combo_zones", [])
    ]

    # Map
    m = raw.get("map", {"center": [46.5, 2.5], "zoom": 6})
    map_config = MapConfig(center=m["center"], zoom=m["zoom"])

    return DiseaseConfig(
        id=raw["id"],
        name=raw["name"],
        subtitle=raw.get("subtitle", ""),
        description=raw.get("description", ""),
        accent_color=raw.get("accent_color", "#333333"),
        excel_file=raw["excel_file"],
        zones=zones,
        sheets=sheets,
        dept_expansion=dept_expansion,
        derived_zones=derived_zones,
        combo_zones=combo_zones,
        map=map_config,
        regulatory=raw.get("regulatory", {}),
    )


def load_all_configs(config_dir: str | Path) -> dict[str, DiseaseConfig]:
    """Charge tous les fichiers YAML d'un repertoire."""
    config_dir = Path(config_dir)
    configs = {}
    for path in sorted(config_dir.glob("*.yaml")):
        try:
            cfg = load_disease_config(path)
            configs[cfg.id] = cfg
        except Exception as e:
            print(f"  ⚠ Erreur chargement {path.name}: {e}")
    return configs
