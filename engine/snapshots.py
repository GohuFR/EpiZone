"""
EpiZone — Moteur de snapshots

Pour chaque date-cle, calcule l'etat complet du zonage :
quelles communes sont dans quelle zone.

Gere :
- La priorite entre zones (ZP > ZS > ZR, configurable)
- Les zones combinees (ZR+ZV pour la MHE)
- Le pre-calcul de tous les snapshots pour navigation instantanee
"""

from __future__ import annotations

import pandas as pd
import numpy as np

from .config import DiseaseConfig


# ── Identification des dates-cles ────────────────────────────────────────────

def compute_key_dates(periodes: pd.DataFrame) -> list[pd.Timestamp]:
    """
    Identifie les dates ou au moins une commune change de statut.
    Entre deux dates-cles consecutives, la carte est identique.
    """
    all_dates = pd.concat([
        periodes["date_debut"].dropna(),
        periodes["date_fin"].dropna(),
    ]).unique()

    dates = sorted(pd.to_datetime(all_dates))
    return dates


# ── Calcul du statut a une date ──────────────────────────────────────────────

def statut_a_date(
    d: pd.Timestamp,
    periodes: pd.DataFrame,
    config: DiseaseConfig,
) -> pd.DataFrame:
    """
    Pour une date donnee, retourne la liste des communes actives
    et leur zone, en appliquant les regles de priorite et de
    combinaison configurees.

    Parametres
    ----------
    d : pd.Timestamp
        Date a evaluer.
    periodes : pd.DataFrame
        Periodes normalisees (sortie du loader, eventuellement expandees).
    config : DiseaseConfig
        Configuration de la maladie.

    Retour
    ------
    pd.DataFrame avec colonnes : code_insee, commune, departement, region, zone
    """
    # Filtrer les periodes actives a la date d
    # Intervalle : [date_debut, date_fin[
    actives = periodes[
        (periodes["date_debut"] <= d)
        & (periodes["date_fin"].isna() | (periodes["date_fin"] > d))
    ].copy()

    if actives.empty:
        return pd.DataFrame(
            columns=["code_insee", "commune", "departement", "region", "zone"]
        )

    # ── Cas avec zones combinees (ex: MHE ZR+ZV) ─────────────────────
    if config.combo_zones:
        return _statut_with_combos(actives, config)

    # ── Cas standard : priorite simple ────────────────────────────────
    return _statut_with_priority(actives, config)


def _statut_with_priority(
    actives: pd.DataFrame,
    config: DiseaseConfig,
) -> pd.DataFrame:
    """
    Resout les superpositions par priorite :
    garde la zone de plus faible indice de priorite (= plus restrictive).
    """
    priorities = config.zone_priorities

    actives = actives.copy()
    actives["_prio"] = actives["zone"].map(priorities).fillna(99)

    # Pour chaque commune, garder la zone de plus haute priorite (plus petit numero)
    result = (
        actives
        .sort_values("_prio")
        .groupby("code_insee", as_index=False)
        .first()
    )

    return result[["code_insee", "commune", "departement", "region", "zone"]]


def _statut_with_combos(
    actives: pd.DataFrame,
    config: DiseaseConfig,
) -> pd.DataFrame:
    """
    Gere les zones combinees (ex: ZR+ZV pour la MHE).
    100% vectorise : pivot + conditions numpy, pas de lambda/apply.
    """
    priorities = config.zone_priorities

    # Attributs de base par commune
    base = (
        actives
        .sort_values("zone")
        .groupby("code_insee", as_index=False)
        .first()[["code_insee", "commune", "departement", "region"]]
    )

    # Pivot : colonnes booleennes par zone
    zone_flags = (
        actives[["code_insee", "zone"]]
        .drop_duplicates()
        .assign(_flag=True)
        .pivot_table(index="code_insee", columns="zone", values="_flag",
                     fill_value=False, aggfunc="first")
        .reset_index()
    )

    merged = base.merge(zone_flags, on="code_insee", how="left")

    # Resoudre la zone finale — vectorise
    # 1. Combos d'abord
    merged["zone"] = None
    for combo in config.combo_zones:
        # Toutes les zones requises doivent etre True
        mask = pd.Series(True, index=merged.index)
        for req in combo.requires:
            if req in merged.columns:
                mask = mask & (merged[req] == True)
            else:
                mask = pd.Series(False, index=merged.index)
                break
        merged.loc[mask & merged["zone"].isna(), "zone"] = combo.id

    # 2. Pour le reste, zone de plus haute priorite
    remaining = merged["zone"].isna()
    if remaining.any():
        zone_ids = sorted(
            [z.id for z in config.zones if z.id in zone_flags.columns],
            key=lambda z: priorities.get(z, 99)
        )
        for z in zone_ids:
            if z in merged.columns:
                mask = remaining & (merged[z] == True) & merged["zone"].isna()
                merged.loc[mask, "zone"] = z

    return merged[["code_insee", "commune", "departement", "region", "zone"]]


# ── Pre-calcul de tous les snapshots ─────────────────────────────────────────

def build_all_snapshots(
    periodes: pd.DataFrame,
    config: DiseaseConfig,
    dates_cles: list[pd.Timestamp] | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Pre-calcule l'etat du zonage pour chaque date-cle.

    Retour
    ------
    dict mappant date (str ISO) -> DataFrame de statuts.
    """
    if dates_cles is None:
        dates_cles = compute_key_dates(periodes)

    print(f"  Pre-calcul de {len(dates_cles)} snapshots... ", end="", flush=True)

    snapshots = {}

    for d in dates_cles:
        statuts = statut_a_date(d, periodes, config)
        snapshots[str(d.date())] = statuts

    # Stats
    if snapshots:
        max_communes = max(len(df) for df in snapshots.values())
        min_communes = min(len(df) for df in snapshots.values())
        print(f"OK (min={min_communes}, max={max_communes} communes)")
    else:
        print("aucun snapshot")

    return snapshots


# ── Utilitaires ──────────────────────────────────────────────────────────────

def snapshot_stats(snapshots: dict[str, pd.DataFrame], config: DiseaseConfig) -> pd.DataFrame:
    """
    Retourne un tableau resumant le nombre de communes par zone
    pour chaque date-cle. Utile pour debug et visualisation.
    """
    records = []
    zone_ids = [z.id for z in config.zones]

    for date_str, df in sorted(snapshots.items()):
        row = {"date": date_str, "total": len(df)}
        for z in zone_ids:
            row[z] = (df["zone"] == z).sum() if not df.empty else 0
        records.append(row)

    return pd.DataFrame(records)
