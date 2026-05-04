"""
EpiZone — Easter egg : CORVEX-Ω
═══════════════════════════════
Jeu de propagation épidémique déclenché par "siphano" dans
la barre de recherche du calculateur.

Mécanique :
  - Un pathogène émerge dans l'une des 4 écoles vétérinaires françaises
  - Il se propage de département en département par voisinage
  - Les 3 autres écoles mènent des recherches vaccinales
  - Le joueur utilise les doses disponibles pour bloquer la propagation
  - Score, timer, leaderboard persistant
"""

from __future__ import annotations

import json
import random
from datetime import datetime
from pathlib import Path

import pandas as pd

LEADERBOARD_PATH = Path("data/leaderboard_corvex.json")

# ── Écoles vétérinaires ──────────────────────────────────────────────────────

SCHOOLS: dict[str, dict] = {
    "vetagro": {
        "name": "VetAgro Sup",
        "city": "Marcy-l'Étoile",
        "dept": "69",
        "lat": 45.770, "lon": 4.700,
        "color": "#FF6F00",
    },
    "enva": {
        "name": "EnvA",
        "city": "Maisons-Alfort",
        "dept": "94",
        "lat": 48.813, "lon": 2.422,
        "color": "#1565C0",
    },
    "oniris": {
        "name": "Oniris",
        "city": "Nantes",
        "dept": "44",
        "lat": 47.210, "lon": -1.562,
        "color": "#2E7D32",
    },
    "envt": {
        "name": "ENVT",
        "city": "Toulouse",
        "dept": "31",
        "lat": 43.604, "lon": 1.443,
        "color": "#6A1B9A",
    },
}

# ── Adjacence départementale (France métropolitaine) ────────────────────────
# Source : COG INSEE, voisinage par frontière commune.

DEPT_ADJ: dict[str, list[str]] = {
    "01": ["39", "71", "73", "74", "69", "38", "42"],
    "02": ["60", "80", "59", "08", "51", "77", "95"],
    "03": ["63", "43", "42", "69", "71", "58", "23", "18", "15"],
    "04": ["05", "38", "26", "84", "83", "06"],
    "05": ["04", "38", "26", "73"],
    "06": ["04", "83"],
    "07": ["26", "38", "42", "43", "48", "30", "34", "84"],
    "08": ["02", "51", "55", "57"],
    "09": ["31", "11", "66", "65"],
    "10": ["51", "52", "77", "89", "21"],
    "11": ["09", "31", "34", "66"],
    "12": ["15", "43", "48", "30", "34", "81", "82", "46"],
    "13": ["84", "83", "30"],
    "14": ["50", "61", "27", "76"],
    "15": ["12", "43", "63", "19", "46", "03"],
    "16": ["17", "79", "86", "87", "24", "33"],
    "17": ["16", "33", "79", "85"],
    "18": ["03", "23", "36", "41", "45", "58"],
    "19": ["15", "23", "24", "46", "87", "63"],
    "21": ["10", "52", "70", "89", "58", "71", "39"],
    "22": ["29", "35", "56"],
    "23": ["03", "18", "19", "36", "63", "87"],
    "24": ["16", "19", "33", "40", "46", "47", "87"],
    "25": ["39", "70", "90", "21", "68"],
    "26": ["04", "05", "07", "38", "84"],
    "27": ["14", "28", "60", "76", "78", "61"],
    "28": ["27", "41", "45", "61", "72", "78"],
    "29": ["22", "56"],
    "30": ["07", "12", "34", "48", "84", "13"],
    "31": ["09", "11", "32", "65", "81", "82"],
    "32": ["31", "33", "40", "47", "64", "65", "82"],
    "33": ["16", "17", "24", "32", "40", "47"],
    "34": ["07", "11", "12", "30", "48", "81"],
    "35": ["22", "44", "49", "50", "56", "53"],
    "36": ["18", "23", "37", "41", "86"],
    "37": ["36", "41", "49", "72", "86"],
    "38": ["01", "04", "05", "07", "26", "73", "69"],
    "39": ["01", "21", "25", "70", "71"],
    "40": ["24", "32", "33", "47", "64"],
    "41": ["28", "36", "37", "45", "72"],
    "42": ["01", "03", "07", "43", "63", "69", "71"],
    "43": ["03", "07", "12", "15", "42", "63", "48"],
    "44": ["35", "49", "56", "85"],
    "45": ["18", "28", "41", "58", "77", "89"],
    "46": ["12", "15", "19", "24", "47", "82"],
    "47": ["24", "32", "33", "40", "46", "82"],
    "48": ["07", "12", "15", "30", "34", "43"],
    "49": ["35", "37", "44", "53", "72", "79", "85", "86"],
    "50": ["14", "35", "61"],
    "51": ["02", "08", "10", "52", "55", "77"],
    "52": ["08", "10", "21", "51", "55", "67", "68", "70"],
    "53": ["35", "49", "61", "72"],
    "54": ["55", "57", "67", "88"],
    "55": ["08", "51", "52", "54", "57"],
    "56": ["22", "29", "35", "44"],
    "57": ["08", "54", "55", "67", "88"],
    "58": ["03", "18", "21", "45", "71", "89"],
    "59": ["02", "62"],
    "60": ["02", "27", "76", "77", "80", "95"],
    "61": ["14", "27", "28", "35", "50", "53", "72"],
    "62": ["59", "80"],
    "63": ["03", "15", "19", "23", "42", "43", "69"],
    "64": ["32", "40", "65"],
    "65": ["09", "31", "32", "64"],
    "66": ["09", "11"],
    "67": ["52", "54", "57", "68", "88"],
    "68": ["25", "52", "67", "70", "90"],
    "69": ["01", "03", "38", "42", "71"],
    "70": ["21", "25", "39", "52", "68", "88", "90"],
    "71": ["01", "03", "21", "39", "42", "58", "69"],
    "72": ["28", "37", "41", "49", "53", "61"],
    "73": ["01", "04", "05", "38", "74"],
    "74": ["01", "73"],
    "75": ["92", "93", "94"],
    "76": ["14", "27", "60", "80"],
    "77": ["02", "10", "45", "51", "60", "89", "91", "93"],
    "78": ["27", "28", "91", "92", "95"],
    "79": ["16", "17", "37", "49", "86"],
    "80": ["02", "60", "62", "76"],
    "81": ["12", "30", "31", "34", "82"],
    "82": ["12", "31", "32", "46", "47", "81"],
    "83": ["04", "06", "13", "84"],
    "84": ["04", "07", "13", "26", "30", "83"],
    "85": ["44", "49", "79", "17"],
    "86": ["16", "36", "37", "49", "79", "87"],
    "87": ["16", "19", "23", "24", "36", "63", "86"],
    "88": ["52", "54", "57", "67", "68", "70"],
    "89": ["10", "21", "45", "58", "77"],
    "90": ["25", "68", "70"],
    "91": ["77", "78", "92", "94", "95"],
    "92": ["75", "78", "91", "93", "95"],
    "93": ["75", "77", "92", "94"],
    "94": ["75", "77", "91", "93"],
    "95": ["27", "60", "78", "91", "92"],
    "2A": ["2B"],
    "2B": ["2A"],
}

ALL_DEPTS = sorted(DEPT_ADJ.keys())
N_DEPTS = len(ALL_DEPTS)

# ── Événements aléatoires ────────────────────────────────────────────────────

EVENTS = [
    {
        "id": "mutation",
        "icon": "🧬",
        "label": "Mutation détectée !",
        "desc": "CORVEX-Ω a muté - contamination par saut géographique !",
        "effect": "jump",
        "rarity": 4,
    },
    {
        "id": "confinement",
        "icon": "🔒",
        "label": "Confinement préfectoral",
        "desc": "Un département est temporairement imperméable (5 ticks).",
        "effect": "block",
        "rarity": 2,
    },
    {
        "id": "oms_don",
        "icon": "💉",
        "label": "Don d'urgence de l'OMS",
        "desc": "+5 doses de vaccin disponibles immédiatement.",
        "effect": "vaccine",
        "rarity": 2,
    },
    {
        "id": "vecteur",
        "icon": "🦟",
        "label": "Nouveau vecteur identifié !",
        "desc": "La propagation s'accélère pendant 5 ticks.",
        "effect": "speed",
        "rarity": 3,
    },
    {
        "id": "mobilisation",
        "icon": "🏥",
        "label": "Mobilisation vétérinaire nationale",
        "desc": "Toutes les recherches accélèrent pendant 5 ticks.",
        "effect": "research",
        "rarity": 4,
    },
    {
        "id": "reseau_ferroviaire",
        "icon": "🚄",
        "label": "Propagation par réseau TGV !",
        "desc": "Le pathogène emprunte une grande ligne - 2 sauts immédiats.",
        "effect": "rail",
        "rarity": 3,
    },
    {
        "id": "alerte_rouge",
        "icon": "📡",
        "label": "Alerte sanitaire nationale",
        "desc": "Les 3 prochains départements à risque sont révélés.",
        "effect": "reveal",
        "rarity": 2,
    },
    {
        "id": "don_materiel",
        "icon": "🧪",
        "label": "Don de matériel de laboratoire",
        "desc": "Une école aléatoire gagne +30% de progression.",
        "effect": "lab",
        "rarity": 3,
    },
]

# Noeuds ferroviaires principaux (Paris central)
RAIL_HUBS = {
    "75": ["33", "13", "69", "31", "59", "67"],  # Paris → grandes villes
    "69": ["75", "13", "33"],
    "13": ["75", "69", "31"],
    "33": ["75", "31", "44"],
}


# ── Initialisation ───────────────────────────────────────────────────────────

def new_game(source_school_id: str | None = None) -> dict:
    """Initialise un nouveau jeu et retourne l'état initial."""
    if source_school_id is None:
        source_school_id = random.choice(list(SCHOOLS.keys()))

    school = SCHOOLS[source_school_id]
    source_dept = school["dept"]

    # Départements naturellement résistants (6, tirés hors écoles + source)
    school_depts = {s["dept"] for s in SCHOOLS.values()}
    eligible = [d for d in ALL_DEPTS if d not in school_depts and d != source_dept]
    resistant = set(random.sample(eligible, min(7, len(eligible))))

    # Résistance partielle : 8 depts supplémentaires avec 40% de résistance
    eligible2 = [d for d in eligible if d not in resistant]
    partial_resistant = set(random.sample(eligible2, min(8, len(eligible2))))

    # Recherche des 3 autres écoles (progression initiale légèrement variée)
    research = {
        sid: random.randint(0, 5)
        for sid in SCHOOLS
        if sid != source_school_id
    }

    # Multiplicateurs individuels de vitesse de recherche (hétérogénéité)
    # Chaque école a son propre rythme qui évolue au cours de la partie
    research_multipliers = {
        sid: round(random.uniform(0.5, 1.5), 4)
        for sid in research
    }

    # Confinements actifs : dict dept_code → tick_expiry
    blocked_until: dict[str, int] = {}

    # Prochains depts à risque (calculé à la demande par reveal)
    at_risk: list[str] = []

    return {
        "phase": "playing",           # playing | won | lost
        "tick": 0,
        "elapsed": 0,                  # secondes écoulées
        "source_school": source_school_id,
        "infected": [source_dept],
        "vaccinated": [],
        "resistant": list(resistant),
        "partial_resistant": list(partial_resistant),
        "blocked_until": blocked_until,
        "research": research,
        "research_multipliers": research_multipliers,
        "research_phase_change_tick": random.randint(6, 14),
        "vaccines_available": 5,
        "school_completed": [],
        "speed_boost": 0,             # ticks restants
        "research_boost": 0,          # ticks restants
        "events_log": [               # [(icon, msg)] — max 5 affichés
            ("🦠", f"CORVEX-Ω détecté à {school['city']} ! Propagation en cours...")
        ],
        "flash_event": None,          # {"icon", "label", "desc", "tick"} — affiché en banner
        "next_event_tick": random.randint(1, 9),
        "at_risk": at_risk,
        "score": None,
        "result_message": "",
    }


# ── Tick de jeu ──────────────────────────────────────────────────────────────

def game_tick(state: dict) -> dict:
    """Calcule un tick (1 seconde). Retourne le nouvel état."""
    if state["phase"] != "playing":
        return state

    state = _deep_copy_state(state)
    state["tick"] += 1
    state["elapsed"] += 1

    infected = set(state["infected"])
    vaccinated = set(state["vaccinated"])
    resistant = set(state["resistant"])
    partial_resistant = set(state["partial_resistant"])

    # Départements bloqués (confinement) encore actifs
    blocked_until: dict[str, int] = state.get("blocked_until", {})
    active_blocked = {d for d, t in blocked_until.items() if t > state["tick"]}
    # Nettoyer les confinements expirés
    state["blocked_until"] = {d: t for d, t in blocked_until.items() if t > state["tick"]}

    fully_protected = vaccinated | resistant | active_blocked
    partially_protected = partial_resistant - fully_protected

    # ── Propagation ──────────────────────────────────────────────────
    candidates: list[str] = []
    for dept in infected:
        for neighbor in DEPT_ADJ.get(dept, []):
            if neighbor not in infected and neighbor not in fully_protected:
                # Résistance partielle : 40% de chance de bloquer
                if neighbor in partially_protected and random.random() < 0.40:
                    continue
                candidates.append(neighbor)

    candidates = list(set(candidates))
    random.shuffle(candidates)

    n_new = 2 if state["speed_boost"] > 0 else 1
    # Résistance partielle : chaque candidat a 30% de chance supplémentaire d'être bloqué
    final_candidates = []
    for dept in candidates:
        if random.random() < 0.30:  # 30% de blocage naturel aléatoire
            continue
        final_candidates.append(dept)

    for dept in final_candidates[:n_new]:
        infected.add(dept)

    state["infected"] = list(infected)

    if state["speed_boost"] > 0:
        state["speed_boost"] -= 1

    # ── Recherche (hétérogène par école) ──────────────────────────────
    # Changement de phase périodique : chaque école peut accélérer ou stagner
    if state["tick"] >= state.get("research_phase_change_tick", 99):
        new_mults = {}
        for sid in state["research_multipliers"]:
            current = state["research_multipliers"][sid]
            # Variation autour de la valeur actuelle (tendance ±0.4) + reset possible
            delta = random.uniform(-0.5, 0.5)
            new_val = max(0.2, min(2.0, current + delta))
            new_mults[sid] = round(new_val, 2)
        state["research_multipliers"] = new_mults
        state["research_phase_change_tick"] = state["tick"] + random.randint(8, 15)

    # Gain de base lent : 1-2% × multiplicateur individuel
    base_gain = random.uniform(1.0, 2.0)
    if state["research_boost"] > 0:
        base_gain *= 2.5
        state["research_boost"] -= 1

    for sid in list(state["research"].keys()):
        if state["research"][sid] < 100:
            mult = state["research_multipliers"].get(sid, 1.0)
            gain = base_gain * mult + random.uniform(-0.3, 0.3)
            gain = max(0.1, gain)  # jamais négatif
            state["research"][sid] = min(100, state["research"][sid] + gain)
            if state["research"][sid] >= 100 and sid not in state["school_completed"]:
                state["school_completed"].append(sid)
                state["vaccines_available"] += 6
                sname = SCHOOLS[sid]["name"]
                state["events_log"].insert(0, ("💉", f"{sname} a développé le vaccin ! +6 doses"))
                state["flash_event"] = {
                    "icon": "💉",
                    "label": f"Vaccin prêt — {sname} !",
                    "desc": "+6 doses disponibles immédiatement",
                    "tick": state["tick"],
                }

    # ── Production régulière par les écoles ayant terminé ────────────
    # Chaque école complète génère +1 dose toutes les 4 ticks
    if state["school_completed"] and state["tick"] % 4 == 0:
        n_producing = len(state["school_completed"])
        state["vaccines_available"] += n_producing
        if n_producing == 1:
            sname = SCHOOLS[state["school_completed"][0]]["name"]
            state["events_log"].insert(0, ("🧪", f"{sname} produit une dose"))
        else:
            state["events_log"].insert(0, ("🧪", f"{n_producing} écoles produisent {n_producing} dose{'s' if n_producing > 1 else ''}"))

    # ── Événement aléatoire ───────────────────────────────────────────
    if state["tick"] >= state["next_event_tick"]:
        state = _trigger_random_event(state, infected, fully_protected)
        state["next_event_tick"] = state["tick"] + random.randint(6, 12)

    # Garde le log court
    state["events_log"] = state["events_log"][:5]

    # ── Conditions de fin ─────────────────────────────────────────────
    n_infected = len(state["infected"])
    n_schools_done = len(state["school_completed"])

    # Défaite : ≥ 80% de départements infectés
    if n_infected >= int(N_DEPTS * 0.80):
        state["phase"] = "lost"
        state["score"] = _compute_score(state, won=False)
        state["result_message"] = (
            f"CORVEX-Ω a ravagé la France. {n_infected} départements contaminés. "
            f"La profession vétérinaire est anéantie."
        )

    # Victoire : 3 écoles ont fini + ≤ 55% infectés
    elif n_schools_done >= 3 and n_infected <= int(N_DEPTS * 0.55):
        state["phase"] = "won"
        state["score"] = _compute_score(state, won=True)
        saved = N_DEPTS - n_infected
        state["result_message"] = (
            f"Épizootie contenue ! {saved} départements sauvés en "
            f"{_fmt_time(state['elapsed'])}."
        )

    return state


def vaccinate_dept(state: dict, dept_code: str) -> dict:
    """Applique un vaccin sur un département si possible."""
    if state["phase"] != "playing":
        return state
    if state["vaccines_available"] <= 0:
        return state
    infected = set(state["infected"])
    vaccinated = set(state["vaccinated"])
    if dept_code in infected or dept_code in vaccinated:
        return state

    state = _deep_copy_state(state)
    state["vaccinated"].append(dept_code)
    state["vaccines_available"] -= 1
    state["events_log"].insert(0, ("💉", f"Dpt {dept_code} vacciné"))
    state["events_log"] = state["events_log"][:5]
    return state


# ── Événements ───────────────────────────────────────────────────────────────

def _trigger_random_event(state: dict, infected: set, protected: set) -> dict:
    """Tire et applique un événement aléatoire."""
    # Pondération inverse à rarity
    pool = []
    for ev in EVENTS:
        pool.extend([ev] * (6 - ev["rarity"]))
    event = random.choice(pool)

    all_clean = [d for d in ALL_DEPTS if d not in infected and d not in protected]

    # Stocker le flash event pour affichage banner dans l'UI
    state["flash_event"] = {
        "icon": event["icon"],
        "label": event["label"],
        "desc": event["desc"],
        "tick": state["tick"],
    }

    if event["effect"] == "jump" and all_clean:
        target = random.choice(all_clean)
        state["infected"].append(target)
        state["events_log"].insert(0, (event["icon"],
            f"{event['label']} → Dpt {target} contaminé"))

    elif event["effect"] == "rail":
        # Saut via réseau ferroviaire depuis un hub infecté
        jumped = []
        for hub, destinations in RAIL_HUBS.items():
            if hub in infected:
                for dest in destinations:
                    if dest not in infected and dest not in protected:
                        jumped.append(dest)
        if jumped:
            targets = random.sample(jumped, min(2, len(jumped)))
            for t in targets:
                state["infected"].append(t)
            state["events_log"].insert(0, (event["icon"],
                f"{event['label']} → {', '.join(targets)}"))
        else:
            state["events_log"].insert(0, (event["icon"], event["desc"]))

    elif event["effect"] == "block" and all_clean:
        target = random.choice(all_clean)
        state["blocked_until"][target] = state["tick"] + 5
        state["events_log"].insert(0, (event["icon"],
            f"{event['label']} → Dpt {target} confiné (5 ticks)"))

    elif event["effect"] == "vaccine":
        state["vaccines_available"] += 6
        state["events_log"].insert(0, (event["icon"], event["desc"]))

    elif event["effect"] == "speed":
        state["speed_boost"] = 5
        state["events_log"].insert(0, (event["icon"], event["desc"]))

    elif event["effect"] == "research":
        state["research_boost"] = 5
        state["events_log"].insert(0, (event["icon"], event["desc"]))

    elif event["effect"] == "reveal":
        # Identifier les depts les plus à risque (voisins d'infectés non protégés)
        risk_candidates = []
        for dept in infected:
            for nb in DEPT_ADJ.get(dept, []):
                if nb not in infected and nb not in protected:
                    risk_candidates.append(nb)
        at_risk = list(dict.fromkeys(risk_candidates))[:3]
        state["at_risk"] = at_risk
        state["events_log"].insert(0, (event["icon"],
            f"{event['desc']} : {', '.join(at_risk) if at_risk else 'aucun'}"))

    elif event["effect"] == "lab":
        # Booster une école aléatoire non-terminée
        eligible_schools = [
            sid for sid in state["research"]
            if state["research"][sid] < 100
        ]
        if eligible_schools:
            target_school = random.choice(eligible_schools)
            state["research"][target_school] = min(
                100, state["research"][target_school] + 30
            )
            sname = SCHOOLS[target_school]["name"]
            state["events_log"].insert(0, (event["icon"],
                f"{event['desc']} → {sname} +30%"))
        else:
            state["vaccines_available"] += 6
            state["events_log"].insert(0, (event["icon"],
                "Don de matériel → +4 dose (recherches complètes)"))

    return state


# ── Score et leaderboard ─────────────────────────────────────────────────────

def _compute_score(state: dict, won: bool) -> int:
    saved = N_DEPTS - len(state["infected"])
    time_bonus = max(0, 240 - state["elapsed"]) * 3
    school_bonus = len(state["school_completed"]) * 400
    vaccine_bonus = len(state["vaccinated"]) * 30
    victory_bonus = 2500 if won else 0
    return max(0, saved * 40 + time_bonus + school_bonus + vaccine_bonus + victory_bonus)


def _fmt_time(seconds: int) -> str:
    m, s = divmod(seconds, 60)
    return f"{m}:{s:02d}"


def fmt_time(seconds: int) -> str:
    return _fmt_time(seconds)


def load_leaderboard() -> list[dict]:
    if LEADERBOARD_PATH.exists():
        try:
            with open(LEADERBOARD_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []
    return []


def save_score(name: str, state: dict) -> list[dict]:
    """Enregistre le score et retourne le leaderboard mis à jour (top 20)."""
    lb = load_leaderboard()
    entry = {
        "name": (name or "Anonyme")[:20].strip(),
        "score": state.get("score", 0) or 0,
        "won": state["phase"] == "won",
        "saved": N_DEPTS - len(state["infected"]),
        "time": _fmt_time(state["elapsed"]),
        "date": datetime.now().strftime("%d/%m/%Y"),
    }
    lb.append(entry)
    lb = sorted(lb, key=lambda x: x["score"], reverse=True)[:20]
    LEADERBOARD_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(LEADERBOARD_PATH, "w", encoding="utf-8") as f:
        json.dump(lb, f, ensure_ascii=False, indent=2)
    return lb


# ── Utilitaires ───────────────────────────────────────────────────────────────

def _deep_copy_state(state: dict) -> dict:
    """Copie suffisamment profonde pour éviter les mutations d'état Dash."""
    return {
        **state,
        "infected": list(state["infected"]),
        "vaccinated": list(state["vaccinated"]),
        "resistant": list(state["resistant"]),
        "partial_resistant": list(state.get("partial_resistant", [])),
        "blocked_until": dict(state.get("blocked_until", {})),
        "research": dict(state["research"]),
        "research_multipliers": dict(state.get("research_multipliers", {})),
        "school_completed": list(state["school_completed"]),
        "events_log": list(state["events_log"]),
        "at_risk": list(state.get("at_risk", [])),
        "flash_event": state.get("flash_event"),
    }


def build_game_geojson(dept_geo) -> dict:
    """
    Construit le GeoJSON de base pour la carte de jeu depuis le GeoDataFrame
    départemental. Appelé une seule fois au démarrage.
    """
    if dept_geo is None:
        return {"type": "FeatureCollection", "features": []}
    features = []
    for _, row in dept_geo.iterrows():
        code = str(row.get("dept_code", ""))
        nom = str(row.get("dept_nom", code))
        features.append({
            "type": "Feature",
            "properties": {"dept_code": code, "dept_nom": nom},
            "geometry": row.geometry.__geo_interface__,
        })
    return {"type": "FeatureCollection", "features": features}


def get_hideout(state: dict) -> dict:
    """Construit le hideout pour la style function dash-leaflet."""
    return {
        "infected":          state.get("infected", []),
        "vaccinated":        state.get("vaccinated", []),
        "resistant":         state.get("resistant", []),
        "partial_resistant": state.get("partial_resistant", []),
        "at_risk":           state.get("at_risk", []),
        "blocked":           list(state.get("blocked_until", {}).keys()),
        "source_dept":       SCHOOLS[state["source_school"]]["dept"],
    }
