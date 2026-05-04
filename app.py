"""
EpiZone — Visualisation des zones sanitaires (version definitive)

Fonctionnalites :
  - Visualisation temporelle avec slider + play
  - Recherche de commune avec zoom + historique
  - Graphique d'evolution temporelle (sparkline)
  - Calculateur de zones avec stats cheptel RA 2020
  - Import de nouvelles maladies
  - Export cartographique (PNG)
  - Raccourcis clavier (fleches, espace, F)
  - Mode plein ecran carte
"""

from pathlib import Path
import json, base64, re

import numpy as np
import pandas as pd
import geopandas as gpd
import dash
from dash import html, dcc, callback, Input, Output, State, no_update, ALL, clientside_callback
import dash_leaflet as dl
from dash_extensions.javascript import assign
import plotly.graph_objects as go

from engine.config import load_all_configs
from engine.pipeline import load_all_diseases, DiseaseData, build_regions_geojson
from engine.calculator import (
    load_commune_reference, load_commune_contours,
    calculer_zone, communes_to_geojson, communes_to_polygon_geojson, export_to_excel,
)
from engine.importer import analyze_excel, generate_config, save_import, trigger_reload, DEFAULT_COLORS
from engine.cheptel import load_cheptel, compute_cheptel_stats
from engine.easter import (
    new_game, game_tick, vaccinate_dept,
    save_score, load_leaderboard, fmt_time,
    SCHOOLS, N_DEPTS, build_game_geojson, get_hideout,
)

# =============================================================================
# 1. DATA
# =============================================================================

CONFIG_DIR = Path("configs")
DATA_DIR = Path("data")
UPLOAD_DIR = Path("uploads"); UPLOAD_DIR.mkdir(exist_ok=True)
EMPTY = {"type": "FeatureCollection", "features": []}
TILE_DARK = "https://{s}.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}{r}.png"
TILE_LIGHT = "https://{s}.basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}{r}.png"

# ── Noms longs à découper sur deux lignes ────────────────────────────────────
# Chaque entrée produit exactement 2 lignes via <br>.
# Les espaces dans chaque ligne NE wrapperont PAS grâce au divIcon (white-space:nowrap).
_REGION_LABEL_SPLITS = {
    "Auvergne-Rhône-Alpes":        "Auvergne-<br>Rhône-Alpes",
    "Bourgogne-Franche-Comté":     "Bourgogne-<br>Franche-Comté",
    "Centre-Val de Loire":          "Centre-Val<br>de Loire",
    "Nouvelle-Aquitaine":           "Nouvelle-<br>Aquitaine",
    "Pays de la Loire":             "Pays de<br>la Loire",
    "Provence-Alpes-Côte d'Azur":  "Provence-Alpes-<br>Côte d'Azur",
    "Hauts-de-France":              "Hauts-de-<br>France",
    "Île-de-France":                "Île-de-<br>France",
}

# ── Surcharges manuelles de centroïde (WGS84) ────────────────────────────────
# Utilisées quand la forme est irrégulière et que le centroïde géométrique
# du plus grand polygone ne tombe pas au centre visuel de la région.
_CENTROID_OVERRIDES = {
    # Corse : centroïde géométrique trop au sud à cause de la pointe de Bonifacio
    "Corse":            (42.20,  9.10),
    # Pays de la Loire : Loire-Atlantique tire le centroïde trop à l'ouest
    "Pays de la Loire": (47.75, -0.55),
}

def _load_regions_geojson():
    """
    Charge le GeoJSON régions depuis regions_geo.parquet (EPSG:2154 → 4326).
    Centroïde calculé sur le plus grand polygone pour éviter le biais des îles.
    Surcharges manuelles appliquées pour Corse, Pays de la Loire.
    Fallback sur build_regions_geojson() si le fichier est absent.
    """
    parquet_path = DATA_DIR / "regions_geo.parquet"
    if not parquet_path.exists():
        print("  Regions : parquet absent, fallback build_regions_geojson()")
        return build_regions_geojson()

    gdf = gpd.read_parquet(parquet_path).to_crs(epsg=4326)

    features = []
    for _, row in gdf.iterrows():
        nom = row["nom"]

        # ── Centroïde : plus grand polygone ou surcharge manuelle ──
        if nom in _CENTROID_OVERRIDES:
            clat, clng = _CENTROID_OVERRIDES[nom]
        elif row.geometry.geom_type == "MultiPolygon":
            mainland = max(row.geometry.geoms, key=lambda p: p.area)
            c = mainland.centroid
            clat, clng = c.y, c.x
        else:
            c = row.geometry.centroid
            clat, clng = c.y, c.x

        features.append({
            "type": "Feature",
            "geometry": row.geometry.__geo_interface__,
            "properties": {
                "code":      row["code"],
                "nom":       nom,
                "label":     _REGION_LABEL_SPLITS.get(nom, nom),
                "label_lat": clat,
                "label_lng": clng,
            },
        })

    return {"type": "FeatureCollection", "features": features}


def _build_regions_labels_geojson(regions_gj):
    """
    Construit un GeoJSON de POINTS à partir des centroïdes calculés
    dans regions_gj. Utilisé pour la couche de labels (divIcon markers).
    Séparer bordures et labels permet de positionner les labels précisément.
    """
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [f["properties"]["label_lng"],
                                    f["properties"]["label_lat"]],
                },
                "properties": {
                    "nom":   f["properties"]["nom"],
                    "label": f["properties"]["label"],
                },
            }
            for f in regions_gj["features"]
        ],
    }


print("=" * 60)
print("  EpiZone — Chargement")
print("=" * 60)

DISEASES = load_all_diseases(CONFIG_DIR, DATA_DIR, with_geometries=True)
REGIONS_GJ = _load_regions_geojson()
REGIONS_LABELS_GJ = _build_regions_labels_geojson(REGIONS_GJ)
print(f"  Regions : {len(REGIONS_GJ.get('features', []))} polygones, {len(REGIONS_LABELS_GJ.get('features', []))} labels")

# ── Easter egg : GeoJSON départemental ──────────────────────────────────────
from engine.pipeline import _dept_geo as _DEPT_GEO_PIPELINE
import json as _json

_GAME_GJ_PATH = Path("assets/game_dept.json")
try:
    if not _GAME_GJ_PATH.exists() or _GAME_GJ_PATH.stat().st_size < 100:
        # Générer depuis les géométries
        if _DEPT_GEO_PIPELINE is not None and len(_DEPT_GEO_PIPELINE) > 0:
            _gj = build_game_geojson(_DEPT_GEO_PIPELINE)
            _GAME_GJ_PATH.parent.mkdir(parents=True, exist_ok=True)
            _GAME_GJ_PATH.write_text(_json.dumps(_gj, separators=(",", ":")), encoding="utf-8")
            print(f"  Easter egg : GeoJSON généré ({_GAME_GJ_PATH.stat().st_size/1e3:.0f} Ko)")
        else:
            print("  Easter egg : _dept_geo indisponible, easter egg sans carte")
            _gj = {"type": "FeatureCollection", "features": []}
    else:
        _gj = _json.loads(_GAME_GJ_PATH.read_text(encoding="utf-8"))
        print(f"  Easter egg : GeoJSON depuis cache ({_GAME_GJ_PATH.stat().st_size/1e3:.0f} Ko)")
    GAME_DEPT_GJ = _gj
except Exception as _e:
    print(f"  Easter egg : erreur GeoJSON ({_e}) — easter egg sans carte dept")
    GAME_DEPT_GJ = {"type": "FeatureCollection", "features": []}

print(f"\n  Calculateur...")
COMMUNES_REF = load_commune_reference()
COMMUNES_CONTOURS = load_commune_contours()
# Pre-compute search index with smart ranking
SEARCH_IDX = COMMUNES_REF[["code_insee","nom","dep_code"]].copy()
SEARCH_IDX["_nom_low"] = SEARCH_IDX["nom"].str.lower()
SEARCH_IDX["_nom_len"] = SEARCH_IDX["nom"].str.len()
# Heuristic: communes with low INSEE suffix tend to be larger/more important
SEARCH_IDX["_suffix"] = SEARCH_IDX["code_insee"].str[-3:].apply(
    lambda x: int(x) if x.isdigit() else 999)
SEARCH_IDX["_rank"] = SEARCH_IDX["_nom_len"] * 10 + SEARCH_IDX["_suffix"].clip(upper=100)
SEARCH_IDX = SEARCH_IDX.sort_values("_rank")

print(f"  Cheptel RA 2020...")
CHEPTEL = load_cheptel(DATA_DIR)

# Pre-calculer les sparklines pour chaque maladie
SPARKLINES = {}
for did, d in DISEASES.items():
    records = []
    for ds, snap in sorted(d.snapshots.items()):
        row = {"date": ds, "total": len(snap)}
        for z in d.config.zones:
            row[z.id] = (snap["zone"] == z.id).sum() if not snap.empty else 0
        records.append(row)
    SPARKLINES[did] = pd.DataFrame(records)

print(f"\n{'=' * 60}")
print(f"  {len(DISEASES)} maladie(s) — pret")
print("=" * 60)

if not DISEASES:
    raise SystemExit("Aucune maladie chargee.")

# =============================================================================
# 2. HELPERS
# =============================================================================

def stat_card(val, label, color="inherit"):
    t = f"{val:,}" if isinstance(val, int) else str(val)
    return html.Div(className="stat-card", children=[
        html.Div(t, className="stat-value", style={"color": color}),
        html.Div(label, className="stat-label")])

def make_legend(config):
    return html.Div([
        html.Div(className="legend-item", children=[
            html.Div(className="legend-dot", style={"background": z.color}),
            html.Span(f"{z.id} — {z.label}"),
        ]) for z in config.zones])

def make_sparkline(did):
    """Genere un mini graphique d'evolution temporelle."""
    df = SPARKLINES.get(did)
    if df is None or df.empty:
        return go.Figure()
    d = DISEASES[did]
    fig = go.Figure()
    for z in reversed(d.config.zones):
        if z.id in df.columns and df[z.id].sum() > 0:
            # Convert hex color to rgba with 25% opacity
            hc = z.color.lstrip("#")
            r, g, b = int(hc[0:2],16), int(hc[2:4],16), int(hc[4:6],16)
            fig.add_trace(go.Scatter(
                x=list(range(len(df))), y=df[z.id], name=z.id,
                fill="tonexty", mode="none",
                fillcolor=f"rgba({r},{g},{b},0.25)",
                line=dict(color=z.color, width=0),
                hovertemplate=f"{z.id}: %{{y:,}}<extra></extra>",
            ))
    fig.update_layout(
        height=80, margin=dict(l=0, r=0, t=0, b=0),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
        xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        yaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        hovermode="x unified",
        hoverlabel=dict(bgcolor="#1a1f2e", font_color="#e8e6e1",
                        font_size=11, bordercolor="#2a3040"),
    )
    return fig

first_id = list(DISEASES.keys())[0]
first = DISEASES[first_id]
INB = {"flex":"1","color":"#adadad","borderColor":"#3a3a3a"}

# =============================================================================
# 3. LAYOUT
# =============================================================================

pills = html.Div(className="disease-pills", children=[
    html.Button(d.config.name, id={"type":"dp","index":did},
        className="disease-pill active" if did==first_id else "disease-pill",
        n_clicks=0) for did,d in DISEASES.items()])

# ── VISUALISATION ────────────────────────────────────────────────────────────
visu = html.Div(id="visu-panel", children=[
    html.Div("Maladie", className="section-label"), pills,

    # Recherche commune
    html.Div("Rechercher une commune", className="section-label", style={"marginTop":"20px"}),
    html.Div(style={"display":"flex","gap":"6px"}, children=[
        html.Div(style={"flex":"1"}, children=[
            dcc.Dropdown(id="search-com", options=[], placeholder="Nom ou code...",
                         className="dark-dropdown", search_value="")]),
        html.Button(
            id="btn-search", className="btn-pill-x",
            n_clicks=0, title="Rechercher",
            children=[
                html.Div(className="pill-ico pill-ico-search"),
                html.Span("Rechercher", className="pill-txt"),
            ]),
    ]),
    html.Div(id="search-result", style={"marginTop":"6px"}),

    # Slider
    html.Div("Navigation temporelle", className="section-label", style={"marginTop":"20px"}),
    html.Div(id="date-disp", className="date-display"),
    html.Div(className="slider-container", children=[
        dcc.Slider(id="slider", min=0, max=max(0,len(first.dates_cles)-1),
                   value=0, step=1, marks=None, updatemode="drag")]),
    html.Div(className="slider-controls", children=[
        html.Button("▶ Play", id="btn-play", className="btn-play", n_clicks=0),
        html.Div(id="date-range", className="date-range")]),

    # Sparkline
    dcc.Graph(id="sparkline", figure=make_sparkline(first_id),
              config={"displayModeBar": False}, className="sparkline"),

    html.Div("Statistiques", className="section-label", style={"marginTop":"16px"}),
    html.Div(id="stats"),
    html.Div("Legende", className="section-label", style={"marginTop":"20px"}),
    html.Div(id="legend"),
    html.Div(id="regnote")])

# ── CALCULATEUR ──────────────────────────────────────────────────────────────
calc = html.Div(id="calc-panel", style={"display":"none"}, children=[
    html.Div("Foyer(s)", className="section-label"),
    dcc.Dropdown(id="calc-com", options=[], multi=True,
                 placeholder="Tapez au moins 2 caracteres...",
                 className="dark-dropdown", search_value=""),
    html.Div("Recherche par nom ou code INSEE",
             style={"fontSize":"11px","color":"var(--text-muted)","marginTop":"4px"}),

    html.Div("Rayon", className="section-label", style={"marginTop":"18px"}),
    html.Div(style={"display":"flex","alignItems":"center","gap":"10px"}, children=[
        html.Div(style={"flex":"1"}, children=[
            dcc.Slider(id="calc-r", min=1, max=500, value=150, step=1,
                       marks={1:"1",100:"100",200:"200",350:"350",500:"500"},
                       included=True)]),
        dcc.Input(id="calc-r-input", type="number", value=150, min=1, max=500, step=1,
                  className="radius-input"),
        html.Span("km", style={"color":"var(--text-secondary)","fontSize":"13px","fontWeight":"500"}),
    ]),

    html.Div("Methode", className="section-label", style={"marginTop":"14px"}),
    dcc.RadioItems(id="calc-meth",
        options=[{"label":" Centroide (rapide)","value":"centroide"},
                 {"label":" Polygone (precis)","value":"polygone"}],
        value="centroide", className="radio-method"),

    html.Div(style={"display":"flex","gap":"8px","marginTop":"18px"}, children=[
        html.Button("Calculer", id="btn-calc", n_clicks=0,
                    className="btn-action btn-orange", style={"flex":"1"}),
        html.Button("Reinitialiser", id="btn-reset-calc", n_clicks=0,
                    className="btn-action btn-outline", style={"flex":"1"}),
    ]),
    html.Hr(style={"borderColor":"var(--border)","margin":"18px 0"}),
    html.Div(id="calc-stats"), html.Div(id="calc-msgs"),
    dcc.Download(id="calc-dl"),
    html.Button("Exporter en Excel", id="btn-export", n_clicks=0,
                className="btn-action btn-outline",
                style={"width":"100%","marginTop":"8px","display":"none"})])

# ── TÉLÉCHARGEMENT ───────────────────────────────────────────────────────────
dl_panel = html.Div(id="dl-panel", style={"display":"none"}, children=[
    html.Div(className="notice notice-blue", style={"marginBottom":"12px"}, children=[
        html.Div("Données de zonage nettoyées", style={"fontWeight":"600","color":"#e8e6e1",
                                                       "marginBottom":"4px","fontSize":"12px"}),
        html.Div("Téléchargez les données de la maladie sélectionnée au format Excel ou Parquet.",
                 style={"fontSize":"12px","color":"#9a9890"}),
    ]),
    html.Div(id="dl-disease-label", style={"fontSize":"13px","fontWeight":"600",
                                            "marginBottom":"12px","color":"var(--accent)"}),
    html.Div(style={"display":"flex","flexDirection":"column","gap":"8px"}, children=[
        html.Button("⬇ Télécharger Excel (.xlsx)", id="btn-dl-xlsx",
                    className="btn-action", n_clicks=0,
                    style={"width":"100%","textAlign":"left","padding":"10px 14px"}),
        html.Button("⬇ Télécharger Parquet (.parquet)", id="btn-dl-parquet",
                    className="btn-action", n_clicks=0,
                    style={"width":"100%","textAlign":"left","padding":"10px 14px",
                           "background":"var(--bg-secondary)"}),
    ]),
    html.Div(id="dl-msg", style={"marginTop":"10px","fontSize":"12px","color":"#9a9890"}),
    dcc.Download(id="dl-file"),
])

# ── IMPORT ───────────────────────────────────────────────────────────────────
import_panel = html.Div(id="import-panel", style={"display":"none"}, children=[
    html.Div(className="notice notice-blue", children=[
        html.Div("Format attendu", style={"fontWeight":"600","color":"#e8e6e1",
                                            "marginBottom":"6px","fontSize":"12px"}),
        html.Div("Fichier .xlsx avec un ou plusieurs onglets (un par zone)."),
        html.Div(style={"marginTop":"6px"}, children=[
            html.Span("Obligatoire : ", style={"fontWeight":"500","color":"#9a9890"}),
            html.Span("Code INSEE + Date de debut")]),
        html.Div(style={"marginTop":"4px"}, children=[
            html.Span("Recommande : ", style={"fontWeight":"500","color":"#9a9890"}),
            html.Span("Commune, Date de fin, Departement")])]),
    html.Div(id="import-step1", style={"marginTop":"16px"}, children=[
        dcc.Upload(id="import-upload",
            children=html.Div([
                html.Div("Glisser-deposer ou cliquer", style={"fontWeight":"600","fontSize":"13px"}),
                html.Div("Fichier .xlsx", style={"fontSize":"11px","color":"#636058","marginTop":"4px"})]),
            style={"width":"100%","borderWidth":"2px","borderStyle":"dashed",
                   "borderColor":"#2a3040","borderRadius":"8px","textAlign":"center",
                   "padding":"24px 16px","cursor":"pointer","background":"#0c0f16",
                   "color":"#e8e6e1"}, multiple=False, accept=".xlsx,.xls"),
        html.Div(id="import-file-info", style={"fontSize":"12px","color":"#9a9890","marginTop":"8px"})]),
    html.Div(id="import-step2", style={"display":"none"}, children=[
        html.Div("Identifiant court", className="section-label"),
        dcc.Input(id="import-id", type="text", placeholder="ex: iahp", className="dark-input"),
        html.Div("Nom complet", className="section-label", style={"marginTop":"12px"}),
        dcc.Input(id="import-name", type="text", placeholder="ex: Influenza Aviaire HP", className="dark-input"),
        html.Div("Couleur", className="section-label", style={"marginTop":"12px"}),
        dcc.Input(id="import-color", type="text", value="#E65100", className="dark-input"),
        html.Hr(style={"borderColor":"#2a3040","margin":"16px 0"}),
        html.Div(id="import-sheets"),
        html.Button("Generer la configuration", id="btn-import-gen", n_clicks=0,
                    className="btn-action btn-blue", style={"width":"100%","marginTop":"16px"})]),
    html.Div(id="import-step3", style={"display":"none"}, children=[html.Div(id="import-result")]),
    dcc.Store(id="import-analysis", data=None), dcc.Store(id="import-fpath", data=None)])

# ── SIDEBAR ──────────────────────────────────────────────────────────────────
sidebar = html.Div(id="sidebar", className="sidebar", children=[
    html.Div(className="sidebar-header", children=[
        html.Div(style={"display":"flex","justifyContent":"space-between","alignItems":"center"}, children=[
            html.Div(className="app-title-row", children=[
                html.Div("Epi", style={"fontWeight":"700","color":"var(--accent)"}),
                html.Div("Zone", style={"fontWeight":"400","color":"var(--text-secondary)"}),
            ]),
            html.Div(style={"display":"flex","gap":"4px"}, children=[
                html.Button(
                    id="btn-theme",
                    className="btn-pill-x",
                    title="Changer de thème",
                    n_clicks=0,
                    children=[
                        html.Div(className="pill-ico pill-ico-theme"),
                        html.Span("Mode clair",   className="pill-txt pill-txt-dark"),
                        html.Span("Mode sombre",  className="pill-txt pill-txt-light"),
                    ]
                ),
            ])]),
        html.Div(style={"display":"flex","gap":"4px","marginTop":"12px"}, children=[
            html.Button("Visualisation", id="bm-v", className="disease-pill active",
                style={"flex":"1","borderColor":"#6a6af4","color":"#6a6af4"}, n_clicks=0),
            html.Button("Calculateur", id="bm-c", className="disease-pill", style=INB, n_clicks=0),
            html.Button("Import", id="bm-i", className="disease-pill", style=INB, n_clicks=0)])]),
    html.Div(className="sidebar-body", children=[visu, calc, import_panel])])

# ── MAP ──────────────────────────────────────────────────────────────────────
sty_z = assign("""function(f){var p=f.properties;return{
    fillColor:p.color,color:p.color,weight:0.7,opacity:0.8,fillOpacity:0.5};}""")
oef_z = assign("""function(f,l){var p=f.properties;
    l.bindTooltip('<b>'+p.commune+'</b> - '+p.zone,{sticky:true,className:'tip'});
    l.on('mouseover',function(){this.setStyle({weight:2.5,color:'#fff',fillOpacity:0.72});this.bringToFront();});
    l.on('mouseout',function(){this.setStyle({weight:0.7,color:p.color,fillOpacity:0.5});});
    l.on('click',function(){L.popup({className:'dark-popup'}).setLatLng(l.getBounds().getCenter())
        .setContent('<b>'+p.commune+'</b><br>INSEE : '+p.code_insee+'<br>Dept : '
        +(p.departement||'')+'<br>Zone : <span style=\"color:'+p.color+';font-weight:700\">'+p.zone+'</span>')
        .openOn(l._map);});}""")
ptl_c = assign("""function(f,ll){var p=f.properties;
    if(p.type==='foyer')return L.circleMarker(ll,{radius:8,fillColor:'#f85149',color:'#f85149',fillOpacity:0.9,weight:2});
    return L.circleMarker(ll,{radius:2,color:'#E65100',fillColor:'#FF6D00',fillOpacity:0.5,stroke:false});}""")
sty_cp = assign("""function(f){return{fillColor:'#E65100',color:'#E65100',weight:0.7,opacity:0.8,fillOpacity:0.5};}""")
oef_cp = assign("""function(f,l){var p=f.properties;
    l.bindTooltip(p.commune,{sticky:true});
    l.on('mouseover',function(){if(this.setStyle){this.setStyle({weight:2.5,color:'#fff',fillOpacity:0.72});this.bringToFront();}});
    l.on('mouseout',function(){if(this.setStyle)this.setStyle({weight:0.7,color:'#E65100',fillOpacity:0.5});});}""")
oef_buf = assign("""function(f,l){var p=f.properties;
    if(p.type==='foyer')l.bindPopup('<b>FOYER</b><br>'+p.commune);}""")

# ── onEachFeature régions : couche séparée pour les labels (divIcon markers) ─
# Les labels sont positionnés via REGIONS_LABELS_GJ (points sur les centroïdes)
# plutôt que bindTooltip, pour garantir le positionnement exact.
_ptl_region_labels = assign("""function(f, ll) {
    return L.marker(ll, {
        icon: L.divIcon({
            className: 'region-label-marker',
            html: '<div class=\"region-label\">' + f.properties.label + '</div>',
            iconSize: [0, 0],
            iconAnchor: [0, 0]
        }),
        interactive: false,
        keyboard: false,
        zIndexOffset: -2000
    });
}""")

app = dash.Dash(__name__, suppress_callback_exceptions=True, title="EpiZone", update_title=None,
                meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1, maximum-scale=1"}])
server = app.server

app.layout = html.Div(id="app-container", children=[
    dcc.Store(id="cur-d", data=first_id), dcc.Store(id="cur-m", data="visu"),
    dcc.Store(id="calc-store", data=None),
    dcc.Store(id="play-active", data=False),
    html.Button(id="btn-advance", style={"display":"none"}, n_clicks=0),
    dcc.Store(id="theme-store", data="dark"),
    dcc.Store(id="dates-store", data={
        "dates": [d.strftime("%d/%m/%Y") for d in first.dates_cles],
        "color": first.config.accent_color,
    }),

    # ── Easter egg stores & interval ──────────────────────────────────
    dcc.Store(id="game-state", data=None),
    dcc.Store(id="game-lb", data=load_leaderboard()),
    dcc.Interval(id="game-tick", interval=2500, n_intervals=0, disabled=True),

    # ── Overlay easter egg ────────────────────────────────────────────
    html.Div(id="easter-overlay", style={"display": "none"}, children=[
        # Header
        html.Div(className="easter-header", children=[
            html.Div(className="easter-pathogen", children=[
                html.Span("🦠", style={"fontSize": "20px"}),
                html.Span(" CORVEX-Ω", style={"fontWeight": "700", "color": "#ff4444", "letterSpacing": "2px"}),
            ]),
            html.Div(id="game-timer", className="easter-timer", children="00:00"),
            html.Div(id="game-score-display", className="easter-score", children="Score : 0"),
            html.Button("✕ Quitter", id="btn-quit-game", className="easter-quit-btn", n_clicks=0),
        ]),

        # Corps du jeu
        html.Div(className="easter-body", children=[

            # ── Panneau gauche ────────────────────────────────────────
            html.Div(className="easter-panel", children=[

                # Info source
                html.Div(id="game-source-info", className="easter-source-info"),

                # Barres de recherche
                html.Div(className="easter-section-title", children="🔬 Recherche vaccinale"),
                html.Div(id="game-research-bars", className="easter-research-bars"),

                # Bouton vacciner
                html.Div(className="easter-vaccine-zone", children=[
                    html.Div(id="game-vaccine-count", className="easter-vaccine-count",
                             children="💉 0 dose disponible"),
                    html.Div(className="easter-vaccine-hint",
                             children="Cliquez sur un département sain pour vacciner"),
                ]),

                # Stats
                html.Div(id="game-stats", className="easter-stats"),

                # Log événements
                html.Div(className="easter-section-title", children="📋 Événements"),
                html.Div(id="game-events-log", className="easter-events-log"),
            ]),

            # ── Carte ─────────────────────────────────────────────────
            html.Div(className="easter-map-zone", children=[
                dl.Map(
                    id="game-map",
                    center=[46.5, 2.5], zoom=5,
                    style={"width": "100%", "height": "100%"},
                    children=[
                        dl.TileLayer(
                            url="https://{s}.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}{r}.png",
                            attribution="CARTO",
                        ),
                        dl.GeoJSON(
                            id="game-geojson",
                            data=None,       # envoyé via callback uniquement au déclenchement
                            hideout={"infected": [], "vaccinated": [], "resistant": [],
                                     "partial_resistant": [], "at_risk": [],
                                     "blocked": [], "source_dept": ""},
                            options=dict(
                                style=assign("""
                                function(f, context) {
                                    var code = f.properties.dept_code;
                                    var ho = context.hideout;
                                    if (code === ho.source_dept)
                                        return {fillColor:'#ff0000', fillOpacity:0.9,
                                                color:'#ff6666', weight:2};
                                    if (ho.vaccinated.indexOf(code) !== -1)
                                        return {fillColor:'#1565C0', fillOpacity:0.85,
                                                color:'#90CAF9', weight:1.5};
                                    if (ho.infected.indexOf(code) !== -1)
                                        return {fillColor:'#B71C1C', fillOpacity:0.85,
                                                color:'#EF9A9A', weight:1};
                                    if (ho.at_risk.indexOf(code) !== -1)
                                        return {fillColor:'#E65100', fillOpacity:0.5,
                                                color:'#FFCC80', weight:1.5,
                                                dashArray:'4 3'};
                                    if (ho.blocked.indexOf(code) !== -1)
                                        return {fillColor:'#F57F17', fillOpacity:0.6,
                                                color:'#FFF176', weight:2,
                                                dashArray:'6 3'};
                                    if (ho.resistant.indexOf(code) !== -1)
                                        return {fillColor:'#1B5E20', fillOpacity:0.4,
                                                color:'#A5D6A7', weight:1};
                                    if (ho.partial_resistant.indexOf(code) !== -1)
                                        return {fillColor:'#2E7D32', fillOpacity:0.2,
                                                color:'#555', weight:0.8};
                                    return {fillColor:'#424242', fillOpacity:0.55,
                                            color:'#666', weight:0.8};
                                }
                                """),
                                onEachFeature=assign("""
                                function(f, layer) {
                                    layer.bindTooltip(
                                        f.properties.dept_nom + ' (' + f.properties.dept_code + ')',
                                        {sticky: true, className: 'game-tooltip'}
                                    );
                                }
                                """),
                            ),
                        ),
                    ],
                ),

                # Légende
                html.Div(className="easter-legend", children=[
                    html.Div([html.Span(className="legend-dot ld-infected"), " Infecté"]),
                    html.Div([html.Span(className="legend-dot ld-vaccinated"), " Vacciné"]),
                    html.Div([html.Span(className="legend-dot ld-resistant"), " Résistant"]),
                    html.Div([html.Span(className="legend-dot ld-atrisk"), " À risque"]),
                    html.Div([html.Span(className="legend-dot ld-blocked"), " Confiné"]),
                ]),
            ]),
        ]),

        # ── Flash event banner (événement aléatoire, s'efface après 3 ticks) ──
        html.Div(id="game-flash-banner", style={"display": "none"},
                 className="easter-flash-banner"),

        # ── Écran de fin (au niveau overlay, au-dessus de Leaflet) ───────────
        html.Div(id="game-result-screen", style={"display": "none"}, className="easter-result-screen", children=[
            html.Div(className="easter-result-box", children=[
                html.Div(id="game-result-icon", style={"fontSize": "60px", "textAlign": "center"}),
                html.Div(id="game-result-title", className="easter-result-title"),
                html.Div(id="game-result-msg", className="easter-result-msg"),
                html.Div(id="game-result-score", className="easter-result-score"),
                html.Div(className="easter-name-input-zone", children=[
                    dcc.Input(id="game-player-name", type="text",
                              placeholder="Votre nom (leaderboard)...",
                              maxLength=20, className="easter-name-input"),
                    html.Button("Enregistrer", id="btn-save-score",
                                className="easter-save-btn", n_clicks=0),
                ]),
                html.Div(id="game-leaderboard-display", className="easter-leaderboard"),
                html.Button("✕ Fermer le jeu", id="btn-quit-from-result",
                            className="easter-quit-result-btn", n_clicks=0),
            ]),
        ]),
    ]),

    # Hidden div for keyboard events
    html.Div(id="kb-listener", tabIndex="0",
             style={"position":"fixed","opacity":"0","width":"0","height":"0"}),

    sidebar,

    html.Div(id="map-area", className="map-area", children=[
        # Mobile hamburger
        html.Button("☰", id="btn-mobile-menu", className="btn-mobile-menu", n_clicks=0),
        html.Div(id="mobile-overlay", className="mobile-overlay", n_clicks=0),

        # ── Bouton d'extraction flottant (haut droite) ────────────────────────
        html.Div(id="map-dl-widget", className="map-dl-widget", children=[
            html.Button("⬇ Extraction", id="btn-dl-toggle", className="map-dl-btn", n_clicks=0),
            html.Div(id="dl-dropdown", className="map-dl-dropdown", style={"display":"none"}, children=[
                html.Div(id="dl-disease-label", className="map-dl-title"),
                html.Button("Excel (.xlsx)", id="btn-dl-xlsx", className="map-dl-option", n_clicks=0),
                html.Button("Parquet (.parquet)", id="btn-dl-parquet", className="map-dl-option", n_clicks=0),
                html.Div(id="dl-msg", className="map-dl-msg"),
            ]),
        ]),
        dcc.Download(id="dl-file"),

        dl.Map(id="map", center=first.config.map.center, zoom=first.config.map.zoom,
            children=[
                dl.TileLayer(id="tile-layer", url=TILE_DARK, attribution="CARTO"),
                # Couche labels régions : marqueurs divIcon positionnés sur les centroïdes
                dl.GeoJSON(id="regions-labels-layer", data=REGIONS_LABELS_GJ,
                           options=dict(pointToLayer=_ptl_region_labels)),
                dl.GeoJSON(id="gl", data=EMPTY, options=dict(style=sty_z, onEachFeature=oef_z)),
                dl.GeoJSON(id="cl-buf", data=EMPTY,
                           options=dict(pointToLayer=ptl_c, onEachFeature=oef_buf)),
                dl.GeoJSON(id="cl-pts", data=EMPTY,
                           options=dict(pointToLayer=ptl_c, style=sty_cp, onEachFeature=oef_cp)),
                dl.GeoJSON(id="search-marker", data=EMPTY,
                           options=dict(pointToLayer=assign(
                               """function(f,ll){return L.circleMarker(ll,{
                                   radius:10,fillColor:'#ffffff',color:'#6a6af4',
                                   fillOpacity:0.9,weight:3});}"""),
                               onEachFeature=assign(
                               """function(f,l){l.bindTooltip('<b>'+f.properties.commune+'</b>',
                                   {permanent:true,direction:'top',className:'tip',offset:[0,-10]});}""")))],
            style={"width":"100%","height":"100vh"}),
        html.Div(id="no-geo"),

        html.Div(id="kb-help", className="kb-help", children=[
            html.Span("← → "), html.Span("naviguer", style={"color":"#636058"}),
            html.Span("  Espace ", style={"marginLeft":"10px"}),
            html.Span("play/pause", style={"color":"#636058"}),
        ]),
    ])])


# =============================================================================
# 4. CLIENTSIDE CALLBACKS (JS)
# =============================================================================

# Keyboard shortcuts
app.clientside_callback(
    """
    function(id) {
        if (window._kb_bound) return dash_clientside.no_update;
        window._kb_bound = true;
        document.addEventListener('keydown', function(e) {
            // Don't capture when typing in inputs
            if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;
            if (e.target.className && e.target.className.includes('Select')) return;

            if (e.key === 'ArrowRight') {
                var btn = document.querySelector('#slider .rc-slider-handle');
                if (btn) { var ev = new KeyboardEvent('keydown',{key:'ArrowRight',bubbles:true}); btn.dispatchEvent(ev); }
            } else if (e.key === 'ArrowLeft') {
                var btn = document.querySelector('#slider .rc-slider-handle');
                if (btn) { var ev = new KeyboardEvent('keydown',{key:'ArrowLeft',bubbles:true}); btn.dispatchEvent(ev); }
            } else if (e.key === ' ') {
                e.preventDefault();
                var playBtn = document.getElementById('btn-play');
                if (playBtn) playBtn.click();
            }
        });

        // Translate Dash dropdown texts to French
        new MutationObserver(function() {
            document.querySelectorAll('.dash-dropdown-search').forEach(function(el) {
                if (el.placeholder === 'Search') el.placeholder = 'Rechercher...';
            });
            document.querySelectorAll('.dash-dropdown-action-button').forEach(function(el) {
                if (el.textContent === 'Select All') el.textContent = 'Tout selectionner';
                if (el.textContent === 'Deselect All') el.textContent = 'Tout deselectionner';
            });
        }).observe(document.body, {childList: true, subtree: true});

        // Adapte la taille des labels de régions au niveau de zoom
        // Adapte la taille des labels régions au zoom (CSS variable uniquement)
        // Le switch de tuile est géré par un callback Dash sur Input("map","zoom")
        function _applyLabelSize(zoom) {
            var s = zoom <= 4 ? '8px'
                  : zoom <= 5 ? '9px'
                  : zoom <= 6 ? '10px'
                  : zoom <= 7 ? '11px'
                  : '12px';
            document.documentElement.style.setProperty('--region-label-size', s);
        }
        function _attachZoomListener() {
            var el = document.querySelector('.leaflet-container');
            if (!el) { setTimeout(_attachZoomListener, 800); return; }
            // Leaflet 1.x stocke la map sur _leaflet_map
            var lmap = el._leaflet_map;
            if (!lmap) { setTimeout(_attachZoomListener, 800); return; }
            lmap.on('zoomend', function() { _applyLabelSize(lmap.getZoom()); });
            _applyLabelSize(lmap.getZoom());
        }
        setTimeout(_attachZoomListener, 1500);

        return dash_clientside.no_update;
    }
    """,
    Output("kb-listener", "style"),
    Input("kb-listener", "id"),
)


# Instant date display (clientside = no server roundtrip, updates during drag)
app.clientside_callback(
    """
    function(sliderValue, datesData) {
        if (!datesData || !datesData.dates || datesData.dates.length === 0)
            return [dash_clientside.no_update, dash_clientside.no_update];
        var idx = Math.max(0, Math.min(sliderValue || 0, datesData.dates.length - 1));
        return [datesData.dates[idx], {color: datesData.color}];
    }
    """,
    [Output("date-disp", "children"), Output("date-disp", "style")],
    [Input("slider", "value"), Input("dates-store", "data")],
)

# Tile switch piloté par le zoom Dash natif de dl.Map
app.clientside_callback(
    """
    function(zoom, theme) {
        if (zoom === undefined || zoom === null) return dash_clientside.no_update;
        var withLabels = (zoom >= 8);
        var dark = (theme !== 'light');
        if (dark)
            return withLabels
                ? 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png'
                : 'https://{s}.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}{r}.png';
        return withLabels
            ? 'https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png'
            : 'https://{s}.basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}{r}.png';
    }
    """,
    Output("tile-layer", "url", allow_duplicate=True),
    Input("map", "zoom"),
    State("theme-store", "data"),
    prevent_initial_call=True,
)

# Theme toggle (light/dark + tile swap, respecte le zoom courant)
app.clientside_callback(
    """
    function(n, currentTheme) {
        if (!n) return [dash_clientside.no_update, dash_clientside.no_update, dash_clientside.no_update];
        var newTheme = (currentTheme === 'dark') ? 'light' : 'dark';
        document.documentElement.setAttribute('data-theme', newTheme);
        document.body.setAttribute('data-theme', newTheme);
        // Met à jour la classe du bouton toggle pour l'animation CSS
        var btn = document.getElementById('btn-theme');
        if (btn) {
            btn.setAttribute('data-theme', newTheme);
        }
        var zoom = 6;
        var el = document.querySelector('.leaflet-container');
        if (el) {
            for (var k in el) {
                if (el[k] && typeof el[k].getZoom === 'function') {
                    zoom = el[k].getZoom(); break;
                }
            }
        }
        var withLabels = (zoom >= 8);
        var tileUrl = (newTheme === 'dark')
            ? (withLabels
                ? 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png'
                : 'https://{s}.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}{r}.png')
            : (withLabels
                ? 'https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png'
                : 'https://{s}.basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}{r}.png');
        return [newTheme, tileUrl];
    }
    """,
    [Output("theme-store", "data"),
     Output("tile-layer", "url")],
    Input("btn-theme", "n_clicks"),
    State("theme-store", "data"),
    prevent_initial_call=True,
)

# Mobile sidebar toggle
app.clientside_callback(
    """function(n) { if(n) window.location.reload(); return dash_clientside.no_update; }""",
    Output("kb-listener", "title"),
    Input("btn-reload-page", "n_clicks"),
    prevent_initial_call=True,
)

app.clientside_callback(
    """
    function(nMenu, nOverlay) {
        var sidebar = document.getElementById('sidebar');
        var overlay = document.getElementById('mobile-overlay');
        if (!sidebar) return dash_clientside.no_update;
        var isOpen = sidebar.classList.contains('sidebar-open');
        if (isOpen) {
            sidebar.classList.remove('sidebar-open');
            if (overlay) overlay.classList.remove('overlay-visible');
        } else {
            sidebar.classList.add('sidebar-open');
            if (overlay) overlay.classList.add('overlay-visible');
        }
        return dash_clientside.no_update;
    }
    """,
    Output("btn-mobile-menu", "className"),
    [Input("btn-mobile-menu", "n_clicks"), Input("mobile-overlay", "n_clicks")],
    prevent_initial_call=True,
)

@callback(
    [Output("cur-m","data"), Output("visu-panel","style"), Output("calc-panel","style"),
     Output("import-panel","style"),
     Output("bm-v","className"), Output("bm-c","className"), Output("bm-i","className"),
     Output("bm-v","style"), Output("bm-c","style"), Output("bm-i","style"),
     Output("gl","data",allow_duplicate=True),
     Output("cl-buf","data",allow_duplicate=True), Output("cl-pts","data",allow_duplicate=True),
     Output("map-dl-widget","style")],
    [Input("bm-v","n_clicks"), Input("bm-c","n_clicks"), Input("bm-i","n_clicks")],
    State("cur-m","data"), prevent_initial_call=True)
def toggle(n1,n2,n3,cm):
    b = dash.callback_context.triggered[0]["prop_id"].split(".")[0]
    h={"display":"none"}; a="disease-pill active"; i="disease-pill"
    show_dl={}; hide_dl={"display":"none"}
    if b=="bm-v": return("visu",{},h,h, a,i,i,
        {"flex":"1","borderColor":"#6a6af4","color":"#6a6af4"},INB,INB,
        no_update,EMPTY,EMPTY, show_dl)
    if b=="bm-c": return("calc",h,{},h, i,a,i,
        INB,{"flex":"1","borderColor":"#E65100","color":"#E65100"},INB,
        EMPTY,EMPTY,EMPTY, hide_dl)
    return("import",h,h,{}, i,i,a,
        INB,INB,{"flex":"1","borderColor":"#1976D2","color":"#1976D2"},
        EMPTY,EMPTY,EMPTY, hide_dl)



# =============================================================================
# 6. VISUALISATION
# =============================================================================

@callback(Output("cur-d","data"),
    [Input({"type":"dp","index":did},"n_clicks") for did in DISEASES], prevent_initial_call=True)
def sel(*_):
    ctx=dash.callback_context
    if not ctx.triggered: return no_update
    try: return json.loads(ctx.triggered[0]["prop_id"].split(".")[0])["index"]
    except: return no_update

@callback([Output("slider","min"),Output("slider","max"),Output("slider","value"),
           Output("date-range","children"),Output("legend","children"),
           Output("regnote","children"),Output("map","center"),Output("map","zoom"),
           Output("sparkline","figure"),Output("dates-store","data"),
           Output("search-marker","data",allow_duplicate=True),
           Output("search-result","children",allow_duplicate=True),
           Output("search-com","value")],
    Input("cur-d","data"),
    prevent_initial_call='initial_duplicate')
def upd_d(did):
    if did not in DISEASES: return[no_update]*13
    d=DISEASES[did]; c=d.config
    dr=f"{d.date_min.strftime('%d/%m/%Y')} → {d.date_max.strftime('%d/%m/%Y')}" if d.dates_cles else ""
    reg=None
    parts=[c.regulatory.get("arrete",""),c.regulatory.get("note","")]
    parts=[p for p in parts if p]
    if parts: reg=html.Div(className="notice notice-gray",children=[html.Div(p) for p in parts])
    dates_data = {
        "dates": [dt.strftime("%d/%m/%Y") for dt in d.dates_cles],
        "color": c.accent_color,
    }
    return (0, max(0,len(d.dates_cles)-1), 0, dr, make_legend(c), reg,
            c.map.center, c.map.zoom, make_sparkline(did), dates_data,
            EMPTY, None, None)

@callback([Output("gl","data"),Output("stats","children"),Output("no-geo","children")],
    [Input("slider","value"),Input("cur-d","data")])
def upd_map(sv,did):
    if did not in DISEASES: return[no_update]*3
    d=DISEASES[did]; c=d.config
    if not d.dates_cles: return EMPTY,None,None
    idx=max(0,min(sv or 0,len(d.dates_cles)-1))
    dt=d.dates_cles[idx]; ds=str(dt.date())
    gj=d.geojson_at(ds) or EMPTY
    ng=None
    if not d.has_geometries:
        ng=html.Div(className="no-geo-banner",children=[
            html.H3("Geometries non chargees"),
            html.P("Supprimez cache/ et relancez avec internet.")])
    s=d.snapshot_at(ds); st=[]
    if not s.empty:
        for z in c.zones:
            n=(s["zone"]==z.id).sum()
            if n>0 or len(c.zones)<=4: st.append(stat_card(n,f"Communes en {z.id}",z.color))
        st.append(html.Div(className="stat-row",children=[
            stat_card(len(s),"Total communes"),
            stat_card(s["departement"].nunique(),"Departements")]))
    return gj,st,ng

# ── Recherche commune ────────────────────────────────────────────────────────

@callback(Output("search-com","options"),
    Input("search-com","search_value"),
    State("search-com","value"))
def search_opts(s, selected):
    opts = []
    if selected:
        row = COMMUNES_REF[COMMUNES_REF["code_insee"]==selected]
        if not row.empty:
            r = row.iloc[0]
            opts.append({"label": f"{r.nom} ({r.dep_code})", "value": r.code_insee})
    if not s or len(s)<2: return opts
    q = s.lower().strip()
    already = {o["value"] for o in opts}

    # SEARCH_IDX is pre-sorted by _rank (short name + small suffix = important commune)
    # 1. Exact name match
    exact = SEARCH_IDX[SEARCH_IDX["_nom_low"] == q]
    # 2. Starts with
    starts = SEARCH_IDX[SEARCH_IDX["_nom_low"].str.startswith(q)
                         & ~SEARCH_IDX.index.isin(exact.index)]
    # 3. Contains anywhere in name or code
    contains = SEARCH_IDX[(SEARCH_IDX["_nom_low"].str.contains(q, na=False)
                           | SEARCH_IDX["code_insee"].str.contains(q, na=False))
                           & ~SEARCH_IDX.index.isin(exact.index)
                           & ~SEARCH_IDX.index.isin(starts.index)]

    for df in [exact, starts, contains]:
        for _,r in df.head(15 - len(opts)).iterrows():
            if r.code_insee not in already:
                opts.append({"label": f"{r.nom} ({r.dep_code})", "value": r.code_insee})
                already.add(r.code_insee)
            if len(opts) >= 15: break
        if len(opts) >= 15: break
    return opts

@callback(
    [Output("search-result","children"),
     Output("search-marker","data")],
    Input("btn-search","n_clicks"),
    [State("search-com","value"), State("cur-d","data")],
    prevent_initial_call=True)
def search_commune(n_clicks, code, did):
    if not code: return None, EMPTY
    row = COMMUNES_REF[COMMUNES_REF["code_insee"]==code]
    if row.empty: return html.Div("Commune introuvable", style={"color":"#d29922","fontSize":"12px"}), EMPTY
    r = row.iloc[0]
    lat = float(r["lat"])
    lon = float(r["lon"])
    print(f"  Recherche : {r['nom']} ({code}) → lat={lat}, lon={lon}")

    # Historique dans la maladie courante
    info_parts = [html.Div(f"{r['nom']} ({r['dep_code']})",
                            style={"fontWeight":"600","color":"#e8e6e1","marginBottom":"4px"})]
    d = DISEASES.get(did)
    if d:
        per = d.periodes[d.periodes["code_insee"]==code]
        if per.empty:
            info_parts.append(html.Div("Jamais en zone pour cette maladie",
                              style={"color":"#636058","fontStyle":"italic"}))
        else:
            for _,p in per.iterrows():
                fin = pd.Timestamp(p["date_fin"]).strftime("%d/%m/%Y") if pd.notna(p["date_fin"]) else "en cours"
                deb = pd.Timestamp(p["date_debut"]).strftime("%d/%m/%Y")
                z = p["zone"]
                color = d.config.zone_colors.get(z, "#888")
                info_parts.append(html.Div(style={"display":"flex","gap":"6px","alignItems":"center","padding":"2px 0"}, children=[
                    html.Div(style={"width":"8px","height":"8px","borderRadius":"2px","background":color,"flexShrink":"0"}),
                    html.Span(f"{z} : {deb} → {fin}", style={"fontSize":"11px","color":"#9a9890"})]))

    # Marqueur sur la commune
    marker = {
        "type": "FeatureCollection",
        "features": [{
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {"commune": r["nom"], "code_insee": code,
                           "lat": lat, "lon": lon},
        }],
    }

    return html.Div(style={"fontSize":"12px"}, children=info_parts), marker

# Clientside zoom to search marker (no server roundtrip, no map re-render)
app.clientside_callback(
    """
    function(markerData) {
        if (!markerData || !markerData.features || markerData.features.length === 0)
            return dash_clientside.no_update;
        var f = markerData.features[0];
        var lat = f.properties.lat;
        var lon = f.properties.lon;
        // Find the leaflet map and fly to
        setTimeout(function() {
            var el = document.querySelector('.leaflet-container');
            if (el && el._leaflet_id) {
                // Enumerate properties to find the map object
                for (var k in el) {
                    if (el[k] && el[k]._zoom !== undefined && el[k].flyTo) {
                        el[k].flyTo([lat, lon], 12, {duration: 0.8});
                        break;
                    }
                }
            }
        }, 200);
        return dash_clientside.no_update;
    }
    """,
    Output("kb-listener", "n_clicks"),
    Input("search-marker", "data"),
    prevent_initial_call=True,
)

# ── Pills + play ─────────────────────────────────────────────────────────────

for did in DISEASES:
    @callback(Output({"type":"dp","index":did},"className"),Input("cur-d","data"))
    def _c(c,p=did): return "disease-pill active" if c==p else "disease-pill"
    @callback(Output({"type":"dp","index":did},"style"),Input("cur-d","data"))
    def _s(c,p=did):
        if c==p: return{"--accent":DISEASES[p].config.accent_color,
                        "borderColor":DISEASES[p].config.accent_color,
                        "color":DISEASES[p].config.accent_color}
        return{}

@callback([Output("play-active","data"),Output("btn-play","children")],
    Input("btn-play","n_clicks"),State("play-active","data"),prevent_initial_call=True)
def play(n,active): return not active,"⏸ Pause" if not active else "▶ Play"

# Hidden button for advancing the slider (triggered by clientside)
@callback(Output("slider","value",allow_duplicate=True),
    Input("btn-advance","n_clicks"),
    [State("slider","value"),State("slider","max")],
    prevent_initial_call=True)
def advance_slider(n, v, mx):
    return 0 if (v or 0) + 1 > mx else (v or 0) + 1

# Adaptive play: after gl.data renders, wait 300ms then click advance
app.clientside_callback(
    """
    function(glData, isPlaying) {
        if (window._playTimer) { clearTimeout(window._playTimer); window._playTimer = null; }
        if (!isPlaying) return dash_clientside.no_update;
        window._playTimer = setTimeout(function() {
            var btn = document.getElementById('btn-advance');
            if (btn) btn.click();
        }, 300);
        return dash_clientside.no_update;
    }
    """,
    Output("kb-listener", "accessKey"),
    [Input("gl", "data"), Input("play-active", "data")],
    prevent_initial_call=True,
)

@callback(Output("app-container","style"),Input("cur-d","data"))
def acc(d): return{"--accent":DISEASES[d].config.accent_color} if d in DISEASES else{}


# =============================================================================
# 7. CALCULATEUR
# =============================================================================

@callback(Output("calc-com","options"),
    Input("calc-com","search_value"),State("calc-com","value"))
def filt(s,sel):
    opts=[]
    if sel:
        for c in sel:
            r=COMMUNES_REF[COMMUNES_REF["code_insee"]==c]
            if not r.empty:
                r=r.iloc[0]; opts.append({"label": f"{r.nom} ({r.dep_code})", "value": r.code_insee})
    if not s or len(s)<2: return opts
    q = s.lower().strip()
    already={o["value"] for o in opts}
    # Same smart search as visualisation
    exact = SEARCH_IDX[SEARCH_IDX["_nom_low"] == q]
    starts = SEARCH_IDX[SEARCH_IDX["_nom_low"].str.startswith(q) & ~SEARCH_IDX.index.isin(exact.index)]
    contains = SEARCH_IDX[(SEARCH_IDX["_nom_low"].str.contains(q, na=False)
                           | SEARCH_IDX["code_insee"].str.contains(q, na=False))
                           & ~SEARCH_IDX.index.isin(exact.index) & ~SEARCH_IDX.index.isin(starts.index)]
    for df in [exact, starts, contains]:
        for _,r in df.head(20 - len(opts)).iterrows():
            if r.code_insee not in already:
                opts.append({"label": f"{r.nom} ({r.dep_code})", "value": r.code_insee})
                already.add(r.code_insee)
            if len(opts) >= 20: break
        if len(opts) >= 20: break
    return opts

# Sync slider ↔ input (bidirectional)
app.clientside_callback(
    """function(v) { return v || 150; }""",
    Output("calc-r-input", "value"),
    Input("calc-r", "value"),
)
app.clientside_callback(
    """function(v) { return Math.max(1, Math.min(500, v || 150)); }""",
    Output("calc-r", "value", allow_duplicate=True),
    Input("calc-r-input", "value"),
    prevent_initial_call=True,
)

# Reset calculator
@callback(
    [Output("calc-com","value",allow_duplicate=True),
     Output("calc-r","value",allow_duplicate=True),
     Output("calc-meth","value",allow_duplicate=True),
     Output("cl-buf","data",allow_duplicate=True),
     Output("cl-pts","data",allow_duplicate=True),
     Output("calc-stats","children",allow_duplicate=True),
     Output("calc-msgs","children",allow_duplicate=True),
     Output("btn-export","style",allow_duplicate=True)],
    Input("btn-reset-calc","n_clicks"),
    prevent_initial_call=True)
def reset_calc(n):
    return ([], 150, "centroide", EMPTY, EMPTY, None, None,
            {"width":"100%","marginTop":"8px","display":"none"})

@callback(
    [Output("cl-buf","data",allow_duplicate=True),Output("cl-pts","data",allow_duplicate=True),
     Output("calc-stats","children"),Output("calc-msgs","children"),
     Output("calc-store","data"),Output("btn-export","style"),
     Output("map","center",allow_duplicate=True),Output("map","zoom",allow_duplicate=True)],
    Input("btn-calc","n_clicks"),
    [State("calc-com","value"),State("calc-r","value"),State("calc-meth","value")],
    prevent_initial_call=True)
def do_calc(n,codes,rayon,meth):
    if not codes: return[no_update]*8
    res=calculer_zone(codes,rayon or 150,COMMUNES_REF,methode=meth or "centroide",contours=COMMUNES_CONTOURS)
    st=[]
    if res.n_communes>0:
        st.append(stat_card(res.n_communes,"Communes en zone","#E65100"))
        st.append(html.Div(className="stat-row",children=[
            stat_card(res.n_departements,"Departements"),
            stat_card(len(res.foyers),"Foyer(s)","#f85149")]))
        if CHEPTEL.is_loaded():
            cs=compute_cheptel_stats(res.communes,COMMUNES_REF,CHEPTEL)
            if cs:
                st.append(stat_card(f"{cs['total_vaches']:,}","Bovins (lait.+allait.)","#4CAF50"))
                st.append(html.Div(className="stat-row",children=[
                    stat_card(f"{cs['vaches_lait']:,}","V. laitieres","#58a6ff"),
                    stat_card(f"{cs['vaches_nourr']:,}","V. allaitantes","#d2a8ff")]))
                st.append(stat_card(f"{cs['total_exploit']:,}","Exploitations bovines"))
                agg=[]
                if cs['n_regions']>0: agg.append(f"{cs['n_regions']} reg.")
                if cs['n_depts_complets']>0: agg.append(f"{cs['n_depts_complets']} dep.")
                if cs['n_communes_individuelles']>0: agg.append(f"{cs['n_communes_individuelles']} com.")
                txt="Agregation RA 2020 : "+" + ".join(agg)
                if cs['n_secret']>0: txt+=f" ({cs['n_secret']} valeurs secretes)"
                st.append(html.Div(txt,style={"fontSize":"11px","color":"#636058","marginTop":"4px","fontStyle":"italic"}))
    msgs=None
    if res.inconnus: msgs=html.Div(f"⚠ Codes inconnus : {', '.join(res.inconnus)}",
                                     style={"fontSize":"12px","color":"#d29922","marginTop":"8px"})
    buf=res.buffer_geojson or EMPTY
    pts=(communes_to_polygon_geojson(res.communes,COMMUNES_CONTOURS)
         if meth=="polygone" and COMMUNES_CONTOURS is not None and res.n_communes>0
         else communes_to_geojson(res.communes) if res.n_communes>0 else EMPTY)
    fly_center=no_update; fly_zoom=no_update
    if not res.foyers.empty:
        fly_center=[float(res.foyers["lat"].mean()),float(res.foyers["lon"].mean())]; fly_zoom=7
    exp={"width":"100%","marginTop":"8px","display":"block" if res.n_communes>0 else "none"}
    store={"codes":codes,"rayon":rayon or 150,"meth":meth or "centroide"} if res.n_communes>0 else None
    return buf,pts,st,msgs,store,exp,fly_center,fly_zoom

@callback(Output("calc-dl","data"),Input("btn-export","n_clicks"),State("calc-store","data"),
    prevent_initial_call=True)
def dl_xl(n,st):
    if not st: return no_update
    res=calculer_zone(st["codes"],st["rayon"],COMMUNES_REF,
                       methode=st.get("meth","centroide"),contours=COMMUNES_CONTOURS)
    if res.n_communes==0: return no_update
    if CHEPTEL.is_loaded():
        ch=CHEPTEL.communes.rename(columns={"code":"code_insee"})
        res.communes=res.communes.merge(
            ch[["code_insee","exploit_lait","exploit_nourr","vaches_lait","vaches_nourr"]],
            on="code_insee",how="left")
    return dcc.send_bytes(export_to_excel(res),f"zone_{st['codes'][0]}_{st['rayon']}km.xlsx")


# =============================================================================
# 8. IMPORT
# =============================================================================

@callback(
    [Output("import-analysis","data"),Output("import-fpath","data"),
     Output("import-file-info","children"),Output("import-step2","style"),
     Output("import-sheets","children"),Output("import-id","value"),Output("import-name","value")],
    Input("import-upload","contents"),State("import-upload","filename"),prevent_initial_call=True)
def on_upload(contents,filename):
    if not contents: return[no_update]*7
    _,data=contents.split(",")
    fp=UPLOAD_DIR/filename
    with open(fp,"wb") as f: f.write(base64.b64decode(data))
    try: analysis=analyze_excel(fp)
    except Exception as e:
        return None,None,html.Div(f"⚠ {e}",style={"color":"#f85149"}),{"display":"none"},None,"",""
    sheets_ui=[]
    for i,sh in enumerate(analysis["sheets"]):
        col=DEFAULT_COLORS[i%len(DEFAULT_COLORS)]
        hopts=[{"label":h,"value":h} for h in sh["headers"]]
        hopts_opt=[{"label":"—","value":""}]+hopts
        mp=sh["mapping"]; pre={}
        for field,idx in mp.items():
            if idx<len(sh["headers"]): pre[field]=sh["headers"][idx]
        sheets_ui.append(html.Div(className="sheet-card",style={"borderLeftColor":col},children=[
            html.Div(style={"display":"flex","justifyContent":"space-between"}, children=[
                html.Div(f"Onglet : {sh['name']}",style={"fontWeight":"600","fontSize":"13px","color":"#e8e6e1"}),
                html.Div(f"{sh['n_rows']} lignes",style={"fontSize":"11px","color":"#636058"})]),
            html.Div("Zone",style={"fontSize":"11px","color":"#9a9890","marginTop":"10px","marginBottom":"4px"}),
            dcc.Input(id={"type":"imp-zid","index":i},type="text",value=sh["name"],className="dark-input-sm"),
            html.Div("Couleur",style={"fontSize":"11px","color":"#9a9890","marginTop":"8px","marginBottom":"4px"}),
            dcc.Input(id={"type":"imp-zcol","index":i},type="text",value=col,className="dark-input-sm",
                      style={"width":"50%","color":col}),
            html.Div("Colonnes",style={"fontSize":"11px","color":"#9a9890","marginTop":"10px","marginBottom":"6px"}),
            html.Div(style={"display":"grid","gridTemplateColumns":"1fr 1fr","gap":"6px"},children=[
                html.Div(["Code INSEE",dcc.Dropdown(id={"type":"imp-ci","index":i},options=hopts,value=pre.get("code_insee"),
                    className="dark-dropdown",clearable=False,style={"fontSize":"12px"})]),
                html.Div(["Commune",dcc.Dropdown(id={"type":"imp-cc","index":i},options=hopts_opt,value=pre.get("commune",""),
                    className="dark-dropdown",style={"fontSize":"12px"})]),
                html.Div(["Date debut",dcc.Dropdown(id={"type":"imp-cd","index":i},options=hopts,value=pre.get("date_debut"),
                    className="dark-dropdown",clearable=False,style={"fontSize":"12px"})]),
                html.Div(["Date fin",dcc.Dropdown(id={"type":"imp-cf","index":i},options=hopts_opt,value=pre.get("date_fin",""),
                    className="dark-dropdown",style={"fontSize":"12px"})]),
                html.Div(["Departement",dcc.Dropdown(id={"type":"imp-cdp","index":i},options=hopts_opt,value=pre.get("departement",""),
                    className="dark-dropdown",style={"fontSize":"12px"})]),
                html.Div(["Region",dcc.Dropdown(id={"type":"imp-cr","index":i},options=hopts_opt,value=pre.get("region",""),
                    className="dark-dropdown",style={"fontSize":"12px"})])
            ])  # close grid children + grid Div
        ])  # close sheet-card children + sheet-card Div
        )  # close append
    info=html.Div([html.Span(f"✓ {filename}",style={"color":"#4CAF50","fontWeight":"500"}),
                   html.Span(f" — {len(analysis['sheets'])} onglet(s)",style={"color":"#9a9890"})])
    sid=re.sub(r'[^a-z0-9]','',filename.lower().replace("zr_","").replace(".xlsx",""))
    return analysis,str(fp),info,{},sheets_ui,sid,""

@callback([Output("import-step3","style"),Output("import-result","children")],
    Input("btn-import-gen","n_clicks"),
    [State("import-analysis","data"),State("import-fpath","data"),
     State("import-id","value"),State("import-name","value"),State("import-color","value"),
     State({"type":"imp-zid","index":ALL},"value"),State({"type":"imp-zcol","index":ALL},"value"),
     State({"type":"imp-ci","index":ALL},"value"),State({"type":"imp-cc","index":ALL},"value"),
     State({"type":"imp-cd","index":ALL},"value"),State({"type":"imp-cf","index":ALL},"value"),
     State({"type":"imp-cdp","index":ALL},"value"),State({"type":"imp-cr","index":ALL},"value")],
    prevent_initial_call=True)
def gen_import(n,analysis,fp,did,dname,acol,zids,zcols,cis,ccs,cds,cfs,cdps,crs):
    if not analysis or not did: return{"display":"none"},None
    did=re.sub(r'[^a-z0-9_]','',did.lower().strip())
    if not did: return{},html.Div("⚠ Identifiant invalide",style={"color":"#f85149"})
    sheets_cfg=[]
    for i,sh in enumerate(analysis["sheets"]):
        if i>=len(zids): break
        cols={"code_insee":cis[i] if i<len(cis) else None,"date_debut":cds[i] if i<len(cds) else None,
              "date_fin":cfs[i] if i<len(cfs) and cfs[i] else None}
        if i<len(ccs) and ccs[i]: cols["commune"]=ccs[i]
        if i<len(cdps) and cdps[i]: cols["departement"]=cdps[i]
        if i<len(crs) and crs[i]: cols["region"]=crs[i]
        if not cols.get("code_insee") or not cols.get("date_debut"):
            return{},html.Div(f"⚠ Onglet '{sh['name']}' : INSEE et Date debut obligatoires",style={"color":"#f85149"})
        sheets_cfg.append({"sheet_name":sh["name"],"zone_id":zids[i],"columns":cols,
            "color":zcols[i] if i<len(zcols) else DEFAULT_COLORS[i%len(DEFAULT_COLORS)],
            "label":zids[i],"priority":i+1})
    try:
        yml=generate_config(did,dname or did.upper(),Path(fp).name,sheets_cfg,accent_color=acol or "#E65100")
        res=save_import(yml,did,Path(fp),CONFIG_DIR,DATA_DIR)
        # Declencher le rechargement automatique (mode debug)
        trigger_reload()
        return{},html.Div([
            html.Div("✓ Configuration generee",style={"color":"#4CAF50","fontWeight":"600","fontSize":"14px","marginBottom":"12px"}),
            html.Div(className="notice notice-gray",children=[
                html.Div(f"Fichiers crees :"),
                html.Div(f"• {res['yaml_path']}",style={"fontFamily":"monospace","fontSize":"11px","color":"#e8e6e1"}),
                html.Div(f"• {res['excel_path']}",style={"fontFamily":"monospace","fontSize":"11px","color":"#e8e6e1","marginBottom":"8px"}),
                html.Div("L'application redémarre automatiquement.",
                         style={"color":"#58a6ff","fontWeight":"500"}),
                html.Div("Cliquez ci-dessous pour recharger la page.",
                         style={"fontSize":"11px","color":"#636058","marginTop":"4px"})]),
            html.Button("Recharger la page", id="btn-reload-page", n_clicks=0,
                        className="btn-action btn-blue",
                        style={"width":"100%","marginTop":"12px"}),
            html.Div("YAML genere",className="section-label",style={"marginTop":"16px"}),
            html.Pre(yml,className="code-block")])
    except Exception as e:
        return{},html.Div(f"⚠ {e}",style={"color":"#f85149"})




# =============================================================================
# EASTER EGG — CORVEX-Ω
# =============================================================================

# ── Déclencheur : "siphano" dans n'importe quelle barre de recherche ─────────
@callback(
    Output("easter-overlay", "style"),
    Output("game-state", "data"),
    Output("game-tick", "disabled"),
    Output("game-geojson", "data"),
    Input("calc-com", "search_value"),
    Input("search-com", "search_value"),
    State("game-state", "data"),
    prevent_initial_call=True,
)
def trigger_easter(calc_val, search_val, current_state):
    val = calc_val or search_val or ""
    if val.lower().strip() != "siphano":
        return no_update, no_update, no_update, no_update
    if current_state and current_state.get("phase") == "playing":
        return no_update, no_update, no_update, no_update
    state = new_game()
    return {"display": "flex"}, state, False, GAME_DEPT_GJ


# Quand l'overlay devient visible → forcer Leaflet à recalculer sa taille
app.clientside_callback(
    """
    function(style) {
        if (!style || style.display === 'none') return window.dash_clientside.no_update;
        // Leaflet ignore les changements de taille dans un display:none
        // dispatchEvent('resize') force invalidateSize() sur toutes les cartes Leaflet
        setTimeout(function() {
            window.dispatchEvent(new Event('resize'));
        }, 120);
        setTimeout(function() {
            window.dispatchEvent(new Event('resize'));
        }, 400);
        return window.dash_clientside.no_update;
    }
    """,
    Output("game-timer", "style"),
    Input("easter-overlay", "style"),
    prevent_initial_call=True,
)


# ── Tick de jeu ──────────────────────────────────────────────────────────────
@callback(
    Output("game-state", "data", allow_duplicate=True),
    Input("game-tick", "n_intervals"),
    State("game-state", "data"),
    prevent_initial_call=True,
)
def on_game_tick(_, state):
    if not state or state.get("phase") != "playing":
        return no_update
    return game_tick(state)


# ── Mise à jour de l'interface de jeu ────────────────────────────────────────
@callback(
    Output("game-geojson", "hideout"),
    Output("game-timer", "children"),
    Output("game-score-display", "children"),
    Output("game-research-bars", "children"),
    Output("game-vaccine-count", "children"),
    Output("game-stats", "children"),
    Output("game-events-log", "children"),
    Output("game-source-info", "children"),
    Output("game-result-screen", "style"),
    Output("game-result-icon", "children"),
    Output("game-result-title", "children"),
    Output("game-result-msg", "children"),
    Output("game-result-score", "children"),
    Output("game-tick", "disabled", allow_duplicate=True),
    Output("game-flash-banner", "children"),
    Output("game-flash-banner", "style"),
    Input("game-state", "data"),
    prevent_initial_call=True,
)
def update_game_ui(state):
    if not state:
        return (
            no_update, "00:00", "Score : 0", [], "💉 0 dose", [],
            [], "", {"display": "none"}, "", "", "", "", True,
            "", {"display": "none"},
        )

    phase = state.get("phase", "playing")
    elapsed = state.get("elapsed", 0)
    infected = state.get("infected", [])
    vaccines = state.get("vaccines_available", 0)
    research = state.get("research", {})
    research_multipliers = state.get("research_multipliers", {})
    school_completed = state.get("school_completed", [])
    events_log = state.get("events_log", [])
    source_id = state.get("source_school", "enva")
    score = state.get("score") or 0
    flash = state.get("flash_event")
    current_tick = state.get("tick", 0)

    # ── Hideout pour la carte ─────────────────────────────────────────
    hideout = get_hideout(state)

    # ── Timer ──────────────────────────────────────────────────────────
    timer = fmt_time(elapsed)

    # ── Score ──────────────────────────────────────────────────────────
    saved = N_DEPTS - len(infected)
    if phase == "playing":
        live_score = saved * 40 + len(school_completed) * 400 + len(state.get("vaccinated", [])) * 30
    else:
        live_score = score
    score_str = f"Score : {live_score:,}".replace(",", " ")

    # ── Barres de recherche avec multiplicateur visible ────────────────
    research_bars = []
    for sid, school in SCHOOLS.items():
        if sid == source_id:
            continue
        prog = research.get(sid, 0)
        done = sid in school_completed
        bar_color = "#4CAF50" if done else school["color"]
        mult = research_multipliers.get(sid, 1.0)

        # Indicateur de vitesse : flèche selon le multiplicateur
        if done:
            speed_icon, speed_color = "✓", "#4CAF50"
        elif mult >= 1.5:
            speed_icon, speed_color = "⚡", "#FFD54F"
        elif mult >= 0.9:
            speed_icon, speed_color = "→", "#888"
        else:
            speed_icon, speed_color = "⏸", "#ef5350"

        label = f"{'✓ ' if done else ''}{school['name']}"
        research_bars.append(html.Div(className="school-bar-wrap", children=[
            html.Div(className="school-bar-label", children=[
                html.Span(label, style={"color": bar_color}),
                html.Div(style={"display": "flex", "gap": "6px", "alignItems": "center"}, children=[
                    html.Span(speed_icon, style={"color": speed_color, "fontSize": "11px"}),
                    html.Span(f"{int(prog)}%", className="school-bar-pct"),
                ]),
            ]),
            html.Div(className="school-bar-track", children=[
                html.Div(className="school-bar-fill",
                         style={"width": f"{prog}%", "backgroundColor": bar_color}),
            ]),
        ]))

    # ── Vaccins ────────────────────────────────────────────────────────
    dose_txt = f"💉 {vaccines} dose{'s' if vaccines != 1 else ''} disponible{'s' if vaccines > 1 else ''}"
    if vaccines > 0:
        dose_txt = html.Span(dose_txt, style={"color": "#4FC3F7", "fontWeight": "600"})

    # ── Stats ──────────────────────────────────────────────────────────
    n_infected = len(infected)
    n_vaccinated = len(state.get("vaccinated", []))
    pct = int(n_infected / N_DEPTS * 100)
    stats = html.Div(className="easter-stats-grid", children=[
        html.Div(["🦠 Infectés", html.Span(f"{n_infected} ({pct}%)", style={"color": "#ef5350"})], className="easter-stat"),
        html.Div(["💉 Vaccinés", html.Span(str(n_vaccinated), style={"color": "#42A5F5"})], className="easter-stat"),
        html.Div(["🛡 Sauvés",  html.Span(str(saved), style={"color": "#66BB6A"})], className="easter-stat"),
    ])

    # ── Log événements ──────────────────────────────────────────────────
    log_items = [
        html.Div(className="easter-event-item", children=[
            html.Span(icon, className="event-icon"),
            html.Span(msg, className="event-msg"),
        ])
        for icon, msg in events_log
    ]

    # ── Info source ────────────────────────────────────────────────────
    src_school = SCHOOLS[source_id]
    source_info = html.Div([
        html.Div("⚠ Foyer initial", className="source-label"),
        html.Div(src_school["name"], style={"fontWeight": "700", "color": "#ff6666"}),
        html.Div(src_school["city"], style={"fontSize": "11px", "color": "#888"}),
    ])

    # ── Flash banner (affiché seulement pendant 3 ticks après l'événement) ─
    flash_children = ""
    flash_style = {"display": "none"}
    if flash and (current_tick - flash.get("tick", 0)) <= 3:
        flash_children = html.Div(className="easter-flash-inner", children=[
            html.Span(flash["icon"], className="flash-icon"),
            html.Div(children=[
                html.Div(flash["label"], className="flash-label"),
                html.Div(flash["desc"], className="flash-desc"),
            ]),
        ])
        flash_style = {"display": "flex"}

    # ── Fin de partie ──────────────────────────────────────────────────
    if phase in ("won", "lost"):
        won = phase == "won"
        result_style = {
            "display": "flex",
            "position": "fixed",
            "inset": "0",
            "alignItems": "center",
            "justifyContent": "center",
            "background": "rgba(0,0,0,0.88)",
            "zIndex": "99999",
        }
        return (
            hideout, timer, score_str, research_bars, dose_txt, stats, log_items,
            source_info, result_style,
            "🎉" if won else "💀",
            "Épizootie contenue !" if won else "La France est perdue...",
            state.get("result_message", ""),
            f"Score final : {score:,}".replace(",", " "),
            True,
            flash_children, flash_style,
        )

    return (
        hideout, timer, score_str, research_bars, dose_txt, stats, log_items,
        source_info, {"display": "none"}, "", "", "", "", False,
        flash_children, flash_style,
    )


# ── Clic sur la carte → vacciner ─────────────────────────────────────────────
@callback(
    Output("game-state", "data", allow_duplicate=True),
    Input("game-geojson", "clickData"),
    State("game-state", "data"),
    prevent_initial_call=True,
)
def on_map_click(click_data, state):
    if not state or state.get("phase") != "playing":
        return no_update
    if not click_data:
        return no_update
    dept_code = click_data.get("properties", {}).get("dept_code", "")
    if not dept_code:
        return no_update
    return vaccinate_dept(state, dept_code)


# ── Quitter le jeu ────────────────────────────────────────────────────────────
@callback(
    Output("easter-overlay", "style", allow_duplicate=True),
    Output("game-state", "data", allow_duplicate=True),
    Output("game-tick", "disabled", allow_duplicate=True),
    Input("btn-quit-game", "n_clicks"),
    prevent_initial_call=True,
)
def quit_game(n):
    if not n:
        return no_update, no_update, no_update
    return {"display": "none"}, None, True


# ── Sauvegarder le score ──────────────────────────────────────────────────────
@callback(
    Output("game-lb", "data"),
    Output("game-leaderboard-display", "children"),
    Output("btn-save-score", "disabled"),
    Output("btn-save-score", "children"),
    Output("game-player-name", "disabled"),
    Input("btn-save-score", "n_clicks"),
    State("game-player-name", "value"),
    State("game-state", "data"),
    prevent_initial_call=True,
)
def on_save_score(n, name, state):
    if not n or not state or state.get("phase") == "playing":
        return no_update, no_update, no_update, no_update, no_update
    lb = save_score(name or "Anonyme", state)
    return lb, _render_leaderboard(lb), True, "✓ Enregistré", True


# ── Quitter depuis l'écran de résultat ───────────────────────────────────────
@callback(
    Output("easter-overlay", "style", allow_duplicate=True),
    Output("game-state", "data", allow_duplicate=True),
    Output("game-tick", "disabled", allow_duplicate=True),
    Output("btn-save-score", "disabled", allow_duplicate=True),
    Output("btn-save-score", "children", allow_duplicate=True),
    Output("game-player-name", "disabled", allow_duplicate=True),
    Input("btn-quit-from-result", "n_clicks"),
    prevent_initial_call=True,
)
def quit_from_result(n):
    if not n:
        return no_update, no_update, no_update, no_update, no_update, no_update
    return {"display": "none"}, None, True, False, "Enregistrer", False


def _render_leaderboard(lb: list[dict]):
    if not lb:
        return html.Div("Aucun score enregistré.", style={"color": "#888", "textAlign": "center"})
    rows = [
        html.Div(className="lb-row lb-header", children=[
            html.Span("#", className="lb-rank"),
            html.Span("Joueur", className="lb-name"),
            html.Span("Score", className="lb-score"),
            html.Span("Sauvés", className="lb-saved"),
            html.Span("Temps", className="lb-time"),
        ])
    ]
    for i, entry in enumerate(lb[:10], 1):
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else str(i)
        won_style = {"color": "#66BB6A"} if entry.get("won") else {"color": "#ef5350"}
        rows.append(html.Div(className="lb-row", children=[
            html.Span(medal, className="lb-rank"),
            html.Span(entry.get("name", "?"), className="lb-name", style=won_style),
            html.Span(f"{entry.get('score', 0):,}".replace(",", " "), className="lb-score"),
            html.Span(str(entry.get("saved", 0)), className="lb-saved"),
            html.Span(entry.get("time", "?"), className="lb-time"),
        ]))
    return html.Div(rows, className="lb-table")



# =============================================================================
# TÉLÉCHARGEMENT DES DONNÉES CLEAN
# =============================================================================

# Toggle ouverture/fermeture du dropdown d'extraction
@callback(
    Output("dl-dropdown", "style"),
    Input("btn-dl-toggle", "n_clicks"),
    State("dl-dropdown", "style"),
    prevent_initial_call=True,
)
def toggle_dl_dropdown(n, current_style):
    if not n:
        return no_update
    if current_style and current_style.get("display") == "none":
        return {"display": "block"}
    return {"display": "none"}


@callback(
    Output("dl-disease-label", "children"),
    Input("cur-d", "data"),
)
def update_dl_label(disease_id):
    if not disease_id or disease_id not in DISEASES:
        return "Aucune maladie sélectionnée"
    name = DISEASES[disease_id].config.name
    clean_xlsx = Path("data/clean") / f"{disease_id}_periodes.xlsx"
    clean_pq   = Path("data/clean") / f"{disease_id}_periodes.parquet"
    avail = []
    if clean_xlsx.exists(): avail.append(f"xlsx {clean_xlsx.stat().st_size//1024} Ko")
    if clean_pq.exists():   avail.append(f"parquet {clean_pq.stat().st_size//1024} Ko")
    suffix = f" ({', '.join(avail)})" if avail else " (fichiers indisponibles)"
    return f"{name}{suffix}"


@callback(
    Output("dl-file", "data", allow_duplicate=True),
    Output("dl-msg", "children", allow_duplicate=True),
    Input("btn-dl-xlsx", "n_clicks"),
    State("cur-d", "data"),
    prevent_initial_call=True,
)
def dl_xlsx(n, disease_id):
    if not n or not disease_id:
        return no_update, no_update
    path = Path("data/clean") / f"{disease_id}_periodes.xlsx"
    if not path.exists():
        return no_update, "⚠ Fichier introuvable — supprimez le cache et redémarrez."
    name = DISEASES[disease_id].config.name if disease_id in DISEASES else disease_id
    fname = f"EpiZone_{name.replace(' ','_')}_{pd.Timestamp.now().strftime('%Y%m%d')}.xlsx"
    return dcc.send_file(str(path), filename=fname), ""


@callback(
    Output("dl-file", "data", allow_duplicate=True),
    Output("dl-msg", "children", allow_duplicate=True),
    Input("btn-dl-parquet", "n_clicks"),
    State("cur-d", "data"),
    prevent_initial_call=True,
)
def dl_parquet(n, disease_id):
    if not n or not disease_id:
        return no_update, no_update
    path = Path("data/clean") / f"{disease_id}_periodes.parquet"
    if not path.exists():
        return no_update, "⚠ Fichier introuvable — supprimez le cache et redémarrez."
    name = DISEASES[disease_id].config.name if disease_id in DISEASES else disease_id
    fname = f"EpiZone_{name.replace(' ','_')}_{pd.Timestamp.now().strftime('%Y%m%d')}.parquet"
    return dcc.send_file(str(path), filename=fname), ""


if __name__ == "__main__":
    import os
    is_dev = os.environ.get("EPIZONE_DEV", "0") == "1"
    app.run(
        debug=is_dev,
        dev_tools_ui=False,
        dev_tools_props_check=False,
        host="0.0.0.0",
        port=8050,
    )
