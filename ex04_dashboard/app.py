import json
import numpy as np
import plotly.express as px
import streamlit as st
from pathlib import Path
from queries import (get_query_kpi, get_query_time,
                     get_query_rev_time, get_query_renta,
                     get_query_revenue_day, get_query_borough,
                     get_query_map, get_query_rate,
                     get_query_vendor, get_query_pay,
                     get_query_corr, get_query_profit)

# Configuration of the page
st.set_page_config(page_title="template streamlit", layout="wide")

# --- CONNECTION ---
conn = st.connection("my_db", type="sql")

BASE_DIR = Path(__file__).parent
GDF_PATH = BASE_DIR / "../data/streamlit/New_York_map.geojson"


@st.cache_data(ttl=600)
def get_data(query):
    return conn.query(query)


@st.cache_data
def test_geojson_local():
    try:
        with open(GDF_PATH, "r") as f:
            data = json.load(f)
        return data, "Succès : Le fichier est valide !"
    except json.JSONDecodeError as e:
        return None, f"Erreur de lecture : {e}"
    except FileNotFoundError:
        return None, "No file"


# --- SIDEBAR ---
with st.sidebar:
    st.title("Navigation")
    menu = st.radio(
        "Sections",
        ["Vue d'ensemble", "Analyse Temporelle",
         "Analyse Geographique", "other", "datascientist"]
    )
    st.markdown("---")

if menu == "Vue d'ensemble":
    st.header("Indicateurs cles de performance")
    df_kpi = get_data(get_query_kpi())

    col1, col2, col3 = st.columns(3)
    col1.metric("Courses totales", f"{df_kpi['total_trips'][0]:,.0f}")
    col2.metric("Prix moyen", f"{df_kpi['avg_fare'][0]:.2f} $")
    col3.metric("Distance moyenne", f"{df_kpi['avg_distance'][0]:.2f} miles")

elif menu == "Analyse Temporelle":
    st.header("Etude du trafic dans le temps")

    st.subheader("Affluence par heure de la journee")
    df_time = get_data(get_query_time())
    st.line_chart(df_time, x="hour", y="nb_trips")

    st.subheader("Revenu total par heure")
    df_rev_time = get_data(get_query_rev_time())
    st.area_chart(df_rev_time, x="hour", y="revenue")

    st.subheader("Rentabilité horaire moyenne ($/min)")
    df_renta = get_data(get_query_renta())
    st.line_chart(df_renta, x="hour", y="renta")

    st.subheader("Revenu moyen par jour de la semaine")
    df_day = get_data(get_query_revenue_day())
    st.bar_chart(df_day, x="day_name", y="total_revenue")

elif menu == "Analyse Geographique":
    options = {
        "nombre courses": {"col": "COUNT(f.total_amount)",
                           "label": "nombre de course"},
        "Revenu total": {"col": "SUM(f.total_amount)",
                         "label": "Total Revenue"},
        "Revenu moyen": {"col": "AVG(f.total_amount)",
                         "label": "Avg Revenue"},
        "tips total": {"col": "SUM(f.tip_amount)",
                       "label": "Total Tips"},
        "tips moyen": {"col": "AVG(f.tip_amount)",
                       "label": "Average Tip"}
    }

    col_sel1, col_sel2, col_sel3 = st.columns(3)
    with col_sel1:
        selection = st.selectbox("Indicateur principal", list(options.keys()))
        metric_sql = options[selection]["col"]
        label_titre = options[selection]["label"]
    with col_sel2:
        flux_type = st.radio("Type de flux",
                             ["Départ (Pickup)", "Arrivée (Dropoff)"])
        loc_col = "PULocationID" if "Départ" in flux_type else "DOLocationID"
    with col_sel3:
        view_level = st.radio("Niveau de détail",
                              ["Zone", "Quartier (Borough)"])

    st.header(f"Analyse : {label_titre} par {view_level}")

    df_borough = get_data(get_query_borough(loc_col, metric_sql))

    ca, cb = st.columns(2)
    with ca:
        st.subheader("Volume de trajets")
        st.bar_chart(df_borough, x="borough_name", y="volume")
    with cb:
        st.subheader(label_titre)
        st.bar_chart(df_borough, x="borough_name", y="selected_metric")

    df_map = get_data(get_query_map(loc_col, metric_sql))
    df_map['location_id'] = df_map['location_id'].astype(int)

    geojson_data, message = test_geojson_local()
    if geojson_data:
        if view_level == "Quartier (Borough)":
            df_map['valeur_affichage'] = (
                df_map.groupby('borough_name')
                ['metric_couleur'].transform('sum'))
            hover_name = "borough_name"
        else:
            df_map['valeur_affichage'] = (
                df_map)['metric_couleur']
            hover_name = "zone_name"

        df_map['color_scale'] = (
            np.log10(df_map['valeur_affichage'] + 1))
        fig = px.choropleth_mapbox(
            df_map, geojson=geojson_data, locations="location_id",
            featureidkey="properties.locationid", color="color_scale",
            color_continuous_scale="Viridis", mapbox_style="carto-positron",
            zoom=9.5, center={"lat": 40.7128, "lon": -74.0060}, opacity=0.6,
            hover_name=hover_name,
            hover_data={"location_id": False, "borough_name": True,
                        "nb_trajets": True, "rev_total": ":.2f",
                        "valeur_affichage": ":.2f", "color_scale": False},
            labels={"valeur_affichage": label_titre}
        )
        fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
        st.plotly_chart(fig, use_container_width=True, key="map_zoom_v2")
    else:
        st.error(f"Erreur GeoJSON : {message}")

elif menu == "other":
    options = {
        "nombre courses": {"col": "COUNT(f.total_amount)",
                           "label": "nombre de course"},
        "Revenu total": {"col": "SUM(f.total_amount)",
                         "label": "Total Revenue"},
        "Revenu moyen": {"col": "AVG(f.total_amount)",
                         "label": "Avg Revenue"},
        "tips total": {"col": "SUM(f.tip_amount)",
                       "label": "Total Tips"},
        "tips moyen": {"col": "AVG(f.tip_amount)",
                       "label": "Average Tip"}
    }

    st.header("Analyses par Dimensions")
    selection = st.selectbox(
        "Choisir l'indicateur à mesurer :", list(options.keys()))
    metric_sql = options[selection]["col"]
    label_titre = options[selection]["label"]

    st.subheader(f"{label_titre} par Type de Tarif")
    df_rate = get_data(get_query_rate(metric_sql))
    st.bar_chart(df_rate, x="description", y="value")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Par Vendeur")
        df_vendor = get_data(get_query_vendor(metric_sql))
        st.bar_chart(df_vendor, x="vendor_name", y="value")
    with col2:
        st.subheader("Par Mode de Paiement")
        df_pay = get_data(get_query_pay(metric_sql))
        st.bar_chart(df_pay, x="payment_type", y="value")

elif menu == "datascientist":
    st.header("Datascientist")
    st.subheader("Correlation map between  soms numerical data")
    df_corr = get_data(get_query_corr()).corr()
    fig_corr = px.imshow(df_corr,
                         text_auto=".2f", zmin=-1, zmax=1)
    st.plotly_chart(fig_corr, use_container_width=True)

    st.subheader("Analyse du bénéfice")
    df_profit = get_data(get_query_profit())
    df_profit['taux_marge'] = (
        df_profit['benefice_moyen'] / df_profit['tot']) * 100
    marge_val = 100 - df_profit['taux_marge'].mean()
    st.write(
        f"En moyenne, les taxes et frais représentent {marge_val:.1f}% "
        f"du prix de la course.")
