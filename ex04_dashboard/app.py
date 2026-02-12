import streamlit as st
import pandas as pd
import json
import plotly.express as px
import requests

# Configuration de la page
st.set_page_config(page_title="tamplate streamlit", layout="wide")

# --- CONNEXION ---
conn = st.connection("my_db", type="sql")


@st.cache_data(ttl=600)
def get_data(query):
    """Fonction unique pour executer les requetes SQL"""
    return conn.query(query)


@st.cache_data
def test_geojson_local():
    try:
        with open("geoson.geojson", "r") as f:
            data = json.load(f)
        return data, "Succès : Le fichier est valide !"
    except json.JSONDecodeError as e:
        return None, f"Erreur de lecture : {e}"
    except FileNotFoundError:
        return None, "Erreur : Le fichier nyc_zones.geojson n'est pas dans le dossier."


# --- SIDEBAR ---
with st.sidebar:
    st.title("Navigation")
    menu = st.radio("Sections",
                    ["Vue d'ensemble", "Analyse Temporelle", "Analyse Geographique", "other", "datascientist"])
    st.markdown("---")


if menu == "Vue d'ensemble":
    st.header("Indicateurs cles de performance")

    query_kpi = """
                SELECT COUNT(*)           as total_trips, \
                       AVG(total_amount)  as avg_fare, \
                       AVG(trip_distance) as avg_distance
                FROM fact_trips \
                """
    df_kpi = get_data(query_kpi)

    col1, col2, col3 = st.columns(3)
    col1.metric("Courses totales", f"{df_kpi['total_trips'][0]:,.0f}")
    col2.metric("Prix moyen", f"{df_kpi['avg_fare'][0]:.2f} $")
    col3.metric("Distance moyenne", f"{df_kpi['avg_distance'][0]:.2f} miles")


elif menu == "Analyse Temporelle":
    st.header("Etude du trafic dans le temps")

    query_time = """
                 SELECT t.hour, COUNT(*) as nb_trips
                 FROM fact_trips f
                          JOIN dim_time t ON f.tpep_pickup_datetime = t.full_datetime
                 GROUP BY t.hour \
                 ORDER BY t.hour \
                 """
    df_time = get_data(query_time)

    st.subheader("Affluence par heure de la journee")
    st.line_chart(df_time, x="hour", y="nb_trips")

    st.subheader("Revenu total par heure")
    query_rev_time = """
                     SELECT t.hour, SUM(f.total_amount) as revenue
                     FROM fact_trips f
                              JOIN dim_time t ON f.tpep_pickup_datetime = t.full_datetime
                     GROUP BY t.hour \
                     ORDER BY t.hour \
                     """
    df_rev_time = get_data(query_rev_time)
    st.area_chart(df_rev_time, x="hour", y="revenue")

    st.subheader("Rentabilité horaire moyenne ($/min)")
    query_renta = """
                  SELECT t.hour, \
                         AVG(f.total_amount / \
                             NULLIF(EXTRACT(EPOCH FROM (f.tpep_dropoff_datetime - f.tpep_pickup_datetime)) / 60, \
                                    0)) as renta
                  FROM fact_trips f
                           JOIN dim_time t ON f.tpep_pickup_datetime = t.full_datetime
                  WHERE f.total_amount > 0
                    AND f.tpep_dropoff_datetime > f.tpep_pickup_datetime
                  GROUP BY t.hour
                  ORDER BY t.hour \
                  """
    df_renta = get_data(query_renta)
    st.line_chart(df_renta, x="hour", y="renta")

    query_revenue_day = """
                        SELECT TO_CHAR(f.tpep_pickup_datetime, 'Day') as day_name, \
                               AVG(f.total_amount)                    as total_revenue
                        FROM fact_trips f
                        GROUP BY day_name \

                        """
    df_day = get_data(query_revenue_day)

    st.subheader("Revenu moyen par jour de la semaine")
    st.bar_chart(df_day, x="day_name", y="total_revenue")

elif menu == "Analyse Geographique":


    options = {
        "Revenu total": {"col": "SUM(f.total_amount)", "label": "Revenu Total"},
        "Revenu moyen": {"col": "AVG(f.total_amount)", "label": "Revenu Moyen"},
        "tips total": {"col": "SUM(f.tip_amount)", "label": "Total des Pourboires"},
        "tips moyen": {"col": "AVG(f.tip_amount)", "label": "Pourboire Moyen"}
    }

    col_sel1, col_sel2, col_sel3 = st.columns(3)

    with col_sel1:
        selection = st.selectbox("Indicateur principal", list(options.keys()))
        metric_sql = options[selection]["col"]
        label_titre = options[selection]["label"]

    with col_sel2:
        flux_type = st.radio("Type de flux", ["Départ (Pickup)", "Arrivée (Dropoff)"])
        loc_col = "pulocationid" if "Départ" in flux_type else "dolocationid"

    with col_sel3:
        view_level = st.radio("Niveau de détail", ["Zone", "Quartier (Borough)"])

    st.header(f"Analyse : {label_titre} par {view_level}")

    query_borough = f"""
        SELECT 
            b.borough_name, 
            COUNT(*) as volume, 
            {metric_sql} as selected_metric
        FROM fact_trips f
        JOIN dim_location l ON f.{loc_col} = l.location_id
        JOIN dim_borough b ON l.borough_id = b.borough_id
        GROUP BY b.borough_name
        ORDER BY volume DESC
    """
    df_borough = get_data(query_borough)

    ca, cb = st.columns(2)
    with ca:
        st.subheader("Volume de trajets")
        st.bar_chart(df_borough, x="borough_name", y="volume")
    with cb:
        st.subheader(label_titre)
        st.bar_chart(df_borough, x="borough_name", y="selected_metric")

    query_map = f"""
            SELECT 
                l.location_id, 
                l.zone_name, 
                b.borough_name,
                b.borough_id,
                COUNT(*) as nb_trajets,
                SUM(f.total_amount) as rev_total,
                AVG(f.tip_amount) as tip_avg,
                {metric_sql} as metric_couleur
            FROM fact_trips f
            JOIN dim_location l ON f.{loc_col} = l.location_id
            JOIN dim_borough b ON l.borough_id = b.borough_id
            GROUP BY l.location_id, l.zone_name, b.borough_name, b.borough_id
        """
    df_map = get_data(query_map)
    df_map['location_id'] = df_map['location_id'].astype(int)

    geojson_data, message = test_geojson_local()

    if geojson_data:
        import numpy as np

        if view_level == "Quartier (Borough)":
            df_map['valeur_affichage'] = df_map.groupby('borough_name')['metric_couleur'].transform('sum')
            hover_name = "borough_name"
        else:
            df_map['valeur_affichage'] = df_map['metric_couleur']
            hover_name = "zone_name"

        df_map['color_scale'] = np.log10(df_map['valeur_affichage'] + 1)

        fig = px.choropleth_mapbox(
            df_map,
            geojson=geojson_data,
            locations="location_id",
            featureidkey="properties.locationid",
            color="color_scale",
            color_continuous_scale="Viridis",
            mapbox_style="carto-positron",
            zoom=9.5,
            center={"lat": 40.7128, "lon": -74.0060},
            opacity=0.6,
            hover_name=hover_name,
            hover_data={
                "location_id": False,
                "borough_name": True,
                "nb_trajets": True,
                "rev_total": ":.2f",
                "valeur_affichage": ":.2f",
                "color_scale": False
            },
            labels={"valeur_affichage": label_titre}
        )

        fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
        st.plotly_chart(fig, use_container_width=True, key="map_zoom_v2")
    else:
        st.error(f"Erreur GeoJSON : {message}")

elif menu == "other":

    options = {
        "Revenu total": {"col": "SUM(f.total_amount)", "label": "Revenu Total"},
        "Revenu moyen": {"col": "AVG(f.total_amount)", "label": "Revenu Moyen"},
        "tips total": {"col": "SUM(f.tip_amount)", "label": "Total des Pourboires"},
        "tips moyen": {"col": "AVG(f.tip_amount)", "label": "Pourboire Moyen"}
    }

    st.header("Analyses par Dimensions")

    selection = st.selectbox("Choisir l'indicateur à mesurer :", list(options.keys()))
    metric_sql = options[selection]["col"]
    label_titre = options[selection]["label"]

    st.subheader(f"{label_titre} par Type de Tarif")
    query_rate = f"""
                 SELECT r.description, {metric_sql} as value
                 FROM Fact_Trips f
                 JOIN Dim_RateCode r ON f.RatecodeID = r.RatecodeID
                 GROUP BY r.description
                 ORDER BY value DESC
                 """
    df_rate = get_data(query_rate)
    st.bar_chart(df_rate, x="description", y="value")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader(f"Par Vendeur")
        query_vendor = f"""
                       SELECT v.vendor_name, {metric_sql} as value
                       FROM Fact_Trips f
                       JOIN Vendor v ON f.VendorID = v.vendorID
                       GROUP BY v.vendor_name
                       ORDER BY value DESC
                       """
        df_vendor = get_data(query_vendor)
        st.bar_chart(df_vendor, x="vendor_name", y="value")

    with col2:
        st.subheader(f"Par Mode de Paiement")
        query_pay = f"""
                    SELECT p.payment_type, {metric_sql} as value
                    FROM Fact_Trips f
                    JOIN Dim_Payment p ON f.payment_type = p.payment_id
                    GROUP BY p.payment_type
                    ORDER BY value DESC
                    """
        df_pay = get_data(query_pay)
        st.bar_chart(df_pay, x="payment_type", y="value")

elif menu == "datascientist":
    st.subheader("Datascientist")
    st.write("Corrélation des variables numériques")

    query_corr = """
                 SELECT passenger_count, trip_distance, fare_amount, tip_amount, total_amount
                 FROM fact_trips
                 LIMIT 5000 -- On limite pour la performance  \
                 """
    df_corr = get_data(query_corr).corr()

    fig_corr = px.imshow(
        df_corr,
        text_auto=".2f",
        zmin=-1, zmax=1
    )
    st.plotly_chart(fig_corr, use_container_width=True)

    st.subheader("Analyse du bénéfice")

    query_profit = """
                   SELECT AVG(f.total_amount)                                                                 as tot,
                          AVG(f.total_amount - (f.mta_tax + f.improvement_surcharge + f.congestion_surcharge +
                                                f.Airport_fee))                                               as benefice_moyen
                   FROM Fact_Trips f
                            JOIN Dim_Time t ON f.tpep_pickup_datetime = t.full_datetime

                   """
    df_profit = get_data(query_profit)

    # taux de marge
    df_profit['taux_marge'] = (df_profit['benefice_moyen'] / df_profit['tot']) * 100

    st.write(
        f"En moyenne, les taxes et frais représentent **{100 - df_profit['taux_marge'].mean():.1f}%** du prix de la course.")
    st.write("On a fait le test de mettre des graphiques qui le représentait mais c'était pas très intéressant")


