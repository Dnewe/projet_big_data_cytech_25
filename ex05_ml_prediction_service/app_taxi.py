import folium
import geopandas as gpd
import pandas as pd
import requests
import streamlit as st
from shapely.geometry import Point
from streamlit_folium import st_folium
from pathlib import Path
from test import load_model, inferencence

# Configuration of the page
st.set_page_config(page_title="Template", layout="wide")

artifacts = load_model()
model = artifacts['model']

BASE_DIR = Path(__file__).parent
GDF_PATH = BASE_DIR / "../data/streamlit/New_York_map.geojson"
LOOKUP_PATH = BASE_DIR / "../data/streamlit/taxi_zone_lookup.csv"


@st.cache_data
def load_geo_data():
    """Load GeoJSON and taxi zone lookup data."""
    gdf = gpd.read_file(GDF_PATH)
    lookup_df = pd.read_csv(LOOKUP_PATH)
    return gdf, lookup_df


def location_id(lon, lat, geojson):
    """Identify the taxi zone ID and names based on coordinates.

    Parameters
    ----------
    lon : float
        Longitude of the point.
    lat : float
        Latitude of the point.
    geojson : geopandas.GeoDataFrame
        The GeoJSON data containing zone geometries.

    Returns
    -------


    """
    point = Point(lon, lat)
    match = geojson[geojson.geometry.contains(point)]
    if not match.empty:
        result = match.iloc[0]
        return result['locationid'], result['zone'], result['borough']
    return 264, "Unknown", "Unknown"


def get_route_distanc(lat1, lon1, lat2, lon2):
    """Retrieve route distance and duration from OSRM API.

    Parameters
    ----------
    lat1 : float
        Latitude of departure point.
    lon1 : float
        Longitude of departure point.
    lat2 : float
        Latitude of arrival point.
    lon2 : float
        Longitude of arrival point.

    Returns
    -------

    """
    url = (f"http://router.project-osrm.org/route/v1/driving/"
           f"{lon1},{lat1};{lon2},{lat2}?overview=false")
    try:
        r = requests.get(url)
        data = r.json()
        if data['code'] == 'Ok':
            meters = data['routes'][0]['distance']
            distance_miles = meters / 1609.34
            duration_min = data['routes'][0]['duration'] / 60
            return distance_miles, duration_min
    except Exception as e:
        print(f"Erreur : {e}")
    return None, None


geo_df, lookup_df = load_geo_data()

st.title("Prédiction -- NYC Yellow Taxi")
st.markdown("---")

st.sidebar.header("Paramètre")

rate_mapping = {
    1: "Standard", 2: "JFK", 3: "Newark", 4: "Nassau/Westchester",
    5: "Negotiated fare", 99: "Unknown"
}
vendor_mapping = {
    1: "Creative Mobile Technologies", 2: "Curb Mobility"
}
day_mapping = {
    1: "Lundi", 2: "Mardi", 3: "Mercredi", 4: "Jeudi",
    5: "Vendredi", 6: "Samedi", 7: "Dimanche"
}

ratecodeID = st.sidebar.selectbox(
    "RatecodeID", options=list(rate_mapping.keys()),
    format_func=lambda x: rate_mapping[x]
)
vendorID = st.sidebar.selectbox(
    "VendorID", options=list(vendor_mapping.keys()),
    format_func=lambda x: vendor_mapping[x]
)
day = st.sidebar.selectbox(
    "Day", options=list(day_mapping.keys()),
    format_func=lambda x: day_mapping[x]
)

passenger_count = st.sidebar.number_input(
    "Passenger count", min_value=0, max_value=7, step=1
)
hour = st.sidebar.number_input(
    "Hour", min_value=0, max_value=24, step=1
)

if 'trip_dist' not in st.session_state:
    st.session_state.trip_dist = 0.0
if 'trip_duration' not in st.session_state:
    st.session_state.trip_duration = 0.0

X = {
    'hour': hour, 'day': day, 'duration': st.session_state.trip_duration,
    'PULocationID': 100, 'DOLocationID': 100,
    'passenger_count': passenger_count,
    'RatecodeID': ratecodeID, 'VendorID': vendorID,
    'trip_distance': st.session_state.trip_dist
}

col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Sélection du trajet")

    if 'dep' not in st.session_state:
        st.session_state.dep = [40.7128, -74.0060]
    if 'arr' not in st.session_state:
        st.session_state.arr = [40.7580, -73.9855]

    mode = st.radio(
        "Cliquer sur la carte pour placer :",
        ["Le Départ", "L'Arrivée"],
        horizontal=True
    )

    m = folium.Map(location=st.session_state.dep, zoom_start=12)

    folium.Marker(
        st.session_state.dep, popup="Départ",
        icon=folium.Icon(color="green")
    ).add_to(m)
    folium.Marker(
        st.session_state.arr, popup="Arrivée",
        icon=folium.Icon(color="red")
    ).add_to(m)

    map_data = st_folium(
        m, height=450, width=None, key="main_map",
        returned_objects=["last_clicked"]
    )

    if map_data and map_data.get("last_clicked"):
        new_lat = map_data["last_clicked"]["lat"]
        new_lon = map_data["last_clicked"]["lng"]

        if mode == "Le Départ":
            st.session_state.dep = [new_lat, new_lon]
        else:
            st.session_state.arr = [new_lat, new_lon]
        st.rerun()

    st.markdown("---")

    id_dep, zone_dep, boroygh_dep = location_id(
        st.session_state.dep[1], st.session_state.dep[0], geo_df
    )
    id_arr, zone_arr, borough_arr = location_id(
        st.session_state.arr[1], st.session_state.arr[0], geo_df
    )

    X['PULocationID'] = id_dep
    X['DOLocationID'] = id_arr

    c1, c2 = st.columns(2)
    with c1:
        st.info(f"**Départ** : {zone_dep}, {boroygh_dep}")
    with c2:
        st.error(f"**Arrivée** : {zone_arr}, {borough_arr}")

with col2:
    st.subheader("Estimation")

    manual_mode = st.checkbox("Mode manuel", value=False)

    if st.button("Calculer les zones", use_container_width=True,
                 type="primary"):
        dist_api, time_api = get_route_distanc(
            st.session_state.dep[0], st.session_state.dep[1],
            st.session_state.arr[0], st.session_state.arr[1]
        )
        if dist_api is not None:
            st.session_state.trip_dist = dist_api
            st.session_state.trip_duration = time_api
            st.rerun()
        else:
            st.warning("Données d'itinéraire manquantes ou erreur API")

    if manual_mode:
        c1, c2 = st.columns(2)
        st.session_state.trip_dist = c1.number_input(
            "Distance", value=float(st.session_state.trip_dist)
        )
        st.session_state.trip_duration = c2.number_input(
            "Temps", value=float(st.session_state.trip_duration)
        )
    else:
        if st.session_state.trip_dist > 0:
            m1, m2 = st.columns(2)
            m1.metric("Distance", f"{st.session_state.trip_dist:.2f} mi")
            m2.metric("Durée", f"{st.session_state.trip_duration:.0f} min")
        else:
            st.write("Veuillez calculer l'itinéraire ou passer en manuel.")

    X['trip_distance'] = st.session_state.trip_dist
    X['duration'] = st.session_state.trip_duration

    st.markdown("---")

    if st.button("Prédiction", use_container_width=True):
        prediction = inferencence(X, artifacts)
        st.success(f"**Prix prédit : {prediction[0]:.2f} $**")

        with st.expander("Vérifier les data"):
            for key, value in X.items():
                st.write(f"**{key}** : `{value}`")

if __name__ == "__main__":
    geo_df, _ = load_geo_data()
    longitude = -73.9821938
    latitude = 40.7685167
    x, y, _ = location_id(longitude, latitude, geo_df)
    print(x)
    print(y)

    test1 = -73.7797278
    test_lat = 40.6446124

    x, y, z = location_id(test1, test_lat, geo_df)
    print(x)
    print(y)
    print(z)

    rahh = get_route_distanc(latitude, longitude, test_lat, test1)
    print(rahh)
