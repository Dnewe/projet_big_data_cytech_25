def get_query_kpi():
    """
    Generate the SQL query for high-level Key Performance Indicators.

    Returns
    -------
    str
        SQL query to calculate total trips, average fare, and average distance.
    """
    return """
           SELECT COUNT(*)           as total_trips,
                  AVG(total_amount)  as avg_fare,
                  AVG(trip_distance) as avg_distance
           FROM fact_trips
           """


def get_query_time():
    """
    Generate the SQL query for trip counts aggregated by hour.

    Returns
    -------
    str
        SQL query to join fact_trips and dim_time to count trips per hour.
    """
    return """
           SELECT t.hour, COUNT(*) as nb_trips
           FROM fact_trips f
                    JOIN dim_time t ON f.tpep_pickup_datetime = t.full_datetime
           GROUP BY t.hour
           ORDER BY t.hour
           """


def get_query_rev_time():
    """
    Generate the SQL query for total revenue aggregated by hour.

    Returns
    -------
    str
        SQL query to calculate sum of total_amount per hour.
    """
    return """
           SELECT t.hour, SUM(f.total_amount) as revenue
           FROM fact_trips f
                    JOIN dim_time t ON f.tpep_pickup_datetime = t.full_datetime
           GROUP BY t.hour
           ORDER BY t.hour
           """


def get_query_renta():
    """
    Generate the SQL query for average hourly profitability ($/min).

    Returns
    -------
    str
        SQL query calculating revenue per minute, excluding invalid durations.
    """
    return """
           SELECT t.hour,
                  AVG(f.total_amount /
                      NULLIF(EXTRACT(EPOCH FROM
                                     (f.tpep_dropoff_datetime -
                                      f.tpep_pickup_datetime)) / 60,
                             0)) as renta
           FROM fact_trips f
                    JOIN dim_time t
                         ON f.tpep_pickup_datetime = t.full_datetime
           WHERE f.total_amount > 0
             AND f.tpep_dropoff_datetime > f.tpep_pickup_datetime
           GROUP BY t.hour
           ORDER BY t.hour
           """


def get_query_revenue_day():
    """
    Generate the SQL query for average revenue aggregated by day of the week.

    Returns
    -------
    str
        SQL query using TO_CHAR to group revenue by weekday name.
    """
    return """
           SELECT TO_CHAR(f.tpep_pickup_datetime, 'Day') as day_name,
                  AVG(f.total_amount)                    as total_revenue
           FROM fact_trips f
           GROUP BY day_name
           """


def get_query_borough(loc_col, metric_sql):
    """
    Generate the SQL query for trip volume and a specific metric by borough.

    Parameters
    ----------
    loc_col : str
        The column name for location (e.g., 'pulocationid' or 'dolocationid').
    metric_sql : str
        The SQL aggregate function to apply (e.g., 'SUM(total_amount)').

    Returns
    -------
    str
        SQL query joined with dim_location and dim_borough.
    """
    return f"""
        SELECT
            b.borough_name,
            COUNT(*) as volume,
            {metric_sql} as selected_metric
        FROM fact_trips f
        JOIN dim_location l ON f."{loc_col}" = l.location_id
        JOIN dim_borough b ON l.borough_id = b.borough_id
        GROUP BY b.borough_name
        ORDER BY volume DESC
    """


def get_query_map(loc_col, metric_sql):
    """
    Generate the SQL query for detailed mapping data by taxi zone.

    Parameters
    ----------
    loc_col : str
        The column name for location (e.g., 'pulocationid' or 'dolocationid').
    metric_sql : str
        The SQL aggregate function for color scaling.

    Returns
    -------
    str
        SQL query containing location IDs, names, and various trip metrics.
    """
    return f"""
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
        JOIN dim_location l ON f."{loc_col}" = l.location_id
        JOIN dim_borough b ON l.borough_id = b.borough_id
        GROUP BY l.location_id, l.zone_name, b.borough_name, b.borough_id
    """


# Pour les 3 requets suivante on a du inverser les ordres (faire un left join)
# pour avoir les vendeurs meme si ils ont 0 trajets par exemple
def get_query_rate(metric_sql):
    """
    Generate the SQL query for metrics aggregated by rate code.

    Parameters
    ----------
    metric_sql : str
        The SQL aggregate function to apply.

    Returns
    -------
    str
        SQL query using LEFT JOIN to include all rate codes.
    """
    return f"""
        SELECT r.description, COALESCE({metric_sql}, 0) as value
        FROM Dim_RateCode r
        LEFT JOIN Fact_Trips f ON r.ratecodeid = f."RatecodeID"
        GROUP BY r.description
        ORDER BY value DESC
    """


def get_query_vendor(metric_sql):
    """
    Generate the SQL query for metrics aggregated by vendor.

    Parameters
    ----------
    metric_sql : str
        The SQL aggregate function to apply.

    Returns
    -------
    str
        SQL query using LEFT JOIN to include all vendors.
    """
    return f"""
        SELECT v.vendor_name, COALESCE({metric_sql}, 0) as value
        FROM Vendor v
        LEFT JOIN Fact_Trips f ON v."vendorid" = f."VendorID"
        GROUP BY v.vendor_name
        ORDER BY value DESC
    """


def get_query_pay(metric_sql):
    """
    Generate the SQL query for metrics aggregated by payment type.

    Parameters
    ----------
    metric_sql : str
        The SQL aggregate function to apply.

    Returns
    -------
    str
        SQL query using LEFT JOIN to include all payment methods.
    """
    return f"""
        SELECT p.payment_type, COALESCE({metric_sql}, 0) as value
        FROM Dim_Payment p
        LEFT JOIN Fact_Trips f ON p.payment_id = f.payment_type
        GROUP BY p.payment_type
        ORDER BY value DESC
    """


def get_query_corr():
    """
    Generate the SQL query for numerical variable correlation analysis.

    Returns
    -------
    str
        SQL query limited to 5000 rows for performance.
    """
    return """
           SELECT passenger_count,
                  trip_distance,
                  fare_amount,
                  tip_amount,
                  total_amount
           FROM fact_trips
           LIMIT 5000
           """


def get_query_profit():
    """
    Generate the SQL query for average total amount and net profit.

    Returns
    -------
    str
        SQL query calculating profit by subtracting taxes and fees.
    """
    return """
           SELECT AVG(f.total_amount) as tot,
                  AVG(f.total_amount - (
                      f.mta_tax +
                      f.improvement_surcharge +
                      f.congestion_surcharge +
                      f."Airport_fee"
                      ))              as benefice_moyen
           FROM Fact_Trips f
                    JOIN Dim_Time t
                         ON f.tpep_pickup_datetime = t.full_datetime
           """
