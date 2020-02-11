
"""MIKAELA- 
- I realize a couple of the files are routed to file paths on my computer. I figure I'll fix this when I put it up on EC2 next month
- I made an assumption about what to name the Geojson file--I hope that's okay. 
- The 5. Monthly Summary 'nonoperational_M' column  is a count of the # of vehicles that were 'removed from service b/c of maintenace' at any point during the month
    rather than the count of the total number of removals. So a vehicle that had 3 different 'blocked' periods in "%s-%s_revel_vehicles.csv" would only be counted once. If you'd rather I count total
    it be total removals (counting some vehicles more than once), I can change it. 
- the time formats have been fixed. I hope I understood correctly that they're supposed to be in UTC rather than EST. 
- I realize I'll have to rewrite the code for revel_summary for next month since the values will be different. Also, the Pandas 'value set to copy of a slice' error is
    annoying but it works well enough for now. 
- obv

- Thanks Mikaela!
"""


import pandas as pd
import numpy as np
import psycopg2
import statistics
import pandas.io.sql as psql
##not sure if psql in ec2
from datetime import datetime
from datetime import timedelta
import calendar
import json
import shapely.geometry
import geopandas as gpd


conn = psycopg2.connect(host="ef-warehouse.cwkdbly7jpmk.eu-west-1.rds.amazonaws.com",database="07c0bd8b7c84", user="07c0bd8b7c84_analytics", password="FXKsYgL49vvs6phkifCvoLXafkHxPN")

one_week_ago = datetime.now()-timedelta(days=14)
year = one_week_ago.strftime('%Y')
month = one_week_ago.strftime('%m')

# # 1. Aggregated User Data
# ### main query for user info that calculates the number of trips, avg trip length, and the standard deviation
def user_data(conn):
    print('running user_data')
    user_query = """
    SELECT
        r.user_id,
        COUNT(r.id) AS num_trips,
        AVG(r.distance_in_meters/1609.34) AS mean_trip_length,
        STDDEV(r.distance_in_meters/1609.34) AS std_trip_length
    FROM 
        analytics.rentals r
    WHERE 
        r.system_id LIKE 'washingtondc'
        AND r.price_before_discount > 0 
        AND r.distance_in_meters > 0 
        AND r.billable_duration_in_seconds >= 120 
        AND COALESCE(r.charge_state, 'paid') not in ('canceled', 'refunded')
        AND DATE_TRUNC('month', r.system_date) = (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month')
    GROUP BY
        r.user_id;
    """
    # ### query for user_id and their distances
    second_user_query = """
    SELECT
        user_id,
        distance_in_meters/1609.34 AS distance_in_miles
    FROM 
        analytics.rentals
    WHERE    
        system_id LIKE 'washingtondc'
        AND price_before_discount > 0 
        AND distance_in_meters > 0 
        AND billable_duration_in_seconds >= 120 
        AND COALESCE(charge_state, 'paid') not in ('canceled', 'refunded')
        AND DATE_TRUNC('month', system_date) = (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month')
    ;
    """

    # transfer sql queries into dataframes (one for users and one for each individual ride
    df_us_group = psql.read_sql_query(user_query, conn)
    df_rental_length = psql.read_sql_query(second_user_query, conn)

    # ### doing a groupby of the user id to find the median distance for each rider
    rental_grp = df_rental_length.groupby(by='user_id').median()
    df_us_group = df_us_group.merge(rental_grp, how='left', on='user_id')

    # changing the median column to the correct name for the file
    df_us_group['median_trip_length'] = df_us_group['distance_in_miles']

    # ###### limiting the user dataframe to just the columns that we need
    df_us_group = df_us_group[['user_id', 'num_trips', 'mean_trip_length', 'median_trip_length', 'std_trip_length']]


    df_us_group.to_csv("%s-%s_revel_users.csv" % (year, month), index=False, encoding='utf-8')

    print('made user_data file')

    ####mb do something with the NaNs in std_trip_length


# # 2. Aggregated Vehicle Data
#Set up the base query w/ entered service, mean trip length, std and 
def vehicle_data(conn):
    print('running vehicle_data')

    #queryA finds entered service date, avg & stddev ride length and the # of days a vehicle COULD have been in operation that month (total days in that month unless entered_service date occurs w/in the month)
    #queryB converts started_at/ended_at to the first/last days of the month if they fall outside the month in question so only the time during that month is counted
    main_query = """
    WITH queryA AS (
    SELECT
        r.vehicle_id,
        v.license_plate,
        v.chassis_number,
        MIN(vt.started_at) FILTER (WHERE vt.operationality LIKE 'operational')::timestamptz AS entered_service, 
        AVG(r.distance_in_meters/1607.34) AS mean_trip_length,
        STDDEV(r.distance_in_meters/1607.34) AS std_trip_length,
        CASE
            WHEN DATE_TRUNC('month', MIN(vt.started_at) FILTER (WHERE vt.operationality LIKE 'operational')) = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
                THEN (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL'1 day') - MIN(vt.started_at) FILTER (WHERE vt.operationality LIKE 'operational')
            ELSE (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 day') - DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
        END AS possible_days
    FROM
        analytics.rentals r
    INNER JOIN
        analytics.vehicle_transitions vt
        ON vt.vehicle_id = r.vehicle_id
    LEFT OUTER JOIN
         analytics.vehicles v
         ON v.id = r.vehicle_id
    WHERE
        r.system_id LIKE 'washingtondc'
        AND r.price_before_discount > 0 
        AND r.distance_in_meters > 0 
        AND r.billable_duration_in_seconds >= 120 
        AND COALESCE(r.charge_state, 'paid') NOT IN ('canceled', 'refunded')
        AND DATE_TRUNC('month', r.system_date) = (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month')
    GROUP BY
        r.vehicle_id, v.license_plate, v.chassis_number),
    queryB AS (
    SELECT
        CASE
            WHEN vt.started_at < (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month') THEN (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month')
            ELSE vt.started_at
        END AS start_block,
        CASE
            WHEN vt.ended_at > (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 day') THEN (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 day')
            WHEN vt.ended_at IS NULL THEN (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 day')
            ELSE vt.ended_at
        END AS end_block,
        vt.vehicle_id,
        CASE
            WHEN vt.operationality = 'nonoperational' AND 'battery_discharged' = ANY (vt.operationality_reason) AND NOT ('blocked' = ANY (vt.operationality_reason)) THEN 'Battery Discharged'
            WHEN vt.operationality = 'nonoperational' AND 'blocked' = ANY(vt.operationality_reason) THEN 'Blocked'
            ELSE vt.operationality
        END AS operationality_cleaned
    FROM
        analytics.vehicle_transitions vt
    WHERE vt.system_id LIKE 'washingtondc'
        AND (vt.started_at BETWEEN (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month') AND (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 day')
            OR vt.ended_at BETWEEN (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month') AND (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 day'))
        
    ),
    queryC AS (
    SELECT
        COALESCE(SUM(end_block-start_block), INTERVAL '0 day') AS time_out,
        vehicle_id
    FROM
        queryB
    WHERE operationality_cleaned ILIKE 'blocked'
    GROUP BY vehicle_id)

    SELECT 
        a.vehicle_id,
        UPPER(a.license_plate) AS license_plate,
        a.chassis_number AS vin_number,
        a.entered_service,
        a.mean_trip_length,
        a.std_trip_length,
        COALESCE(a.possible_days - c.time_out, a.possible_days) AS num_days_in_service
    FROM 
        queryA a
    LEFT OUTER JOIN
        queryC c
    ON a.vehicle_id = c.vehicle_id;
    """
    just_rentals = """
    SELECT
        vehicle_id,
        distance_in_meters/1609.34 AS distance_in_miles
    FROM 
        analytics.rentals
    WHERE    
        system_id LIKE 'washingtondc'
        AND price_before_discount > 0 
        AND distance_in_meters > 0 
        AND billable_duration_in_seconds >= 120 
        AND COALESCE(charge_state, 'paid') NOT IN ('canceled', 'refunded')
        AND DATE_TRUNC('month', system_date) = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
    ;
    """
    # COUNT(vehicle_id) FILTER (WHERE vehicle_idWHERE day BETWEEN start_block AND end_block  )

    service_counts = """
    WITH operationality_details as (
    SELECT
        vt.vehicle_id
        , vt.system_id
        , vt.operationality
        , s.time_zone
        , case
            when vt.operationality = 'nonoperational' and 'battery_discharged' = any (vt.operationality_reason) and not('blocked' = any (vt.operationality_reason)) then 'battery_discharged'
            when vt.operationality = 'nonoperational' and 'blocked' = any(vt.operationality_reason) then 'blocked'
            else vt.operationality
        end as operationality_category
        , coalesce(lag(case
            when vt.operationality = 'nonoperational' and 'battery_discharged' = any (vt.operationality_reason) and not('blocked' = any (vt.operationality_reason)) then 'battery_discharged'
            when vt.operationality = 'nonoperational' and 'blocked' = any(vt.operationality_reason) then 'blocked'
            else vt.operationality
        end) over (partition by vehicle_id order by started_at), 'new vehicle') as last_operationality_category
        , vt.started_at
        , coalesce(vt.ended_at, timezone(s.time_zone, now()::timestamptz)) as ended_at
    from analytics.vehicle_transitions vt
    join analytics.systems s
        on s.id = vt.system_id
    join analytics.vehicles v
        on v.id = vt.vehicle_id
        and v.vehicle_kind_name = 'Niu'
    ),
    operationality_periods AS (
    SELECT
       od.*
        , SUM(case when last_operationality_category != operationality_category then 1 else 0 end) over (partition by vehicle_id, system_id order by vehicle_id, system_id, started_at) as period
    FROM operationality_details od
    ORDER BY vehicle_id, system_id, started_at
    ),
    final_operationality_periods AS (
    SELECT
        vehicle_id
        , system_id
        , time_zone
        , operationality_category
        , period
        , min(started_at) as started_at
        , max(ended_at) as ended_at
    FROM operationality_periods op
    GROUP BY vehicle_id, system_id, time_zone, operationality_category, period
    ORDER BY vehicle_id, started_at
    )

    SELECT 
        vehicle_id,
        COUNT(distinct(period)) AS maintenance
    FROM final_operationality_periods 
    WHERE started_at BETWEEN (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL'1 month') AND (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL'1 day')
        AND system_id LIKE 'washingtondc'
        AND operationality_category LIKE 'blocked'
    GROUP BY vehicle_id;
    """
    ##make things like city into parameters?

    ##transfer the queries into pandas dataframes
    df_vehicle_data = psql.read_sql_query(main_query, conn)
    df_rental_info = psql.read_sql_query(just_rentals, conn)
    df_maintenance_counts = psql.read_sql_query(service_counts, conn)

    #groupby to find the median ride length for each vehicle
    grp_df_rental = df_rental_info.groupby(by='vehicle_id').median()
    meta_vehicle = df_vehicle_data.merge(grp_df_rental, how='outer', on='vehicle_id')
    meta_vehicle = meta_vehicle.merge(df_maintenance_counts, how='left', on='vehicle_id')

    #making a column with the correct name for the file
    meta_vehicle['median_trip_length'] = meta_vehicle['distance_in_miles']
    meta_vehicle['exit_service'] = np.nan
    meta_vehicle['num_days_in_service'] = (meta_vehicle['num_days_in_service']/np.timedelta64(1,'h'))/24
    meta_vehicle['DC_vehicle_id'] = meta_vehicle['license_plate']


    # changing the name of the dataframe so we can use the requested 'vehicle id' column name for the vin number
    final_meta_vehicle = meta_vehicle[['vin_number', 'DC_vehicle_id',  'entered_service', 'num_days_in_service', 'mean_trip_length', 'median_trip_length', 'std_trip_length', 'maintenance', 'exit_service']]
    final_meta_vehicle['vehicle_id'] = final_meta_vehicle['vin_number']

    final_meta_vehicle = final_meta_vehicle[['vehicle_id', 'DC_vehicle_id',  'entered_service', 'num_days_in_service', 'mean_trip_length', 'median_trip_length', 'std_trip_length', 'maintenance', 'exit_service']]
    final_meta_vehicle.to_csv("%s-%s_revel_vehicles.csv" % (year, month), index=False, encoding='utf-8')


    print('vehicle_data finished')


# # 3. Aggregated Trip Data
def trip_data(conn):
    print('running trip_data')

    trip_data_query = """
    SELECT
        r.id AS trip_id,
        v.chassis_number AS vehicle_id,
        r.start_point_lat::numeric(7,5) AS start_lat,
        r.start_point_lon::numeric(7,5) AS start_lon,
        r.end_point_lat::numeric(7,5) AS end_lat,
        r.end_point_lon::numeric(7,5) AS end_lon,
        r.system_started_at::timestamptz AS start_time,
        r.system_ended_at::timestamptz AS end_time,
        r.distance_in_meters/1609.34 AS trip_length
    FROM
        analytics.rentals r
    INNER JOIN
        analytics.vehicles v
        ON v.id = r.vehicle_id        
    WHERE
        DATE_TRUNC('month', r.system_started_at) = (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month')
        AND r.system_id LIKE 'washingtondc'
        AND r.price_before_discount > 0 
        AND r.distance_in_meters > 0 
        AND r.billable_duration_in_seconds >= 120 
        AND coalesce(r.charge_state, 'paid') not in ('canceled', 'refunded')
    ;
    """

    df_trip_data = psql.read_sql_query(trip_data_query, conn)

    df_trip_data.to_csv("%s-%s_revel_trips.csv" % (year, month), index=False, encoding='utf-8')

    print('trip_data finished')


# # 4. Trip Waypoints
def trip_waypoints():
    print('connecting to redshift')

    conn2 = psycopg2.connect(dbname="niu", host="revel-redshift.cttxfropuj9g.us-east-2.redshift.amazonaws.com",  port="5439", user="revel", password="Fe1!lowes")
    print('connected, now running trip_waypoints')

    trip_waypoint_query = """
    SELECT
    r.rental_id as trip_id
        , json_extract_array_element_text(json_extract_path_text(e.fields, 'location'), 1)::numeric(7,5) as lat
        , json_extract_array_element_text(json_extract_path_text(e.fields, 'location'), 0)::numeric(7,5) as lon
        , timezone('UTC', (timestamptz 'epoch' + e.timestamp/1000 * interval '1 second')::timestamptz)::timestamptz as time
    from public.rentals r
    join public.niu_events e
        on e.deviceid = r.identifier
        and e.timestamp between r.started_at_ms and r.ended_at_ms
    where system_id = 'washingtondc'
        and msgcode = 'location'
        and DATE_TRUNC('month', timezone('UTC', (timestamptz 'epoch' + r.started_at_ms/1000 * interval '1 second')::timestamptz)) = DATE_TRUNC('month', DATE_TRUNC('month', getdate()) - interval '1 day')
        and r.is_valid_rental = 1
    ;
    """

    df_trip_waypoint = psql.read_sql_query(trip_waypoint_query, conn2)
    df_trip_waypoint.to_csv("%s-%s_revel_waypoints.csv" % (year, month), index=False, encoding='utf-8')

    print('finished trip_waypoints')


# # 5. Revel_Summary
def revel_summary(conn):
    print("starting revel_summary")
    querya = """
    WITH operationality_details as (
    SELECT
        vt.vehicle_id
        , vt.system_id
        , vt.operationality
        , s.time_zone
        , case
            when vt.operationality = 'nonoperational' and 'battery_discharged' = any (vt.operationality_reason) and not('blocked' = any (vt.operationality_reason)) then 'battery_discharged'
            when vt.operationality = 'nonoperational' and 'blocked' = any(vt.operationality_reason) then 'blocked'
            else vt.operationality
        end as operationality_category
        , coalesce(lag(case
            when vt.operationality = 'nonoperational' and 'battery_discharged' = any (vt.operationality_reason) and not('blocked' = any (vt.operationality_reason)) then 'battery_discharged'
            when vt.operationality = 'nonoperational' and 'blocked' = any(vt.operationality_reason) then 'blocked'
            else vt.operationality
        end) over (partition by vehicle_id order by started_at), 'new vehicle') as last_operationality_category
        , vt.started_at
        , coalesce(vt.ended_at, timezone(s.time_zone, now()::timestamptz)) as ended_at
    from analytics.vehicle_transitions vt
    join analytics.systems s
        on s.id = vt.system_id
    join analytics.vehicles v
        on v.id = vt.vehicle_id
        and v.vehicle_kind_name = 'Niu'
    ),
    operationality_periods AS (
    SELECT
       od.*
        , SUM(case when last_operationality_category != operationality_category then 1 else 0 end) over (partition by vehicle_id, system_id order by vehicle_id, system_id, started_at) as period
    FROM operationality_details od
    ORDER BY vehicle_id, system_id, started_at
    ),
    final_operationality_periods AS (
    SELECT
        vehicle_id
        , system_id
        , time_zone
        , operationality_category
        , period
        , min(started_at) as started_at
        , max(ended_at) as ended_at
    FROM operationality_periods op
    GROUP BY vehicle_id, system_id, time_zone, operationality_category, period
    ORDER BY vehicle_id, started_at
    )

    SELECT 
        COUNT(DISTINCT(vehicle_id)) AS nonoperational_M
    FROM final_operationality_periods 
    WHERE started_at BETWEEN (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL'1 month') AND (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL'1 day')
        AND system_id LIKE 'washingtondc'
        AND operationality_category LIKE 'blocked'
    ;
    """
    queryb = """
    SELECT
        COUNT(r.id) AS total_trips
    FROM analytics.rentals r 
    WHERE
        r.price_before_discount > 0 
        AND r.distance_in_meters > 0 
        AND r.billable_duration_in_seconds >= 120 
        AND COALESCE(r.charge_state, 'paid') NOT IN ('canceled', 'refunded')
        AND r.system_id LIKE 'washingtondc'
        AND DATE_TRUNC('month', r.system_started_at) = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month');
    """

    df_number_rentals = psql.read_sql_query(queryb, conn)
    df_number_maintenance = psql.read_sql_query(querya, conn)


    df_number_rentals['nonoperational_M'] = df_number_maintenance.iloc[0,0]
    df_number_rentals['total_vehicles'] = 407
    df_number_rentals['nonoperational_LS'] = 0
    df_number_rentals['M_lights'] = 33
    df_number_rentals['M_wheeltire'] = 1
    df_number_rentals['M_seat'] = 5
    df_number_rentals['M_brakes'] = 7
    df_number_rentals['M_frame'] = 19
    df_number_rentals['M_gearsystem'] = 0
    df_number_rentals['M_lock'] = 0
    df_number_rentals['M_battery'] = 13
    df_number_rentals['M_otherrepair'] = 0


    df_number_rentals = df_number_rentals[['total_trips', 'total_vehicles', 'nonoperational_LS', 'nonoperational_M', 'M_lights', 'M_wheeltire', 'M_seat', 'M_brakes', 'M_frame', 'M_gearsystem', 'M_lock', 'M_battery', 'M_otherrepair']]


    df_number_rentals.to_csv("/Users/amyveggeberg/Desktop/super/%s-%s_revel_summary.csv" % (year, month), index=False, encoding='utf-8')
    print("finished revel summary")


# # Other_3. Service Area
def geo_service_area(conn):
    print('starting the geojson')
    geoquery = """
    SELECT
        geofence_geojson
    from analytics.systems s
    where id = 'washingtondc'
    ;
    """

    systems = psql.read_sql_query(geoquery, conn)
    systems['geofence_geom'] = systems['geofence_geojson'].apply(lambda x: shapely.geometry.shape(json.loads(x)))
    systems = gpd.GeoDataFrame(systems, geometry='geofence_geom')

    systems.to_file("/Users/amyveggeberg/Desktop/super/%s-%s_revel_service_area.geojson" % (year, month), driver='GeoJSON')
    print('Geojson doneskies!')


def revel_safety():
    df_safety = pd.DataFrame(columns=['trip_id', 'vehicle_id', 'incident_severity', 'electrical_vehicle_check', 'police_report_number', 'incident_notes', 'incident_lat', 'incident_lon', 'vehicle_incident_path', 'incident_time'])

    df_safety.to_csv("/Users/amyveggeberg/Desktop/super/%s-%s_revel_safety.csv" % (year, month), encoding='utf-8')
    print("finished blank revel safety csv")    


user_data(conn)
vehicle_data(conn)
trip_data(conn)
trip_waypoints()
revel_summary(conn)
geo_service_area(conn)
revel_safety()



