import streamlit as st
import pandas as pd
import json
import requests
import folium
from streamlit_folium import folium_static
import plotly.express as px
import io
import re # For parsing geometry string

# --- Configuration ---
st.set_page_config(layout="wide", page_title="City Parking Data Comparison")

# --- Data Loading Functions ---

@st.cache_data # Using st.cache_data for DataFrame caching
def load_heidelberg_data():
    """Loads Heidelberg parking data from local CSV files."""
    data = {}
    heidelberg_files = {
        'parking_garage': r"c:\Users\kavya\Downloads\Heidelberg\Parking-Garadge 1.csv",
        'disabled_parking': r"c:\Users\kavya\Downloads\Heidelberg\Disabled 1.csv",
        'historical_p001': r"c:\Users\kavya\Downloads\Heidelberg\Parkhausbelegungsstände_in_Heidelberg_historical_data_p001_offstreetparking_2022_10_28-2023_07_17 (1) 1.csv", # Corrected filename
        'current_p00': r"c:\Users\kavya\Downloads\Heidelberg\Parkhausbelegungsstände_in_Heidelberg_urn_ngsiv2_offstreetparking_p00_offstreetparking (1) 1.csv"
    }

    for key, file_name in heidelberg_files.items():
        try:
            df = pd.read_csv(file_name)
            
            # Special handling for disabled_parking to parse 'geometry' column
            if key == 'disabled_parking' and 'geometry' in df.columns:
                # Extract coordinates from 'POINT (lon lat)' string
                df[['longitude', 'latitude']] = df['geometry'].str.extract(r'POINT \((\S+) (\S+)\)').astype(float)
                df = df.drop(columns=['geometry'])
            
            # Ensure consistent column names for location for mapping purposes later
            if key == 'parking_garage':
                if 'lat' in df.columns and 'lon' in df.columns:
                    df = df.rename(columns={'lat': 'latitude', 'lon': 'longitude'})
                else:
                    st.warning(f"Latitude/Longitude columns ('lat', 'lon') not found in {file_name}. Map markers might be affected.")
            
            data[key] = df
        except FileNotFoundError:
            st.error(f"Error: The file '{file_name}' was not found. Please ensure it's in the correct directory.")
            return None
        except Exception as e:
            st.error(f"An unexpected error occurred while loading '{file_name}': {e}")
            return None
    
    if data:
        st.success("All Heidelberg data loaded successfully!")
    else:
        st.warning("No Heidelberg data could be loaded.")
        return None
    return data

@st.cache_data # Using st.cache_data for DataFrame caching
def load_bonn_data():
    """Loads Bonn parking data from GitHub GeoJSON/JSON URLs with robust error handling."""
    bonn_urls = {
        'resident_parking_1': "https://raw.githubusercontent.com/SRH-Heidelberg-University-ADSA/Data4HD/main/Bewohnerparkgebiete1.geojson",
        'resident_parking_2': "https://raw.githubusercontent.com/SRH-Heidelberg-University-ADSA/Data4HD/main/Bewohnerparkgebiete2.geojson",
        'park_and_ride': "https://raw.githubusercontent.com/SRH-Heidelberg-University-ADSA/Data4HD/main/Park%20%26%20Ride%20Parkpl%C3%A4tze.json",
        'parking_garages': "https://raw.githubusercontent.com/SRH-Heidelberg-University-ADSA/Data4HD/main/Parkh%C3%A4user%20Standorte.geojson",
        'general_parking': "https://raw.githubusercontent.com/SRH-Heidelberg-University-ADSA/Data4HD/main/Standorte%20der%20Parkpl%C3%A4tze%20(PKW-%2C%20Motorrad-%2C%20Wohnmobil/Wohnwagen-%20und%20Busparkpl%C3%A4tze).geojson",
        'bus_parking': "https://raw.githubusercontent.com/SRH-Heidelberg-University-ADSA/Data4HD/main/Standorte%20der%20Busparkpl%C3%A4tze.geojson",
        'motorcycle_parking': "https://raw.githubusercontent.com/SRH-Heidelberg-University-ADSA/Data4HD/main/Standorte%20der%20Motorradparkpl%C3%A4tze.geojson", # Corrected URL
        'parking_bonn_koeln_osm': "https://raw.githubusercontent.com/SRH-Heidelberg-University-ADSA/Data4HD/main/parking_bonn_koel_osm.geojson"
    }

    data = {}
    for key, url in bonn_urls.items():
        try:
            response = requests.get(url)
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
            
            content_json = json.loads(response.text)

            if key == 'park_and_ride': # This is a direct JSON, not GeoJSON
                data[key] = pd.DataFrame(content_json)
            else: # GeoJSON
                features = content_json.get('features', [])
                df_rows = []
                for feature in features:
                    properties = feature.get('properties', {})
                    geometry = feature.get('geometry') # Get geometry, can be None

                    coords = None
                    if geometry and geometry.get('coordinates'):
                        coords = geometry.get('coordinates')

                    # Initialize longitude and latitude to None
                    properties['longitude'] = None
                    properties['latitude'] = None

                    if geometry and coords is not None: # Proceed only if geometry and coordinates exist
                        geom_type = geometry.get('type')
                        if geom_type == 'Point':
                            if len(coords) >= 2:
                                properties['longitude'] = coords[0]
                                properties['latitude'] = coords[1]
                        elif geom_type == 'Polygon':
                            # For polygons, take the first point of the first ring as a representative
                            if coords and len(coords) > 0 and len(coords[0]) > 0 and len(coords[0][0]) >= 2:
                                properties['longitude'] = coords[0][0][0]
                                properties['latitude'] = coords[0][0][1]
                    df_rows.append(properties)
                
                if df_rows:
                    data[key] = pd.DataFrame(df_rows)
                else:
                    data[key] = pd.DataFrame() # Empty DataFrame if no features
        except requests.exceptions.RequestException as e:
            st.error(f"Error fetching Bonn data from {url}: {e}")
            return None
        except json.JSONDecodeError as e:
            st.error(f"Error decoding JSON from {url}: {e}")
            return None
        except Exception as e:
            st.error(f"An unexpected error occurred while processing {url}: {e}")
            return None
    
    if data:
        st.success("Bonn data loaded successfully!")
    else:
        st.warning("No Bonn data could be loaded.")
        return None
    return data

# --- Helper function for data quality ---
def get_missing_values_report(data_dict, city_name):
    report = []
    for dataset_name, df in data_dict.items():
        if not df.empty:
            missing_counts = df.isnull().sum()
            total_rows = df.shape[0]
            for col, count in missing_counts.items():
                if count > 0: # Only include columns with missing values
                    percentage = (count / total_rows) * 100 if total_rows > 0 else 0
                    report.append({
                        'City': city_name,
                        'Dataset': dataset_name,
                        'Column': col,
                        'Missing Count': count,
                        'Missing Percentage': percentage # Store as float for plotting
                    })
    return pd.DataFrame(report)


# --- Main Dashboard ---
def main():
    st.title("Heidelberg vs. Bonn: Parking Data Dashboard 🚗")
    st.markdown("""
    Explore and compare parking data from Heidelberg and Bonn to identify strengths and areas for improvement in city data portals, with Bonn serving as a benchmark for Heidelberg.
    """)

    heidelberg_data = load_heidelberg_data()
    bonn_data = load_bonn_data()

    if heidelberg_data is None or bonn_data is None:
        st.error("Dashboard cannot load due to data loading errors. Please resolve the issues mentioned above.")
        return

    st.sidebar.header("Dashboard Controls")
    selected_view = st.sidebar.radio(
        "Select Section:",
        ("Overall Summary", "Data Assets Overview", "Dataset Attributes", "Data Quality Dashboard", "Geographic Distribution", "Recommendations")
    )
    
    # Filter for map display
    selected_parking_types_map = st.sidebar.multiselect(
        "Parking Types for Map:",
        ["All", "Parking Garages", "Disabled Parking", "Park & Ride", "Resident Zones", "General Parking", "Bus Parking", "Motorcycle Parking"],
        default="All",
        key='map_type_filter'
    )
    selected_city_map = st.sidebar.radio("Map Focus:", ("Both Cities", "Heidelberg", "Bonn"), key='map_city_filter')


    # --- Section: Overall Summary ---
    if selected_view == "Overall Summary":
        st.subheader("1. Overall Parking Infrastructure Summary")
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("### Heidelberg Data Highlights")
            num_garages_hd = heidelberg_data['parking_garage'].shape[0] if 'parking_garage' in heidelberg_data and not heidelberg_data['parking_garage'].empty else 0
            total_spots_hd = heidelberg_data['parking_garage']['totalSpotNumber'].sum() if 'parking_garage' in heidelberg_data and not heidelberg_data['parking_garage'].empty and 'totalSpotNumber' in heidelberg_data['parking_garage'].columns else 0
            num_disabled_hd = heidelberg_data['disabled_parking'].shape[0] if 'disabled_parking' in heidelberg_data and not heidelberg_data['disabled_parking'].empty else 0

            st.metric("Parking Garages (Count)", num_garages_hd)
            st.metric("Total Garage Spots (Est.)", f"{total_spots_hd:,.0f}")
            st.metric("Disabled Spots (Count)", num_disabled_hd)
            st.markdown("*(Heidelberg excels in dynamic occupancy data for garages)*")

        with col2:
            st.markdown("### Bonn Data Highlights")
            num_garages_bn = bonn_data['parking_garages'].shape[0] if 'parking_garages' in bonn_data and not bonn_data['parking_garages'].empty else 0
            num_pr_bn = bonn_data['park_and_ride'].shape[0] if 'park_and_ride' in bonn_data and not bonn_data['park_and_ride'].empty else 0
            num_resident_bn = (bonn_data['resident_parking_1'].shape[0] + bonn_data['resident_parking_2'].shape[0]) if ('resident_parking_1' in bonn_data and 'resident_parking_2' in bonn_data) and (not bonn_data['resident_parking_1'].empty or not bonn_data['resident_parking_2'].empty) else 0
            num_motorcycle_bn = bonn_data['motorcycle_parking'].shape[0] if 'motorcycle_parking' in bonn_data and not bonn_data['motorcycle_parking'].empty else 0
            num_bus_bn = bonn_data['bus_parking'].shape[0] if 'bus_parking' in bonn_data and not bonn_data['bus_parking'].empty else 0
            num_general_bn = bonn_data['general_parking'].shape[0] if 'general_parking' in bonn_data and not bonn_data['general_parking'].empty else 0


            st.metric("Parking Garages (Count)", num_garages_bn)
            st.metric("Park & Ride Locations (Count)", num_pr_bn)
            st.metric("Resident Parking Zones (Count)", num_resident_bn)
            st.metric("Motorcycle Parking (Count)", num_motorcycle_bn)
            st.metric("Bus Parking (Count)", num_bus_bn)
            if num_general_bn == 0 and 'general_parking' in bonn_data and bonn_data['general_parking'].empty:
                st.warning("General Parking data for Bonn could not be loaded due to a URL error.")
            st.markdown("*(Bonn offers a broad static inventory of on-street parking types)*")


        st.markdown("---")
        st.subheader("2. Comparative Visualizations")

        st.markdown("#### Parking Facility Counts by Type")
        
        col_hd_counts, col_bn_counts = st.columns(2)

        with col_hd_counts:
            st.markdown("##### Heidelberg")
            heidelberg_counts_data = {
                'Parking Type': ['Parking Garages', 'Disabled Parking'],
                'Count': [num_garages_hd, num_disabled_hd]
            }
            hd_counts_df = pd.DataFrame(heidelberg_counts_data)
            fig_hd_counts = px.bar(
                hd_counts_df,
                x='Count',
                y='Parking Type',
                orientation='h',
                title='Heidelberg Parking Facilities',
                labels={'Count': 'Number of Facilities'},
                height=300
            )
            fig_hd_counts.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_hd_counts, use_container_width=True)

        with col_bn_counts:
            st.markdown("##### Bonn")
            bonn_counts_data = {
                'Parking Type': ['Parking Garages', 'Park & Ride', 'Resident Zones', 'Motorcycle Parking', 'Bus Parking'],
                'Count': [num_garages_bn, num_pr_bn, num_resident_bn, num_motorcycle_bn, num_bus_bn]
            }
            bn_counts_df = pd.DataFrame(bonn_counts_data)
            fig_bn_counts = px.bar(
                bn_counts_df,
                x='Count',
                y='Parking Type',
                orientation='h',
                title='Bonn Parking Facilities',
                labels={'Count': 'Number of Facilities'},
                height=300
            )
            fig_bn_counts.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_bn_counts, use_container_width=True)


    # --- Section: Data Assets Overview ---
    if selected_view == "Data Assets Overview":
        st.subheader("2. Data Assets: What Each City Provides")
        st.markdown("Below is an overview of the primary parking datasets available for each city, including their size and content summary.")

        col_assets_hd, col_assets_bn = st.columns(2)

        with col_assets_hd:
            st.markdown("#### Heidelberg Data Assets")
            hd_asset_summary = []
            for name, df in heidelberg_data.items():
                if not df.empty:
                    hd_asset_summary.append({
                        'Dataset Name': name,
                        'Rows': df.shape[0],
                        'Columns': df.shape[1],
                        'Description': {
                            'parking_garage': "Static information about parking garages (ID, name, address, capacity, coordinates).",
                            'disabled_parking': "Locations of dedicated disabled parking spots (name, operator, type, coordinates).",
                            'historical_p001': "Historical occupancy data (occupied vs. total spots) for a specific parking garage with timestamps.",
                            'current_p00': "Current/recent occupancy data for a specific parking garage."
                        }.get(name, "No description available.")
                    })
                else:
                    hd_asset_summary.append({'Dataset Name': name, 'Rows': 0, 'Columns': 0, 'Description': "No data loaded or empty."})
            
            st.dataframe(pd.DataFrame(hd_asset_summary).set_index('Dataset Name'), use_container_width=True)
            st.markdown("*(Heidelberg's strength: granular dynamic occupancy data for garages)*")


        with col_assets_bn:
            st.markdown("#### Bonn Data Assets")
            bonn_asset_summary = []
            for name, df in bonn_data.items():
                if not df.empty:
                    bonn_asset_summary.append({
                        'Dataset Name': name,
                        'Rows': df.shape[0],
                        'Columns': df.shape[1],
                        'Description': {
                            'resident_parking_1': "Geographical areas for resident parking zones (Part 1).",
                            'resident_parking_2': "Geographical areas for resident parking zones (Part 2).",
                            'park_and_ride': "Locations of Park & Ride facilities, often with capacity information.",
                            'parking_garages': "Static locations of parking garages.",
                            'general_parking': "Comprehensive locations for various general and specialized on-street parking types.",
                            'bus_parking': "Specific locations for bus parking.",
                            'motorcycle_parking': "Specific locations for motorcycle parking.",
                            'parking_bonn_koeln_osm': "OSM-derived parking data for Bonn and Cologne, likely covering various on-street and off-street parking points."
                        }.get(name, "No description available.")
                    })
                else:
                    bonn_asset_summary.append({'Dataset Name': name, 'Rows': 0, 'Columns': 0, 'Description': "No data loaded or empty."})
            
            st.dataframe(pd.DataFrame(bonn_asset_summary).set_index('Dataset Name'), use_container_width=True)
            st.markdown("*(Bonn's strength: broad static inventory of on-street parking types)*")


    # --- Section: Dataset Attributes Comparison ---
    if selected_view == "Dataset Attributes":
        st.subheader("3. Dataset Attributes: What Information is Available")
        st.markdown("This section compares the attributes (columns/fields) present in each city's datasets. Observing Bonn's comprehensive attributes can help Heidelberg identify areas to enrich its data.")

        col_attr_hd, col_attr_bn = st.columns(2)

        with col_attr_hd:
            st.markdown("#### Heidelberg Dataset Attributes")
            hd_attr_data = []
            for name, df in heidelberg_data.items():
                if not df.empty:
                    hd_attr_data.append({'Dataset': name, 'Attributes': ", ".join(df.columns.tolist())})
            if hd_attr_data:
                st.dataframe(pd.DataFrame(hd_attr_data), hide_index=True, use_container_width=True)
            else:
                st.info("No data loaded or empty for Heidelberg datasets.")


        with col_attr_bn:
            st.markdown("#### Bonn Dataset Attributes")
            bonn_attr_data = []
            for name, df in bonn_data.items():
                if not df.empty:
                    bonn_attr_data.append({'Dataset': name, 'Attributes': ", ".join(df.columns.tolist())})
            if bonn_attr_data:
                st.dataframe(pd.DataFrame(bonn_attr_data), hide_index=True, use_container_width=True)
            else:
                st.info("No data loaded or empty for Bonn datasets.")

        st.markdown("""
        *Comparing attributes highlights where Heidelberg might expand its data collection (e.g., more granular on-street restrictions, detailed facility info).*
        """)

    # --- Section: Data Quality Dashboard ---
    if selected_view == "Data Quality Dashboard":
        st.subheader("4. Data Quality Dashboard")
        st.markdown("This section visualizes the completeness of datasets by showing the percentage of missing values per column for each dataset. A higher percentage indicates more missing information for that attribute.")

        st.markdown("#### Heidelberg Missing Values")
        for dataset_name, df in heidelberg_data.items():
            if not df.empty:
                missing_report = get_missing_values_report({dataset_name: df}, "Heidelberg")
                if not missing_report.empty:
                    fig = px.bar(
                        missing_report,
                        x='Missing Percentage',
                        y='Column',
                        orientation='h',
                        title=f'Missing Values in Heidelberg: {dataset_name}',
                        labels={'Missing Percentage': 'Missing %', 'Column': 'Attribute'},
                        height=min(400, 50 * len(missing_report)), # Adjust height dynamically
                        range_x=[0, 100]
                    )
                    fig.update_layout(yaxis={'categoryorder':'total ascending'}) # Order by missing percentage
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info(f"No missing values found in Heidelberg: {dataset_name}.")
            else:
                st.info(f"Heidelberg dataset '{dataset_name}' is empty.")

        st.markdown("#### Bonn Missing Values")
        for dataset_name, df in bonn_data.items():
            if not df.empty:
                missing_report = get_missing_values_report({dataset_name: df}, "Bonn")
                if not missing_report.empty:
                    fig = px.bar(
                        missing_report,
                        x='Missing Percentage',
                        y='Column',
                        orientation='h',
                        title=f'Missing Values in Bonn: {dataset_name}',
                        labels={'Missing Percentage': 'Missing %', 'Column': 'Attribute'},
                        height=min(400, 50 * len(missing_report)), # Adjust height dynamically
                        range_x=[0, 100]
                    )
                    fig.update_layout(yaxis={'categoryorder':'total ascending'}) # Order by missing percentage
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info(f"No missing values found in Bonn: {dataset_name}.")
            else:
                st.info(f"Bonn dataset '{dataset_name}' is empty.")

        st.markdown("""
        *These visualizations help quickly identify which datasets and attributes have the most data gaps, impacting their utility and reliability.*
        """)

    # --- Section: Geographic Distribution ---
    if selected_view == "Geographic Distribution":
        st.subheader("5. Geographic Distribution of Parking Facilities")
        st.markdown("Navigate the map to explore parking locations. Use the sidebar filter to select specific types.")


        # Determine initial map center and zoom based on selected city
        map_center = [50.0, 8.0] # Default to central Germany
        map_zoom = 7

        if selected_city_map == "Heidelberg":
            map_center = [49.4076, 8.6908] # Heidelberg coordinates
            map_zoom = 13
        elif selected_city_map == "Bonn":
            map_center = [50.7374, 7.0982] # Bonn coordinates
            map_zoom = 13

        m = folium.Map(location=map_center, zoom_start=map_zoom)

        # Add Heidelberg data to map
        if selected_city_map in ("Both Cities", "Heidelberg"):
            if ("Parking Garages" in selected_parking_types_map or "All" in selected_parking_types_map) and 'parking_garage' in heidelberg_data and not heidelberg_data['parking_garage'].empty:
                for idx, row in heidelberg_data['parking_garage'].iterrows():
                    lat = row.get('latitude')
                    lon = row.get('longitude')
                    if pd.notna(lat) and pd.notna(lon):
                        folium.Marker(
                            location=[lat, lon],
                            popup=f"Heidelberg Garage: {row.get('name', 'N/A')}<br>Total Spots: {row.get('totalSpotNumber', 'N/A')}",
                            icon=folium.Icon(color='blue', icon='car', prefix='fa')
                        ).add_to(m)

            if ("Disabled Parking" in selected_parking_types_map or "All" in selected_parking_types_map) and 'disabled_parking' in heidelberg_data and not heidelberg_data['disabled_parking'].empty:
                for idx, row in heidelberg_data['disabled_parking'].iterrows():
                    lat = row.get('latitude')
                    lon = row.get('longitude')
                    if pd.notna(lat) and pd.notna(lon):
                        folium.Marker(
                            location=[lat, lon],
                            popup=f"Heidelberg Disabled: {row.get('BEZEICHNUN', 'N/A')}",
                            icon=folium.Icon(color='purple', icon='wheelchair', prefix='fa')
                        ).add_to(m)


        # Add Bonn data to map
        if selected_city_map in ("Both Cities", "Bonn"):
            if ("Parking Garages" in selected_parking_types_map or "All" in selected_parking_types_map) and 'parking_garages' in bonn_data and not bonn_data['parking_garages'].empty:
                for idx, row in bonn_data['parking_garages'].iterrows():
                    if pd.notna(row.get('latitude')) and pd.notna(row.get('longitude')):
                        folium.Marker(
                            location=[row['latitude'], row['longitude']],
                            popup=f"Bonn Garage: {row.get('name', 'N/A')}<br>Capacity: {row.get('capacity', 'N/A')}",
                            icon=folium.Icon(color='red', icon='warehouse', prefix='fa')
                        ).add_to(m)
            if ("Park & Ride" in selected_parking_types_map or "All" in selected_parking_types_map) and 'park_and_ride' in bonn_data and not bonn_data['park_and_ride'].empty:
                for idx, row in bonn_data['park_and_ride'].iterrows():
                    if pd.notna(row.get('latitude')) and pd.notna(row.get('longitude')):
                        folium.Marker(
                            location=[row['latitude'], row['longitude']],
                            popup=f"Bonn P&R: {row.get('name', 'N/A')}<br>Capacity: {row.get('capacity', 'N/A')}",
                            icon=folium.Icon(color='green', icon='train', prefix='fa')
                        ).add_to(m)
            if ("Resident Zones" in selected_parking_types_map or "All" in selected_parking_types_map) and ('resident_parking_1' in bonn_data and not bonn_data['resident_parking_1'].empty or 'resident_parking_2' in bonn_data and not bonn_data['resident_parking_2'].empty):
                if 'resident_parking_1' in bonn_data and not bonn_data['resident_parking_1'].empty:
                    for idx, row in bonn_data['resident_parking_1'].iterrows():
                        if pd.notna(row.get('latitude')) and pd.notna(row.get('longitude')):
                            folium.CircleMarker(
                                location=[row['latitude'], row['longitude']],
                                radius=5,
                                color='orange',
                                fill=True,
                                fill_color='orange',
                                popup=f"Bonn Resident Zone: {row.get('bezeichnung', 'N/A')}"
                            ).add_to(m)
                if 'resident_parking_2' in bonn_data and not bonn_data['resident_parking_2'].empty:
                    for idx, row in bonn_data['resident_parking_2'].iterrows():
                        if pd.notna(row.get('latitude')) and pd.notna(row.get('longitude')):
                            folium.CircleMarker(
                                location=[row['latitude'], row['longitude']],
                                radius=5,
                                color='orange',
                                fill=True,
                                fill_color='orange',
                                popup=f"Bonn Resident Zone: {row.get('bezeichnung', 'N/A')}"
                            ).add_to(m)
            if ("Motorcycle Parking" in selected_parking_types_map or "All" in selected_parking_types_map) and 'motorcycle_parking' in bonn_data and not bonn_data['motorcycle_parking'].empty:
                for idx, row in bonn_data['motorcycle_parking'].iterrows():
                    if pd.notna(row.get('latitude')) and pd.notna(row.get('longitude')):
                        folium.Marker(
                            location=[row['latitude'], row['longitude']],
                            popup=f"Bonn Motorcycle Parking: {row.get('bezeichnung', 'N/A')}",
                            icon=folium.Icon(color='lightgray', icon='motorcycle', prefix='fa')
                        ).add_to(m)
            if ("Bus Parking" in selected_parking_types_map or "All" in selected_parking_types_map) and 'bus_parking' in bonn_data and not bonn_data['bus_parking'].empty:
                for idx, row in bonn_data['bus_parking'].iterrows():
                    if pd.notna(row.get('latitude')) and pd.notna(row.get('longitude')):
                        folium.Marker(
                            location=[row['latitude'], row['longitude']],
                            popup=f"Bonn Bus Parking: {row.get('bezeichnung', 'N/A')}",
                            icon=folium.Icon(color='darkblue', icon='bus', prefix='fa')
                        ).add_to(m)

        folium_static(m, width=1000, height=600)

        st.markdown("""
        *This interactive map visually contrasts the spatial distribution of parking facilities. Bonn's data allows for mapping more specific on-street parking types.*
        """)


    # --- Section: Recommendations ---
    if selected_view == "Recommendations":
        st.subheader("6. Recommendations for Heidelberg's Open Data Portal")
        st.markdown("""
        Based on the comparative analysis with Bonn's data, here are key recommendations for Heidelberg to enhance its open parking data:
        """)

        recommendations_data = [
            {
                "Issue": "Heidelberg's current data lacks granular detail for various on-street parking types (e.g., resident zones, motorcycle, bus parking).",
                "Recommendation": "Introduce new datasets and categories for on-street parking types, similar to Bonn's comprehensive approach.",
                "Benefit": "Provides a more complete picture of the city's overall parking infrastructure, aiding urban planning and diverse user navigation."
            },
            {
                "Issue": "Explicit capacity information for on-street parking is often missing in Heidelberg's datasets.",
                "Recommendation": "Include `capacity` attributes consistently across all new and existing on-street parking datasets.",
                "Benefit": "Enables quantitative analysis of on-street availability and improves resource allocation for better parking management."
            },
            {
                "Issue": "Dynamic occupancy data is currently limited to parking garages.",
                "Recommendation": "Investigate and implement methods (e.g., sensors) to collect and publish real-time/historical occupancy data for key on-street parking areas.",
                "Benefit": "Extends Heidelberg's existing strength in dynamic data to the on-street environment, significantly enhancing utility for drivers seeking real-time information."
            },
            {
                "Issue": "Potential data gaps in critical fields (e.g., location coordinates, identifiers) across datasets.",
                "Recommendation": "Implement regular data quality checks and robust validation processes to ensure the completeness and reliability of all open data.",
                "Benefit": "Guarantees higher reliability and usability of all open data for applications, analyses, and informed decision-making."
            },
            {
                "Issue": "Maximize the impact and value derived from open data initiatives.",
                "Recommendation": "Actively highlight and promote the diverse types of parking data available (both existing and newly added) to encourage third-party app development, academic research, and public engagement.",
                "Benefit": "Increases the overall impact and value derived from Heidelberg's open data portal, fostering innovation and better citizen services."
            }
        ]

        # Display recommendations without columns, using clear formatting
        for i, rec in enumerate(recommendations_data):
            st.markdown(f"**{i+1}. Issue:** {rec['Issue']}")
            st.markdown(f"**Recommendation:** {rec['Recommendation']}")
            st.markdown(f"**Benefit:** {rec['Benefit']}")
            if i < len(recommendations_data) - 1:
                st.markdown("---") # Separator between each recommendation set
        
if __name__ == "__main__":
    main()
