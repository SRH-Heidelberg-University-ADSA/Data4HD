import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from datetime import datetime, timedelta
import random
import json
import os
import folium
from streamlit_folium import folium_static

# --- Configuration and Page Setup ---
st.set_page_config(
    layout="wide",
    page_title="Transparency & Insights: Heidelberg City Council Data"
)
st.title("Transparency & Insights: Heidelberg Council Data")
st.markdown("""
Welcome to the interactive dashboard for Heidelberg City Council data. This application provides insights into various aspects of city governance, including projects, and decisions. Use the sidebar to navigate between data categories and filter the visualizations.

Note: Data for **Decisions** and **People** is loaded from the provided JSON files. Other sections use simulated data for demonstration purposes.
""")

# --- Define the base directory for the data files ---
# This path has been updated to point directly to the directory
# where you unzipped the files.
BASE_PATH = '/Users/azadehhabibiandehkordi/Heidelberg_Council_JSON'

# --- Data Loading and Preprocessing Functions (using real data) ---

@st.cache_data
def load_and_preprocess_decisions():
    """
    Loads and preprocesses council decisions data from provided JSON file.
    """
    file_path = os.path.join(BASE_PATH, 'tagesordnungspunkte-ratsinformationssystem-stadt-heidelberg-oparl_33f7b659-43f4-4d57-b43b-30ed5d7802d6.json')
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        df = pd.DataFrame(data['data'])
        
        # Clean and process data
        df['created'] = pd.to_datetime(df['created']).dt.tz_convert(None)
        
        # Extract the decision status from the 'result' field
        # The 'result' field can sometimes be empty, so handle it gracefully
        df['status'] = df['result'].fillna('No result').str.strip()
        
        # Filter out "No result" and "Kenntnis genommen" (taken note of)
        df_filtered = df[~df['status'].isin(['No result', 'Kenntnis genommen'])]

        return df, df_filtered
    except FileNotFoundError:
        st.error(f"Decisions data file not found at: {file_path}. Please ensure the file is in the correct directory.")
        return pd.DataFrame(), pd.DataFrame()

@st.cache_data
def load_and_preprocess_people():
    """
    Loads and preprocesses council members and their organization data.
    """
    try:
        people_path = os.path.join(BASE_PATH, 'personen-ratsinformationssystem-stadt-heidelberg-oparl_f4cff9e2-a2fc-4ba9-a7b0-955e312b72cd.json')
        org_path = os.path.join(BASE_PATH, 'organisationen-ratsinformationssystem-stadt-heidelberg-oparl_c9b68473-42c7-4992-a574-6618caba978c.json')
        membership_path = os.path.join(BASE_PATH, 'mitgliedschaften-ratsinformationssystem-stadt-heidelberg-oparl_8c2e8115-15bf-4a03-858a-a4277df36b87.json')

        with open(people_path, 'r', encoding='utf-8') as f:
            people_data = json.load(f)['data']
        with open(org_path, 'r', encoding='utf-8') as f:
            org_data = json.load(f)['data']
        with open(membership_path, 'r', encoding='utf-8') as f:
            membership_data = json.load(f)['data']

        # Create DataFrames
        df_people = pd.DataFrame(people_data)
        df_orgs = pd.DataFrame(org_data)
        df_memberships = pd.DataFrame(membership_data)

        # Merge memberships with people data first
        df_members = df_memberships.merge(
            df_people[['id', 'name']],
            left_on='person',
            right_on='id',
            how='left'
        ).rename(columns={'name': 'name_person'})

        # Now merge the result with organizations data
        df_members = df_members.merge(
            df_orgs[['id', 'name']],
            left_on='organization',
            right_on='id',
            how='left'
        ).rename(columns={'name': 'name_org'})

        # Select final columns and rename for display
        df_members_final = df_members[['name_person', 'name_org', 'role', 'startDate']]
        df_members_final.columns = ['Name', 'Organization', 'Role', 'Start Date']
        
        # Fill any missing values with a placeholder for a cleaner display
        df_members_final = df_members_final.fillna('Unknown')

        # Clean up organization names, focus on political groups
        df_members_final['Organization'] = df_members_final['Organization'].str.replace('Fraktion der ', '').str.replace('Fraktionsgemeinschaft ', '')
        
        return df_members_final
    except FileNotFoundError as e:
        st.error(f"A people/organizations data file was not found. Please ensure all required JSON files are in the correct directory. Error: {e}")
        return pd.DataFrame()


# --- Data Simulation Functions (kept for categories without provided data) ---

@st.cache_data
def load_and_preprocess_budgets():
    st.warning("This data is simulated for demonstration purposes. A real-world application would use actual budget data.")
    # The fix is here:
    # Ensure all lists have the same length (20)
    financial_years = pd.to_datetime(np.repeat([f'202{i}-01-01' for i in range(1, 5)], 5))
    departments = ['Administration', 'Public Services', 'Culture & Education', 'Infrastructure', 'Community Projects'] * 4
    
    data = {
        'financial_year': financial_years,
        'department': departments,
        'planned_budget': np.random.randint(100, 500, size=20) * 1000000,
        'expenditure': np.random.randint(80, 450, size=20) * 1000000
    }
    df = pd.DataFrame(data)
    df['efficiency'] = (df['expenditure'] / df['planned_budget']) * 100
    return df

@st.cache_data
def load_and_preprocess_projects():
    st.warning("This data is simulated for demonstration purposes. A real-world application would use actual project data.")
    start_date = datetime(2022, 1, 1)
    end_date = datetime.now()
    project_names = [f'Project {i+1}' for i in range(30)]
    data = {
        'project_name': project_names,
        'start_date': [start_date + timedelta(days=random.randint(0, 700)) for _ in range(30)],
        'status': [random.choice(['Ongoing', 'Completed', 'Planned']) for _ in range(30)],
        'department': [random.choice(['Planning', 'Public Works', 'Community']) for _ in range(30)],
        'progress_percent': [random.randint(0, 100) if random.choice([True, False]) else None for _ in range(30)],
        'latitude': [random.uniform(49.38, 49.42) for _ in range(30)],
        'longitude': [random.uniform(8.65, 8.72) for _ in range(30)]
    }
    df = pd.DataFrame(data)
    return df

@st.cache_data
def load_and_preprocess_services():
    st.warning("This data is simulated for demonstration purposes. A real-world application would use actual service data.")
    services = ['Housing', 'Waste Management', 'Education', 'Culture', 'Transport']
    data = {
        'year': np.repeat(np.arange(2021, 2025), len(services)),
        'service_type': services * 4,
        'planned_usage': np.random.randint(1000, 5000, size=20),
        'actual_usage': np.random.randint(900, 5200, size=20)
    }
    df = pd.DataFrame(data)
    return df

@st.cache_data
def load_and_preprocess_demographics():
    st.warning("This data is simulated for demonstration purposes. A real-world application would use actual demographic data.")
    age_groups = ['0-14', '15-29', '30-44', '45-59', '60-74', '75+']
    years = [2018, 2019, 2020, 2021, 2022, 2023]
    data = {
        'year': np.repeat(years, len(age_groups)),
        'age_group': age_groups * len(years),
        'population': np.random.randint(10000, 30000, size=len(age_groups) * len(years)),
        'migration_in': np.random.randint(500, 2000, size=len(age_groups) * len(years)),
        'migration_out': np.random.randint(400, 1800, size=len(age_groups) * len(years)),
    }
    df = pd.DataFrame(data)
    return df

# --- Sidebar for Navigation ---
st.sidebar.header("Explore Data Categories")
category = st.sidebar.radio(
    "Select a category:",
    ["Decisions", "People", "Budgets", "Projects", "Services", "Demographics"]
)

# --- Dynamic Content based on selected category ---
st.sidebar.markdown("---")
st.sidebar.header("Filter Options")

if category == "Decisions":
    df_all_decisions, df_decisions = load_and_preprocess_decisions()
    st.subheader("Council Decisions")

    if not df_all_decisions.empty:
        # Date range selector
        min_date = df_all_decisions['created'].min().date()
        max_date = df_all_decisions['created'].max().date()
        date_range = st.sidebar.date_input(
            "Select Date Range:",
            (min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )

        if len(date_range) == 2:
            filtered_df = df_decisions[
                (df_decisions['created'].dt.date >= date_range[0]) &
                (df_decisions['created'].dt.date <= date_range[1])
            ]
        else:
            filtered_df = df_decisions

        # Status filter
        status_options = sorted(filtered_df['status'].unique())
        selected_statuses = st.sidebar.multiselect("Filter by Status:", status_options, default=status_options)
        filtered_df = filtered_df[filtered_df['status'].isin(selected_statuses)]
        
        # KPIs
        st.markdown("### Key Metrics")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Agenda Items", df_all_decisions.shape[0])
        with col2:
            st.metric("Total Filtered Decisions", filtered_df.shape[0])
        with col3:
            approved_count = (filtered_df['status'] == 'Beschlossen').sum()
            st.metric("Decided ('Beschlossen')", approved_count)

        # Visualizations
        st.markdown("### Decision Trends Over Time")
        decision_trend = filtered_df.groupby(filtered_df['created'].dt.to_period('M')).size().reset_index(name='count')
        decision_trend['created'] = decision_trend['created'].astype(str)
        fig_line = px.line(
            decision_trend,
            x='created',
            y='count',
            title='Number of Decisions Per Month',
            labels={'created': 'Month', 'count': 'Number of Decisions'},
            template='plotly_white',
        )
        st.plotly_chart(fig_line, use_container_width=True)

        st.markdown("### Decision Status Distribution")
        status_counts = filtered_df['status'].value_counts().reset_index()
        status_counts.columns = ['status', 'count']
        
        # Changed from pie chart to horizontal bar chart
        fig_bar = px.bar(
            status_counts,
            x='count',
            y='status',
            orientation='h', # The key to making the bars horizontal
            text='count',
            title='Distribution of Decision Statuses',
            labels={'count': 'Number of Decisions', 'status': 'Decision Status'},
            color_discrete_sequence=px.colors.qualitative.Plotly
        )
        fig_bar.update_layout(
            yaxis={'categoryorder': 'total ascending'},
            xaxis_title="Number of Decisions",
            yaxis_title=""
        )
        st.plotly_chart(fig_bar, use_container_width=True)

        st.markdown("---")
        st.markdown("### 💡 Insights")
        if not status_counts.empty:
            most_common_status = status_counts.loc[status_counts['count'].idxmax()]
            st.write(f"The most frequent decision status is **'{most_common_status['status']}'**.")
        
        st.markdown("### Full List of Filtered Decisions")
        st.dataframe(filtered_df[['name', 'status', 'created']].sort_values(by='created', ascending=False), use_container_width=True)


elif category == "People":
    df_members = load_and_preprocess_people()
    st.subheader("Heidelberg City Council Members")

    if not df_members.empty:
        # Organization filter
        org_options = sorted(df_members['Organization'].unique())
        selected_orgs = st.sidebar.multiselect("Filter by Organization/Party:", org_options, default=org_options)
        filtered_df = df_members[df_members['Organization'].isin(selected_orgs)]

        # KPIs
        st.markdown("### Key Metrics")
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Members", df_members['Name'].nunique())
        with col2:
            st.metric("Filtered Members", filtered_df['Name'].nunique())

        # Visualizations
        st.markdown("### Members by Organization")
        org_counts = filtered_df.groupby('Organization')['Name'].nunique().reset_index(name='count')
        
        # Corrected: Changed from pie chart to horizontal bar chart
        fig_bar = px.bar(
            org_counts.sort_values('count', ascending=False),
            x='count',
            y='Organization',
            orientation='h',
            title='Number of Members by Political Organization',
            labels={'count': 'Number of Members', 'Organization': 'Organization'},
            template='plotly_white'
        )
        st.plotly_chart(fig_bar, use_container_width=True)

        st.markdown("### List of Council Members")
        st.dataframe(filtered_df[['Name', 'Organization']].sort_values(by='Organization'), use_container_width=True)


elif category == "Budgets":
    df_budgets = load_and_preprocess_budgets()
    st.subheader("Budgets & Expenditures (Simulated Data)")

    # Time range slider
    years = sorted(df_budgets['financial_year'].dt.year.unique())
    selected_years = st.sidebar.slider(
        "Select Financial Year Range:",
        min_value=min(years),
        max_value=max(years),
        value=(min(years), max(years))
    )
    filtered_df = df_budgets[
        (df_budgets['financial_year'].dt.year >= selected_years[0]) &
        (df_budgets['financial_year'].dt.year <= selected_years[1])
    ]

    # Department filter
    departments = filtered_df['department'].unique()
    selected_departments = st.sidebar.multiselect("Filter by Department:", departments, default=departments)
    filtered_df = filtered_df[filtered_df['department'].isin(selected_departments)]

    # KPIs
    st.markdown("### Key Metrics")
    col1, col2 = st.columns(2)
    with col1:
        total_budget = filtered_df['planned_budget'].sum()
        st.metric("Total Planned Budget", f"€{total_budget:,.0f}")
    with col2:
        total_expenditure = filtered_df['expenditure'].sum()
        st.metric("Total Expenditure", f"€{total_expenditure:,.0f}")

    # Visualizations
    st.markdown("### Budget Trends Over Time")
    budget_trend_df = filtered_df.groupby('financial_year')[['planned_budget', 'expenditure']].sum().reset_index()
    fig_line = px.line(
        budget_trend_df,
        x='financial_year',
        y=['planned_budget', 'expenditure'],
        title='Budget vs. Expenditure Over Time',
        labels={'value': 'Amount (€)', 'financial_year': 'Financial Year', 'variable': 'Type'},
        template='plotly_white',
    )
    st.plotly_chart(fig_line, use_container_width=True)

    st.markdown("### Departmental Expenditure Distribution")
    dept_expenditure = filtered_df.groupby('department')['expenditure'].sum().reset_index()
    fig_pie = px.pie(
        dept_expenditure,
        values='expenditure',
        names='department',
        title='Expenditure by Department',
        template='plotly_white',
    )
    fig_pie.update_traces(textposition='inside', textinfo='percent+label')
    st.plotly_chart(fig_pie, use_container_width=True)

    st.markdown("---")
    st.markdown("### 💡 Insights")
    avg_efficiency = (total_expenditure / total_budget) * 100 if total_budget > 0 else 0
    st.write(f"The average spending efficiency across the selected period is **{avg_efficiency:.2f}%**.")
    highest_spending_dept = dept_expenditure.loc[dept_expenditure['expenditure'].idxmax()]
    st.write(f"The department with the highest total expenditure is **{highest_spending_dept['department']}**.")


elif category == "Projects":
    df_projects = load_and_preprocess_projects()
    st.subheader("Community Projects & Initiatives (Simulated Data)")

    # Project status filter
    status_options = df_projects['status'].unique()
    selected_status = st.sidebar.multiselect("Filter by Project Status:", status_options, default=status_options)
    filtered_df = df_projects[df_projects['status'].isin(selected_status)]

    # KPIs
    st.markdown("### Key Metrics")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Projects", filtered_df.shape[0])
    with col2:
        st.metric("Ongoing Projects", (filtered_df['status'] == 'Ongoing').sum())
    with col3:
        st.metric("Completed Projects", (filtered_df['status'] == 'Completed').sum())

    # Visualizations
    st.markdown("### Project Status Overview")
    status_counts = filtered_df['status'].value_counts().reset_index()
    status_counts.columns = ['status', 'count']
    fig_bar = px.bar(
        status_counts,
        x='status',
        y='count',
        title='Number of Projects by Status',
        labels={'status': 'Status', 'count': 'Number of Projects'},
        color='status',
        template='plotly_white',
    )
    st.plotly_chart(fig_bar, use_container_width=True)
    
    st.markdown("### Geographic Distribution of Projects ")
    m = folium.Map(location=[49.4076, 8.6908], zoom_start=13)
    for index, row in filtered_df.iterrows():
        if pd.notna(row['latitude']) and pd.notna(row['longitude']):
            folium.Marker(
                location=[row['latitude'], row['longitude']],
                popup=f"Project: {row['project_name']}<br>Status: {row['status']}",
                icon=folium.Icon(color='blue', icon='info-sign')
            ).add_to(m)
    
    folium_static(m)

elif category == "Services":
    df_services = load_and_preprocess_services()
    st.subheader("Public Services Usage (Simulated Data)")

    # Service type filter
    service_options = df_services['service_type'].unique()
    selected_services = st.sidebar.multiselect("Filter by Service Type:", service_options, default=service_options)
    filtered_df = df_services[df_services['service_type'].isin(selected_services)]

    # KPIs
    st.markdown("### Key Metrics")
    col1, col2 = st.columns(2)
    with col1:
        total_planned = filtered_df['planned_usage'].sum()
        st.metric("Total Planned Usage", f"{total_planned:,.0f}")
    with col2:
        total_actual = filtered_df['actual_usage'].sum()
        st.metric("Total Actual Usage", f"{total_actual:,.0f}")

    # Visualizations
    st.markdown("### Planned vs. Actual Service Usage Trends")
    st.markdown("This chart separates each service into its own pane for a clearer, more professional comparison of planned vs. actual usage over time.")
    
    df_melted = filtered_df.melt(
        id_vars=['year', 'service_type'],
        value_vars=['planned_usage', 'actual_usage'],
        var_name='usage_type',
        value_name='usage_count'
    )
    
    fig_line = px.line(
        df_melted,
        x='year',
        y='usage_count',
        color='usage_type',
        facet_col='service_type',
        facet_col_wrap=2,
        title='Planned vs. Actual Service Usage Trends by Type',
        labels={'usage_count': 'Usage Count', 'year': 'Year', 'usage_type': 'Usage Type'},
        template='plotly_white',
    )
    fig_line.update_layout(height=400)
    fig_line.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
    fig_line.update_xaxes(showgrid=True)
    st.plotly_chart(fig_line, use_container_width=True)

    st.markdown("### Usage Variance by Service Type")
    st.markdown("This bar chart highlights the difference between actual and planned usage, making it easy to see which services are exceeding or falling short of expectations.")
    
    service_usage_df = filtered_df.groupby('service_type')[['planned_usage', 'actual_usage']].sum().reset_index()
    service_usage_df['variance'] = service_usage_df['actual_usage'] - service_usage_df['planned_usage']
    
    fig_bar = px.bar(
        service_usage_df,
        x='service_type',
        y='variance',
        title='Service Usage Variance (Actual - Planned)',
        labels={'variance': 'Usage Variance', 'service_type': 'Service Type'},
        template='plotly_white',
    )
    st.plotly_chart(fig_bar, use_container_width=True)

elif category == "Demographics":
    df_demographics = load_and_preprocess_demographics()
    st.subheader("Demographic Insights (Simulated Data)")

    # Year slider
    years = sorted(df_demographics['year'].unique())
    selected_years = st.sidebar.slider(
        "Select Year Range:",
        min_value=min(years),
        max_value=max(years),
        value=(min(years), max(years))
    )
    filtered_df = df_demographics[
        (df_demographics['year'] >= selected_years[0]) &
        (df_demographics['year'] <= selected_years[1])
    ]

    # KPIs
    st.markdown("### Key Metrics")
    total_pop = filtered_df.groupby('year')['population'].sum().reset_index()
    col1, col2 = st.columns(2)
    with col1:
        start_pop = total_pop[total_pop['year'] == selected_years[0]]['population'].sum()
        end_pop = total_pop[total_pop['year'] == selected_years[1]]['population'].sum()
        growth = ((end_pop - start_pop) / start_pop) * 100 if start_pop > 0 else 0
        st.metric("Total Population Growth", f"{growth:,.2f}%")
    with col2:
        net_migration = filtered_df['migration_in'].sum() - filtered_df['migration_out'].sum()
        st.metric("Net Migration", f"{net_migration:,.0f}")

    # Visualizations
    st.markdown("### Population Growth by Year")
    fig_pop = px.line(
        total_pop,
        x='year',
        y='population',
        title='Total Population Over Time',
        labels={'year': 'Year', 'population': 'Population Count'},
        template='plotly_white',
    )
    st.plotly_chart(fig_pop, use_container_width=True)

    st.markdown("### Population Distribution by Age Group")
    age_pop = filtered_df.groupby('age_group')['population'].sum().reset_index()
    fig_bar = px.bar(
        age_pop,
        x='age_group',
        y='population',
        title='Population by Age Group',
        labels={'age_group': 'Age Group', 'population': 'Population Count'},
        template='plotly_white',
        category_orders={"age_group": ['0-14', '15-29', '30-44', '45-59', '60-74', '75+']}
    )
    st.plotly_chart(fig_bar, use_container_width=True)

# --- Instructions to Run the App ---
st.sidebar.markdown("---")
st.sidebar.markdown("""
### How to Run this App:
1.  Save the code as `heidelberg_dashboard.py`.
2.  Ensure you have the required packages installed:
    `pip install streamlit pandas plotly-express folium`
3.  Run the app from your terminal:
    `streamlit run heidelberg_dashboard.py`
""")

# --- Recommendations Section ---
st.markdown("---")
st.header("Recommendations & Insights")
st.markdown("""
This dashboard leverages the provided open data from Heidelberg's city council. Based on this, here are some recommendations for how this data can be further utilized:

1.  **Integrate more data:** Incorporate additional datasets, such as budget or project information, to create a more comprehensive and holistic view of city governance.
2.  **Granular analysis:** Use the `name` field in the Decisions section to perform text analysis and identify recurring themes and topics of discussion.
3.  **Real-time updates:** Implement a live connection to the OParl API to ensure the dashboard always shows the latest data.
4.  **Community Engagement:** Add features for citizens to comment on or interact with specific decisions and documents, fostering greater public participation.
""")
