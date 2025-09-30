# Data4HD: Collaborative Open Data for a Transparent and Sustainable Heidelberg 🌳

This repository contains the full project work, analysis, and final report for the Data4HD case study, a four-month Master's degree project (April - September 2025) conducted in collaboration between SRH University Heidelberg and the City of Heidelberg.

## 🚀 Overview

The Data4HD project aimed to analyze and enhance the City of Heidelberg's open data infrastructure to foster transparency, accountability, and data-driven urban governance. Our team conducted a comprehensive analysis of existing datasets, benchmarked Heidelberg's practices against other leading German cities, and developed a strategic roadmap with actionable recommendations.

## 🎯 Key Focus Areas

Our research was guided by three central, user-centric questions:

* **🅿️ Accessible Parking Infrastructure**: How well is Heidelberg equipped with accessible parking spaces, and are there areas that need improvement?
* **🌦️ Impact of Weather on Mobility**: How do weather conditions like temperature and precipitation affect traffic volume and transportation choices in Heidelberg?
* **🏛️ Analysis of Open Council Data**: Which agenda items have shaped political discourse in Heidelberg over the past years, and how have priorities shifted over time?

## 🔬 Methodology

Our approach combined data analysis, comparative research, and user-centric design principles:

* **Agile Framework**: The project was managed in sprints, allowing for iterative progress and adaptation to new findings.
* **User-Centric Design**: We developed two key personas—Dr. Evelyn Reed (Data Scientist) and David Miller (Everyday Resident)—to ensure our analysis and recommendations remained grounded in real-world needs.
* **Comparative Analysis**: We systematically benchmarked Heidelberg's open data against other cities, primarily Bonn for its comprehensive parking data and Karlsruhe for its developer-friendly council data API.
* **Data Processing & Modeling**: We programmatically acquired data via APIs, processed it using Python, and developed an Entity-Relationship Diagram (ERD) to model the complex relationships within Heidelberg's parking datasets.

## 📊 Key Findings

Our analysis across the three focus areas yielded several key insights:

### Parking Infrastructure

* **Strength**: Heidelberg excels in providing high-quality, dynamic (real-time) occupancy data for its off-street parking garages.
* **Weakness**: The city's open data portal lacks a broad inventory of static on-street parking data (e.g., resident zones, motorcycle parking), an area where the City of Bonn provides extensive datasets.
* **Data Quality**: We identified significant data quality issues, including logical errors (e.g., negative counts), pervasive missing values, and inconsistent, unstructured data, especially in the disabled parking dataset.

### Mobility & Climate

* **Transportation Mix**: Private cars are the dominant mode of transport at 35%, but sustainable options like cycling (25%) and public transport (20%) are strong contenders.
* **Climate Progress**: Heidelberg has successfully reduced its CO₂ emissions by 40% since 1990. However, climate data also shows a clear trend of increasing annual rainfall, highlighting the need for data-driven climate adaptation strategies.

### Open Council Data

* **Accessibility Barrier**: Heidelberg primarily provides council data as PDF documents. While transparent, this format is not machine-readable and hinders automated analysis.
* **Best Practice Example**: The City of Karlsruhe offers a developer-friendly REST API for its council data, enabling scalable research and the development of civic tech applications—a model Heidelberg could adopt.

## ✨ Recommendations

Based on our findings, we proposed five key strategic recommendations for the City of Heidelberg:

* **Expand Data Inventory**: Introduce new datasets for on-street parking types to provide a holistic view of the city's infrastructure.
* **Enrich Data Attributes**: Consistently include critical attributes like capacity, tariffs, and operating_hours in all parking datasets.
* **Extend Dynamic Data**: Leverage existing strengths by implementing methods to collect and publish real-time occupancy data for key on-street parking areas.
* **Implement Robust Data Quality Assurance**: Establish a formal data governance framework with automated validation and clear metadata standards.
* **Promote Data Usage & Foster Innovation**: Actively engage with the local tech community and universities to encourage the development of third-party applications.

## 💻 Technology Stack

This project utilized a variety of tools for data analysis, visualization, and presentation:

* **Data Analysis**: Python (with Pandas, Matplotlib)
* **Dashboarding & Web Apps**: Streamlit
* **Diagramming & Design**: Draw.io, Canva
* **Data Handling**: Microsoft Excel
* **Reporting**: LaTeX (via Overleaf)

## ✍️ Authors

This project was developed by a team of three Master's students from SRH University Heidelberg:
* Kavya Gopalaiah
* Samhitha Kalinganahali Sureshkumar
* Azadeh Habibiandehkordi

## 📜 Final Report

For a complete and detailed account of our methodology, deep-dive analysis, and comprehensive findings, please see our Final Project Report. (Note: You will need to place your final PDF report in a `/report` folder and name it `Final_Report.pdf` for this link to work).
