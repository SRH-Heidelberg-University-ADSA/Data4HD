import requests
import pandas as pd

# Fetch all parking-related datasets
api_search_url = "https://ckan.datenplattform.heidelberg.de/api/3/action/package_search?q=parking"

try:
    # Add headers and timeout to prevent hanging
    response = requests.get(
        api_search_url,
        headers={'Accept': 'application/json'},
        timeout=10
    )
    
    # Check if request was successful
    response.raise_for_status()
    
    # Debug raw response
    print(f"Raw response (first 200 chars): {response.text[:200]}")
    
    # Try parsing JSON
    data = response.json()
    
    # Extract dataset names and IDs
    datasets = data['result']['results']
    for dataset in datasets:
        print(f"\nDataset: {dataset['title']} (ID: {dataset['id']})")
        for resource in dataset['resources']:
            print(f"  → Resource: {resource['name']} (Format: {resource['format']}, URL: {resource['url']})")

except requests.exceptions.RequestException as e:
    print(f"\n❌ Request failed: {e}")
    print(f"Status code: {response.status_code if 'response' in locals() else 'N/A'}")
    print(f"Response text: {response.text if 'response' in locals() else 'N/A'}")

except ValueError as e:
    print(f"\n❌ Invalid JSON response: {e}")
    print(f"Response content: {response.text if 'response' in locals() else 'N/A'}")

except KeyError as e:
    print(f"\n❌ Unexpected response format. Missing key: {e}")
    print(f"Full response: {data if 'data' in locals() else 'N/A'}")