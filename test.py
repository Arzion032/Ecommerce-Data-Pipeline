import requests

url = "https://microdata.worldbank.org/index.php/api/tables/data/fcv/wld_2021_rtfp_v02_m?limit=15&offset=0&country=Philippines&fields=country,rice,l_rice,o_rice,trust_rice,o_rice_various,c_rice_various"
response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    print(data)
else:
    print(f"Request failed with status code {response.status_code}")