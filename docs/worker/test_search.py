import requests

API_URL = 'https://fci-canvass.fci-canvass.workers.dev'
API_KEY = 'ohiofcicanvass7312'

headers = {'X-FCI-Key': API_KEY}

# Test 1: basic voter fetch (no name filter)
r = requests.get(API_URL + '/api/voters', headers=headers, params={'municipality': 'LANCASTER', 'limit': 2})
print(f"Basic fetch: {r.status_code}")
if r.ok:
    data = r.json()
    voters = data.get('voters', [])
    print(f"Got {len(voters)} voters")
    if voters:
        v = voters[0]
        print(f"Sample voter keys: {list(v.keys())[:10]}")
        print(f"Has donations: {'donations' in v}")
        print(f"Sample: name={v.get('name') or v.get('n')}, party={v.get('party')}")

# Test 2: last_name search
r2 = requests.get(API_URL + '/api/voters', headers=headers, params={'last_name': 'SMITH', 'limit': 3})
print(f"\nName search: {r2.status_code}")
print(r2.text[:300])
