
import requests, json

API_URL = 'https://fci-canvass.fci-canvass.workers.dev'
API_KEY = 'ohiofcicanvass7312'
ADMIN_KEY = '2026ad#min#oh#fci#LA'

headers = {
    'X-FCI-Key': API_KEY,
    'X-FCI-Admin': ADMIN_KEY,
    'Content-Type': 'application/json',
}

# Test 1: ping
r = requests.get(API_URL + '/api/ping')
print(f"Ping: {r.status_code} {r.text[:100]}")

# Test 2: minimal voter load
payload = {
    "voters": [{
        "id": "test_donor_123",
        "n": "TEST VOTER",
        "a": "123 TEST ST, Lancaster",
        "party": "D",
        "municipality": "LANCASTER",
        "lat": 39.7,
        "lon": -82.6,
        "donations": [{"voter": "TEST VOTER", "committee": "ACTBLUE", "lean": "D", "amount": 25.0}]
    }],
    "replace": False
}
r2 = requests.post(API_URL + '/api/admin/load-voters', headers=headers, json=payload)
print(f"Load voter: {r2.status_code} {r2.text[:300]}")
