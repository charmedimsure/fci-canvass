import csv
with open('district12.txt', encoding='utf-8-sig') as f:
    r = csv.DictReader(f)
    for row in r:
        addr = row.get('RESIDENTIAL_ADDRESS1','')
        if 'PRESTON TRAILS' in addr.upper():
            print("Found Pickerington voter:")
            print(f"  STATE_REPRESENTATIVE_DISTRICT: {repr(row.get('STATE_REPRESENTATIVE_DISTRICT',''))}")
            print(f"  STATE_SENATE_DISTRICT: {repr(row.get('STATE_SENATE_DISTRICT',''))}")
            print(f"  CONGRESSIONAL_DISTRICT: {repr(row.get('CONGRESSIONAL_DISTRICT',''))}")
            print(f"  COUNTY_NUMBER: {repr(row.get('COUNTY_NUMBER',''))}")
            print(f"  RESIDENTIAL_ADDRESS1: {repr(addr)}")
            break
print("Done")
