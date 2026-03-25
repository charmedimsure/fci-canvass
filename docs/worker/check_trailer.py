import csv
with open('district12.txt', encoding='utf-8-sig') as f:
    r = csv.DictReader(f)
    for row in r:
        addr = row.get('RESIDENTIAL_ADDRESS1','') or (row.get('STNUM','') + ' ' + row.get('STNAME',''))
        if '1570' in addr and 'MAIN' in addr.upper():
            print("Found:")
            for k,v in row.items():
                if v and any(x in k.upper() for x in ['ADDR','MAIL','CITY','ZIP','STNUM','STNAME','APT','LOT','MADDR','SECOND']):
                    print(f"  {k}: {v}")
            break
print("Done")
