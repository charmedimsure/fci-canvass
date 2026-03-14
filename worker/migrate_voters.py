#!/usr/bin/env python3
"""
FCI FieldMap — Voter Data Migration to Cloudflare D1 (v3)
Decodes compressed voter data and uploads to Cloudflare Worker.

Usage:
    python3 migrate_voters.py --html fieldmap.html \
                               --url https://fci-canvass.YOUR_NAME.workers.dev \
                               --key YOUR_API_KEY \
                               --admin-key YOUR_ADMIN_KEY
"""

import argparse, json, re, sys, time, hashlib
import urllib.request, urllib.error

# Embedded field lookup — decodes numeric indices to human-readable strings
FIELD_LOOKUP = {"cong": {"0": "12TH CONGRESSIONAL"}, "sen": {"0": "OHIO STATE SENATE 20"}, "hou": {"0": "OHIO ST HOUSE REP 69", "1": "OHIO ST HOUSE REP 73"}, "precn": {"0": "AMANDA A", "1": "AMANDA B", "2": "BALTIMORE VILLAGE A", "3": "BALTIMORE VILLAGE B", "4": "BERNE A", "5": "BERNE B", "6": "BERNE C", "7": "BERNE D", "8": "BLOOM A", "9": "BLOOM B", "10": "BLOOM C", "11": "BLOOM D", "12": "BLOOM E", "13": "BREMEN VILLAGE", "14": "CLEARCREEK A", "15": "CLEARCREEK B", "16": "COLUMBUS CITY A", "17": "COLUMBUS CITY B", "18": "COLUMBUS CITY C", "19": "COLUMBUS CITY D", "20": "COLUMBUS CITY E", "21": "COLUMBUS CITY F", "22": "GREENFIELD A", "23": "GREENFIELD B", "24": "GREENFIELD C", "25": "GREENFIELD D", "26": "HOCKING A", "27": "HOCKING B", "28": "HOCKING C", "29": "LANCASTER CITY 1-A", "30": "LANCASTER CITY 1-B", "31": "LANCASTER CITY 1-C", "32": "LANCASTER CITY 1-D", "33": "LANCASTER CITY 1-E", "34": "LANCASTER CITY 2-A", "35": "LANCASTER CITY 2-B", "36": "LANCASTER CITY 2-C", "37": "LANCASTER CITY 2-D", "38": "LANCASTER CITY 3-A", "39": "LANCASTER CITY 3-B", "40": "LANCASTER CITY 3-C", "41": "LANCASTER CITY 3-D", "42": "LANCASTER CITY 3-E", "43": "LANCASTER CITY 4-A", "44": "LANCASTER CITY 4-B", "45": "LANCASTER CITY 4-C", "46": "LANCASTER CITY 4-D", "47": "LANCASTER CITY 5-A", "48": "LANCASTER CITY 5-B", "49": "LANCASTER CITY 5-C", "50": "LANCASTER CITY 5-D", "51": "LANCASTER CITY 6-A", "52": "LANCASTER CITY 6-B", "53": "LANCASTER CITY 6-C", "54": "LANCASTER CITY 6-D", "55": "LIBERTY A", "56": "LIBERTY B", "57": "LIBERTY C", "58": "LIBERTY D", "59": "LITHOPOLIS VILLAGE A", "60": "LITHOPOLIS VILLAGE B", "61": "MADISON A", "62": "PICKERINGTON CITY A", "63": "PICKERINGTON CITY B", "64": "PICKERINGTON CITY C", "65": "PICKERINGTON CITY D", "66": "PICKERINGTON CITY E", "67": "PICKERINGTON CITY F", "68": "PICKERINGTON CITY G", "69": "PICKERINGTON CITY H", "70": "PICKERINGTON CITY I", "71": "PICKERINGTON CITY J", "72": "PICKERINGTON CITY K", "73": "PICKERINGTON CITY L", "74": "PICKERINGTON CITY M", "75": "PICKERINGTON CITY N", "76": "PICKERINGTON CITY O", "77": "PICKERINGTON CITY P", "78": "PICKERINGTON CITY Q", "79": "PLEASANT A", "80": "PLEASANT B", "81": "PLEASANT C", "82": "PLEASANT D", "83": "PLEASANT E", "84": "PLEASANT F", "85": "RICHLAND EAST", "86": "RICHLAND WEST", "87": "RUSHCREEK A", "88": "RUSHCREEK B", "89": "VIOLET A", "90": "VIOLET B", "91": "VIOLET C", "92": "VIOLET D", "93": "VIOLET E", "94": "VIOLET F", "95": "VIOLET G", "96": "VIOLET H", "97": "VIOLET I", "98": "VIOLET J", "99": "VIOLET K", "100": "VIOLET L", "101": "VIOLET M", "102": "VIOLET N", "103": "VIOLET O", "104": "VIOLET P", "105": "VIOLET Q", "106": "VIOLET R", "107": "WALNUT A", "108": "WALNUT B", "109": "WALNUT C", "110": "WALNUT D"}, "sch": {"0": "AMANDA-CLEARCRK LSD", "1": "BERNE UNION LSD", "2": "BLOOM-CARROLL LSD", "3": "CANAL WINCHESTER LSD", "4": "FAIRFIELD UNION LSD", "5": "LANCASTER CSD", "6": "LIBERTY UNION LSD", "7": "PICKERINGTON LSD", "8": "SW LICKING LSD", "9": "WALNUT TOWNSHIP LSD"}, "city": {"0": "COLUMBUS CITY", "1": "LANCASTER CITY", "2": "PICKERINGTON CITY", "3": "REYNOLDSBURG CITY"}, "twp": {"0": "AMANDA TWP", "1": "BERNE TWP", "2": "BLOOM TWP", "3": "CLEARCREEK TWP", "4": "GREENFIELD TWP", "5": "HOCKING TWP", "6": "LIBERTY TWP", "7": "MADISON TWP", "8": "MONTGOMERY TWP", "9": "PLEASANT TWP", "10": "RICHLAND TWP", "11": "RUSHCREEK TWP", "12": "VIOLET TWP", "13": "WALNUT TWP"}, "vil": {"0": "BALTIMORE VLG", "1": "BREMEN VLG", "2": "CARROLL VLG", "3": "LITHOPOLIS VLG", "4": "RUSHVILLE VLG"}, "prec": {"0": "01AA  1", "1": "01AA  2", "2": "01AB  1", "3": "01AB  2", "4": "01AB  3", "5": "02BA  1", "6": "02BB  1", "7": "03BA  1", "8": "03BA  2", "9": "03BA  3", "10": "03BB  1", "11": "03BC  1", "12": "03BD  1", "13": "03BD  2", "14": "04BA  1", "15": "04BA  2", "16": "04BA  3", "17": "04BB  1", "18": "04BB  2", "19": "04BC  1", "20": "04BC  2", "21": "04BD  1", "22": "04BD  2", "23": "04BE  1", "24": "04BE  2", "25": "05BV  1", "26": "06CA  1", "27": "06CA  2", "28": "06CB  1", "29": "07CA  1", "30": "07CB  1", "31": "07CC  1", "32": "07CD  1", "33": "07CE  1", "34": "07CF  1", "35": "08GA  1", "36": "08GA  2", "37": "08GA  3", "38": "08GA  4", "39": "08GB  1", "40": "08GB  2", "41": "08GC  1", "42": "08GC  2", "43": "08GD  1", "44": "08GD  2", "45": "09HA  1", "46": "09HA  2", "47": "09HB  1", "48": "09HC  1", "49": "09HC  2", "50": "101A  1", "51": "101B  1", "52": "101C  1", "53": "101D  1", "54": "101E  1", "55": "101E  2", "56": "102A  1", "57": "102B  1", "58": "102C  1", "59": "102D  1", "60": "103A  1", "61": "103B  1", "62": "103C  1", "63": "103D  1", "64": "103E  1", "65": "104A  1", "66": "104B  1", "67": "104C  1", "68": "104D  1", "69": "105A  1", "70": "105B  1", "71": "105C  2", "72": "105D  1", "73": "105D  2", "74": "106A  1", "75": "106B  1", "76": "106C  1", "77": "106D  1", "78": "11LA  1", "79": "11LA  2", "80": "11LA  3", "81": "11LA  4", "82": "11LB  1", "83": "11LB  2", "84": "11LC  1", "85": "11LC  2", "86": "11LC  3", "87": "11LC  4", "88": "11LD  1", "89": "11LD  2", "90": "12LA  1", "91": "12LA  2", "92": "12LA  3", "93": "12LB  1", "94": "13MA  1", "95": "13MA  2", "96": "14PA  1", "97": "14PB  1", "98": "14PC  1", "99": "14PD  1", "100": "14PE  1", "101": "14PF  1", "102": "14PF  2", "103": "14PG  1", "104": "14PH  1", "105": "14PI  1", "106": "14PJ  1", "107": "14PK  1", "108": "14PL  1", "109": "14PM  1", "110": "14PN  1", "111": "14PO  1", "112": "14PP  1", "113": "14PQ  1", "114": "15PA  1", "115": "15PA  2", "116": "15PB  1", "117": "15PB  2", "118": "15PB  3", "119": "15PC  1", "120": "15PC  2", "121": "15PD  1", "122": "15PD  2", "123": "15PE  1", "124": "15PF  1", "125": "16RE  1", "126": "16RE  2", "127": "16RW  1", "128": "16RW  2", "129": "17RA  1", "130": "17RB  1", "131": "18VA  1", "132": "18VA  2", "133": "18VA  3", "134": "18VB  1", "135": "18VB  2", "136": "18VB  3", "137": "18VC  1", "138": "18VC  3", "139": "18VC  4", "140": "18VD  1", "141": "18VD  2", "142": "18VD  3", "143": "18VD  4", "144": "18VE  1", "145": "18VF  1", "146": "18VG  1", "147": "18VH  1", "148": "18VI  1", "149": "18VJ  1", "150": "18VK  1", "151": "18VL  1", "152": "18VM  1", "153": "18VN  1", "154": "18VO  1", "155": "18VP  1", "156": "18VQ  1", "157": "18VR  1", "158": "19WA  1", "159": "19WA  2", "160": "19WA  3", "161": "19WA  4", "162": "19WB  1", "163": "19WB  2", "164": "19WB  3", "165": "19WC  1", "166": "19WC  2", "167": "19WD  1", "168": "19WD  2", "169": "19WD  3", "170": "19WD  5"}}

def lookup(fl, key, idx):
    if idx is None: return ''
    return fl.get(key, {}).get(str(idx), str(idx))

def decode_voter(v, fl):
    vid = hashlib.md5(f"{v.get('n','')}|{v.get('a','')}".encode()).hexdigest()[:16]
    decoded = dict(v)
    decoded['id']           = vid
    decoded['stHouse']      = lookup(fl, 'hou',  v.get('hou'))
    decoded['stSenate']     = lookup(fl, 'sen',  v.get('sen'))
    decoded['congDist']     = lookup(fl, 'cong', v.get('cong'))
    decoded['precinct']     = lookup(fl, 'prec', v.get('prec'))
    decoded['precinctName'] = lookup(fl, 'precn',v.get('precn'))
    decoded['township']     = lookup(fl, 'twp',  v.get('twp'))
    decoded['municipality'] = lookup(fl, 'city', v.get('city'))
    decoded['village']      = lookup(fl, 'vil',  v.get('vil')) if v.get('vil') is not None else ''
    decoded['ward']         = ''
    decoded['score']        = str(v.get('vot', ''))
    decoded['party']        = v.get('pty', '')
    decoded['lat']          = v.get('lat')
    decoded['lon']          = v.get('lon')
    return decoded

def extract_voters(html_path):
    print(f"Reading {html_path} ...")
    with open(html_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Try compressed chunk format first (_VD0.._VD9)
    chunks = re.findall(r'const _VD\d+=(\[.*?\]);', content, re.DOTALL)
    if chunks:
        print(f"Found {len(chunks)} compressed chunks, parsing...")
        all_voters = []
        for i, chunk in enumerate(chunks):
            voters = json.loads(chunk)
            all_voters.extend(voters)
            print(f"  Chunk {i}: {len(voters)} households")
    else:
        # Single VOTER_DATA array
        m = re.search(r'(?:const |var )?VOTER_DATA\s*=\s*(\[.+?\]);', content, re.DOTALL)
        if not m:
            print("ERROR: No voter data found. Point --html at the original file with embedded voter data.")
            sys.exit(1)
        print("Found VOTER_DATA array, parsing...")
        all_voters = json.loads(m.group(1))
        print(f"  {len(all_voters)} households")

    print(f"\nTotal: {len(all_voters):,} households")
    print("Decoding compressed fields...")
    decoded = [decode_voter(v, FIELD_LOOKUP) for v in all_voters]
    print("Decoding complete.")
    return decoded

def upload_voters(voters, worker_url, api_key, admin_key, batch_size=500):
    url = worker_url.rstrip('/') + '/api/admin/load-voters'
    headers = {
        'Content-Type': 'application/json',
        'X-FCI-Key':    api_key,
        'X-FCI-Admin':  admin_key,
        'User-Agent':   'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    }
    total   = len(voters)
    batches = (total + batch_size - 1) // batch_size
    loaded  = 0
    print(f"\nUploading {total:,} households in {batches} batches...")
    print(f"Target: {url}\n")
    for i in range(0, total, batch_size):
        batch   = voters[i:i+batch_size]
        batch_n = i // batch_size + 1
        payload = json.dumps({'replace': i == 0, 'voters': batch}).encode('utf-8')
        req = urllib.request.Request(url, data=payload, headers=headers, method='POST')
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                result = json.loads(resp.read())
                loaded += result.get('loaded', len(batch))
                print(f"  Batch {batch_n}/{batches} — {loaded:,}/{total:,} ({loaded/total*100:.0f}%)")
        except urllib.error.HTTPError as e:
            print(f"\nERROR on batch {batch_n}: HTTP {e.code} — {e.read().decode()}")
            sys.exit(1)
        except Exception as e:
            print(f"\nERROR on batch {batch_n}: {e}")
            sys.exit(1)
        if i + batch_size < total:
            time.sleep(0.2)
    print(f"\n✅ Done! {loaded:,} households loaded into D1.")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--html',       required=True)
    ap.add_argument('--url',        required=True)
    ap.add_argument('--key',        required=True)
    ap.add_argument('--admin-key',  required=True)
    ap.add_argument('--batch-size', type=int, default=500)
    args = ap.parse_args()
    voters = extract_voters(args.html)
    upload_voters(voters, args.url, args.key, args.admin_key, args.batch_size)

if __name__ == '__main__':
    main()
