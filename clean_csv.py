import pandas as pd
import re
from collections import defaultdict

INPUT_FILE = ""
OUTPUT_FILE = ""

def normalize_phone(num):
    if pd.isna(num):
        return None
    s = str(num).strip()
    digits = re.sub(r'\D', '', s)
    if len(digits) == 11 and digits.startswith('1'):
        digits = digits[1:]
    return digits if digits else None

def classify_type(t):
    if pd.isna(t): return None
    s = str(t).lower()
    if "sms" in s: return "sms"
    if "voice" in s or "tty" in s or "tts" in s: return "voice"
    return None

def build_address(row):
    addr1 = str(row.get('Primary Address 1') or '').strip()
    addr2 = str(row.get('Primary Address 2') or '').strip()
    street = f"{addr1} {addr2}".strip() if addr2 else addr1
    city = str(row.get('Primary City') or '').strip()
    state = str(row.get('Primary State') or '').strip()
    zipc = str(row.get('Primary Zip') or '').strip()
    country = str(row.get('Primary Country') or '').strip()
    return f"{street} | {city} | {state} | {zipc} | {country}"

df = pd.read_csv(INPUT_FILE, low_memory=False)

# Rename Primary Key → databaseID
if "Primary Key" in df.columns:
    df = df.rename(columns={"Primary Key":"databaseID"})

# Identify device type/value columns
device_pairs = []
for col in df.columns:
    m = re.match(r"Device (\d+) Type", col)
    if m:
        num = m.group(1)
        if f"Device {num} Value" in df.columns:
            device_pairs.append((col, f"Device {num} Value"))

# Extract email for grouping
df["__email"] = None
for i, row in df.iterrows():
    for tcol, vcol in device_pairs:
        if str(row[tcol]).lower() == "email" and pd.notna(row[vcol]):
            df.at[i,"__email"] = str(row[vcol]).strip()
            break

# Merge contacts *by email* (preserving group memberships)
merged = []
seen = {}

for _, row in df.iterrows():
    email = row["__email"]
    if pd.isna(email):
        merged.append(row)
        continue
    
    key = email.lower()
    if key not in seen:
        seen[key] = {"row": row.copy(), "phones": defaultdict(set)}
    
    for tcol, vcol in device_pairs:
        num = normalize_phone(row[vcol])
        typ = classify_type(row[tcol])
        if num and typ:
            seen[key]["phones"][num].add(typ)

for email, bundle in seen.items():
    base = bundle["row"].copy()
    phones = []
    for num, types in bundle["phones"].items():
        has_sms = "sms" in types
        has_voice = "voice" in types
        if has_sms and not has_voice:
            phones.append(f"{num}||1")
        elif has_voice and not has_sms:
            phones.append(f"{num}||2")
        else:
            phones.append(num)
    base["Phone"] = ";".join(sorted(set(phones)))
    merged.append(base)

df_final = pd.DataFrame(merged)
df_final.drop(columns=["__email"], errors="ignore", inplace=True)

# Division → Custom Attribute
if "Division" in df_final.columns:
    df_final.rename(columns={"Division":"Custom Attribute"}, inplace=True)

# Add formatted Address column
df_final["Address"] = df_final.apply(build_address, axis=1)

# Ensure strings for phone + databaseID
if "databaseID" in df_final.columns:
    df_final["databaseID"] = df_final["databaseID"].astype(str)
df_final["Phone"] = df_final["Phone"].astype(str)

df_final.to_csv(OUTPUT_FILE, index=False)
print("Cleaning complete →", OUTPUT_FILE)
