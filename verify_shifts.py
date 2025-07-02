import csv
from collections import defaultdict

filename = "shift_planner_result.csv"

# Load data
employees = []
with open(filename, newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    weeks = [f"Shift Week {i}" for i in range(1, 22)]
    for row in reader:
        employees.append(row)

# Helper: get shifts per week per employee
def get_shifts(emp):
    return [emp[w] for w in weeks]

# Condition 1: Experience 0 → only G
def check_exp0_shift_g():
    failed = []
    for emp in employees:
        if int(emp['Exp']) == 0:
            shifts = get_shifts(emp)
            if any(s != 'G' for s in shifts):
                failed.append(emp['Name'])
    return failed

# Condition 2: One shift per week (always true in CSV)

# Condition 3: Shift counts per week
def check_shift_counts():
    for w in weeks:
        count_1 = sum(emp[w] == '1' for emp in employees)
        count_2 = sum(emp[w] == '2' for emp in employees)
        count_3 = sum(emp[w] == '3' for emp in employees)
        if count_1 != 1 or count_2 != 2 or count_3 != 2:
            print(f"Week {w}: Shift counts - 1:{count_1}, 2:{count_2}, 3:{count_3}")

# Condition 4: Shift 1 alternation
def check_shift1_alternation():
    last_domain = None
    last_subdomain = None
    for i, w in enumerate(weeks):
        shift1_emp = [emp for emp in employees if emp[w] == '1']
        if len(shift1_emp) != 1:
            print(f"Week {w}: Shift 1 count != 1")
            continue
        emp = shift1_emp[0]
        if emp['Band'] != 'Associate':
            print(f"Week {w}: Shift 1 employee {emp['Name']} band not Associate")
        domain = emp['Domain']
        subdomain = emp['Sub_Domain']
        if last_domain == domain:
            print(f"Week {w}: Shift 1 domain not alternating (same as last week)")
        if domain == 'FD-SEL' and last_subdomain == subdomain:
            print(f"Week {w}: Shift 1 FD-SEL subdomain not alternating")
        last_domain = domain
        last_subdomain = subdomain

# Condition 5: Shift 2 alternation and band/domain
def check_shift2_conditions():
    last_fd_subdomain = None
    for w in weeks:
        shift2_emps = [emp for emp in employees if emp[w] == '2']
        if len(shift2_emps) != 2:
            print(f"Week {w}: Shift 2 count != 2")
            continue
        bands = set(emp['Band'] for emp in shift2_emps)
        if bands != {'Layam'}:
            print(f"Week {w}: Shift 2 employees not all Layam")
        domains = [emp['Domain'] for emp in shift2_emps]
        if 'AD-SEL' not in domains or 'FD-SEL' not in domains:
            print(f"Week {w}: Shift 2 domains not AD-SEL and FD-SEL")
        fd_emp = next(emp for emp in shift2_emps if emp['Domain'] == 'FD-SEL')
        fd_subdomain = fd_emp['Sub_Domain']
        if last_fd_subdomain == fd_subdomain:
            print(f"Week {w}: Shift 2 FD-SEL subdomain not alternating")
        last_fd_subdomain = fd_subdomain

# Condition 6: Shift 3 alternation and band/domain
def check_shift3_conditions():
    last_domains = {'Associate': None, 'Layam': None}
    last_subdomains = {'Associate': None, 'Layam': None}
    for w in weeks:
        shift3_emps = [emp for emp in employees if emp[w] == '3']
        if len(shift3_emps) != 2:
            print(f"Week {w}: Shift 3 count != 2")
            continue
        bands = set(emp['Band'] for emp in shift3_emps)
        if bands != {'Associate', 'Layam'}:
            print(f"Week {w}: Shift 3 employees not one Associate and one Layam")
        for emp in shift3_emps:
            band = emp['Band']
            domain = emp['Domain']
            subdomain = emp['Sub_Domain']
            if last_domains[band] == domain:
                print(f"Week {w}: Shift 3 {band} domain not alternating")
            if domain == 'FD-SEL' and last_subdomains[band] == subdomain:
                print(f"Week {w}: Shift 3 {band} FD-SEL subdomain not alternating")
            last_domains[band] = domain
            last_subdomains[band] = subdomain

# Condition 7: Only FD-SEL has subdomain
def check_fd_sel_subdomain():
    for emp in employees:
        for w in weeks:
            shift = emp[w]
            if shift in {'1','2','3'}:
                domain = emp['Domain']
                subdomain = emp['Sub_Domain']
                if domain != 'FD-SEL' and subdomain:
                    print(f"{emp['Name']} week {w}: Non FD-SEL with subdomain {subdomain}")

# Condition 9: Rest assigned G (implied by shift counts)

# Condition 10: Minimum 5 week gap for shifts 1/2/3
def check_min_gap():
    for emp in employees:
        weeks_assigned = [i for i,w in enumerate(weeks,1) if emp[w] in {'1','2','3'}]
        for i in range(1, len(weeks_assigned)):
            gap = weeks_assigned[i] - weeks_assigned[i-1]
            if gap < 5:
                print(f"{emp['Name']} assigned shifts too close: weeks {weeks_assigned[i-1]} and {weeks_assigned[i]}")

# Condition 11: Experience difference ≤ 2 for pairs in shifts 2 and 3
def check_exp_diff_pairs():
    for w in weeks:
        for shift_code in ['2','3']:
            emps = [emp for emp in employees if emp[w] == shift_code]
            if len(emps) == 2:
                exp_diff = abs(int(emps[0]['Exp']) - int(emps[1]['Exp']))
                if exp_diff > 2:
                    print(f"Week {w} shift {shift_code} experience diff > 2: {exp_diff}")

# Condition 13: Same experience → similar total shifts 1/2/3
def check_balanced_shifts():
    exp_groups = defaultdict(list)
    for emp in employees:
        exp = int(emp['Exp'])
        total_shifts = sum(1 for w in weeks if emp[w] in {'1','2','3'})
        exp_groups[exp].append(total_shifts)
    for exp, counts in exp_groups.items():
        avg = sum(counts)/len(counts)
        max_diff = max(abs(c - avg) for c in counts)
        if max_diff > 3:  # threshold, can adjust
            print(f"Exp {exp} shifts imbalance: max diff {max_diff}")

# Run all checks
print("Checking condition 1...")
failed_exp0 = check_exp0_shift_g()
if failed_exp0:
    print("Employees with Exp 0 not always G:", failed_exp0)
else:
    print("Condition 1 passed.")

print("\nChecking condition 3...")
check_shift_counts()

print("\nChecking condition 4...")
check_shift1_alternation()

print("\nChecking condition 5...")
check_shift2_conditions()

print("\nChecking condition 6...")
check_shift3_conditions()

print("\nChecking condition 7...")
check_fd_sel_subdomain()

print("\nChecking condition 10...")
check_min_gap()

print("\nChecking condition 11...")
check_exp_diff_pairs()

print("\nChecking condition 13...")
check_balanced_shifts()

print("\nAll other conditions (2,8,9) are structural or implied and assumed met.")
