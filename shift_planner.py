import sqlite3
import random

# Connect to SQLite database
conn = sqlite3.connect('SEL_Employess_Data.db')
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS ShiftAssignments")

# Create ShiftAssignments table with week column
cursor.execute("""
    CREATE TABLE IF NOT EXISTS ShiftAssignments (
        assignment_id INTEGER PRIMARY KEY AUTOINCREMENT,
        employee_id INTEGER,
        shift_code TEXT,
        week INTEGER,
        FOREIGN KEY (employee_id) REFERENCES Employees(id)
    )
""")

# Clear old assignments
cursor.execute("DELETE FROM ShiftAssignments")

# Helper: Check if employee was assigned this shift in last 5 weeks
def assigned_recently(eid, shift_code, current_week):
    if current_week <= 5:
        return False  # first few weeks: can't go negative
    cursor.execute("""
        SELECT week FROM ShiftAssignments 
        WHERE employee_id = ? AND shift_code = ? AND week >= ?
    """, (eid, shift_code, current_week - 5))
    return cursor.fetchone() is not None

# Helper: Get employee's experience
def get_employee_experience(eid):
    cursor.execute("SELECT Experience FROM Employees WHERE id = ?", (eid,))
    return cursor.fetchone()[0]

# Helper: Get total number of shifts assigned to an employee so far
def get_total_shifts(eid):
    cursor.execute("""
        SELECT COUNT(*) FROM ShiftAssignments WHERE employee_id = ?
    """, (eid,))
    return cursor.fetchone()[0]


# Generate 21 weeks
for week in range(1, 22):
    assigned_ids = set()

    # Rule 1: Experience = 0 → Shift G
    cursor.execute("SELECT id FROM Employees WHERE Experience = 0")
    shift_g_exp0 = [row[0] for row in cursor.fetchall()]
    for eid in shift_g_exp0:
        cursor.execute(
            "INSERT INTO ShiftAssignments (employee_id, shift_code, week) VALUES (?, ?, ?)",
            (eid, 'G', week)
        )
        assigned_ids.add(eid)

    def get_next(conditions):
        sql = f"""
            SELECT id, Name, Band, Domain, Sub_Domain 
            FROM Employees 
            WHERE id NOT IN ({','.join(map(str, assigned_ids)) if assigned_ids else 'NULL'})
              AND {conditions}
        """
        cursor.execute(sql)
        return cursor.fetchall()

    # SHIFT 1 — Associate
    shift1_needed = 1
    last_domain1 = None
    last_subdomain1 = None

    eligible_1 = get_next("Band='Associate'")
    eligible_1 = list(eligible_1)
    eligible_1.sort(key=lambda row: get_total_shifts(row[0]))  # Rule 12: balance usage
    random.shuffle(eligible_1)  # still random within lowest

    for eid, _, _, domain, sub_domain in eligible_1:
        if shift1_needed == 0:
            break
        if last_domain1 and domain == last_domain1:
            continue
        if domain == 'FD-SEL' and sub_domain == last_subdomain1:
            continue
        if assigned_recently(eid, '1', week):   # Rule 10: 5-week gap
            continue
        cursor.execute(
            "INSERT INTO ShiftAssignments (employee_id, shift_code, week) VALUES (?, ?, ?)",
            (eid, '1', week)
        )
        assigned_ids.add(eid)
        shift1_needed -= 1
        last_domain1 = domain
        if domain == 'FD-SEL':
            last_subdomain1 = sub_domain


    # SHIFT 2 — Layam: 1 AD-SEL + 1 FD-SEL
    shift2_needed = {'AD-SEL': 1, 'FD-SEL': 1}
    last_subdomain2 = None
    first_exp2 = None  

    eligible_2 = get_next("Band='Layam'")
    eligible_2 = list(eligible_2)
    eligible_2.sort(key=lambda row: get_total_shifts(row[0]))
    random.shuffle(eligible_2)

    for eid, _, _, domain, sub_domain in eligible_2:
        exp = get_employee_experience(eid)
        if domain == 'FD-SEL':
            if shift2_needed['FD-SEL'] == 0:
                continue
            if last_subdomain2 and sub_domain == last_subdomain2:
                continue
            if assigned_recently(eid, '2', week):
                continue
            if first_exp2 is None:
                first_exp2 = exp
            else:
                if abs(first_exp2 - exp) > 2:
                    continue  
            cursor.execute(
                "INSERT INTO ShiftAssignments (employee_id, shift_code, week) VALUES (?, ?, ?)",
                (eid, '2', week)
            )
            assigned_ids.add(eid)
            shift2_needed['FD-SEL'] -= 1
            last_subdomain2 = sub_domain
        elif domain == 'AD-SEL' and shift2_needed['AD-SEL'] > 0:
            if assigned_recently(eid, '2', week):
                continue
            if first_exp2 is None:
                first_exp2 = exp
            else:
                if abs(first_exp2 - exp) > 2:
                    continue
            cursor.execute(
                "INSERT INTO ShiftAssignments (employee_id, shift_code, week) VALUES (?, ?, ?)",
                (eid, '2', week)
            )
            assigned_ids.add(eid)
            shift2_needed['AD-SEL'] -= 1
        if sum(shift2_needed.values()) == 0:
            break


    # SHIFT 3 — 1 Associate + 1 Layam
    shift3_needed = {'Associate': 1, 'Layam': 1}
    last_domain3 = None
    last_subdomain3 = None
    first_exp3 = None

    eligible_3 = get_next("Band IN ('Associate', 'Layam')")
    eligible_3 = list(eligible_3)
    eligible_3.sort(key=lambda row: get_total_shifts(row[0]))
    random.shuffle(eligible_3)

    for eid, _, band, domain, sub_domain in eligible_3:
        if shift3_needed[band] == 0:
            continue
        if last_domain3 and domain == last_domain3:
            continue
        if domain == 'FD-SEL' and sub_domain == last_subdomain3:
            continue
        if assigned_recently(eid, '3', week):
            continue
        exp = get_employee_experience(eid)
        if first_exp3 is None:
            first_exp3 = exp
        else:
            if abs(first_exp3 - exp) > 2:
                continue
        cursor.execute(
            "INSERT INTO ShiftAssignments (employee_id, shift_code, week) VALUES (?, ?, ?)",
            (eid, '3', week)
        )
        assigned_ids.add(eid)
        shift3_needed[band] -= 1
        last_domain3 = domain
        if domain == 'FD-SEL':
            last_subdomain3 = sub_domain
        if sum(shift3_needed.values()) == 0:
            break


    # Remaining → Shift G
    remaining = get_next("1=1")
    for eid, *_ in remaining:
        cursor.execute(
            "INSERT INTO ShiftAssignments (employee_id, shift_code, week) VALUES (?, ?, ?)",
            (eid, 'G', week)
        )
        assigned_ids.add(eid)

conn.commit()

# Pivot: build full plan per employee
print("\nFinal Shift Planner Result (Pivoted 21 Weeks):\n")

# Get all employees
cursor.execute("""
    SELECT id, Name, Band, Experience, Domain, Sub_Domain
    FROM Employees
""")
employees = cursor.fetchall()

# Build shift map
cursor.execute("""
    SELECT employee_id, shift_code, week FROM ShiftAssignments
""")
shift_map = {}
for eid, shift_code, week in cursor.fetchall():
    if eid not in shift_map:
        shift_map[eid] = {}
    shift_map[eid][week] = shift_code

# Print header
header = ["Name", "Band", "Exp", "Domain", "Sub_Domain"] + [f"Shift Week {w}" for w in range(1, 22)]
print("\t".join(header))

rows_for_csv = []

# Print rows
for eid, name, band, exp, domain, sub_domain in employees:
    row = [name, band, str(exp), domain or '', sub_domain or '']
    for week in range(1, 22):
        row.append(shift_map.get(eid, {}).get(week, '-'))
    print("\t".join(row))
    rows_for_csv.append(row)
    
import csv
with open("shift_planner_result.csv", "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(header)
    writer.writerows(rows_for_csv)

print("\n Final result saved to 'shift_planner_result.csv'\n")

conn.close()
