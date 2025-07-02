import sqlite3

# Connect to the local SQLite database
conn = sqlite3.connect('SEL_Employess_Data.db')
cursor = conn.cursor()

# Ensure the ShiftAssignments table exists
cursor.execute("""
    CREATE TABLE IF NOT EXISTS ShiftAssignments (
        assignment_id INTEGER PRIMARY KEY AUTOINCREMENT,
        employee_id INTEGER,
        shift_code TEXT,
        FOREIGN KEY (employee_id) REFERENCES Employees(id)
    )
""")

# Clear any existing shift assignments
cursor.execute("DELETE FROM ShiftAssignments")

# Track assigned employees to prevent duplicates
assigned_ids = set()

# Assign shift G to all employees with zero experience
cursor.execute("SELECT id FROM Employees WHERE Experience = 0")
shift_g_exp0 = [row[0] for row in cursor.fetchall()]
for eid in shift_g_exp0:
    cursor.execute("INSERT INTO ShiftAssignments (employee_id, shift_code) VALUES (?, ?)", (eid, 'G'))
    assigned_ids.add(eid)

# Utility to get eligible employees based on custom filter conditions
def get_next(conditions):
    sql = f"""
    SELECT id, Name, Band, Domain, Sub_Domain 
    FROM Employees 
    WHERE id NOT IN ({','.join(map(str, assigned_ids)) if assigned_ids else 'NULL'})
      AND {conditions}
    """
    cursor.execute(sql)
    return cursor.fetchall()

# Allocate shift 1: 1 Associate, alternating Domain and Sub_Domain rules
shift1_needed = 1
last_domain1 = None
last_subdomain1 = None

eligible_1 = get_next("Band='Associate'")
for eid, name, band, domain, sub_domain in eligible_1:
    if shift1_needed == 0:
        break
    if last_domain1 is not None:
        if domain == last_domain1:
            continue
        if domain == 'FD-SEL' and sub_domain == last_subdomain1:
            continue
    cursor.execute("INSERT INTO ShiftAssignments (employee_id, shift_code) VALUES (?, ?)", (eid, '1'))
    assigned_ids.add(eid)
    shift1_needed -= 1
    last_domain1 = domain
    if domain == 'FD-SEL':
        last_subdomain1 = sub_domain

# Allocate shift 2: 2 Layam employees, 1 AD-SEL and 1 FD-SEL, alternating Sub_Domain
shift2_needed = {'AD-SEL': 1, 'FD-SEL': 1}
last_subdomain2 = None

eligible_2 = get_next("Band='Layam'")
for eid, name, band, domain, sub_domain in eligible_2:
    if domain == 'FD-SEL':
        if shift2_needed['FD-SEL'] == 0:
            continue
        if last_subdomain2 and sub_domain == last_subdomain2:
            continue
        cursor.execute("INSERT INTO ShiftAssignments (employee_id, shift_code) VALUES (?, ?)", (eid, '2'))
        assigned_ids.add(eid)
        shift2_needed['FD-SEL'] -= 1
        last_subdomain2 = sub_domain
    elif domain == 'AD-SEL' and shift2_needed['AD-SEL'] > 0:
        cursor.execute("INSERT INTO ShiftAssignments (employee_id, shift_code) VALUES (?, ?)", (eid, '2'))
        assigned_ids.add(eid)
        shift2_needed['AD-SEL'] -= 1
    if sum(shift2_needed.values()) == 0:
        break

# Allocate shift 3: 1 Associate and 1 Layam, alternating Domain and Sub_Domain
shift3_needed = {'Associate': 1, 'Layam': 1}
last_domain3 = None
last_subdomain3 = None

eligible_3 = get_next("Band IN ('Associate', 'Layam')")
for eid, name, band, domain, sub_domain in eligible_3:
    if shift3_needed[band] == 0:
        continue
    if last_domain3 is not None:
        if domain == last_domain3:
            continue
        if domain == 'FD-SEL' and sub_domain == last_subdomain3:
            continue
    cursor.execute("INSERT INTO ShiftAssignments (employee_id, shift_code) VALUES (?, ?)", (eid, '3'))
    assigned_ids.add(eid)
    shift3_needed[band] -= 1
    last_domain3 = domain
    if domain == 'FD-SEL':
        last_subdomain3 = sub_domain
    if sum(shift3_needed.values()) == 0:
        break

# Assign shift G to any remaining unassigned employees
remaining = get_next("1=1")
for eid, _, _, _, _ in remaining:
    cursor.execute("INSERT INTO ShiftAssignments (employee_id, shift_code) VALUES (?, ?)", (eid, 'G'))
    assigned_ids.add(eid)

conn.commit()

# Display the final shift assignments in a tabular format
print("\nFinal Shift Planner Result:\n")
cursor.execute("""
    SELECT 
        sa.shift_code, 
        e.Name, 
        e.Band, 
        e.Experience, 
        e.Domain, 
        e.Sub_Domain
    FROM ShiftAssignments sa
    JOIN Employees e ON sa.employee_id = e.id
    ORDER BY sa.shift_code, e.Name
""")
rows = cursor.fetchall()

print(f"{'Shift':<6} {'Name':<20} {'Band':<15} {'Exp':<5} {'Domain':<8} {'Sub_Domain':<12}")
print("-"*70)
for r in rows:
    print(
        f"{str(r[0]):<6} "
        f"{str(r[1] or ''):<20} "
        f"{str(r[2] or ''):<15} "
        f"{str(r[3] or ''):<5} "
        f"{str(r[4] or ''):<8} "
        f"{str(r[5] or ''):<12}"
    )

conn.close()
