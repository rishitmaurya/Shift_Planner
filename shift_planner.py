import sqlite3
import random
import csv

class ShiftPlanner:
    def __init__(self, db_path='SEL_Employess_Data.db', weeks=21):
        self.db_path = db_path
        self.weeks = weeks
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

    # def assigned_recently(self, eid, shift_code, current_week):
    #     if current_week <= 5:
    #         return False  # first few weeks: can't go negative
    #     self.cursor.execute("""
    #         SELECT week FROM ShiftAssignments 
    #         WHERE employee_id = ? AND shift_code = ? AND week >= ?
    #     """, (eid, shift_code, current_week - 5))
    #     return self.cursor.fetchone() is not None

    def assigned_shift_1_2_3_recently(self, eid, current_week, gap=4):
        if current_week <= gap:
            return False
        self.cursor.execute("""
            SELECT week FROM ShiftAssignments
            WHERE employee_id = ? AND shift_code IN ('1', '2', '3') AND week >= ?
        """, (eid, current_week - gap))
        return self.cursor.fetchone() is not None

    def get_employee_experience(self, eid):
        self.cursor.execute("SELECT Experience FROM Employees WHERE id = ?", (eid,))
        return self.cursor.fetchone()[0]

    def get_total_shifts(self, eid):
        self.cursor.execute("""
            SELECT COUNT(*) FROM ShiftAssignments WHERE employee_id = ?
        """, (eid,))
        return self.cursor.fetchone()[0]

    def get_shift_1_2_3_count(self, eid):
        self.cursor.execute("""
            SELECT COUNT(*) FROM ShiftAssignments 
            WHERE employee_id = ? AND shift_code IN ('1', '2', '3')
        """, (eid,))
        return self.cursor.fetchone()[0]

    def get_last_shift_1_2_3(self, eid):
        self.cursor.execute("""
            SELECT shift_code FROM ShiftAssignments
            WHERE employee_id = ? AND shift_code IN ('1', '2', '3')
            ORDER BY week DESC LIMIT 1
        """, (eid,))
        row = self.cursor.fetchone()
        return row[0] if row else None

    def get_next(self, assigned_ids, conditions):
        sql = f"""
            SELECT id, Name, Band, Domain, Sub_Domain 
            FROM Employees 
            WHERE id NOT IN ({','.join(map(str, assigned_ids)) if assigned_ids else 'NULL'})
              AND {conditions}
        """
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def run(self):
        self.cursor.execute("DROP TABLE IF EXISTS ShiftAssignments")

        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS ShiftAssignments (
                assignment_id INTEGER PRIMARY KEY AUTOINCREMENT,
                employee_id INTEGER,
                shift_code TEXT,
                week INTEGER,
                FOREIGN KEY (employee_id) REFERENCES Employees(id)
            )
        """)

        self.cursor.execute("DELETE FROM ShiftAssignments")

        for week in range(1, self.weeks + 1):
            assigned_ids = set()

            # Rule 1: Experience = 0 → Shift G
            self.cursor.execute("SELECT id FROM Employees WHERE Experience = 0")
            shift_g_exp0 = [row[0] for row in self.cursor.fetchall()]
            for eid in shift_g_exp0:
                self.cursor.execute(
                    "INSERT INTO ShiftAssignments (employee_id, shift_code, week) VALUES (?, ?, ?)",
                    (eid, 'G', week)
                )
                assigned_ids.add(eid)

            # SHIFT 1 — Associate
            shift1_needed = 1
            last_domain1 = None
            last_subdomain1 = None

            eligible_1 = self.get_next(assigned_ids, "Band='Associate'")
            eligible_1 = list(eligible_1)
            eligible_1.sort(key=lambda row: self.get_shift_1_2_3_count(row[0]))
            

            for eid, _, _, domain, sub_domain in eligible_1:
                if shift1_needed == 0:
                    break
                if last_domain1 and domain == last_domain1:
                    continue
                if domain == 'FD-SEL' and sub_domain == last_subdomain1:
                    continue
                if self.assigned_shift_1_2_3_recently(eid, week):
                    continue
                last_shift = self.get_last_shift_1_2_3(eid)
                if last_shift == '1':
                    continue
                self.cursor.execute(
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

            eligible_2 = self.get_next(assigned_ids, "Band='Layam'")
            eligible_2 = list(eligible_2)
            eligible_2.sort(key=lambda row: self.get_shift_1_2_3_count(row[0]))
            random.shuffle(eligible_2)

            for eid, _, _, domain, sub_domain in eligible_2:
                exp = self.get_employee_experience(eid)
                last_shift = self.get_last_shift_1_2_3(eid)
                if domain == 'FD-SEL':
                    if shift2_needed['FD-SEL'] == 0:
                        continue
                    if last_subdomain2 and sub_domain == last_subdomain2:
                        continue
                    if self.assigned_shift_1_2_3_recently(eid, week):
                        continue
                    if last_shift == '2':
                        continue
                    if first_exp2 is None:
                        first_exp2 = exp
                    else:
                        if abs(first_exp2 - exp) > 2:
                            continue  
                    self.cursor.execute(
                        "INSERT INTO ShiftAssignments (employee_id, shift_code, week) VALUES (?, ?, ?)",
                        (eid, '2', week)
                    )
                    assigned_ids.add(eid)
                    shift2_needed['FD-SEL'] -= 1
                    last_subdomain2 = sub_domain
                elif domain == 'AD-SEL' and shift2_needed['AD-SEL'] > 0:
                    if self.assigned_shift_1_2_3_recently(eid, week):
                        continue
                    if last_shift == '2':
                        continue
                    if first_exp2 is None:
                        first_exp2 = exp
                    else:
                        if abs(first_exp2 - exp) > 2:
                            continue
                    self.cursor.execute(
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

            eligible_3 = self.get_next(assigned_ids, "Band IN ('Associate', 'Layam')")
            eligible_3 = list(eligible_3)
            eligible_3.sort(key=lambda row: self.get_shift_1_2_3_count(row[0]))
            random.shuffle(eligible_3)

            for eid, _, band, domain, sub_domain in eligible_3:
                if shift3_needed[band] == 0:
                    continue
                if last_domain3 and domain == last_domain3:
                    continue
                if domain == 'FD-SEL' and sub_domain == last_subdomain3:
                    continue
                if self.assigned_shift_1_2_3_recently(eid, week):
                    continue
                last_shift = self.get_last_shift_1_2_3(eid)
                if last_shift == '3':
                    continue
                exp = self.get_employee_experience(eid)
                if first_exp3 is None:
                    first_exp3 = exp
                else:
                    if abs(first_exp3 - exp) > 2:
                        continue
                self.cursor.execute(
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
            remaining = self.get_next(assigned_ids, "1=1")
            for eid, *_ in remaining:
                self.cursor.execute(
                    "INSERT INTO ShiftAssignments (employee_id, shift_code, week) VALUES (?, ?, ?)",
                    (eid, 'G', week)
                )
                assigned_ids.add(eid)

        self.conn.commit()

        # Pivot: build full plan per employee
        print("\nFinal Shift Planner Result (Pivoted 21 Weeks):\n")

        self.cursor.execute("""
            SELECT id, Name, Band, Experience, Domain, Sub_Domain
            FROM Employees
        """)
        employees = self.cursor.fetchall()

        self.cursor.execute("""
            SELECT employee_id, shift_code, week FROM ShiftAssignments
        """)
        shift_map = {}
        for eid, shift_code, week in self.cursor.fetchall():
            if eid not in shift_map:
                shift_map[eid] = {}
            shift_map[eid][week] = shift_code

        header = ["Name", "Band", "Exp", "Domain", "Sub_Domain"] + [f"Shift Week {w}" for w in range(1, self.weeks + 1)]
        print("\t".join(header))

        rows_for_csv = []

        for eid, name, band, exp, domain, sub_domain in employees:
            row = [name, band, str(exp), domain or '', sub_domain or '']
            for week in range(1, self.weeks + 1):
                row.append(shift_map.get(eid, {}).get(week, '-'))
            print("\t".join(row))
            rows_for_csv.append(row)

        with open("shift_planner_result.csv", "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(header)
            writer.writerows(rows_for_csv)

        print("\nFinal result saved to 'shift_planner_result.csv'\n")

        self.conn.close()
