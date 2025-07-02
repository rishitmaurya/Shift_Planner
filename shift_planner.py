import sqlite3
import random
import csv

class ShiftPlanner:
    def __init__(self, db_path='SEL_Employess_Data.db', weeks=21):
        self.db_path = db_path
        self.weeks = weeks
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

    def assigned_shift_1_2_3_recently(self, eid, current_week, gap=5):
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

    def get_next(self, assigned_ids, conditions, band=None, experience=None):
        filters = []
        if band:
            filters.append(f"Band = '{band}'")
        if experience is not None:
            filters.append(f"Experience = {experience}")
        if conditions:
            filters.append(conditions)
        where_clause = " AND ".join(filters) if filters else "1=1"
        sql = f"""
            SELECT id, Name, Band, Experience, Domain, Sub_Domain
            FROM Employees
            WHERE id NOT IN ({','.join(map(str, assigned_ids)) if assigned_ids else 'NULL'})
              AND {where_clause}
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

        # Track last domain and subdomain per shift globally across weeks
        last_domain_shift1 = None
        last_subdomain_shift1 = None

        last_subdomain_shift2 = None

        last_domain_shift3 = {'Associate': None, 'Layam': None}
        last_subdomain_shift3 = {'Associate': None, 'Layam': None}

        def group_shuffle(lst):
            from itertools import groupby
            result = []
            for _, group in groupby(lst, key=lambda x: self.get_shift_1_2_3_count(x[0])):
                group_list = list(group)
                random.shuffle(group_list)
                result.extend(group_list)
            return result

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

            eligible_1 = self.get_next(assigned_ids, "Band='Associate'")
            eligible_1 = list(eligible_1)
            eligible_1.sort(key=lambda row: self.get_shift_1_2_3_count(row[0]))
            eligible_1 = group_shuffle(eligible_1)

            # First pass: strict alternation
            for eid, _, _, exp, domain, sub_domain in eligible_1:
                if shift1_needed == 0:
                    break
                if last_domain_shift1 and domain == last_domain_shift1:
                    continue
                if domain == 'FD-SEL' and sub_domain == last_subdomain_shift1:
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
                last_domain_shift1 = domain
                last_subdomain_shift1 = sub_domain

            # Fallback pass: relax alternation constraints if needed
            if shift1_needed > 0:
                for eid, _, _, exp, domain, sub_domain in eligible_1:
                    if eid in assigned_ids:
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
                    last_domain_shift1 = domain
                    last_subdomain_shift1 = sub_domain
                    break

            # SHIFT 2 — Layam: 1 AD-SEL + 1 FD-SEL
            shift2_needed = {'AD-SEL': 1, 'FD-SEL': 1}

            eligible_2 = self.get_next(assigned_ids, "Band='Layam'")
            eligible_2 = list(eligible_2)
            eligible_2.sort(key=lambda row: self.get_shift_1_2_3_count(row[0]))
            eligible_2 = group_shuffle(eligible_2)

            # First pass: strict alternation
            for eid, _, _, exp, domain, sub_domain in eligible_2:
                if domain == 'FD-SEL':
                    if shift2_needed['FD-SEL'] == 0:
                        continue
                    if last_subdomain_shift2 and sub_domain == last_subdomain_shift2:
                        continue
                    if self.assigned_shift_1_2_3_recently(eid, week):
                        continue
                    last_shift = self.get_last_shift_1_2_3(eid)
                    if last_shift == '2':
                        continue
                    self.cursor.execute(
                        "INSERT INTO ShiftAssignments (employee_id, shift_code, week) VALUES (?, ?, ?)",
                        (eid, '2', week)
                    )
                    assigned_ids.add(eid)
                    shift2_needed['FD-SEL'] -= 1
                    last_subdomain_shift2 = sub_domain
                elif domain == 'AD-SEL' and shift2_needed['AD-SEL'] > 0:
                    if self.assigned_shift_1_2_3_recently(eid, week):
                        continue
                    last_shift = self.get_last_shift_1_2_3(eid)
                    if last_shift == '2':
                        continue
                    self.cursor.execute(
                        "INSERT INTO ShiftAssignments (employee_id, shift_code, week) VALUES (?, ?, ?)",
                        (eid, '2', week)
                    )
                    assigned_ids.add(eid)
                    shift2_needed['AD-SEL'] -= 1
                if sum(shift2_needed.values()) == 0:
                    break

            # Fallback pass: relax alternation constraints if needed
            if sum(shift2_needed.values()) > 0:
                for eid, _, _, exp, domain, sub_domain in eligible_2:
                    if eid in assigned_ids:
                        continue
                    if self.assigned_shift_1_2_3_recently(eid, week):
                        continue
                    last_shift = self.get_last_shift_1_2_3(eid)
                    if last_shift == '2':
                        continue
                    if domain in shift2_needed and shift2_needed[domain] > 0:
                        self.cursor.execute(
                            "INSERT INTO ShiftAssignments (employee_id, shift_code, week) VALUES (?, ?, ?)",
                            (eid, '2', week)
                        )
                        assigned_ids.add(eid)
                        shift2_needed[domain] -= 1
                    if sum(shift2_needed.values()) == 0:
                        break

            # SHIFT 3 — 1 Associate + 1 Layam
            shift3_needed = {'Associate': 1, 'Layam': 1}

            eligible_3 = self.get_next(assigned_ids, "Band IN ('Associate', 'Layam')")
            eligible_3 = list(eligible_3)
            eligible_3.sort(key=lambda row: self.get_shift_1_2_3_count(row[0]))
            eligible_3 = group_shuffle(eligible_3)

            # First pass: strict alternation
            for eid, _, band, exp, domain, sub_domain in eligible_3:
                if shift3_needed[band] == 0:
                    continue
                if last_domain_shift3[band] and domain == last_domain_shift3[band]:
                    continue
                if domain == 'FD-SEL' and last_subdomain_shift3[band] == sub_domain:
                    continue
                if self.assigned_shift_1_2_3_recently(eid, week):
                    continue
                last_shift = self.get_last_shift_1_2_3(eid)
                if last_shift == '3':
                    continue
                self.cursor.execute(
                    "INSERT INTO ShiftAssignments (employee_id, shift_code, week) VALUES (?, ?, ?)",
                    (eid, '3', week)
                )
                assigned_ids.add(eid)
                shift3_needed[band] -= 1
                last_domain_shift3[band] = domain
                if domain == 'FD-SEL':
                    last_subdomain_shift3[band] = sub_domain
                else:
                    last_subdomain_shift3[band] = None
                if sum(shift3_needed.values()) == 0:
                    break

            # Fallback pass: relax alternation constraints if needed
            if sum(shift3_needed.values()) > 0:
                for eid, _, band, exp, domain, sub_domain in eligible_3:
                    if eid in assigned_ids:
                        continue
                    if self.assigned_shift_1_2_3_recently(eid, week):
                        continue
                    last_shift = self.get_last_shift_1_2_3(eid)
                    if last_shift == '3':
                        continue
                    if shift3_needed.get(band, 0) > 0:
                        self.cursor.execute(
                            "INSERT INTO ShiftAssignments (employee_id, shift_code, week) VALUES (?, ?, ?)",
                            (eid, '3', week)
                        )
                        assigned_ids.add(eid)
                        shift3_needed[band] -= 1
                        last_domain_shift3[band] = domain
                        if domain == 'FD-SEL':
                            last_subdomain_shift3[band] = sub_domain
                        else:
                            last_subdomain_shift3[band] = None
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
