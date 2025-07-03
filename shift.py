import sqlite3
import random
import csv
from collections import defaultdict

class ShiftPlanner:
    def __init__(self, db_path='SEL_Employess_Data.db', weeks=21):
        self.db_path = db_path
        self.weeks = weeks
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

    def assigned_shift_recently(self, eid, current_week, gap=5, last_assigned_week=None):
        # Check in-memory assignments first
        if last_assigned_week and current_week - last_assigned_week[eid] < gap:
            return True
        # Then check DB (for previous runs)
        if current_week <= gap:
            return False
        self.cursor.execute("""
            SELECT week FROM ShiftAssignments
            WHERE employee_id = ? 
            AND shift_code IN ('1','2','3') 
            AND week > ?
        """, (eid, current_week - gap))
        return self.cursor.fetchone() is not None

    def get_shift_count(self, eid):
        self.cursor.execute("""
            SELECT COUNT(*) FROM ShiftAssignments 
            WHERE employee_id = ? AND shift_code IN ('1', '2', '3')
        """, (eid,))
        return self.cursor.fetchone()[0]

    def get_last_shift(self, eid):
        self.cursor.execute("""
            SELECT shift_code FROM ShiftAssignments
            WHERE employee_id = ? AND shift_code IN ('1', '2', '3')
            ORDER BY week DESC LIMIT 1
        """, (eid,))
        row = self.cursor.fetchone()
        return row[0] if row else None

    def get_employee_experience(self, eid):
        self.cursor.execute("SELECT Experience FROM Employees WHERE id = ?", (eid,))
        return self.cursor.fetchone()[0]

    def get_employees(self, exclude_ids=None, band=None, domain=None, sub_domain=None, exp=None):
        filters = []
        if exclude_ids:
            filters.append(f"id NOT IN ({','.join(map(str, exclude_ids))})")
        if band:
            filters.append(f"Band = '{band}'")
        if domain:
            filters.append(f"Domain = '{domain}'")
        if sub_domain:
            filters.append(f"Sub_Domain = '{sub_domain}'")
        if exp is not None:
            filters.append(f"Experience = {exp}")
        where_clause = " AND ".join(filters) if filters else "1=1"
        sql = f"""
            SELECT id, Name, Band, Experience, Domain, Sub_Domain
            FROM Employees
            WHERE {where_clause}
        """
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def get_balanced_candidate(self, candidates, week, last_domain=None, last_subdomain=None, shift_code=None, domain_alternate=None, subdomain_alternate=None):
        # Filter by alternation
        filtered = []
        for row in candidates:
            eid, _, _, _, domain, sub_domain = row
            if domain_alternate and domain == last_domain:
                continue
            if subdomain_alternate and domain == 'FD-SEL' and sub_domain == last_subdomain:
                continue
            if self.assigned_shift_recently(eid, week, gap=5):
                continue
            last_shift = self.get_last_shift(eid)
            if last_shift == shift_code:
                continue
            filtered.append(row)
        # Sort by least shifts assigned so far, then random
        random.shuffle(filtered)
        filtered.sort(key=lambda row: self.get_shift_count(row[0]))
        return filtered

    def get_pair_for_shift(self, candidates, week, domains, last_subdomain=None):
        # domains: list of required domains, e.g. ['AD-SEL', 'FD-SEL']
        pairs = []
        for c1 in candidates:
            for c2 in candidates:
                if c1[0] == c2[0]:
                    continue
                if {c1[4], c2[4]} == set(domains):
                    # FD-SEL alternation
                    fd = c1 if c1[4] == 'FD-SEL' else c2
                    if last_subdomain and fd[5] == last_subdomain:
                        continue
                    # 5 week gap
                    if self.assigned_shift_recently(c1[0], week, gap=5) or self.assigned_shift_recently(c2[0], week, gap=5):
                        continue
                    # Experience difference ≤ 2
                    if abs(int(c1[3]) - int(c2[3])) > 2:
                        continue
                    # Not assigned same shift last week
                    if self.get_last_shift(c1[0]) == '2' or self.get_last_shift(c2[0]) == '2':
                        continue
                    pairs.append((c1, c2))
        # Sort by total shifts assigned (balance)
        pairs.sort(key=lambda pair: max(self.get_shift_count(pair[0][0]), self.get_shift_count(pair[1][0])))
        if pairs:
            return pairs[0]
        return None

    def get_pair_for_shift3(self, candidates, week, last_domain, last_subdomain):
        # Need 1 Associate, 1 Layam, alternating domain/subdomain
        associates = [c for c in candidates if c[2] == 'Associate']
        layams = [c for c in candidates if c[2] == 'Layam']
        for a in associates:
            for l in layams:
                # Alternation for both
                if a[4] == last_domain['Associate']:
                    continue
                if l[4] == last_domain['Layam']:
                    continue
                if a[4] == 'FD-SEL' and a[5] == last_subdomain['Associate']:
                    continue
                if l[4] == 'FD-SEL' and l[5] == last_subdomain['Layam']:
                    continue
                # 5 week gap
                if self.assigned_shift_recently(a[0], week, gap=5) or self.assigned_shift_recently(l[0], week, gap=5):
                    continue
                # Experience difference ≤ 2
                if abs(int(a[3]) - int(l[3])) > 2:
                    continue
                # Not assigned same shift last week
                if self.get_last_shift(a[0]) == '3' or self.get_last_shift(l[0]) == '3':
                    continue
                return (a, l)
        return None

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

        last_domain_shift1 = None
        last_subdomain_shift1 = None
        last_fd_subdomain_shift2 = None
        last_domain_shift3 = {'Associate': None, 'Layam': None}
        last_subdomain_shift3 = {'Associate': None, 'Layam': None}

        # Track shift counts by experience for balancing (req 13)
        shift_counts_by_exp = defaultdict(lambda: defaultdict(int))
        last_assigned_week = defaultdict(lambda: -100)
        
        # from collections import deque

        # # Build round-robin queues for each group (band, experience)
        # group_queues = defaultdict(deque)
        # for eid, name, band, exp, domain, sub_domain in self.get_employees():
        #     if band in ('Associate', 'Layam') and int(exp) > 0:
        #         group_queues[(band, int(exp))].append(eid)
        
        for week in range(1, self.weeks + 1):
            assigned_ids = set()

            # 1. Experience 0 → Shift G (req 1, 7)
            exp0_emps = self.get_employees(exp=0)
            for eid, *_ in exp0_emps:
                self.cursor.execute(
                    "INSERT INTO ShiftAssignments (employee_id, shift_code, week) VALUES (?, ?, ?)",
                    (eid, 'G', week)
                )
                assigned_ids.add(eid)

            # Shift 1: 1 Associate, alternate domain/subdomain (req 2, 3, 4, 10, 13)
            associates = self.get_employees(exclude_ids=assigned_ids, band='Associate')
            associates = [a for a in associates if int(a[3]) > 0]
            associates.sort(key=lambda a: (shift_counts_by_exp[int(a[3])][a[0]], self.get_shift_count(a[0])))
            candidates = []
            for a in associates:
                eid, _, _, _, domain, sub_domain = a
                if self.assigned_shift_recently(eid, week, gap=5, last_assigned_week=last_assigned_week):
                    continue
                if last_domain_shift1 and domain == last_domain_shift1:
                    continue
                if domain == 'FD-SEL' and last_subdomain_shift1 and sub_domain == last_subdomain_shift1:
                    continue
                # Do not assign same shift type as last week
                if self.get_last_shift(eid) == '1':
                    continue
                candidates.append(a)
            # Sort by fewest total shifts, then random
            random.shuffle(candidates)
            candidates.sort(key=lambda a: (shift_counts_by_exp[int(a[3])][a[0]], self.get_shift_count(a[0])))
            chosen = candidates[0] if candidates else None
            if not chosen:
                # fallback: relax alternation but NEVER relax 5-week gap
                for a in associates:
                    eid = a[0]
                    if eid in assigned_ids:
                        continue
                    if self.assigned_shift_recently(eid, week, gap=5, last_assigned_week=last_assigned_week):
                        continue
                    chosen = a
                    print(f"WARNING: Relaxed alternation for Shift 1 in week {week}")
                    break
            if chosen:
                eid, _, _, exp, domain, sub_domain = chosen
                self.cursor.execute(
                    "INSERT INTO ShiftAssignments (employee_id, shift_code, week) VALUES (?, ?, ?)",
                    (eid, '1', week)
                )
                assigned_ids.add(eid)
                last_domain_shift1 = domain
                last_subdomain_shift1 = sub_domain
                shift_counts_by_exp[int(exp)][eid] += 1
                last_assigned_week[eid] = week
                
            else:
                print(f"WARNING: No valid candidate for Shift 1 in week {week} (5-week gap strictly enforced)")

            # Shift 2: 2 Layam, 1 AD-SEL, 1 FD-SEL, alternate FD-SEL subdomain (req 2, 3, 5, 10, 11, 12, 13)
            layams = self.get_employees(exclude_ids=assigned_ids, band='Layam')
            layams = [l for l in layams if int(l[3]) > 0]
            pairs = []
            for i, l1 in enumerate(layams):
                for l2 in layams[i+1:]:
                    if l1[0] == l2[0]:
                        continue
                    domains = {l1[4], l2[4]}
                    if domains != {'AD-SEL', 'FD-SEL'}:
                        continue
                    fd = l1 if l1[4] == 'FD-SEL' else l2
                    if last_fd_subdomain_shift2 and fd[5] == last_fd_subdomain_shift2:
                        continue
                    if self.assigned_shift_recently(l1[0], week, gap=5, last_assigned_week=last_assigned_week) or self.assigned_shift_recently(l2[0], week, gap=5, last_assigned_week=last_assigned_week):
                        continue
                    if abs(int(l1[3]) - int(l2[3])) > 2:
                        continue
                    exps = sorted([int(l1[3]), int(l2[3])])
                    if not (exps == [1,3] or exps == [2,2] or exps[1]-exps[0]<=2):
                        continue
                    if self.get_last_shift(l1[0]) == '2' or self.get_last_shift(l2[0]) == '2':
                        continue
                    bal_max = max(shift_counts_by_exp[int(l1[3])][l1[0]], shift_counts_by_exp[int(l2[3])][l2[0]])
                    bal_sum = shift_counts_by_exp[int(l1[3])][l1[0]] + shift_counts_by_exp[int(l2[3])][l2[0]]
                    pairs.append((bal_max, bal_sum, l1, l2))
            pairs.sort(key=lambda x: (x[0], x[1]))
            chosen_pair = pairs[0][2:] if pairs else None
            if not chosen_pair:
                # fallback: relax alternation but NEVER relax 5-week gap
                fallback_pairs = []
                for i, l1 in enumerate(layams):
                    for l2 in layams[i+1:]:
                        if l1[0] == l2[0]:
                            continue
                        domains = {l1[4], l2[4]}
                        if domains != {'AD-SEL', 'FD-SEL'}:
                            continue
                        # STRICT: still enforce 5-week gap!
                        if self.assigned_shift_recently(l1[0], week, gap=5, last_assigned_week=last_assigned_week) or self.assigned_shift_recently(l2[0], week, gap=5, last_assigned_week=last_assigned_week):
                            continue
                        if abs(int(l1[3]) - int(l2[3])) > 2:
                            continue
                        exps = sorted([int(l1[3]), int(l2[3])])
                        if not (exps == [1,3] or exps == [2,2] or exps[1]-exps[0]<=2):
                            continue
                        fd = l1 if l1[4] == 'FD-SEL' else l2
                        subdomain_penalty = 0 if not last_fd_subdomain_shift2 or fd[5] != last_fd_subdomain_shift2 else 1
                        bal = max(shift_counts_by_exp[exps[0]][l1[0]], shift_counts_by_exp[exps[1]][l2[0]])
                        fallback_pairs.append((subdomain_penalty, bal, l1, l2))
                fallback_pairs.sort(key=lambda x: (x[0], x[1]))
                chosen_pair = fallback_pairs[0][2:] if fallback_pairs else None
                if chosen_pair:
                    print(f"WARNING: Relaxed alternation for Shift 2 in week {week}")
            if chosen_pair:
                for l in chosen_pair:
                    eid, _, _, exp, domain, sub_domain = l
                    self.cursor.execute(
                        "INSERT INTO ShiftAssignments (employee_id, shift_code, week) VALUES (?, ?, ?)",
                        (eid, '2', week)
                    )
                    assigned_ids.add(eid)
                    shift_counts_by_exp[int(exp)][eid] += 1
                    last_assigned_week[eid] = week
                    if domain == 'FD-SEL':
                        last_fd_subdomain_shift2 = sub_domain
            else:
                print(f"WARNING: No valid pair for Shift 2 in week {week} (5-week gap strictly enforced)")
                
            # Shift 3: 1 Associate + 1 Layam, alternate domain/subdomain for both (req 2, 3, 6, 10, 11, 12, 13)
            candidates = self.get_employees(exclude_ids=assigned_ids)
            associates = [c for c in candidates if c[2] == 'Associate' and int(c[3]) > 0]
            layams = [c for c in candidates if c[2] == 'Layam' and int(c[3]) > 0]

            def valid_pairs(associates, layams, enforce_assoc_alt=True, enforce_layam_alt=True, enforce_gap=True):
                pairs = []
                for a in associates:
                    for l in layams:
                        # Alternation for Associate
                        if enforce_assoc_alt and last_domain_shift3['Associate'] and a[4] == last_domain_shift3['Associate']:
                            continue
                        if enforce_assoc_alt and a[4] == 'FD-SEL' and last_subdomain_shift3['Associate'] and a[5] == last_subdomain_shift3['Associate']:
                            continue
                        # Alternation for Layam
                        if enforce_layam_alt and last_domain_shift3['Layam'] and l[4] == last_domain_shift3['Layam']:
                            continue
                        if enforce_layam_alt and l[4] == 'FD-SEL' and last_subdomain_shift3['Layam'] and l[5] == last_subdomain_shift3['Layam']:
                            continue
                        # 5 week gap
                        if enforce_gap and (self.assigned_shift_recently(a[0], week, gap=5, last_assigned_week=last_assigned_week) or self.assigned_shift_recently(l[0], week, gap=5, last_assigned_week=last_assigned_week)):
                            continue
                        # Experience difference ≤ 2
                        if abs(int(a[3]) - int(l[3])) > 2:
                            continue
                        # Experience pairs mostly (3,1) or (2,2)
                        exps = sorted([int(a[3]), int(l[3])])
                        if not (exps == [1,3] or exps == [2,2] or exps[1]-exps[0]<=2):
                            continue
                        if self.get_last_shift(a[0]) == '3' or self.get_last_shift(l[0]) == '3':
                            continue
                        # Prefer Layam FD-SEL subdomain alternation if possible
                        layam_fd_penalty = 0
                        if l[4] == 'FD-SEL' and last_subdomain_shift3['Layam'] and l[5] == last_subdomain_shift3['Layam']:
                            layam_fd_penalty = 1
                        bal_max = max(shift_counts_by_exp[int(a[3])][a[0]], shift_counts_by_exp[int(l[3])][l[0]])
                        bal_sum = shift_counts_by_exp[int(a[3])][a[0]] + shift_counts_by_exp[int(l[3])][l[0]]
                        pairs.append((layam_fd_penalty, bal_max, bal_sum, a, l))
                random.shuffle(pairs)
                pairs.sort(key=lambda x: (x[0], x[1], x[2]))
                return pairs

            # Try strict alternation for both
            pairs = valid_pairs(associates, layams, enforce_assoc_alt=True, enforce_layam_alt=True, enforce_gap=True)
            if not pairs:
                # Relax Layam alternation only
                pairs = valid_pairs(associates, layams, enforce_assoc_alt=True, enforce_layam_alt=False, enforce_gap=True)
                if pairs:
                    print(f"WARNING: Relaxed Layam alternation for Shift 3 in week {week}")
            if not pairs:
                # Relax Associate alternation only
                pairs = valid_pairs(associates, layams, enforce_assoc_alt=False, enforce_layam_alt=True, enforce_gap=True)
                if pairs:
                    print(f"WARNING: Relaxed Associate alternation for Shift 3 in week {week}")
            if not pairs:
                # Relax both alternations (last resort)
                pairs = valid_pairs(associates, layams, enforce_assoc_alt=False, enforce_layam_alt=False, enforce_gap=True)
                if pairs:
                    print(f"WARNING: Relaxed both alternations for Shift 3 in week {week}")

            chosen_pair = pairs[0][3:] if pairs else None
            if chosen_pair:
                for c in chosen_pair:
                    eid, _, band, exp, domain, sub_domain = c
                    self.cursor.execute(
                        "INSERT INTO ShiftAssignments (employee_id, shift_code, week) VALUES (?, ?, ?)",
                        (eid, '3', week)
                    )
                    assigned_ids.add(eid)
                    shift_counts_by_exp[int(exp)][eid] += 1
                    last_assigned_week[eid] = week
                    last_domain_shift3[band] = domain
                    last_subdomain_shift3[band] = sub_domain
            else:
                print(f"WARNING: No valid pair for Shift 3 in week {week} (5-week gap strictly enforced)")

            # 5. Rest → Shift G (req 2, 9)
            remaining = self.get_employees(exclude_ids=assigned_ids)
            for eid, *_ in remaining:
                self.cursor.execute(
                    "INSERT INTO ShiftAssignments (employee_id, shift_code, week) VALUES (?, ?, ?)",
                    (eid, 'G', week)
                )
                assigned_ids.add(eid)

        self.conn.commit()

        # Output results (req 8)
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
            # Only FD-SEL should have subdomain (req 7)
            sub_domain = sub_domain if domain == 'FD-SEL' else ''
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


# Usage
if __name__ == "__main__":
    planner = ShiftPlanner()
    planner.run()
