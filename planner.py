import sqlite3
import pandas as pd
import random

class ShiftPlanner:
    def __init__(
        self,
        db_path: str,
        output_csv: str,
        table_name: str = "Employees",
        num_weeks: int = 21,
    ):
        """
        db_path: path to your SEL_Employess_Data.db
        output_csv: path where the final CSV will be saved
        table_name: the table to read (default "Employees")
        num_weeks: how many week columns to add
        """
        self.db_path     = db_path
        self.output_csv  = output_csv
        self.table_name  = table_name
        self.num_weeks   = num_weeks
        self.df          = pd.DataFrame()

        # cycle for shift 1
        self.shift1_cycle = [
            ("AD-SEL",    None),
            ("FD-SEL",    "Transmission"),
            ("FD-SEL",    "Hydraulic"),
        ]
        # 2-step cycles for shift 2 & shift 3 sub-domain toggles
        self.shift2_cycle = ["Transmission", "Hydraulic"]
        self.shift3_cycle = ["Hydraulic",    "Transmission"]

    def load_employees(self) -> None:
        """Read the entire Employees table into a DataFrame."""
        conn = sqlite3.connect(self.db_path)
        try:
            self.df = pd.read_sql_query(f"SELECT * FROM {self.table_name}", conn)
        finally:
            conn.close()

    def add_weeks(self) -> None:
        """Add Week 1 … Week N columns (empty by default)."""
        for wk in range(1, self.num_weeks + 1):
            self.df[f"Week {wk}"] = ""
            
    def check_exp_never_assigned(self):
        """
        Print a warning for any experienced employee who never got a shift.
        """
        week_cols = [f"Week {wk}" for wk in range(1, self.num_weeks + 1)]
        exp = pd.to_numeric(self.df["Experience"], errors="coerce").fillna(0)
        exp_idxs = self.df.index[exp > 0].tolist()
        for i in exp_idxs:
            if all(self.df.at[i, col] == "" or self.df.at[i, col] == "G" for col in week_cols):
                print(f"Warning: Employee {self.df.at[i, 'Name']} (index {i}) never assigned a shift.")

    def assign_zero_exp_shift(self) -> None:
        """
        For every employee whose `Experience` == 0,
        fill all Week columns with "G" (the G shift).
        """
        week_cols = [f"Week {wk}" for wk in range(1, self.num_weeks + 1)]
        if "Experience" not in self.df.columns:
            return
        exp  = pd.to_numeric(self.df["Experience"], errors="coerce").fillna(-1)
        mask = exp == 0
        self.df.loc[mask, week_cols] = "G"

    def choose_shift1_associate(self, idxs: list[int], wk_num: int) -> list[int]:
        """
        Pick one Associate for shift "1" according to shift1_cycle.
        Move that index to position 0 so it receives "1".
        """
        assoc = [i for i in idxs if self.df.at[i, "Band"] == "Associate"]
        if not assoc:
            return idxs

        dom, sd = self.shift1_cycle[(wk_num - 1) % len(self.shift1_cycle)]
        sel = None
        if sd is not None:
            sel = next(
                (i for i in assoc
                 if self.df.at[i, "Domain"]     == dom and
                    self.df.at[i, "Sub_Domain"] == sd),
                None
            )
        if sel is None:
            sel = next(
                (i for i in assoc
                 if self.df.at[i, "Domain"] == dom),
                None
            )
        if sel is None:
            sel = assoc[0]

        rest = [j for j in idxs if j != sel]
        return [sel] + rest

    def choose_shift2_layam(self, idxs: list[int], wk_num: int) -> list[int]:
        """
        Ensure positions 1 & 2 are from Band "Layam",
        with different Domain and sub-domain toggled per week.
        """
        first  = idxs[0]
        others = idxs[1:]
        layam  = [i for i in others if self.df.at[i, "Band"] == "Layam"]
        if len(layam) < 2:
            return idxs

        # desired sub-domain for the 2nd slot this week
        desired2 = self.shift2_cycle[(wk_num - 1) % len(self.shift2_cycle)]

        # slot2 = Layam with that sub-domain (or fallback to any Layam)
        slot2 = next((i for i in layam
                      if self.df.at[i, "Sub_Domain"] == desired2),
                     layam[0])

        # slot1 = Layam ≠ slot2 and different Domain
        cand1 = [i for i in layam
                 if i != slot2 and
                    self.df.at[i, "Domain"] != self.df.at[slot2, "Domain"]]
        slot1 = cand1[0] if cand1 else next(i for i in layam if i != slot2)

        rest = [i for i in others if i not in (slot1, slot2)]
        return [first, slot1, slot2] + rest

    def choose_shift3_associates(self, idxs: list[int], wk_num: int) -> list[int]:
        """
        Pick exactly two people for shift '3':
          - slot-3: Band=='Associate'
          - slot-4: Band=='Layam'
          - slot-4's Sub_Domain toggles each week via shift3_cycle
          - slot-3's Domain is opposite slot-4's Domain
        """
        head = idxs[:3]
        tail = idxs[3:]
        desired4 = self.shift3_cycle[(wk_num - 1) % len(self.shift3_cycle)]

        # pools
        assoc = [i for i in tail if self.df.at[i, "Band"] == "Associate"]
        layam = [i for i in tail if self.df.at[i, "Band"] == "Layam"]
        if not assoc or not layam:
            return idxs  # fallback

        # pick slot-4 from Layam with desired Sub_Domain
        slot4 = next(
            (i for i in layam
             if self.df.at[i, "Sub_Domain"] == desired4),
            layam[0]
        )
        # pick slot-3 from Associate with opposite Domain
        slot3 = next(
            (i for i in assoc
             if self.df.at[i, "Domain"] != self.df.at[slot4, "Domain"]),
            assoc[0]
        )

        rest = [i for i in tail if i not in (slot3, slot4)]
        return head + [slot3, slot4] + rest
    
    def randomize_exp_shift_assignment(self, week_cols, base_idxs, pattern):
        """
        Randomly assign experienced employees to shifts each week,
        ensuring each week has 1×"1", 2×"2", 2×"3" and
        all experienced employees get at least one shift in the schedule.
        """
        while True:
            # Clear previous assignments
            for col in week_cols:
                self.df.loc[:, col] = ""

            last_assigned: dict[int, int] = {}

            for wk_num, col in enumerate(week_cols, start=1):
                idxs = base_idxs.copy()
                random.shuffle(idxs)  # Randomize order each week

                # pick only those with at least a gap of 5, else 4, else 3
                for gap in (5, 4, 3):
                    elig = [i for i in idxs if wk_num - last_assigned.get(i, 0) >= gap]
                    if len(elig) >= len(pattern):
                        idxs = elig.copy()
                        break
                else:
                    idxs = idxs[:]

                idxs = self.choose_shift1_associate(idxs, wk_num)
                idxs = self.choose_shift2_layam(idxs, wk_num)
                idxs = self.choose_shift3_associates(idxs, wk_num)

                # assign shifts and record assignment week
                for i, shift in zip(idxs, pattern):
                    self.df.at[i, col] = shift
                    last_assigned[i] = wk_num

            # Check if all experienced employees have at least one shift
            all_assigned = True
            for i in base_idxs:
                if all(self.df.at[i, col] == "" or self.df.at[i, col] == "G" for col in week_cols):
                    all_assigned = False
                    break
            if all_assigned:
                break
            
    import sqlite3
import pandas as pd
import random

class ShiftPlanner:
    def __init__(
        self,
        db_path: str,
        output_csv: str,
        table_name: str = "Employees",
        num_weeks: int = 21,
    ):
        """
        db_path: path to your SEL_Employess_Data.db
        output_csv: path where the final CSV will be saved
        table_name: the table to read (default "Employees")
        num_weeks: how many week columns to add
        """
        self.db_path     = db_path
        self.output_csv  = output_csv
        self.table_name  = table_name
        self.num_weeks   = num_weeks
        self.df          = pd.DataFrame()

        # cycle for shift 1
        self.shift1_cycle = [
            ("AD-SEL",    None),
            ("FD-SEL",    "Transmission"),
            ("FD-SEL",    "Hydraulic"),
        ]
        # 2-step cycles for shift 2 & shift 3 sub-domain toggles
        self.shift2_cycle = ["Transmission", "Hydraulic"]
        self.shift3_cycle = ["Hydraulic",    "Transmission"]

    def load_employees(self) -> None:
        """Read the entire Employees table into a DataFrame."""
        conn = sqlite3.connect(self.db_path)
        try:
            self.df = pd.read_sql_query(f"SELECT * FROM {self.table_name}", conn)
        finally:
            conn.close()

    def add_weeks(self) -> None:
        """Add Week 1 … Week N columns (empty by default)."""
        for wk in range(1, self.num_weeks + 1):
            self.df[f"Week {wk}"] = ""
            
    def check_exp_never_assigned(self):
        """
        Print a warning for any experienced employee who never got a shift.
        """
        week_cols = [f"Week {wk}" for wk in range(1, self.num_weeks + 1)]
        exp = pd.to_numeric(self.df["Experience"], errors="coerce").fillna(0)
        exp_idxs = self.df.index[exp > 0].tolist()
        for i in exp_idxs:
            if all(self.df.at[i, col] == "" or self.df.at[i, col] == "G" for col in week_cols):
                print(f"Warning: Employee {self.df.at[i, 'Name']} (index {i}) never assigned a shift.")

    def assign_zero_exp_shift(self) -> None:
        """
        For every employee whose `Experience` == 0,
        fill all Week columns with "G" (the G shift).
        """
        week_cols = [f"Week {wk}" for wk in range(1, self.num_weeks + 1)]
        if "Experience" not in self.df.columns:
            return
        exp  = pd.to_numeric(self.df["Experience"], errors="coerce").fillna(-1)
        mask = exp == 0
        self.df.loc[mask, week_cols] = "G"

    def choose_shift1_associate(self, idxs: list[int], wk_num: int) -> list[int]:
        """
        Pick one Associate for shift "1" according to shift1_cycle.
        Move that index to position 0 so it receives "1".
        """
        assoc = [i for i in idxs if self.df.at[i, "Band"] == "Associate"]
        if not assoc:
            return idxs

        dom, sd = self.shift1_cycle[(wk_num - 1) % len(self.shift1_cycle)]
        sel = None
        if sd is not None:
            sel = next(
                (i for i in assoc
                 if self.df.at[i, "Domain"]     == dom and
                    self.df.at[i, "Sub_Domain"] == sd),
                None
            )
        if sel is None:
            sel = next(
                (i for i in assoc
                 if self.df.at[i, "Domain"] == dom),
                None
            )
        if sel is None:
            sel = assoc[0]

        rest = [j for j in idxs if j != sel]
        return [sel] + rest

    def choose_shift2_layam(self, idxs: list[int], wk_num: int) -> list[int]:
        """
        Ensure positions 1 & 2 are from Band "Layam",
        with different Domain and sub-domain toggled per week.
        """
        first  = idxs[0]
        others = idxs[1:]
        layam  = [i for i in others if self.df.at[i, "Band"] == "Layam"]
        if len(layam) < 2:
            return idxs

        # desired sub-domain for the 2nd slot this week
        desired2 = self.shift2_cycle[(wk_num - 1) % len(self.shift2_cycle)]

        # slot2 = Layam with that sub-domain (or fallback to any Layam)
        slot2 = next((i for i in layam
                      if self.df.at[i, "Sub_Domain"] == desired2),
                     layam[0])

        # slot1 = Layam ≠ slot2 and different Domain
        cand1 = [i for i in layam
                 if i != slot2 and
                    self.df.at[i, "Domain"] != self.df.at[slot2, "Domain"]]
        slot1 = cand1[0] if cand1 else next(i for i in layam if i != slot2)

        rest = [i for i in others if i not in (slot1, slot2)]
        return [first, slot1, slot2] + rest

    def choose_shift3_associates(self, idxs: list[int], wk_num: int) -> list[int]:
        """
        Pick exactly two people for shift '3':
          - slot-3: Band=='Associate'
          - slot-4: Band=='Layam'
          - slot-4's Sub_Domain toggles each week via shift3_cycle
          - slot-3's Domain is opposite slot-4's Domain
        """
        head = idxs[:3]
        tail = idxs[3:]
        desired4 = self.shift3_cycle[(wk_num - 1) % len(self.shift3_cycle)]

        # pools
        assoc = [i for i in tail if self.df.at[i, "Band"] == "Associate"]
        layam = [i for i in tail if self.df.at[i, "Band"] == "Layam"]
        if not assoc or not layam:
            return idxs  # fallback

        # pick slot-4 from Layam with desired Sub_Domain
        slot4 = next(
            (i for i in layam
             if self.df.at[i, "Sub_Domain"] == desired4),
            layam[0]
        )
        # pick slot-3 from Associate with opposite Domain
        slot3 = next(
            (i for i in assoc
             if self.df.at[i, "Domain"] != self.df.at[slot4, "Domain"]),
            assoc[0]
        )

        rest = [i for i in tail if i not in (slot3, slot4)]
        return head + [slot3, slot4] + rest
    
    def randomize_exp_shift_assignment(self, week_cols, base_idxs, pattern):
        """
        Randomly assign experienced employees to shifts each week,
        ensuring each week has 1×"1", 2×"2", 2×"3" and
        all experienced employees get at least one shift in the schedule.
        """
        while True:
            # Clear previous assignments
            for col in week_cols:
                self.df.loc[:, col] = ""

            last_assigned: dict[int, int] = {}

            for wk_num, col in enumerate(week_cols, start=1):
                idxs = base_idxs.copy()
                random.shuffle(idxs)  # Randomize order each week

                # pick only those with at least a gap of 5, else 4, else 3
                for gap in (5, 4, 3):
                    elig = [i for i in idxs if wk_num - last_assigned.get(i, 0) >= gap]
                    if len(elig) >= len(pattern):
                        idxs = elig.copy()
                        break
                else:
                    idxs = idxs[:]

                idxs = self.choose_shift1_associate(idxs, wk_num)
                idxs = self.choose_shift2_layam(idxs, wk_num)
                idxs = self.choose_shift3_associates(idxs, wk_num)

                # assign shifts and record assignment week
                for i, shift in zip(idxs, pattern):
                    self.df.at[i, col] = shift
                    last_assigned[i] = wk_num

            # Check if all experienced employees have at least one shift
            all_assigned = True
            for i in base_idxs:
                if all(self.df.at[i, col] == "" or self.df.at[i, col] == "G" for col in week_cols):
                    all_assigned = False
                    break
            if all_assigned:
                break
            
    def fair_group_shift_assignment(self, week_cols, base_idxs, pattern):
        """
        Assign shifts so that for every group with the same (Experience, Band, Domain, Sub_Domain),
        each member gets almost the same number of shifts, while fulfilling all shift constraints.
        """
        group_cols = ["Experience", "Band", "Domain", "Sub_Domain"]
        group_map = {}
        for i in base_idxs:
            key = tuple(self.df.loc[i, col] for col in group_cols)
            group_map.setdefault(key, []).append(i)

        while True:
            # Clear previous assignments
            for col in week_cols:
                self.df.loc[:, col] = ""

            last_assigned: dict[int, int] = {}

            for wk_num, col in enumerate(week_cols, start=1):
                idxs = base_idxs.copy()
                random.shuffle(idxs)

                # pick only those with at least a gap of 5, else 4, else 3
                for gap in (5, 4, 3):
                    elig = [i for i in idxs if wk_num - last_assigned.get(i, 0) >= gap]
                    if len(elig) >= len(pattern):
                        idxs = elig.copy()
                        break
                else:
                    idxs = idxs[:]

                idxs = self.choose_shift1_associate(idxs, wk_num)
                idxs = self.choose_shift2_layam(idxs, wk_num)
                idxs = self.choose_shift3_associates(idxs, wk_num)

                # assign shifts and record assignment week
                for i, shift in zip(idxs, pattern):
                    self.df.at[i, col] = shift
                    last_assigned[i] = wk_num

            # Check all experienced employees have at least one shift
            all_assigned = True
            for i in base_idxs:
                if all(self.df.at[i, col] == "" or self.df.at[i, col] == "G" for col in week_cols):
                    all_assigned = False
                    break
            if not all_assigned:
                continue

            # Check fairness: for each group, max-min assigned shifts ≤ 1
            fair = True
            for members in group_map.values():
                shift_counts = [
                    sum(self.df.at[i, col] in ("1", "2", "3") for col in week_cols)
                    for i in members
                ]
                if max(shift_counts) - min(shift_counts) > 1:
                    fair = False
                    break
            if fair:
                break

    def assign_exp_shifts(self) -> None:
        """
        For employees with Experience > 0, assign each week:
          1×"1", 2×"2", and 2×"3"
        with Shift-1 via choose_shift1_associate,
        Shift-2 via choose_shift2_layam,
        Shift-3 via choose_shift3_associates.
        Uses randomization to ensure fairness and coverage.
        """
        week_cols = [f"Week {wk}" for wk in range(1, self.num_weeks + 1)]
        exp = pd.to_numeric(self.df["Experience"], errors="coerce").fillna(0)
        base_idxs = self.df.index[exp > 0].tolist()
        pattern = ["1", "2", "2", "3", "3"]

        self.randomize_exp_shift_assignment(week_cols, base_idxs, pattern)

    def enforce_pair_alternation(self, idxs, pos_a, pos_b):
        """
        Ensure idxs[pos_a] & idxs[pos_b] come from different Domains.
        Swap in a later row if needed.
        """
        if pos_a >= len(idxs) or pos_b >= len(idxs):
            return idxs
        a, b = idxs[pos_a], idxs[pos_b]
        if self.df.at[a, "Domain"] == self.df.at[b, "Domain"]:
            for j in range(pos_b + 1, len(idxs)):
                if self.df.at[idxs[j], "Domain"] != self.df.at[a, "Domain"]:
                    idxs[pos_b], idxs[j] = idxs[j], idxs[pos_b]
                    break
        return idxs

    def enforce_subdomain_toggle(self, idxs, pos_a, pos_b, desired_sub):
        """
        After domain alternation, ensure idxs[pos_b] has Sub_Domain == desired_sub.
        Swap in a later row if needed.
        """
        if pos_a >= len(idxs) or pos_b >= len(idxs):
            return idxs
        a, b = idxs[pos_a], idxs[pos_b]
        if self.df.at[b, "Sub_Domain"] == desired_sub:
            return idxs

        domain_a = self.df.at[a, "Domain"]
        for j in range(pos_b + 1, len(idxs)):
            cand = idxs[j]
            if (
                self.df.at[cand, "Domain"]     != domain_a and
                self.df.at[cand, "Sub_Domain"] == desired_sub
            ):
                idxs[pos_b], idxs[j] = idxs[j], idxs[pos_b]
                break
        return idxs

    def print_data(self) -> None:
        """Dump the resulting DataFrame to stdout."""
        print(self.df.to_string(index=False))

    def save_csv(self) -> None:
        """Save the DataFrame to CSV (no index)."""
        self.df.to_csv(self.output_csv, index=False)

    def run(self) -> None:
        """Do everything in order, without touching the DB."""
        self.load_employees()
        self.add_weeks()
        self.assign_zero_exp_shift()
        self.assign_exp_shifts()
        # fill any remaining empty slots with "G"
        for wk in range(1, self.num_weeks + 1):
            col = f"Week {wk}"
            self.df.loc[self.df[col] == "", col] = "G"
            self.check_exp_never_assigned()
        self.print_data()
        self.save_csv()

    def assign_exp_shifts(self, fair_group=True) -> None:
        """
        For employees with Experience > 0, assign each week:
          1×"1", 2×"2", and 2×"3"
        with Shift-1 via choose_shift1_associate,
        Shift-2 via choose_shift2_layam,
        Shift-3 via choose_shift3_associates.
        Uses randomization to ensure fairness and coverage.
        """
        week_cols = [f"Week {wk}" for wk in range(1, self.num_weeks + 1)]
        exp = pd.to_numeric(self.df["Experience"], errors="coerce").fillna(0)
        base_idxs = self.df.index[exp > 0].tolist()
        pattern = ["1", "2", "2", "3", "3"]

        if fair_group:
            self.fair_group_shift_assignment(week_cols, base_idxs, pattern)
        else:
            self.randomize_exp_shift_assignment(week_cols, base_idxs, pattern)

    def enforce_pair_alternation(self, idxs, pos_a, pos_b):
        """
        Ensure idxs[pos_a] & idxs[pos_b] come from different Domains.
        Swap in a later row if needed.
        """
        if pos_a >= len(idxs) or pos_b >= len(idxs):
            return idxs
        a, b = idxs[pos_a], idxs[pos_b]
        if self.df.at[a, "Domain"] == self.df.at[b, "Domain"]:
            for j in range(pos_b + 1, len(idxs)):
                if self.df.at[idxs[j], "Domain"] != self.df.at[a, "Domain"]:
                    idxs[pos_b], idxs[j] = idxs[j], idxs[pos_b]
                    break
        return idxs

    def enforce_subdomain_toggle(self, idxs, pos_a, pos_b, desired_sub):
        """
        After domain alternation, ensure idxs[pos_b] has Sub_Domain == desired_sub.
        Swap in a later row if needed.
        """
        if pos_a >= len(idxs) or pos_b >= len(idxs):
            return idxs
        a, b = idxs[pos_a], idxs[pos_b]
        if self.df.at[b, "Sub_Domain"] == desired_sub:
            return idxs

        domain_a = self.df.at[a, "Domain"]
        for j in range(pos_b + 1, len(idxs)):
            cand = idxs[j]
            if (
                self.df.at[cand, "Domain"]     != domain_a and
                self.df.at[cand, "Sub_Domain"] == desired_sub
            ):
                idxs[pos_b], idxs[j] = idxs[j], idxs[pos_b]
                break
        return idxs

    def print_data(self) -> None:
        """Dump the resulting DataFrame to stdout."""
        print(self.df.to_string(index=False))

    def save_csv(self) -> None:
        """Save the DataFrame to CSV (no index)."""
        self.df.to_csv(self.output_csv, index=False)

    def run(self) -> None:
        """Do everything in order, without touching the DB."""
        self.load_employees()
        self.add_weeks()
        self.assign_zero_exp_shift()
        self.assign_exp_shifts()
        # fill any remaining empty slots with "G"
        for wk in range(1, self.num_weeks + 1):
            col = f"Week {wk}"
            self.df.loc[self.df[col] == "", col] = "G"
            self.check_exp_never_assigned()
        self.print_data()
        self.save_csv()
