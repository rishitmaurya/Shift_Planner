from planner import ShiftPlanner

def main():
    planner = ShiftPlanner(
        db_path="SEL_Employess_Data.db",
        output_csv="updated_employees.csv"
    )
    planner.run()

if __name__ == "__main__":
    main()
