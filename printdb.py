from persistence import *

def main():
    repo.activities.print()
    repo.branches.print()
    repo.employees.print()
    repo.products.print()
    repo.suppliers.print()
    repo.PrintEmployeesReport()
    repo.PrintActivitiesReport()

if __name__ == '__main__':
    main()