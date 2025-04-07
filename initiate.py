from persistence import *

import sys
import os

def add_branche(splittedline : list):
    repo.branches.insert(Branche(splittedline[0],splittedline[1],splittedline[2]))
    
def add_supplier(splittedline : list):
    repo.suppliers.insert(Supplier(splittedline[0],splittedline[1],splittedline[2]))

def add_product(splittedline : list):
    repo.products.insert(Product(splittedline[0],splittedline[1],splittedline[2],splittedline[3]))

def add_employee(splittedline : list):
    repo.employees.insert(Employee(splittedline[0],splittedline[1],splittedline[2],splittedline[3]))

adders = {  "B": add_branche,
            "S": add_supplier,
            "P": add_product,
            "E": add_employee}

def main(args : list):
    inputfilename =  args[1] 
    repo._close()
    # uncomment if needed
    # delete the database file if it exists
    if os.path.isfile("bgumart.db"):
        os.remove("bgumart.db")
    repo.__init__()
    repo.create_tables()
    with open(inputfilename) as inputfile:
        for line in inputfile:
            splittedline : list = line.strip().split(",")
            adders.get(splittedline[0])(splittedline[1:])

if __name__ == '__main__':
    main(sys.argv)