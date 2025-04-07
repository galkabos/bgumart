import sqlite3
import atexit
from dbtools import Dao
 
# Data Transfer Objects:
class Employee(object):
    def __init__(self, id, name, salary, branche):
        self.id = id
        self.name = name
        self.salary = salary
        self.branche = branche
        
class Employees:
    def __init__(self,conn):
        self._conn = conn
        
    def insert(self, Employee): # insert Employee DTO
        self._conn.execute("""
             INSERT INTO employees (id, name, salary, branche)
	      VALUES (?, ?, ?,?)
           """, [Employee.id,  Employee.name,  Employee.salary, Employee.branche])
        
    def print(self):
        print("Employees")
        l1= repo.execute_command("""
                SELECT * FROM employees ORDER BY id ASC
            """)
        for av in l1:
            print("(" + str(av[0]) + "," + str(av[1]) + "," + str(av[2]) + "," + str(av[3]) + ")") 
 
class Supplier(object):
    def __init__(self, id, name, contact_information):
        self.id = id
        self.name = name
        self.contact_information = contact_information     
   

class Suppliers:
    def __init__(self,conn):
        self._conn = conn

    def insert(self, SupplierDTO): # insert Supplier DTO
        self._conn.execute("""
             INSERT INTO suppliers (id, name, contact_information)
	      VALUES (?, ?, ?)
           """, [SupplierDTO.id,  SupplierDTO.name,  SupplierDTO.contact_information])
        
    def print(self):
        print("Suppliers")
        l1= repo.execute_command("""
                SELECT * FROM suppliers ORDER BY id ASC
            """)
        for av in l1:
            print("(" + str(av[0]) + "," + str(av[1]) + "," + str(av[2]) + ")") 
        
class Product(object):
    def __init__(self, id, description , price, quantity):
        self.id = id
        self.description  = description 
        self.price = price
        self.quantity = quantity
        
class Products:
    def __init__(self,conn):
        self._conn = conn
    
    def insert(self, ProductDTO): # insert Product DTO
        self._conn.execute("""
                INSERT INTO products (id, description, price, quantity)
            VALUES (?, ?, ?, ?)
            """, [ProductDTO.id,  ProductDTO.description,  ProductDTO.price, ProductDTO.quantity])
        #Dao.insert(self,ProductDTO)
    
    def update(self, quantityTmp, idTmp):
        self._conn.cursor().execute("""
                UPDATE products SET quantity = (?) WHERE id = (?)
            """, [quantityTmp , idTmp])
    
    def getQuantity(self,productId): 
        return self._conn.cursor().execute("""
                SELECT quantity FROM products WHERE id = (?)
            """, [productId]).fetchone()[0]
        
    def print(self):
        print("Products")
        l1= repo.execute_command("""
                SELECT * FROM products ORDER BY id ASC
            """)
        for av in l1:
            print("(" + str(av[0]) + "," + str(av[1]) + "," + str(av[2]) + "," + str(av[3]) + ")") 

class Branche(object):
    def __init__(self, id, location, number_of_employees):
        self.id = id
        self.location = location
        self.number_of_employees = number_of_employees

class Branches:
    def __init__(self,conn):
        self._conn = conn
    
    def insert(self, BrancheDTO): # insert Branche DTO
        self._conn.execute("""
                INSERT INTO branches (id, location, number_of_employees)
            VALUES (?, ?, ?)
            """, [BrancheDTO.id,  BrancheDTO.location,  BrancheDTO.number_of_employees])
        
    def print(self):
        print("Branches")
        l1= repo.execute_command("""
                SELECT * FROM branches ORDER BY id ASC
            """)
        for av in l1:
            print("(" + str(av[0]) + "," + str(av[1]) + "," + str(av[2]) + ")") 
        

class Activitie(object):
    def __init__(self, product_id, quantity , activator_id, date):
        self.product_id = product_id
        self.quantity  = quantity 
        self.activator_id = activator_id
        self.date = date

class Activities:
    def __init__(self,conn):
        self._conn = conn
        
    def insert(self, ActivitieDTO): # insert Activitie DTO
        self._conn.execute("""
                INSERT INTO activities (product_id, quantity, activator_id, date)
            VALUES (?, ?, ?, ?)
            """, [ActivitieDTO.product_id,  ActivitieDTO.quantity,  ActivitieDTO.activator_id,  ActivitieDTO.date]) 
        
    def print(self):
        print("Activities")
        l1= repo.execute_command("""
                SELECT * FROM activities ORDER BY date ASC
            """)
        for av in l1:
            print("(" + str(av[0]) + "," + str(av[1]) + "," + str(av[2]) + "," + str(av[3]) + ")") 
            
 
#Repository
class Repository(object):
    def __init__(self):
        self._conn = sqlite3.connect('bgumart.db')
        #self._conn.text_factory = bytes
        
        self.employees = Employees(self._conn)
        self.suppliers = Suppliers(self._conn)
        self.products = Products(self._conn)
        self.branches = Branches(self._conn)
        self.activities = Activities(self._conn)
 
    def _close(self):
        self._conn.commit()
        self._conn.close()
 
    def create_tables(self):
        self._conn.executescript("""
            CREATE TABLE employees (
                id              INT         PRIMARY KEY,
                name            TEXT        NOT NULL,
                salary          REAL        NOT NULL,
                branche    INT REFERENCES branches(id)
            );
    
            CREATE TABLE suppliers (
                id                   INTEGER    PRIMARY KEY,
                name                 TEXT       NOT NULL,
                contact_information  TEXT
            );

            CREATE TABLE products (
                id          INTEGER PRIMARY KEY,
                description TEXT    NOT NULL,
                price       REAL NOT NULL,
                quantity    INTEGER NOT NULL
            );

            CREATE TABLE branches (
                id                  INTEGER     PRIMARY KEY,
                location            TEXT        NOT NULL,
                number_of_employees INTEGER
            );
    
            CREATE TABLE activities (
                product_id      INTEGER REFERENCES products(id),
                quantity        INTEGER NOT NULL,
                activator_id    INTEGER NOT NULL,
                date            TEXT    NOT NULL
            );
        """)

    def execute_command(self, script: str) -> list:
        return self._conn.cursor().execute(script).fetchall()
    
    def PrintEmployeesReport(self):
        l1 = repo.execute_command("""
            SELECT employees.name, employees.salary, branches.location,coalesce(tavla.sale,0) as sales
FROM employees LEFT JOIN 
(SELECT employees.name as n, sum(h.t) as sale
FROM employees,(
SELECT activities.activator_id, activities.product_id, activities.quantity, products.price, -1*products.price*activities.quantity as t
FROM products, activities
where products.id = activities.product_id
) as h 
WHERE employees.id = h.activator_id
GROUP BY employees.name
) as tavla ON employees.name = tavla.n
LEFT JOIN branches ON employees.branche = branches.id
ORDER BY employees.name ASC
        """)
        for av in l1:
            print(str(av[0]) + " " + str(av[1]) + " " + str(av[2]) + " " + str(av[3]))
    
    def PrintActivitiesReport(self):
        l1 = repo.execute_command("""
            SELECT tavla2.dateOfActivity, tavla2.itemDecription, tavla2.quantity, coalesce(tavla2.nameOfSeller,"None"), coalesce(suppliers.name,"None")
FROM 
(SELECT tavla1.t11 as t21, tavla1.t12 as dateOfActivity, tavla1.t13 as t23, tavla1.t14 as quantity, tavla1.t15 as itemDecription, employees.name as nameOfSeller
FROM  (
SELECT activities.activator_id as t11, activities.date as t12, activities.product_id as t13, activities.quantity as t14, products.description as t15
FROM products, activities
WHERE products.id = activities.product_id
) as tavla1 LEFT JOIN employees  ON employees.id = tavla1.t11) as tavla2 LEFT JOIN suppliers ON tavla2.t21 = suppliers.id
ORDER BY tavla2.dateOfActivity DESC
        """)
        for av in l1:
            print("(" + str(av[0]) + ", " + str(av[1]) + ", " + str(av[2]) + ", " + str(av[3]) + ", " + str(av[4]) + ")")
     
# singleton
repo = Repository()
atexit.register(repo._close)