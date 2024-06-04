import unittest

from database.database import Database
from executor.executor import QueryExecutor
from parser.lexer import Lexer
from parser.parser import Parser

class TestSQLEngine(unittest.TestCase):
    def setUp(self):
        """Initialize a fresh database and executor for each test."""
        self.db = Database()
        self.executor = QueryExecutor(self.db)

    def execute_sql(self, sql):
        """Helper method to execute SQL commands."""
        lexer = Lexer(sql)
        tokens = lexer.tokenize()
        parser = Parser(tokens)
        ast = parser.parse()
        return self.executor.execute(ast)

    def test_create_table_without_constraints(self):
        """Test creating a table without any constraints."""
        sql = """
        CREATE TABLE products (
            product_id INT,
            product_name TEXT,
            price FLOAT
        );
        """
        result = self.execute_sql(sql)
        self.assertEqual(result, "Table 'products' created successfully.")
        self.assertIn('products', self.db.tables)
        table = self.db.tables['products']
        self.assertEqual(table.schema, {'product_id': int, 'product_name': str, 'price': float})
        self.assertEqual(table.primary_key, [])
        self.assertEqual(table.unique, [])

    def test_create_table_with_constraints(self):
        """Test creating a table with primary key and unique constraints."""
        sql = """
        CREATE TABLE users (
            id INT,
            username TEXT,
            email TEXT,
            PRIMARY KEY (id),
            UNIQUE (username),
            UNIQUE (email)
        );
        """
        result = self.execute_sql(sql)
        self.assertEqual(result, "Table 'users' created successfully.")
        self.assertIn('users', self.db.tables)
        table = self.db.tables['users']
        self.assertEqual(table.schema, {'id': int, 'username': str, 'email': str})
        self.assertEqual(table.primary_key, ['id'])
        self.assertEqual(table.unique, [['username'], ['email']])

    def test_insert_and_select(self):
        """Test inserting data into a table and selecting it."""
        # Create table
        create_sql = """
        CREATE TABLE users (
            id INT,
            name TEXT,
            age INT,
            PRIMARY KEY (id),
            UNIQUE (name)
        );
        """
        self.execute_sql(create_sql)
        
        # Insert data
        insert_sql = "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);"
        result = self.execute_sql(insert_sql)
        self.assertIn("Row inserted with ID", result)
        
        # Select data
        select_sql = "SELECT * FROM users WHERE name = 'Alice';"
        results = self.execute_sql(select_sql)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['id'], 1)
        self.assertEqual(results[0]['name'], 'Alice')
        self.assertEqual(results[0]['age'], 30)

    def test_primary_key_constraint(self):
        """Test enforcing primary key uniqueness."""
        # Create table
        create_sql = """
        CREATE TABLE users (
            id INT,
            name TEXT,
            PRIMARY KEY (id)
        );
        """
        self.execute_sql(create_sql)
        
        # Insert first row
        insert_sql1 = "INSERT INTO users (id, name) VALUES (1, 'Alice');"
        self.execute_sql(insert_sql1)
        
        # Attempt to insert duplicate primary key
        insert_sql2 = "INSERT INTO users (id, name) VALUES (1, 'Bob');"
        with self.assertRaises(ValueError) as context:
            self.execute_sql(insert_sql2)
        self.assertIn("Duplicate primary key", str(context.exception))
    
    def test_unique_constraint(self):
        """Test enforcing unique constraints."""
        # Create table
        create_sql = """
        CREATE TABLE users (
            id INT,
            name TEXT,
            PRIMARY KEY (id),
            UNIQUE (name)
        );
        """
        self.execute_sql(create_sql)
        
        # Insert first row
        insert_sql1 = "INSERT INTO users (id, name) VALUES (1, 'Alice');"
        self.execute_sql(insert_sql1)
        
        # Attempt to insert duplicate unique
        insert_sql2 = "INSERT INTO users (id, name) VALUES (2, 'Alice');"
        with self.assertRaises(ValueError) as context:
            self.execute_sql(insert_sql2)
        self.assertIn("Duplicate unique key", str(context.exception))
    
    def test_foreign_key_constraint(self):
        """Test enforcing foreign key constraints."""
        # Create departments table
        create_dept_sql = """
        CREATE TABLE departments (
            dept_id INT,
            dept_name TEXT,
            PRIMARY KEY (dept_id)
        );
        """
        self.execute_sql(create_dept_sql)
        
        # Create employees table with foreign key
        create_emp_sql = """
        CREATE TABLE employees (
            emp_id INT,
            emp_name TEXT,
            dept_id INT,
            PRIMARY KEY (emp_id),
            FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
        );
        """
        self.execute_sql(create_emp_sql)
        
        # Insert department
        insert_dept_sql = "INSERT INTO departments (dept_id, dept_name) VALUES (1, 'Engineering');"
        self.execute_sql(insert_dept_sql)
        
        # Insert employee with valid foreign key
        insert_emp_sql = "INSERT INTO employees (emp_id, emp_name, dept_id) VALUES (101, 'Alice', 1);"
        self.execute_sql(insert_emp_sql)
        
        # Insert employee with invalid foreign key
        insert_emp_invalid_sql = "INSERT INTO employees (emp_id, emp_name, dept_id) VALUES (102, 'Bob', 2);"
        with self.assertRaises(ValueError) as context:
            self.execute_sql(insert_emp_invalid_sql)
        self.assertIn("Foreign key constraint failed", str(context.exception))
    
    def test_join(self):
        """Test performing INNER JOIN between two tables."""
        # Create departments table
        create_dept_sql = """
        CREATE TABLE departments (
            dept_id INT,
            dept_name TEXT,
            PRIMARY KEY (dept_id)
        );
        """
        self.execute_sql(create_dept_sql)
        
        # Create employees table with foreign key
        create_emp_sql = """
        CREATE TABLE employees (
            emp_id INT,
            emp_name TEXT,
            dept_id INT,
            salary FLOAT,
            PRIMARY KEY (emp_id),
            FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
        );
        """
        self.execute_sql(create_emp_sql)
        
        # Insert departments
        self.execute_sql("INSERT INTO departments (dept_id, dept_name) VALUES (1, 'Engineering');")
        self.execute_sql("INSERT INTO departments (dept_id, dept_name) VALUES (2, 'HR');")
        
        # Insert employees
        self.execute_sql("INSERT INTO employees (emp_id, emp_name, dept_id, salary) VALUES (101, 'Alice', 1, 70000);")
        self.execute_sql("INSERT INTO employees (emp_id, emp_name, dept_id, salary) VALUES (102, 'Bob', 2, 50000);")
        self.execute_sql("INSERT INTO employees (emp_id, emp_name, dept_id, salary) VALUES (103, 'Charlie', 1, 80000);")
        
        # Perform INNER JOIN
        join_sql = """
        SELECT employees.emp_name, departments.dept_name
        FROM employees
        INNER JOIN departments ON employees.dept_id = departments.dept_id;
        """
        results = self.execute_sql(join_sql)
        expected = [
            {'employees.emp_name': 'Alice', 'departments.dept_name': 'Engineering'},
            {'employees.emp_name': 'Bob', 'departments.dept_name': 'HR'},
            {'employees.emp_name': 'Charlie', 'departments.dept_name': 'Engineering'},
        ]
        self.assertEqual(len(results), 3)
        self.assertListEqual(results, expected)
    
    def test_delete_with_foreign_key(self):
        """Test preventing deletion of a row referenced by a foreign key."""
        # Create departments table
        create_dept_sql = """
        CREATE TABLE departments (
            dept_id INT,
            dept_name TEXT,
            PRIMARY KEY (dept_id)
        );
        """
        self.execute_sql(create_dept_sql)
        
        # Create employees table with foreign key
        create_emp_sql = """
        CREATE TABLE employees (
            emp_id INT,
            emp_name TEXT,
            dept_id INT,
            PRIMARY KEY (emp_id),
            FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
        );
        """
        self.execute_sql(create_emp_sql)
        
        # Insert departments
        self.execute_sql("INSERT INTO departments (dept_id, dept_name) VALUES (1, 'Engineering');")
        self.execute_sql("INSERT INTO departments (dept_id, dept_name) VALUES (2, 'HR');")
        
        # Insert employees
        self.execute_sql("INSERT INTO employees (emp_id, emp_name, dept_id) VALUES (101, 'Alice', 1);")
        self.execute_sql("INSERT INTO employees (emp_id, emp_name, dept_id) VALUES (102, 'Bob', 2);")
        
        # Attempt to delete department with employees
        delete_sql = "DELETE FROM departments WHERE dept_id = 1;"
        with self.assertRaises(ValueError) as context:
            self.execute_sql(delete_sql)
        self.assertIn("Cannot delete row", str(context.exception))
        
        # Delete employee
        self.execute_sql("DELETE FROM employees WHERE emp_id = 101;")
        
        # Now delete department
        delete_sql2 = "DELETE FROM departments WHERE dept_id = 1;"
        result = self.execute_sql(delete_sql2)
        self.assertEqual(result, "1 row(s) deleted.")
    
    def test_order_by(self):
        """Test ordering of SELECT query results."""
        # Create table
        create_sql = """
        CREATE TABLE users (
            id INT,
            name TEXT,
            age INT,
            salary FLOAT,
            PRIMARY KEY (id),
            UNIQUE (name)
        );
        """
        self.execute_sql(create_sql)
        
        # Insert data
        self.execute_sql("INSERT INTO users (id, name, age, salary) VALUES (1, 'Alice', 30, 70000);")
        self.execute_sql("INSERT INTO users (id, name, age, salary) VALUES (2, 'Bob', 25, 50000);")
        self.execute_sql("INSERT INTO users (id, name, age, salary) VALUES (3, 'Charlie', 35, 80000);")
        self.execute_sql("INSERT INTO users (id, name, age, salary) VALUES (4, 'Diana', 28, 60000);")
        
        # Select and order by age ascending
        select_sql = "SELECT name, age FROM users ORDER BY age ASC;"
        results = self.execute_sql(select_sql)
        expected = [
            {'name': 'Bob', 'age': 25},
            {'name': 'Diana', 'age': 28},
            {'name': 'Alice', 'age': 30},
            {'name': 'Charlie', 'age': 35},
        ]
        self.assertEqual(results, expected)
        
        # Select and order by salary descending
        select_sql2 = "SELECT name, salary FROM users ORDER BY salary DESC;"
        results2 = self.execute_sql(select_sql2)
        expected2 = [
            {'name': 'Charlie', 'salary': 80000.0},
            {'name': 'Alice', 'salary': 70000.0},
            {'name': 'Diana', 'salary': 60000.0},
            {'name': 'Bob', 'salary': 50000.0},
        ]
        self.assertEqual(results2, expected2)
    
    def test_join_with_aliases_and_multiple_joins(self):
        """Test performing multiple INNER JOINs with table aliases."""
        # Create tables
        self.execute_sql("""
        CREATE TABLE departments (
            dept_id INT,
            dept_name TEXT,
            PRIMARY KEY (dept_id)
        );
        """)
        self.execute_sql("""
        CREATE TABLE employees (
            emp_id INT,
            emp_name TEXT,
            dept_id INT,
            manager_id INT,
            PRIMARY KEY (emp_id),
            FOREIGN KEY (dept_id) REFERENCES departments(dept_id),
            FOREIGN KEY (manager_id) REFERENCES employees(emp_id)
        );
        """)
        
        # Insert departments
        self.execute_sql("INSERT INTO departments (dept_id, dept_name) VALUES (1, 'Engineering');")
        self.execute_sql("INSERT INTO departments (dept_id, dept_name) VALUES (2, 'HR');")
        
        # Insert employees
        self.execute_sql("INSERT INTO employees (emp_id, emp_name, dept_id, manager_id) VALUES (101, 'Alice', 1, NULL);")
        self.execute_sql("INSERT INTO employees (emp_id, emp_name, dept_id, manager_id) VALUES (102, 'Bob', 1, 101);")
        self.execute_sql("INSERT INTO employees (emp_id, emp_name, dept_id, manager_id) VALUES (103, 'Charlie', 2, 101);")
        self.execute_sql("INSERT INTO employees (emp_id, emp_name, dept_id, manager_id) VALUES (104, 'Diana', 1, 102);")
        
        # Perform join to get employee and their department and manager's name
        join_sql = """
        SELECT e.emp_name, d.dept_name, m.emp_name as manager_name
        FROM employees e
        INNER JOIN departments d ON e.dept_id = d.dept_id
        INNER JOIN employees m ON e.manager_id = m.emp_id;
        """
        results = self.execute_sql(join_sql)
        expected = [
            {'e.emp_name': 'Bob', 'd.dept_name': 'Engineering', 'manager_name': 'Alice'},
            {'e.emp_name': 'Charlie', 'd.dept_name': 'HR', 'manager_name': 'Alice'},
            {'e.emp_name': 'Diana', 'd.dept_name': 'Engineering', 'manager_name': 'Bob'},
        ]
        self.assertEqual(results, expected)
    
    def test_insert_with_nonexistent_foreign_key(self):
        """Test inserting data with a foreign key that references a non-existent row."""
        # Create departments table
        self.execute_sql("""
        CREATE TABLE departments (
            dept_id INT,
            dept_name TEXT,
            PRIMARY KEY (dept_id)
        );
        """)
        
        # Create employees table with foreign key
        self.execute_sql("""
        CREATE TABLE employees (
            emp_id INT,
            emp_name TEXT,
            dept_id INT,
            PRIMARY KEY (emp_id),
            FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
        );
        """)
        
        # Attempt to insert employee with non-existent dept_id
        insert_sql = "INSERT INTO employees (emp_id, emp_name, dept_id) VALUES (101, 'Alice', 99);"
        with self.assertRaises(ValueError) as context:
            self.execute_sql(insert_sql)
        self.assertIn("Foreign key constraint failed", str(context.exception))
    
    def test_unique_composite_key(self):
        """Test enforcing a composite unique key."""
        # Create table with composite unique key
        self.execute_sql("""
        CREATE TABLE registrations (
            user_id INT,
            event_id INT,
            registration_date TEXT,
            PRIMARY KEY (user_id, event_id),
            UNIQUE (user_id, registration_date)
        );
        """)
        
        # Insert first registration
        self.execute_sql("INSERT INTO registrations (user_id, event_id, registration_date) VALUES (1, 100, '2023-01-01');")
        
        # Attempt to insert duplicate primary key
        with self.assertRaises(ValueError) as context:
            self.execute_sql("INSERT INTO registrations (user_id, event_id, registration_date) VALUES (1, 100, '2023-01-02');")
        self.assertIn("Duplicate primary key", str(context.exception))
        
        # Attempt to insert duplicate unique key
        with self.assertRaises(ValueError) as context:
            self.execute_sql("INSERT INTO registrations (user_id, event_id, registration_date) VALUES (1, 101, '2023-01-01');")
        self.assertIn("Duplicate unique key", str(context.exception))
        
        # Insert valid registration
        result = self.execute_sql("INSERT INTO registrations (user_id, event_id, registration_date) VALUES (1, 101, '2023-01-02');")
        self.assertIn("Row inserted with ID", result)

    def test_drop_table_with_foreign_keys(self):
        """Test dropping a table that is referenced by a foreign key."""
        # Create tables
        self.execute_sql("""
        CREATE TABLE parent (
            id INT,
            name TEXT,
            PRIMARY KEY (id)
        );
        """)
        self.execute_sql("""
        CREATE TABLE child (
            id INT,
            parent_id INT,
            name TEXT,
            PRIMARY KEY (id),
            FOREIGN KEY (parent_id) REFERENCES parent(id)
        );
        """)
        
        # Insert data
        self.execute_sql("INSERT INTO parent (id, name) VALUES (1, 'Parent1');")
        self.execute_sql("INSERT INTO child (id, parent_id, name) VALUES (1, 1, 'Child1');")
        
        # Attempt to drop parent table
        with self.assertRaises(ValueError) as context:
            self.execute_sql("DROP TABLE parent;")
        self.assertIn("Cannot drop table 'parent'", str(context.exception))
        
        # Drop child table first
        self.execute_sql("DROP TABLE child;")
        
        # Now drop parent table
        result = self.execute_sql("DROP TABLE parent;")
        self.assertEqual(result, "Table 'parent' dropped successfully.")

    def test_update_with_constraints(self):
        """Test updating a row and enforcing constraints."""
        # Create tables
        self.execute_sql("""
        CREATE TABLE departments (
            dept_id INT,
            dept_name TEXT,
            PRIMARY KEY (dept_id),
            UNIQUE (dept_name)
        );
        """)
        self.execute_sql("""
        CREATE TABLE employees (
            emp_id INT,
            emp_name TEXT,
            dept_id INT,
            salary FLOAT,
            PRIMARY KEY (emp_id),
            UNIQUE (emp_name),
            FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
        );
        """)
        
        # Insert departments
        self.execute_sql("INSERT INTO departments (dept_id, dept_name) VALUES (1, 'Engineering');")
        self.execute_sql("INSERT INTO departments (dept_id, dept_name) VALUES (2, 'HR');")
        
        # Insert employees
        self.execute_sql("INSERT INTO employees (emp_id, emp_name, dept_id, salary) VALUES (101, 'Alice', 1, 70000);")
        self.execute_sql("INSERT INTO employees (emp_id, emp_name, dept_id, salary) VALUES (102, 'Bob', 2, 50000);")
        
        # Attempt to update employee name to a name that already exists
        with self.assertRaises(ValueError) as context:
            self.execute_sql("UPDATE employees SET emp_name = 'Alice' WHERE emp_id = 102;")
        self.assertIn("Duplicate unique key", str(context.exception))
        
        # Attempt to update employee's dept_id to a non-existent department
        with self.assertRaises(ValueError) as context:
            self.execute_sql("UPDATE employees SET dept_id = 3 WHERE emp_id = 102;")
        self.assertIn("Foreign key constraint failed", str(context.exception))
        
        # Valid update
        update_sql = "UPDATE employees SET salary = 55000 WHERE emp_id = 102;"
        result = self.execute_sql(update_sql)
        self.assertEqual(result, "1 row(s) updated.")
        
        # Verify update
        select_sql = "SELECT salary FROM employees WHERE emp_id = 102;"
        results = self.execute_sql(select_sql)
        self.assertEqual(results[0]['salary'], 55000.0)

if __name__ == '__main__':
    unittest.main()
