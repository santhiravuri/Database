import mysql.connector
from mysql.connector import Error


def create_connection(host, user, password, database=None):
    try:
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            auth_plugin='mysql_native_password',
            use_pure=True,
            connection_timeout=5
        )
        if conn.is_connected():
            print("Connected to MYSQL")
            return conn
    except Error as e:
        print("Connection Error:", e)
    return None


def create_database(conn):
    cursor = conn.cursor()
    cursor.execute('CREATE DATABASE IF NOT EXISTS SchoolDB')
    print("Database ready")


def create_tables(conn):
    cursor = conn.cursor()
    # Corrected: Removed trailing comma after grade
    cursor.execute("""
                   CREATE TABLE IF NOT EXISTS Students
                   (
                       id
                       INT
                       AUTO_INCREMENT
                       PRIMARY
                       KEY,
                       name
                       VARCHAR
                   (
                       100
                   ),
                       age INT,
                       grade VARCHAR
                   (
                       10
                   )
                       )
                   """)
    # Corrected: Fixed triple-quote syntax
    cursor.execute("""
                   CREATE TABLE IF NOT EXISTS Classes
                   (
                       class_id
                       INT
                       AUTO_INCREMENT
                       PRIMARY
                       KEY,
                       student_id
                       INT,
                       subject
                       VARCHAR
                   (
                       50
                   ),
                       score INT,
                       FOREIGN KEY
                   (
                       student_id
                   ) REFERENCES Students
                   (
                       id
                   )
                       )
                   """)
    print("Tables ready")


def reset_tables(conn):
    cursor = conn.cursor()
    cursor.execute('SET FOREIGN_KEY_CHECKS = 0')
    cursor.execute("TRUNCATE TABLE Classes")
    cursor.execute("TRUNCATE TABLE Students")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
    conn.commit()
    print("Tables cleaned & auto-increment reset")


def insert_data(conn):
    cursor = conn.cursor()
    # Corrected: Added missing commas between tuples
    students = [
        ("Ayesha", 21, "6th"),
        ("Bob", 15, "9th"),
        ("Charlie", 13, "7th"),
        ("David", 14, "8th")
    ]
    # Corrected: Reduced placeholders from 4 to 3
    cursor.executemany(
        "INSERT INTO Students(name, age, grade) VALUES (%s, %s, %s)",
        students
    )

    cursor.execute("SELECT id, name from Students")
    students_ids = {name: sid for sid, name in cursor.fetchall()}

    # Corrected: Added missing commas between tuples
    classes = [
        (students_ids["Ayesha"], "Math", 85),
        (students_ids["Ayesha"], "Science", 90),
        (students_ids["Bob"], "Math", 78),
        (students_ids["Bob"], "Science", 82),
        (students_ids["Charlie"], "Math", 92),
        (students_ids["David"], "Math", 88)
    ]
    cursor.executemany(
        "INSERT INTO Classes(student_id, subject, score) VALUES (%s, %s, %s)",
        classes
    )
    conn.commit()
    print("Sample data inserted safely")


def display_data(conn):
    cursor = conn.cursor()
    print("\n--- Students ---")
    cursor.execute("SELECT * FROM Students")
    for row in cursor.fetchall():
        print(row)
    print("\n--- Student Scores (JOIN) ---")
    cursor.execute("""
                   SELECT s.name, c.subject, c.score
                   FROM Students s
                            JOIN Classes c ON s.id = c.student_id
                   """)
    for row in cursor.fetchall():
        print(row)


def update_and_delete(conn):
    cursor = conn.cursor()
    # Updated to use a name that exists in your list (e.g., Ayesha instead of Alice)
    cursor.execute("UPDATE Students SET grade=%s WHERE name=%s", ("9th", "Ayesha"))

    # Deleting "David" as an example of a clean delete
    target_name = "David"
    cursor.execute("""
                   DELETE
                   FROM Classes
                   WHERE student_id IN (SELECT id FROM Students WHERE name = %s)
                   """, (target_name,))

    cursor.execute("DELETE FROM Students WHERE name = %s", (target_name,))

    conn.commit()
    print(f"Update & delete completed safely for {target_name}")


if __name__ == "__main__":
    print("\n==== PYTHON DBMS PROGRAM STARTED ====\n")
    # Step 1: Connect to server to ensure DB exists
    server = create_connection("localhost", "root", "your_Database_password")
    if server:
        create_database(server)
        server.close()

    # Step 2: Connect to specific database
    db = create_connection("localhost", "root", "your_Database_password", "schooldb")
    if db:
        create_tables(db)
        reset_tables(db)
        insert_data(db)
        display_data(db)
        update_and_delete(db)
        print("\n--- Data After Update/Delete ---")
        display_data(db)
        db.close()

    print("\n==== PROGRAM COMPLETED SUCCESSFULLY ====\n")
