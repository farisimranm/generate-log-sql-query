import psycopg2
from tqdm import tqdm

# Database connection details
host = "localhost"
port = "5432"
database = "activity-audit-log"
user = "postgres"
password = "password"

# Establish a connection to the database
conn = psycopg2.connect(
    host=host,
    port=port,
    database=database,
    user=user,
    password=password
)

# Create a cursor object to execute SQL statements
cursor = conn.cursor()
for i in tqdm(range(1, 29), desc=f"Executing SQL files..."):
    filename = f'sql/audit-log-data/audit-log-data-{i}.sql'
    with open(filename, 'r') as file:
        print(f'\nOpening "{filename}"...')
        sql_script = file.read()
    
    print(f'Executing SQL query...')
    cursor.execute(sql_script)
    
    print(f'Commiting SQL changes...')
    conn.commit()

# cursor.execute("SELECT * FROM tbl_audit_log LIMIT 10")
# rows = cursor.fetchall()

# # Print each row
# for row in rows:
#     print(row + )


# Close the cursor and database connection
cursor.close()
conn.close()