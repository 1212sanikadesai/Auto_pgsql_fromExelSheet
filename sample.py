import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, text
from sqlalchemy import String, Integer, Float, DateTime, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base

# --- Configuration ---
db_url = "postgresql+psycopg2://postgres:Sanika%40123@localhost:5432/sanu"
file_path = 'sample_datasets.xlsx'

# --- SQLAlchemy Setup ---
engine = create_engine(db_url)
metadata = MetaData()
Base = declarative_base()
Session = sessionmaker(bind=engine)
metadata.reflect(bind=engine)

# --- Helper to infer SQLAlchemy types ---
def infer_sqlalchemy_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return Integer
    elif pd.api.types.is_float_dtype(dtype):
        return Float
    elif pd.api.types.is_bool_dtype(dtype):
        return Boolean
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return DateTime
    else:
        return String

# --- Process Excel and create/update tables ---
xls = pd.ExcelFile(file_path)
sheet_names = xls.sheet_names
print("Sheet Names (Table Names):", sheet_names)

for sheet_name in sheet_names:
    df = pd.read_excel(xls, sheet_name=sheet_name, header=0)
    if df.empty or df.columns.empty:
        print(f"Sheet '{sheet_name}' is empty or has no columns. Skipping.")
        continue

    print(f"\nProcessing Sheet/Table: {sheet_name}")
    table_name = sheet_name.lower().replace(" ", "_").replace("-", "_")

    # Treat the first column as primary key
    primary_key_excel = df.columns[0]
    pk_col_name = primary_key_excel.lower().replace(" ", "_").replace("-", "_")

    # Normalize column names
    df.columns = [col.lower().replace(" ", "_").replace("-", "_") for col in df.columns]

    # Re-check metadata before each table
    metadata.reflect(bind=engine)
    existing_table = metadata.tables.get(table_name)

    # Build SQLAlchemy Columns
    sqlalchemy_columns = []
    for col_name in df.columns:
        sql_type = infer_sqlalchemy_type(df[col_name].dtype)
        is_pk = (col_name == pk_col_name)
        sqlalchemy_columns.append(Column(col_name, sql_type, primary_key=is_pk))

    if existing_table is None:
        # Table does not exist, create it
        new_table = Table(table_name, metadata, *sqlalchemy_columns, extend_existing=True)
        new_table.create(engine)
        print(f"  Table '{table_name}' created.")
    else:
        # Table exists, check for schema evolution
        print(f"  Table '{table_name}' exists. Checking for new columns...")
        existing_columns = set(existing_table.columns.keys())
        with engine.connect() as conn:
            with conn.begin():
                for col_name in df.columns:
                    if col_name not in existing_columns:
                        sql_type = infer_sqlalchemy_type(df[col_name].dtype)
                        ddl = text(f'ALTER TABLE "{table_name}" ADD COLUMN "{col_name}" {sql_type().compile(engine.dialect)}')
                        conn.execute(ddl)
                        print(f"    Column '{col_name}' added to '{table_name}'.")

    # Load data
    try:
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"  Data loaded into '{table_name}'.")
    except Exception as e:
        print(f"  Failed to load data into '{table_name}': {e}")

print("\nAll sheets processed.")

# --- CRUD on 'employees' Table ---
if 'employees' in metadata.tables:
    class Employee(Base):
        __table__ = metadata.tables['employees']

        def __init__(self, **kwargs):
            for key, value in kwargs.items():
                if key in self.__table__.columns:
                    setattr(self, key, value)
                else:
                    print(f"Warning: Column '{key}' not found in table.")

        def __repr__(self):
            fields = ", ".join(f"{col.name}={getattr(self, col.name)!r}" for col in self.__table__.columns)
            return f"<Employee({fields})>"

    # CRUD Functions
    def create_employee(data):
        with Session() as session:
            try:
                emp = Employee(**data)
                session.add(emp)
                session.commit()
                return f"Employee added: {data}"
            except Exception as e:
                session.rollback()
                return f"Error adding employee: {e}"

    def read_employees(emp_id=None):
        with Session() as session:
            try:
                if emp_id:
                    return session.query(Employee).filter_by(id=emp_id).all()
                return session.query(Employee).all()
            except Exception as e:
                return f"Error reading: {e}"

    def update_employee(emp_id, update_data):
        with Session() as session:
            try:
                emp = session.query(Employee).filter_by(id=emp_id).first()
                if emp:
                    for key, val in update_data.items():
                        if hasattr(emp, key):
                            setattr(emp, key, val)
                    session.commit()
                    return f"Updated employee {emp_id}"
                return f" Employee {emp_id} not found"
            except Exception as e:
                session.rollback()
                return f"Error updating: {e}"

    def delete_employee(emp_id):
        with Session() as session:
            try:
                emp = session.query(Employee).filter_by(id=emp_id).first()
                if emp:
                    session.delete(emp)
                    session.commit()
                    return f"Deleted employee {emp_id}"
                return f"Employee {emp_id} not found"
            except Exception as e:
                session.rollback()
                return f"Error deleting: {e}"

    # --- Demo CRUD Operations ---
    print("\n--- CRUD on 'employees' ---")

    # CREATE
    new_employee = {'id': 1001, 'name': 'Alice Smith', 'department': 'HR', 'salary': 60000}
    print(create_employee(new_employee))

    # READ ALL
    print("\nAll Employees:")
    for emp in read_employees():
        print(emp)

    # READ ONE
    print("\nEmployee ID 1001:")
    for emp in read_employees(1001):
        print(emp)

    # UPDATE
    print("\nUpdating Employee ID 1001:")
    print(update_employee(1001, {'department': 'Engineering', 'salary': 75000}))

    # DELETE
    print("\nDeleting Employee ID 1001:")
    print(delete_employee(1001))

else:
    print("\n'employees' table not found. Skipping CRUD operations.")
