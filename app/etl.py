import pandas as pd
from sqlalchemy.orm import Session
from .models import Employee
from .db import engine, Base, SessionLocal

def run_etl():
    # Create tables
    Base.metadata.create_all(bind=engine)

    # Step 1: Extract
    df = pd.read_csv("app/sample_data.csv")

    # Step 2: Transform
    # Remove duplicates, filter out low salaries
    df = df.drop_duplicates()
    df = df[df["salary"] >= 65000]

    # Step 3: Load
    session = SessionLocal()
    try:
        session.query(Employee).delete()  # Clear old data
        for _, row in df.iterrows():
            emp = Employee(
                id=int(row["id"]),
                name=row["name"],
                department=row["department"],
                salary=float(row["salary"]),
            )
            session.add(emp)
        session.commit()
        print("✅ ETL process completed successfully.")
    except Exception as e:
        session.rollback()
        print("❌ Error:", e)
    finally:
        session.close()
