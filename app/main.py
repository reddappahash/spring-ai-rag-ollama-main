from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from .db import SessionLocal, engine
from .models import Employee, Base
from .etl import run_etl

app = FastAPI(title="ETL FastAPI Project")

# Run ETL at startup
@app.on_event("startup")
def startup_event():
    run_etl()

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/")
def root():
    return {"message": "Welcome to the ETL FastAPI project"}

@app.get("/employees")
def get_all_employees(db: Session = Depends(get_db)):
    employees = db.query(Employee).all()
    return employees

@app.get("/employees/{department}")
def get_employees_by_department(department: str, db: Session = Depends(get_db)):
    employees = db.query(Employee).filter(Employee.department == department).all()
    return employees
