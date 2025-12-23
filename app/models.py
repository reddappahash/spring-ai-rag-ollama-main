from sqlalchemy import Column, Integer, String, Float
from .db import Base

class Employee(Base):
    __tablename__ = "employees"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    department = Column(String)
    salary = Column(Float)
