# load_student.py

import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from backend_main import Student, Base  # reuse your models

# 1) Load env
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

# 2) DB engine + session
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)

def main():
    # 3) Read CSV (adjust filename if needed)
    df = pd.read_csv("students.csv")

    # Normalize column names
    df.columns = [c.strip().lower() for c in df.columns]

    # Try to map columns
    def get_col(*names):
        for n in names:
            if n.lower() in df.columns:
                return df[n.lower()]
        return None

    usn_col      = get_col("usn")
    name_col     = get_col("name", "student_name")
    sem_col      = get_col("semester", "sem")
    branch_col   = get_col("branch", "dept")
    phone_col    = get_col("phone", "mobile", "phone_no")
    email_col    = get_col("email", "mail")

    if usn_col is None or name_col is None:
        raise ValueError("CSV must contain at least USN and Name columns")

    db = SessionLocal()
    inserted = 0
    updated = 0

    try:
        for i, row in df.iterrows():
            usn = str(usn_col.iloc[i]).strip()
            if not usn:
                continue

            name    = str(name_col.iloc[i]).strip() if name_col is not None else ""
            sem     = str(sem_col.iloc[i]).strip() if sem_col is not None else ""
            branch  = str(branch_col.iloc[i]).strip() if branch_col is not None else ""
            phone   = str(phone_col.iloc[i]).strip() if phone_col is not None else ""
            email   = str(email_col.iloc[i]).strip() if email_col is not None else ""

            # Check if student already exists
            existing = db.query(Student).filter(Student.usn == usn).first()
            if existing:
                existing.name     = name
                existing.semester = sem
                existing.branch   = branch
                existing.phone    = phone
                existing.email    = email
                updated += 1
            else:
                st = Student(
                    usn=usn,
                    name=name,
                    semester=sem,
                    branch=branch,
                    phone=phone,
                    email=email,
                )
                db.add(st)
                inserted += 1

        db.commit()
        print(f"Students load complete. Inserted={inserted}, Updated={updated}")

    except Exception as e:
        db.rollback()
        print("Error while loading students:", e)
    finally:
        db.close()

if __name__ == "__main__":
    main()
