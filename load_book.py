# load_book.py

import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from backend_main import Book, Base

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)

def main():
    # Adjust filename if needed
    df = pd.read_csv("Library_data.csv")
    df.columns = [c.strip().lower() for c in df.columns]

    def get_col(*names):
        for n in names:
            if n.lower() in df.columns:
                return df[n.lower()]
        return None

    acc_col   = get_col("acc. no.", "acc_no", "accessionno", "accession_no")
    title_col = get_col("title")
    author_col = get_col("author")
    subj_col  = get_col("subject")
    rack_col  = get_col("rack_location", "rack", "location")
    copies_col = get_col("copies", "total_copies", "no_of_copies")

    if acc_col is None or title_col is None:
        raise ValueError("CSV must contain at least Accession No and Title")

    db = SessionLocal()
    inserted = 0
    updated = 0

    try:
        for i, row in df.iterrows():
            accession = str(acc_col.iloc[i]).strip()
            if not accession:
                continue

            title  = str(title_col.iloc[i]).strip() if title_col is not None else ""
            author = str(author_col.iloc[i]).strip() if author_col is not None else ""
            subj   = str(subj_col.iloc[i]).strip() if subj_col is not None else ""
            rack   = str(rack_col.iloc[i]).strip() if rack_col is not None else ""
            total  = int(copies_col.iloc[i]) if copies_col is not None and not pd.isna(copies_col.iloc[i]) else 1

            existing = db.query(Book).filter(Book.accession_no == accession).first()
            if existing:
                existing.title          = title
                existing.author         = author
                existing.subject        = subj
                existing.rack_location  = rack
                existing.total_copies   = total
                # keep available_copies as is (current stock)
                updated += 1
            else:
                b = Book(
                    accession_no=accession,
                    title=title,
                    author=author,
                    subject=subj,
                    rack_location=rack,
                    total_copies=total,
                    available_copies=total,
                )
                db.add(b)
                inserted += 1

        db.commit()
        print(f"Books load complete. Inserted={inserted}, Updated={updated}")

    except Exception as e:
        db.rollback()
        print("Error while loading books:", e)
    finally:
        db.close()

if __name__ == "__main__":
    main()
