import logging
import sqlite3
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CellCountDatabase(object):
    """Create the cell count SQLite database"""

    CELL_POPULATIONS = ["b_cell", "cd8_t_cell", "cd4_t_cell", "nk_cell", "monocyte"]

    def __init__(self, db_path="cell_count.db"):
        self.db_path = db_path
        self.conn = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn is not None:
            self.conn.commit()
            self.conn.close()
            self.conn = None
            logger.info("Database connection closed")

    def connect(self):
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        logger.info("Connected to database: %s", self.db_path)

    def _cursor(self):
        if self.conn is None:
            raise RuntimeError("Database not connected. Use: `with CellCountDatabase(...) as db:`")
        return self.conn.cursor()

    def create_schema(self):
        """Create tables needed to store the CSV in normalized form."""
        cur = self._cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS projects (
                project_id TEXT PRIMARY KEY
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS subjects (
                subject_id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL,
                age INTEGER,
                sex TEXT,
                FOREIGN KEY (project_id) REFERENCES projects(project_id)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS samples (
                sample_id TEXT PRIMARY KEY,
                subject_id TEXT NOT NULL,
                project_id TEXT NOT NULL,
                condition TEXT NOT NULL,
                treatment TEXT,
                response TEXT,
                sample_type TEXT NOT NULL,
                time_from_treatment_start REAL NOT NULL,
                FOREIGN KEY (subject_id) REFERENCES subjects(subject_id)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE,
                FOREIGN KEY (project_id) REFERENCES projects(project_id)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE,
                CHECK (time_from_treatment_start >= 0)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS cell_counts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sample_id TEXT NOT NULL,
                population TEXT NOT NULL,
                count INTEGER NOT NULL,
                FOREIGN KEY (sample_id) REFERENCES samples(sample_id)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE,
                CHECK (population IN ('b_cell', 'cd8_t_cell', 'cd4_t_cell', 'nk_cell', 'monocyte')),
                CHECK (count >= 0),
                UNIQUE(sample_id, population)
            )
        """)

        self.conn.commit()
        logger.info("Schema created")

    def load_data(self, csv_path):
        """Load all rows into the database"""
        cur = self._cursor()

        df = pd.read_csv(csv_path)

        df[self.CELL_POPULATIONS] = df[self.CELL_POPULATIONS].fillna(0).astype(int)

        for col in ["condition", "treatment", "response", "sample_type"]:
            df[col] = df[col].astype(str).str.strip().str.lower()

        # Projects
        for project_id in df["project"].drop_duplicates():
            cur.execute("INSERT OR IGNORE INTO projects (project_id) VALUES (?)", (project_id,))

        # Subjects
        subjects = df[["subject", "project", "age", "sex"]].drop_duplicates()
        for _, r in subjects.iterrows():
            cur.execute(
                """
                INSERT OR IGNORE INTO subjects (subject_id, project_id, age, sex)
                VALUES (?, ?, ?, ?)
                """,
                (r["subject"], r["project"], r["age"], r["sex"]),
            )

        # Samples
        samples = df[
            [
                "sample",
                "subject",
                "project",
                "condition",
                "treatment",
                "response",
                "sample_type",
                "time_from_treatment_start",
            ]
        ].drop_duplicates()

        for _, r in samples.iterrows():
            cur.execute(
                """
                INSERT OR IGNORE INTO samples
                    (sample_id, subject_id, project_id, condition, treatment, response, sample_type, time_from_treatment_start)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    r["sample"],
                    r["subject"],
                    r["project"],
                    r["condition"],
                    r["treatment"],
                    r["response"],
                    r["sample_type"],
                    r["time_from_treatment_start"],
                ),
            )

        # Cell counts
        for _, r in df.iterrows():
            for pop in self.CELL_POPULATIONS:
                cur.execute(
                    """
                    INSERT OR IGNORE INTO cell_counts (sample_id, population, count)
                    VALUES (?, ?, ?)
                    """,
                    (r["sample"], pop, int(r[pop])),
                )

        self.conn.commit()
        logger.info("Loaded %d rows from %s", len(df), csv_path)

    def initialize_database(self, csv_path):
        """One-call entry point"""
        self.create_schema()
        self.load_data(csv_path)

    def verify_counts(self):
        """Sanity check"""
        cur = self._cursor()
        counts = {}
        for table in ["projects", "subjects", "samples", "cell_counts"]:
            cur.execute("SELECT COUNT(*) FROM %s" % table)
            counts[table] = int(cur.fetchone()[0])
        return counts


def main():
    db_path = "cell_count.db"
    csv_path = "data/cell-count.csv"

    with CellCountDatabase(db_path) as db:
        db.initialize_database(csv_path)
        print("Loaded:")
        counts = db.verify_counts()
        for table in ["projects", "subjects", "samples", "cell_counts"]:
            print("%s %d" % (table.ljust(12), counts[table]))


if __name__ == "__main__":
    main()