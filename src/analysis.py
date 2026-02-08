import os
import sqlite3
import pandas as pd


DB_PATH = "cell_count.db"
RESULTS_DIR = "results"


def ensure_dir(path):
    if not os.path.isdir(path):
        os.makedirs(path)


def query_df(sql, db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(sql, conn)
    conn.close()
    return df


def write_summary_table(out_csv):
    sql = """
    WITH sample_totals AS (
        SELECT sample_id, SUM(count) AS total_count
        FROM cell_counts
        GROUP BY sample_id
    )
    SELECT
        cc.sample_id AS sample,
        st.total_count,
        cc.population,
        cc.count,
        ROUND(100.0 * cc.count / st.total_count, 2) AS percentage
    FROM cell_counts cc
    JOIN sample_totals st ON cc.sample_id = st.sample_id
    ORDER BY cc.sample_id, cc.population
    """
    df = query_df(sql)
    df.to_csv(out_csv, index=False)
    return df


def responders_dataset():
    sql = """
    WITH sample_totals AS (
        SELECT sample_id, SUM(count) AS total_count
        FROM cell_counts
        GROUP BY sample_id
    )
    SELECT
        cc.sample_id AS sample,
        s.response,
        cc.population,
        ROUND(100.0 * cc.count / st.total_count, 2) AS percentage
    FROM cell_counts cc
    JOIN sample_totals st ON cc.sample_id = st.sample_id
    JOIN samples s ON cc.sample_id = s.sample_id
    WHERE s.condition = 'melanoma'
      AND s.treatment = 'miraclib'
      AND s.sample_type = 'pbmc'
      AND s.response IN ('yes', 'no')
    """
    return query_df(sql)


def save_responders_boxplot(df, out_png):
    import matplotlib.pyplot as plt
    import seaborn as sns

    sns.set_style("whitegrid")
    plt.figure(figsize=(14, 8))

    sns.boxplot(
        data=df,
        x="population",
        y="percentage",
        hue="response"
    )

    plt.xlabel("population")
    plt.ylabel("percentage")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(out_png, dpi=300, bbox_inches="tight")
    plt.close()


def compare_responders(df):
    from scipy import stats

    rows = []
    populations = df["population"].unique()

    for pop in populations:
        pop_df = df[df["population"] == pop]

        responders = pop_df[pop_df["response"] == "yes"]["percentage"]
        non_responders = pop_df[pop_df["response"] == "no"]["percentage"]

        t_stat, p_value = stats.ttest_ind(responders, non_responders)

        resp_mean = responders.mean()
        nonresp_mean = non_responders.mean()

        rows.append({
            "population": pop,
            "responder_mean": round(resp_mean, 2),
            "non_responder_mean": round(nonresp_mean, 2),
            "difference": round(resp_mean - nonresp_mean, 2),
            "t_statistic": round(t_stat, 4),
            "p_value": round(p_value, 4),
        })

    return pd.DataFrame(rows).sort_values("p_value")


def baseline_summary():
    sql_projects = """
    SELECT
        s.project_id,
        COUNT(DISTINCT s.sample_id) AS sample_count
    FROM samples s
    WHERE s.condition = 'melanoma'
      AND s.treatment = 'miraclib'
      AND s.sample_type = 'pbmc'
      AND s.time_from_treatment_start = 0
    GROUP BY s.project_id
    ORDER BY s.project_id
    """

    sql_response = """
    SELECT
        s.response,
        COUNT(DISTINCT s.subject_id) AS subject_count
    FROM samples s
    WHERE s.condition = 'melanoma'
      AND s.treatment = 'miraclib'
      AND s.sample_type = 'pbmc'
      AND s.time_from_treatment_start = 0
      AND s.response IN ('yes', 'no')
    GROUP BY s.response
    ORDER BY s.response
    """

    sql_sex = """
    SELECT
        subj.sex,
        COUNT(DISTINCT s.subject_id) AS subject_count
    FROM samples s
    JOIN subjects subj ON s.subject_id = subj.subject_id
    WHERE s.condition = 'melanoma'
      AND s.treatment = 'miraclib'
      AND s.sample_type = 'pbmc'
      AND s.time_from_treatment_start = 0
    GROUP BY subj.sex
    ORDER BY subj.sex
    """

    sql_avg_b = """
    SELECT
        ROUND(AVG(cc.count), 2) AS avg_b_cells
    FROM cell_counts cc
    JOIN samples s ON cc.sample_id = s.sample_id
    JOIN subjects subj ON s.subject_id = subj.subject_id
    WHERE s.condition = 'melanoma'
      AND s.treatment = 'miraclib'
      AND s.sample_type = 'pbmc'
      AND s.time_from_treatment_start = 0
      AND s.response = 'yes'
      AND subj.sex = 'M'
      AND cc.population = 'b_cell'
    """

    prj = query_df(sql_projects)
    resp = query_df(sql_response)
    sex = query_df(sql_sex)
    avg = query_df(sql_avg_b)

    total_samples = int(prj["sample_count"].sum()) if len(prj) else 0
    avg_b_cells = avg["avg_b_cells"][0] if len(avg) else None

    responders = 0
    non_responders = 0
    if len(resp):
        yes_rows = resp[resp["response"] == "yes"]
        no_rows = resp[resp["response"] == "no"]
        responders = int(yes_rows["subject_count"].values[0]) if len(yes_rows) else 0
        non_responders = int(no_rows["subject_count"].values[0]) if len(no_rows) else 0

    male = 0
    female = 0
    if len(sex):
        m_rows = sex[sex["sex"] == "M"]
        f_rows = sex[sex["sex"] == "F"]
        male = int(m_rows["subject_count"].values[0]) if len(m_rows) else 0
        female = int(f_rows["subject_count"].values[0]) if len(f_rows) else 0

    summary = {
        "baseline_samples": total_samples,
        "baseline_projects": int(len(prj)),
        "baseline_responders": responders,
        "baseline_non_responders": non_responders,
        "baseline_male_subjects": male,
        "baseline_female_subjects": female,
        "avg_b_cells_male_responders": avg_b_cells,
    }

    return prj, resp, sex, pd.DataFrame([summary])


def main():
    import sys

    ensure_dir(RESULTS_DIR)

    mode = sys.argv[1] if len(sys.argv) > 1 else "all"

    if mode in ("all", "summary"):
        out_csv = os.path.join(RESULTS_DIR, "summary_table.csv")
        write_summary_table(out_csv)
        print("wrote %s" % out_csv)

    if mode in ("all", "responders"):
        df = responders_dataset()
        out_png = os.path.join(RESULTS_DIR, "boxplot_responders.png")
        save_responders_boxplot(df, out_png)
        print("wrote %s" % out_png)

        stats_df = compare_responders(df)
        out_stats = os.path.join(RESULTS_DIR, "statistical_results.csv")
        stats_df.to_csv(out_stats, index=False)
        print("wrote %s" % out_stats)

    if mode in ("all", "baseline"):
        prj, resp, sex, summary = baseline_summary()
        out_csv = os.path.join(RESULTS_DIR, "part4_summary.csv")
        summary.to_csv(out_csv, index=False)
        print("wrote %s" % out_csv)

        print(prj.to_string(index=False))
        print(resp.to_string(index=False))
        print(sex.to_string(index=False))


if __name__ == "__main__":
    main()