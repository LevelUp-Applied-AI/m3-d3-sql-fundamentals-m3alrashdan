import sqlite3


# Task 1 — Aggregation: top_departments
def top_departments(db_path):
    """
    Returns top 3 departments by total salary expenditure.
    Output: [(dept_name, total_salary), ...]
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = """
        SELECT d.name, SUM(e.salary) AS total_salary
        FROM employees e
        JOIN departments d ON e.dept_id = d.dept_id
        GROUP BY d.dept_id
        ORDER BY total_salary DESC
        LIMIT 3;
    """

    cursor.execute(query)
    results = cursor.fetchall()

    conn.close()
    return results


# Task 2 — JOIN: employees_with_projects
def employees_with_projects(db_path):
    """
    Returns all employees assigned to at least one project.
    Output: [(employee_name, project_name), ...]
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = """
        SELECT e.name, p.name
        FROM employees e
        INNER JOIN project_assignments pa ON e.emp_id = pa.emp_id
        INNER JOIN projects p ON pa.project_id = p.project_id;
    """

    cursor.execute(query)
    results = cursor.fetchall()

    conn.close()
    return results


# Task 3 — Window Function: salary_rank_by_department
def salary_rank_by_department(db_path):
    """
    Returns employees ranked by salary within each department.
    Output: [(employee_name, dept_name, salary, rank), ...]
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = """
        SELECT
            e.name,
            d.name,
            e.salary,
            RANK() OVER (
                PARTITION BY e.dept_id
                ORDER BY e.salary DESC
            ) AS rank
        FROM employees e
        JOIN departments d ON e.dept_id = d.dept_id
        ORDER BY d.name ASC, rank ASC;
    """

    cursor.execute(query)
    results = cursor.fetchall()

    conn.close()
    return results