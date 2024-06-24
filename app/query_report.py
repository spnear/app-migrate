class QueryReport:
    def __init__(self):
        self.deparments_2021_report = '''
        WITH hiring_report AS (
        SELECT 
        d.id,
        d.department,
        COUNT(1) AS hired
        FROM hired_employees h
        LEFT JOIN departments d on h.department_id = d.id
        WHERE EXTRACT('Year' FROM CAST(datetime as timestamp)) = 2021
        GROUP BY d.id, d.department
        ),
        hiring_avg AS(
            SELECT AVG(hired) AS average_hired
            FROM hiring_report
        )	

        SELECT id, department, hired
        FROM hiring_report hr, hiring_avg ha
        WHERE hr.hired > ha.average_hired
        ORDER BY hired DESC;
        '''

        self.hiring_2021_report = '''
        WITH stage_hired AS(
        SELECT 
            --id,
            --name,
            CAST(datetime as timestamp) as datetime,
            department,
            job
        FROM hired_employees h
        LEFT JOIN departments d on h.department_id = d.id
        LEFT JOIN jobs j on h.job_id = j.id
        ),
        quarter_data AS(
        SELECT 
            department,
            job,
            EXTRACT('QUARTER' FROM datetime) as quarter
        FROM stage_hired
        WHERE EXTRACT('Year' FROM datetime) = 2021
        )

        SELECT
            department,
            job,
            COALESCE(SUM(CASE WHEN quarter = 1 THEN hire_count ELSE 0 END), 0) AS Q1,
            COALESCE(SUM(CASE WHEN quarter = 2 THEN hire_count ELSE 0 END), 0) AS Q2,
            COALESCE(SUM(CASE WHEN quarter = 3 THEN hire_count ELSE 0 END), 0) AS Q3,
            COALESCE(SUM(CASE WHEN quarter = 4 THEN hire_count ELSE 0 END), 0) AS Q4
        FROM (
            SELECT
                department,
                job,
                quarter,
                COUNT(*) AS hire_count
            FROM
                quarter_data
            GROUP BY
                department, job, quarter
        ) AS subquery
        GROUP BY
            department, job
        ORDER BY
            department, job;
        '''