DROP TABLE IF EXISTS departments;
create table departments(
    id INTEGER,
    department VARCHAR(100),
	PRIMARY KEY (id)
);

DROP TABLE IF EXISTS jobs;
create Table jobs(
    id INTEGER,
    job VARCHAR(100),
	PRIMARY KEY (id)
);

DROP TABLE IF EXISTS hired_employees;
CREATE TABLE hired_employees(
    id INTEGER,
    name VARCHAR(100),
    datetime VARCHAR(30),
    department_id INTEGER,
    job_id INTEGER,
	PRIMARY KEY (id),
	FOREIGN KEY (department_id) REFERENCES departments(id),
	FOREIGN KEY (job_id) REFERENCES jobs(id)
);