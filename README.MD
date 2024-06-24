### Hiring data app

This is an application to interact with the hiring database developed with Python and Flask

You can setup your postgresql database using the docker-compose.yml:
```
docker-compose up
```

Also, you must add your .env file with all the variables that you will use for setup this app with these variables:

```
DATABASE_HOST
DATABASE_PORT
DATABASE_NAME
DATABASE_USERNAME
DATABASE_PASSWORD
DB_URL=postgresql://${DATABASE_USERNAME}:${DATABASE_PASSWORD}@${DATABASE_HOST}/${DATABASE_NAME}

NULL_PATH
BACKUP_PATH


ACEESS_KEY
SECRET_ACCESS
```

Remember also create you python virtual enviroment and install the dependencies with:
```
pip install -r requeriments.txt
```

You can run the app using in the /app directory:
```
python3 app.py
```

You have the following endpoints to interact with the database:

/insert/{tables}
HTTP Method: POST

Insert data to the expected table replacing {table} , you can insert between 1 and 1000 rows in your request

To add data to the jobs table:

```
[
    {"id":"191","job":"data engineer"},
    {"id":"192","job":"data analyst"}
]
```

To add data to the hiring_employees table:

```
[
    {"id":"20000","name":"Juan","datetime":"2021-11-07T02:48:42Z","department_id":"2","job_id":"3"},
    {"id":"20001","name":"Sebastian","datetime":"2022-11-07T02:38:42Z","department_id":"1","job_id":"5"}
]
```
To add data to departments table:
```
[
    {"id":"13","department":"Managed Services"},
    {"id":"14","department":"R&D"}
]
```


/report/2021/quarter/
HTTP Method: GET

Obtain the hiring report during the year 2021 by quarter of every department and jobs
![Quarter report 2021](/img/hiring_repartments.png)
You can feed with this data a quarter report in a dashboard:
![Quarter report 2021](/img/tableau_quarter_report.png)

/report/2021/hiring_departments
HTTP Method: GET

Obtain the hiring report during the year by department 2021 
![Report by department2021](/img/quarter_report.png)
You can feed with this data a department report in a dashboard:
![Quarter report 2021](/img/tableau_department_report.png)
