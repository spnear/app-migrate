import flask
from flask import request, jsonify, request
from utils import CommonFunctions, ProcessingData
from database_interface import DatabaseInterface
from fileserver_interface import FileServerInterface
from query_report import QueryReport
app = flask.Flask(__name__)
app.config["DEBUG"] = True


@app.route('/', methods=['GET'])
def home():
    user_ip_info = request.remote_addr
    return jsonify({'Message':f'This is your IP{user_ip_info}'}, 200)

@app.route('/insert/<table>', methods = ['POST'])
def insert_data(table):
    if request.method == 'POST':
        raw_data = request.get_data()
        print(raw_data) 
        functions = CommonFunctions()
        df = functions.read_df_from_json(raw_data)
        rows,cols = df.shape
        if rows>0 and rows<1000 and cols>0:
            processor = ProcessingData(df,table)
            df_to_load, df_nulls = processor.execute_processing()
            database_loader = DatabaseInterface()
            database_load = database_loader.write_to_database(df_to_load, table)
            fileserver_loader = FileServerInterface()
            fileserver_loader.write_to_fileserver(df_nulls,table)
            return jsonify({'insert_status':'success','table':table},200)
        else:
            return jsonify({'insert_status':'failed','description':'Body out of index'},400)

@app.route('/backup/<table>',methods = ['GET'])
def generate_backup_table(table):
    database_loader = DatabaseInterface()
    database_loader.generate_table_backup(table)
    return jsonify({'connection':'success','message':f'backup generated for {table}'})


@app.route('/backups3/<table>',methods = ['GET'])
def generate_backup_table_s3(table):
    database_loader = DatabaseInterface()
    database_loader.generate_table_backup_s3(table)
    return jsonify({'connection':'success','message':f'backup generated for {table}'})


@app.route('/restore/<table>',methods = ['GET'])
def restore_table(table):
    database_loader = DatabaseInterface()
    database_loader.restore_table_backup(table)
    return jsonify({'connection':'success','message':f'{table} was restored'})


@app.route('/report/2021/quarter/', methods=['GET'])
def def_hired_report_2021():
    database_loader = DatabaseInterface()
    functions = CommonFunctions()
    query_report = QueryReport()
    query = query_report.hiring_2021_report
    df=database_loader.read_from_database(query)
    data=functions.df_to_json(df)
    if data:
        return jsonify({'Result':'success','data':data}, 200)
    else:
        return jsonify({'Result':'Something went wrong','data':data}, 400)
    
@app.route('/report/2021/hiring_departments', methods=['GET'])
def def_hiring_departments_report_2021():
    database_loader = DatabaseInterface()
    functions = CommonFunctions()
    query_report = QueryReport()
    query = query_report.deparments_2021_report
    df=database_loader.read_from_database(query)
    data=functions.df_to_json(df)
    if data:
        return jsonify({'Result':'success','data':data}, 200)
    else:
        return jsonify({'Result':'Something went wrong','data':data}, 400)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000)