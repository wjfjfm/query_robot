import pymysql
import itertools
import json
import time

def get_dict_from_cursor(cursor):
    desc = cursor.description
    column_names = [col[0] for col in desc]
    # data = [dict(itertools.izip(column_names, row))
    data = [dict(zip(column_names, row))
            for row in cursor.fetchall()]
    return data

class DBInfo:
    connect_timeout = 1
    charset = 'utf8'
    def __init__(self, host=None, user=None, pswd=None, db=None, port=3306, table=None):
        self.host = host
        self.user = user
        self.pswd = pswd
        self.db   = db
        self.port = port
        self.table = table

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return self.__str__()

    def get_mysql_conn(self):
        return pymysql.connect(host = self.host,
                               user = self.user,
                               password = self.pswd,
                               database = self.db,
                               port = self.port,
                               connect_timeout=self.connect_timeout,
                               charset = self.charset)

class RoboNode:

    def __init__(self):
        self.id = None
        self.next = []
        self.query = None
        self.output = None
        self.if_change = None
        self.if_exist = None
        self.if_report = None
        self.report_interval = None

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return self.__str__()


class QueryRobot:
    def __init__(self):
        self.head_node = RoboNode()
        self.head_node.report_interval = -1
        self.head_node.if_change = False
        self.head_node.if_exist = False
        self.robo_db = None
        self.query_db = None
        self.output_db = None

        self.query_conn = None
        self.output_conn = None

        self.query_interval = 5

        self.result_cache = {}
        self.last_report = {}


    def go_through(self, node):
        print("go through node:")

        to_report = False

        if node.query is None:
            result_dict = []
        else:
            result_dict = self.execute_query(node.query)

        if node.if_change and node.if_exist:
            cache = self.result_cache.get(node.id, None)
            if len(result_dict) > 0 :
                if cache is None:
                    to_report = True
                elif cache != result_dict:
                    to_report = True

        elif node.if_exist:
            if len(result_dict) > 0:
                to_report = True

        elif node.if_change:
            cache = self.result_cache.get(node.id, None)
            if cache and cache != result_dict:
                to_report = True
        else:
            to_report = True


        if time.time() - self.last_report.get(node.id, 0) < node.report_interval:
            to_report = False

        if to_report and node.if_report:
            output_text = self.gen_output_context(node, result_dict)
            print("report context:")
            self.insert_to_output(output_text)

        if to_report:
            self.result_cache[node.id] = result_dict
            self.last_report[node.id] = time.time()
            for n_node in node.next:
                self.go_through(n_node)

    def get_conn(self):
        self.query_conn = self.query_db.get_mysql_conn()
        self.output_conn = self.output_db.get_mysql_conn()
        self.robo_conn = self.robo_db.get_mysql_conn()
        print("got db connection")

    def release_conn(self):
        self.query_conn.close()
        self.query_conn = None

        self.output_conn.close()
        self.output_conn = None

        self.robo_conn.close()
        self.robo_conn = None
        print("released db connection")

    def run_once(self):
        self.get_conn()
        self.update_robo_command()
        self.go_through(self.head_node)
        self.release_conn()

    def run(self):
        last_run = 0
        while True:
            if time.time() - last_run < self.query_interval:
                time.sleep(0.5)
                continue

            last_run = time.time()
            tm = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last_run))

            print(tm)

            self.run_once()

    def update_robo_command(self):
        print("getting robo command...")

        cursor = self.robo_conn.cursor()
        sql = "SELECT id, precheck, query, if_exist, if_change, if_report, output, report_interval FROM {}".format(self.robo_db.table)
        cursor.execute(sql)
        rows = cursor.fetchall()

        id_to_node = {}
        self.head_node.next = []

        for row in rows:
            node = RoboNode()
            node.id = row[0]
            precheck = row[1]
            node.query = row[2]
            node.if_exist = row[3]
            node.if_change = row[4]
            node.if_report = row[5]
            node.output = row[6]
            node.report_interval = row[7]
            id_to_node[node.id] = node


            precheck_node = None
            if precheck:
                precheck_node = id_to_node.get(precheck, None)

            if precheck_node:
                precheck_node.next.append(node)
            else:
                self.head_node.next.append(node)

        print("got robo command:")
        print(self.head_node)

    def execute_query(self, sql):
        cursor = self.query_conn.cursor()
        cursor.execute(sql)
        result_dict = get_dict_from_cursor(cursor)

        self.query_conn.commit()
        return result_dict

    def gen_output_context(self, node, result_dict):
        text = ""
        text += "# " + str(node.output) + "\n\n"
        result_json = json.dumps(result_dict, indent=4, default=str)
        text += "```\n"
        text += str(result_json)
        text += "```"
        return text

    def insert_to_output(self, text):
        row = {}
        row["text"] = str(text)
        row["time"] = str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        self.exe_insert_sql(self.output_conn, self.output_db.table, row)

    @staticmethod
    def exe_insert_sql(conn, table, myDict):
        cursor = conn.cursor()
        placeholders = ', '.join(['%s'] * len(myDict))
        columns = ', '.join(myDict.keys())
        sql = u"INSERT INTO %s ( %s ) VALUES ( %s )" % (table, columns, placeholders)
        # valid in Python 2
        # cursor.execute(sql, myDict.values())
        # Python3
        cursor.execute(sql, list(myDict.values()))
        row_count = cursor.rowcount

        if row_count != 1:
            raise("Failed to insert into db")

        sql = "SELECT LAST_INSERT_ID()"
        cursor.execute(sql)
        rows = cursor.fetchall()

        if len(rows) == 0:
            raise("Failed to get insert id")

        insert_id = rows[0][0]
        conn.commit()
        return insert_id
