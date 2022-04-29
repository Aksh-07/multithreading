from threading import Thread
import sqlite3
import time


def delete_table():
    con = sqlite3.connect("Multiple_thread.db")
    c = con.cursor()
    c.execute("DROP TABLE my_table")
    con.commit()
    con.close()


def create_table():
    con = sqlite3.connect("Multiple_thread.db")
    c = con.cursor()
    c.execute("""CREATE TABLE my_table (
                                column1 INTEGER,
                                column2 INTEGER,
                                column3 INTEGER,
                                column4 TEXT
    )""")
    con.commit()
    con.close()


def task_1():
    con = sqlite3.connect("Multiple_thread.db")
    c = con.cursor()
    c.execute("SELECT rowid, * FROM my_table")
    items = c.fetchmany(5)
    print("task1")
    for item in items:
        print(f"{item[0]}: {item[1]} {item[2]} {item[3]} {item[4]}")
    c.close()


def task_2(id_):
    con = sqlite3.connect("Multiple_thread.db")
    c = con.cursor()
    c.execute("SELECT rowid, * FROM my_table WHERE rowid = (?)", (id_,))
    item = c.fetchone()
    print("task2")
    print(f"{item[0]}: {item[1]} {item[2]} {item[3]} {item[4]}")
    c.close()


def task_3(c1: int, c2: int, c3: int, c4: []):
    con = sqlite3.connect("Multiple_thread.db")
    c = con.cursor()
    p = str(c4)
    c.execute("INSERT INTO my_table VALUES (?,?,?,?)", (c1, c2, c3, p))
    con.commit()

    c.execute("SELECT rowid, * FROM my_table ORDER BY rowid DESC LIMIT 1")
    item = c.fetchone()
    print("task3")
    print(f"{item[0]}: {item[1]} {item[2]} {item[3]} {item[4]}")
    c.close()


def task_4(list_):
    con = sqlite3.connect("Multiple_thread.db")
    c = con.cursor()

    new_item_list = []

    for item_tuple in list_:
        item_list = list(item_tuple)
        item_list[3] = str(item_list[3])
        new_item_tuple = tuple(item_list)
        new_item_list.append(new_item_tuple)

    c.executemany("INSERT INTO my_table VALUES (?,?,?,?)", new_item_list)
    con.commit()
    c.execute("SELECT rowid, * FROM my_table")
    items = c.fetchall()
    print("task4")
    for item in items:
        print(f"{item[0]}: {item[1]} {item[2]} {item[3]} {item[4]}")
    c.close()

# delete_table()
# create_table()


new_list = [(55, 60, 88, [1, 2, 3]), (0, 0, 0, [11, 22, 33]), (1, 1, 1, [111, 222, 333]), (69, 69, 69, [45, 50, 60])]
t1 = Thread(target=task_1, args=())
t2 = Thread(target=task_2, args=(6,))
t3 = Thread(target=task_3, args=(51, 68, 69, [17, 7, 1998]))
t4 = Thread(target=task_4, args=[new_list])

all_tasks = [t1, t2, t3, t4]
start_time = time.time()
for i in all_tasks:
    i.start()
    # i.join()

task_2(20)
task_3(45, 54, 66, [50, 52, 90])
end_time = time.time()
print(f"Total time: {end_time - start_time}")
