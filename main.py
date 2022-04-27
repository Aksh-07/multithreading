from threading import Thread
import sqlite3
import pickle


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
                                column3 INTEGER
    )""")
    con.commit()
    con.close()


def task_1():
    con = sqlite3.connect("Multiple_thread.db")
    c = con.cursor()
    c.execute("SELECT rowid, * FROM my_table")
    item = c.fetchmany(5)
    print("task1")
    print(item)

    c.close()


def task_2(id_):
    con = sqlite3.connect("Multiple_thread.db")
    c = con.cursor()
    c.execute("SELECT rowid, * FROM my_table WHERE rowid = (?)", (id_,))
    item = c.fetchone()
    print("task2")
    print(item)
    c.close()


def task_3(c1: int, c2: int, c3: int):
    con = sqlite3.connect("Multiple_thread.db")
    c = con.cursor()
    # pickle_l = pickle.dumps(c4)
    c.execute("INSERT INTO my_table VALUES (?,?,?)", (c1, c2, c3))
    con.commit()

    c.execute("SELECT rowid, * FROM my_table ORDER BY rowid DESC LIMIT 1")
    item = c.fetchone()
    print("task3")
    print(item)
    c.close()


def task_4(list_):
    con = sqlite3.connect("Multiple_thread.db")
    c = con.cursor()
    c.executemany("INSERT INTO my_table VALUES (?,?,?)", list_)
    con.commit()
    c.execute("SELECT rowid, * FROM my_table")
    items = c.fetchall()
    print("task4")
    for z in items:
        print(z)
    c.close()


# create_table()

new_list = [(55, 60, 88), (0, 0, 0), (1, 1, 1), (69, 69, 69)]
t1 = Thread(target=task_1, args=())
t2 = Thread(target=task_2, args=(6,))
t3 = Thread(target=task_3, args=(51, 68, 69))
t4 = Thread(target=task_4, args=[new_list])

all_tasks = [t1, t2, t3, t4]
new_tasks = []
for i in all_tasks:
    i.start()
    new_tasks.append(i)


for i in new_tasks:
    if i.is_alive() and len(new_tasks) == 1:
        i.join()
        new_tasks.remove(i)
    elif i.is_alive() and len(all_tasks) != 1:
        new_tasks.append(i)

