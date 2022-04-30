from threading import Thread, Lock
import sqlite3
import time


connection = sqlite3.connect("Multiple_thread.db", check_same_thread=False)
cursor = connection.cursor()


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


def task_1(con, c, c1: int, c2: int, c3: int, c4: [], lock):
    with lock:

        p = str(c4)
        c.execute("INSERT INTO my_table VALUES (?,?,?,?)", (c1, c2, c3, p))
        con.commit()

        c.execute("SELECT rowid, * FROM my_table ORDER BY rowid DESC LIMIT 1")
        item = c.fetchone()
        print("task1")
        print(f"{item[0]}: {item[1]} {item[2]} {item[3]} {item[4]}")


def task_2(con, c, list_, lock):
    new_item_list = []

    for item_tuple in list_:
        item_list = list(item_tuple)
        item_list[3] = str(item_list[3])
        new_item_tuple = tuple(item_list)
        new_item_list.append(new_item_tuple)

    with lock:
        c.executemany("INSERT INTO my_table VALUES (?,?,?,?)", new_item_list)
        con.commit()
        c.execute("SELECT rowid, * FROM my_table")
        items = c.fetchall()
        print("task2")
        for item in items:
            print(f"{item[0]}: {item[1]} {item[2]} {item[3]} {item[4]}")


def task_3(con, c, c1: int, c2: int, id_, lock):
    with lock:
        c.execute("UPDATE my_table SET column1 = ?, column2 = ? WHERE rowid = ?", (c1, c2, id_))
        con.commit()

        c.execute("SELECT rowid, * FROM my_table WHERE rowid = ?", id_)
        item = c.fetchone()
        print("task3")
        print("Updated item")
        print(f"{item[0]}: {item[1]} {item[2]} {item[3]} {item[4]}")


def task_4(con, c, id_, lock):
    with lock:
        c.executemany("DELETE FROM my_table WHERE rowid = ?", id_)
        print("task4")
        print(f"Deleted row {id_} Successfully")
        con.commit()

# delete_table()
# create_table()


new_list = [(55, 60, 88, [1, 2, 3]), (0, 0, 0, [11, 22, 33]), (1, 1, 1, [111, 222, 333]), (69, 69, 69, [45, 50, 60])]
lock_ = Lock()
t1 = Thread(target=task_1, args=(connection, cursor, 51, 68, 69, [17, 7, 1998], lock_))
t2 = Thread(target=task_2, args=(connection, cursor, new_list, lock_))
t3 = Thread(target=task_3, args=(connection, cursor, 121, 125, "3", lock_))
t4 = Thread(target=task_4, args=(connection, cursor, "94", lock_))

all_tasks = [t1, t2, t3, t4]
start_time = time.time()

for i in all_tasks:
    i.start()

with lock_:
    cursor.execute("INSERT INTO my_table VALUES (4,5,6,'[11,02,1999]')")
    connection.commit()

    cursor.execute("SELECT rowid, * FROM my_table ORDER BY rowid DESC LIMIT 1")
    item = cursor.fetchone()
    print("new item")
    print(f"{item[0]}: {item[1]} {item[2]} {item[3]} {item[4]}")


for i in all_tasks:
    i.join()


connection.close()

end_time = time.time()
print(f"Total time: {end_time - start_time}")
