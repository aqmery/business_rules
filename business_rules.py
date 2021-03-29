import psycopg2
import random

con = psycopg2.connect(
    host="localhost",
    database="RCMD",
    user="postgres",
    password="HANA5612##deno",
    port="5432")

cur = con.cursor()
cur.execute("SELECT * FROM products")
products = cur.fetchall()
#print(products)

cur.execute("SELECT category, id From products as C where C.category is not NULL")
catagory = cur.fetchall()
#print(catagory)

genDB = "generateDB.txt"



def create_tables(genDB):
    with open(genDB, "r") as file:
        file_object = file.read().replace('\n', '')
    query_list = file_object.split(";")
    print(file_object)
    for query in query_list:
        print(f"query :{query}.")
        if query != "":
            cur.execute(query + ";")
            con.commit()



def catagory_recommendation():
    cur.execute("SELECT category, id  From products as C where C.category is not NULL")
    catagory = cur.fetchall()
    print(type(catagory))
    catagorydict = {}
    for i in catagory:
        if i[0] in catagorydict:
            catagorydict[i[0]].append(i[1])
        else:
            catagorydict[i[0]] = [i[1]]
    for j, k in catagorydict.items():
        print(f" key : {j} |values : {k}")
    return catagorydict




def adddict(dct):
    values = list(dct.values())
    keys = list(dct.keys())
    count = 0
    for i in values:
        print(i[:4])
        print(keys[count])
        cur.execute("INSERT INTO catagory_recommendation (product_catagory, first_recommendation, "
                    "second_recommendation, third_recommendation, fourth_recommendation)"
                    " Values (%s, %s, %s, %s, %s,)", (keys[count], i[0], i[1], i[2], i[3]))
        con.commit()
        count += 1




recommendation = catagory_recommendation()
adddict(recommendation)
#create_tables(genDB)


