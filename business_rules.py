import psycopg2

"""This part above the functions creates a connection to the database
and sets the variable genDB as the file that contains the code to generates the database"""

con = psycopg2.connect(
    host="localhost",
    database="recs",
    user="postgres",
    password="HANA5612##deno",
    port="5432")
cur = con.cursor()
genDB = "generateDB.txt"



def create_tables(genDB):
    """This function generates a part of the database using the generateDB.txt file
    it only generates the part that is used in this file,
    the rest of the database generation is done in using the functions from the group project"""
    with open(genDB, "r") as file:
        file_object = file.read().replace('\n', '')
    query_list = file_object.split(";")
    for query in query_list:
        if query != "":
            cur.execute(query + ";")
            con.commit()



def category_recommendation():
    """This function makes recommendations based on the category of the product,
     it uses postgresql to get this data.

     Returns:
         returns a dictionary with all product id's sorted into the category of that product."""
    cur.execute("SELECT category, product_id  From products as C where C.category is not NULL")
    catagory = cur.fetchall()
    catagorydict = {}
    for i in catagory:
        if i[0] in catagorydict:
            catagorydict[i[0]].append(i[1])
        else:
            catagorydict[i[0]] = [i[1]]
    return catagorydict


def fill_db(dct):
    """This function fills the Postgresql database based on a dictionary,
    it loops over the amount of keys and adds 4 of the values that are associated with that key.
    If the key has less then 4 values it inserts the values it can and leaves the rest as null.

    Args:
        dct: The dictionary that is used to fill the postgresql database."""
    for key, value in dct.items():
        lenght = 4
        if len(value)< 4:
            lenght = len(value)
        db_input = list(value[:lenght])
        db_input.insert(0, key)
        db_input = tuple(db_input)
        cur.execute("INSERT INTO catagory_recommendation"
                    " Values %s", (db_input, ))
        con.commit()


create_tables(genDB)
recommendation = category_recommendation()
fill_db(recommendation)

