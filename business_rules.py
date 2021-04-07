import psycopg2
import pandas.io.sql as psql
import pandas as pd

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
    """This function generates a part of the database using the generateDB.txt file.
    it only generates the part that is used in this file,
    the rest of the database generation is done by using the functions from the group project"""
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
    category = cur.fetchall()
    categorydict = {}
    for product_id in category:
        if product_id[0] in categorydict:
            categorydict[product_id[0]].append(product_id[1])
        else:
            categorydict[product_id[0]] = [product_id[1]]
    return categorydict


def fill_db(dct, table_name):
    """This function fills the Postgresql database based on a dictionary,
    it loops over the amount of keys and adds 4 of the values that are associated with that key.
    If the key has less then 4 values it inserts the values it can and leaves the rest as null.

    Args:
        dct: The dictionary that is used to fill the postgresql database."""
    for key, value in dct.items():
        lenght = min(len(value), 4)
        db_input = tuple([str(key)] + list(value[:lenght]))
        cur.execute(f"INSERT INTO {table_name} Values %s", (db_input, ))
        con.commit()


def deduplication_products(df):
    """this function counts the amount of times a product(_id) has been bought by a certain segment.
    It also orders the dataframe and outputs it ordered by segment and total_quantity.

    Args:
        df: the pandas dataframe that needs to be cleaned up.

    Returns:
        retuns a cleaned dataframe object."""
    prev_product = None
    prev_segment = None
    total_quantity = 0
    cleaned_lst = []
    for index, (product_id, quantity, segment) in df.iterrows():
        if product_id == prev_product and segment == prev_segment:
            total_quantity += quantity
        else:
            cleaned_lst.append((prev_product, total_quantity, prev_segment))
            total_quantity = quantity
            prev_product = product_id
            prev_segment = segment
    cleaned_lst.append((prev_product, total_quantity, prev_segment))
    cleaned_lst = cleaned_lst[1:]
    cleaned_df = pd.DataFrame(cleaned_lst)
    cleaned_df.columns = ["product_id", "total_quantity", "segment"]
    cleaned_df = cleaned_df.sort_values(by=["segment", 'total_quantity'], ascending=False)
    return cleaned_df


def collaborative_filtering():
    """This function pulls the product_id, quantity of the bought products,
    and the segements from the order_products in postgresql.
    It calls the deduplication_products function with the df Arg.

    Returns:
        returns a dictionary with the key as segment and the values as recommendations."""
    df = psql.read_sql_query("""SELECT product_id, quantity, segment FROM ordered_products
                                       INNER JOIN sessions ON sessions.session_id = ordered_products.session_id
                                       order by product_id, segment ASC""", con)
    cleaned_df = deduplication_products(df)

    cur.execute("SELECT distinct segment from sessions")
    segments = cur.fetchall()
    segment_dict = {}
    for segment in segments:
        curr = cleaned_df[cleaned_df["segment"] == segment[0]]
        segment_dict[segment[0]] = curr["product_id"][:4].values.tolist()
    return segment_dict


create_tables(genDB)
fill_db(category_recommendation(), "category_recommendation")
fill_db(collaborative_filtering(), "collaborative_recommendation")

