import psycopg2
import os


conn = psycopg2.connect("host=localhost dbname=AirBNB user=postgres password=Aspen909")

cur = conn.cursor()

cur.execute("""
    CREATE TEMP TABLE IF NOT EXISTS templistings(
    listing_id integer,
    listing_name text,
    host_id	bigint,
    host_name text,
    neighbourhood_group text,
    neighbourhood text,
    latitude decimal,
    longitude decimal,
    room_type text,
    price decimal,
    minimum_nights smallint,
    number_of_reviews smallint,
    last_review date,
    reviews_per_month decimal,
    calculated_host_listings_count smallint,
    availability_365 smallint
)
""")

cur.execute("""
    CREATE TABLE IF NOT EXISTS AirBNB_Pipeline.denver.listings(
    listing_id integer PRIMARY KEY ,
    listing_name text,
    host_id	bigint,
    host_name text,
    neighbourhood_group text,
    neighbourhood text,
    latitude decimal,
    longitude decimal,
    room_type text,
    price decimal,
    minimum_nights smallint,
    number_of_reviews smallint,
    last_review date,
    reviews_per_month decimal,
    calculated_host_listings_count smallint,
    availability_365 smallint
)
""")

cur.execute("""
    CREATE TABLE IF NOT EXISTS AirBNB_Pipeline.denver.reviews (
    listing_id integer,
    review_date date
    )
""")

cur.execute("""
    CREATE TABLE IF NOT EXISTS AirBNB_Pipeline.denver.neighbourhoods(
    neighbourhood_group text, 
    neighbourhood text
    )
""")

listings_csv_files = [i for i in os.listdir("/Users/maxblanchard/PycharmProjects/Airbnb_data_pipeline/AirBNB_Downloads"
                                            "/Listings")]
reviews_csv_files = [i for i in os.listdir("/Users/maxblanchard/PycharmProjects/Airbnb_data_pipeline/AirBNB_Downloads"
                                           "/Reviews")]
neighborhoods_csv_files = [i for i in os.listdir("/Users/maxblanchard/PycharmProjects/Airbnb_data_pipeline"
                                                 "/AirBNB_Downloads/Neighbourhoods")]

for i in listings_csv_files:
    if '2016-05-16' in i:
        break
    with open("/Users/maxblanchard/PycharmProjects/Airbnb_data_pipeline/AirBNB_Downloads/Listings/" + i, 'r') as f:
        next(f)
        print(i)
        cur.copy_expert("""COPY templistings FROM STDIN WITH (FORMAT CSV)""", f)
        cur.execute("""
                INSERT INTO AirBNB_Pipeline.denver.listings(
                    listing_id,
                    listing_name,
                    host_id,
                    host_name,
                    neighbourhood_group,
                    neighbourhood,
                    latitude,
                    longitude,
                    room_type,
                    price,
                    minimum_nights,
                    number_of_reviews,
                    last_review,
                    reviews_per_month,
                    calculated_host_listings_count,
                    availability_365)
                SELECT distinct 
                    listing_id,
                    listing_name,
                    host_id,
                    host_name,
                    neighbourhood_group,
                    neighbourhood,
                    latitude,
                    longitude,
                    room_type,
                    price,
                    minimum_nights,
                    number_of_reviews,
                    last_review,
                    reviews_per_month,
                    calculated_host_listings_count,
                    availability_365
                FROM templistings t
                WHERE t.listing_id not in (
                    select listing_id
                    from AirBNB_Pipeline.denver.listings
                )
            """)

for i in reviews_csv_files:
    with open("/Users/maxblanchard/PycharmProjects/Airbnb_data_pipeline/AirBNB_Downloads/Reviews/" + i, 'r') as f:
        next(f)
        print(i)
        cur.copy_expert("""COPY AirBNB_Pipeline.denver.reviews FROM STDIN WITH (FORMAT CSV)""", f)

for i in neighborhoods_csv_files:
    with open("/Users/maxblanchard/PycharmProjects/Airbnb_data_pipeline/AirBNB_Downloads/Neighbourhoods/"
              + i, 'r') as f:
        next(f)
        cur.copy_expert("""COPY AirBNB_Pipeline.denver.neighbourhoods FROM STDIN WITH (FORMAT CSV)""", f)

conn.commit()
conn.close()