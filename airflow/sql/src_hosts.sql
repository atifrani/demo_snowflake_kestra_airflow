
CREATE OR REPLACE TABLE airbnb.src.src_listings_afw
                    (listings_id integer,
                    listings_url string,
                    listings_name string,
                    room_type string,
                    minimum_nights integer,
                    host_id integer,
                    price_str string,
                    created_at datetime,
                    updated_at datetime);


insert into airbnb.src.src_listings_afw 
                SELECT  id AS listing_id,  name AS listing_name,  listing_url,  room_type,  minimum_nights,
                host_id, price AS price_str, created_at, updated_at 
                FROM airbnb.raw.raw_listings_kst