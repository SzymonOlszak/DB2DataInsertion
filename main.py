import ibm_db
from decimal import Decimal
import json
from datetime import datetime


def parse_decimal(d):
    if isinstance(d, dict) and "$numberDecimal" in d:
        return Decimal(d["$numberDecimal"])
    return None


def parse_date(d):
    if d:
        return datetime.fromisoformat(d["$date"].replace("Z", ""))
    return None


def transfer_json_data(data):
    conn = None
    try:
        conn = ibm_db.connect("DATABASE=SAMPLE;HOSTNAME=localhost;PORT=25001;PROTOCOL=TCPIP;UID=SO2;PWD=pass;", "", "")

    except Exception as e:
        print("Connection error", e)

    address_sql = """
        INSERT INTO airbnb.address (
            street, suburb, government_area, market, country, country_code, longitude, latitude, exact_location
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    address_stmt = ibm_db.prepare(conn, address_sql)

    listing_sql = """
        INSERT INTO airbnb.listing_info(
            listing_id,
            name,
            summary,
            space,
            description,
            property_type,
            room_type,
            bed_type,
            accommodates,
            bedrooms,
            beds,
            bathrooms,
            number_of_reviews,
            last_scraped,
            calendar_last_scraped,
            first_review,
            last_review,
            thumbnail_url,
            medium_url,
            picture_url,
            xl_picture_url,
            host_id,
            address_id
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
    listing_stmt = ibm_db.prepare(conn, listing_sql)

    host_sql = """
    INSERT INTO airbnb.hosts (
        host_id,
        host_name,
        host_location,
        host_about,
        host_response_time,
        host_response_rate,
        host_is_superhost,
        host_has_profile_pic,
        host_identity_verified,
        host_listings_count,
        host_total_listings_count
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    host_stmt = ibm_db.prepare(conn, host_sql)

    verification_sql = """
    INSERT INTO airbnb.host_verifications (host_id, verification)
    VALUES (?, ?)
    """
    verification_stmt = ibm_db.prepare(conn, verification_sql)

    prices_sql = """
    INSERT INTO airbnb.prices (listing_id, price, security_deposit, cleaning_fee, extra_people, guests_included)
    VALUES (?, ?, ?, ?, ?, ?)
    """
    prices_stmt = ibm_db.prepare(conn, prices_sql)

    amenities_sql = """
    MERGE INTO airbnb.amenities t
    USING (VALUES (?)) s(name)
    ON t.name = s.name
    WHEN NOT MATCHED THEN
        INSERT (name) VALUES (s.name)
    """
    amenities_stmt = ibm_db.prepare(conn, amenities_sql)

    list_amenities_sql = """
    MERGE INTO airbnb.list_amenities t
    USING (VALUES (?, ?)) s(listing_id, amenity_id)
    ON t.listing_id = s.listing_id AND t.amenity_id = s.amenity_id
    WHEN NOT MATCHED THEN
        INSERT (listing_id, amenity_id)
        VALUES (s.listing_id, s.amenity_id)
    """
    list_amenities_stmt = ibm_db.prepare(conn, list_amenities_sql)

    reviewers_sql = """
    MERGE INTO airbnb.reviewers t
    USING (VALUES (?, ?)) s(reviewer_id, reviewer_name)
    ON t.reviewer_id = s.reviewer_id
    WHEN NOT MATCHED THEN
        INSERT (reviewer_id, reviewer_name)
        VALUES (s.reviewer_id, s.reviewer_name)
    """
    reviewers_stmt = ibm_db.prepare(conn, reviewers_sql)

    review_scores_sql = """
    MERGE INTO airbnb.review_scores t
    USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?)) 
    s(listing_id, accuracy, cleanliness, checkin, communication, location, value, rating)
    ON t.listing_id = s.listing_id
    WHEN NOT MATCHED THEN
        INSERT (listing_id, accuracy, cleanliness, checkin, communication, location, value, rating)
        VALUES (s.listing_id, s.accuracy, s.cleanliness, s.checkin, s.communication, s.location, s.value, s.rating)
    """
    review_scores_stmt = ibm_db.prepare(conn, review_scores_sql)

    availability_sql = """
    MERGE INTO airbnb.availability t
    USING (VALUES (?, ?, ?, ?, ?))
    s(listing_id, availability_30, availability_60, availability_90, availability_365)
    ON t.listing_id = s.listing_id
    WHEN NOT MATCHED THEN
        INSERT (listing_id, availability_30, availability_60, availability_90, availability_365)
        VALUES (s.listing_id, s.availability_30, s.availability_60, s.availability_90, s.availability_365)
    """
    availability_stmt = ibm_db.prepare(conn, availability_sql)

    # MERGE INTO target_table
    # USING source_table
    # ON merge_condition
    # WHEN MATCHED THEN
    # UPDATE SET column1 = value1[, column2 = value2...]
    # WHEN NOT MATCHED THEN
    # INSERT(column1[, column2...])
    # VALUES(value1[, value2...]);

    for listing in data:

        #ADDRESS
        addr = listing.get("address", {})
        loc = addr.get("location", {})

        coordinates = loc.get("coordinates", [None, None])

        street = addr.get("street")
        suburb = addr.get("suburb")
        government_area = addr.get("government_area")
        market = addr.get("market")
        country = addr.get("country")
        country_code = addr.get("country_code")
        longitude = coordinates[0]
        latitude = coordinates[1]
        exact_location = loc.get("is_location_exact")

        address_params = (street, suburb, government_area, market, country, country_code, longitude, latitude, exact_location)
        ibm_db.execute(address_stmt, address_params)

        identity_stmt = ibm_db.exec_immediate(conn, "VALUES IDENTITY_VAL_LOCAL()")
        row = ibm_db.fetch_tuple(identity_stmt)
        address_id = row[0]

        # LISTING_INFO
        listing_id = listing["_id"]
        name = listing.get("name")
        summary = listing.get("summary")
        space = listing.get("space")
        description = listing.get("description")
        property_type = listing.get("property_type")
        room_type = listing.get("room_type")
        bed_type = listing.get("bed_type")
        accommodates = listing.get("accommodates")
        bedrooms = listing.get("bedrooms")
        beds = listing.get("beds")

        bathrooms = parse_decimal(listing.get("bathrooms"))

        number_of_reviews = listing.get("number_of_reviews")

        last_scraped = parse_date(listing.get("last_scraped"))
        calendar_last_scraped = parse_date(listing.get("calendar_last_scraped"))
        first_review = parse_date(listing.get("first_review"))
        last_review = parse_date(listing.get("last_review"))

        images = listing.get("images", {})

        thumbnail_url = images.get("thumbnail_url")
        medium_url = images.get("medium_url")
        picture_url = images.get("picture_url")
        xl_picture_url = images.get("xl_picture_url")

        host_id = listing["host"]["host_id"]

        listing_params = (listing_id,name,summary, space, description, property_type, room_type, bed_type, accommodates,
            bedrooms, beds, bathrooms, number_of_reviews, last_scraped, calendar_last_scraped, first_review,
            last_review, thumbnail_url, medium_url, picture_url, xl_picture_url, host_id, address_id)

        ibm_db.execute(listing_stmt, listing_params)

        #HOST
        host = listing.get("host")

        host_params = (
            host_id,
            host.get("host_url"),
            host.get("host_name"),
            host.get("host_location"),
            host.get("host_about"),
            host.get("host_response_time"),
            host.get("host_thumbnail_url"),
            host.get("host_picture_url"),
            host.get("host_neighbourhood"),
            host.get("host_response_rate"),
            host.get("host_is_superhost"),
            host.get("host_has_profile_pic"),
            host.get("host_identity_verified"),
            host.get("host_listings_count"),
            host.get("host_total_listings_count"),
            host.get("host_listings_count")
        )
        ibm_db.execute(host_stmt, host_params)

        #HOST_VERIFICATION
        host_verifications = host.get("host_verifications", [])

        for v in host_verifications:
            ibm_db.execute(verification_stmt, (host_id, v))


        #PRICES
        guests = int(parse_decimal(listing.get("guests_included")))

        prices_params = (
            listing_id,
            parse_decimal(listing.get("price")),
            parse_decimal(listing.get("weekly_price")),
            parse_decimal(listing.get("monthly_price")),
            parse_decimal(listing.get("cleaning_fee")),
            parse_decimal(listing.get("extra_people")),
            guests
        )
        ibm_db.execute(prices_stmt, prices_params)

        #AMENITIES
        amenitites = listing.get("amenities")

        for a in amenitites:
            ibm_db.execute(amenities_stmt, a)

        #AMENITITES_LIST
        identity_stmt_amenity = ibm_db.exec_immediate(conn, "VALUES IDENTITY_VAL_LOCAL()")
        row_amenity = ibm_db.fetch_tuple(identity_stmt_amenity)
        amenity_id = row_amenity[0]

        list_amenities_params = (
            listing_id,
            amenity_id
        )

        ibm_db.execute(list_amenities_stmt, list_amenities_params)

        # amenities_sql = """
        #     MERGE INTO airbnb.amenities t
        #     USING (VALUES (?)) s(name)
        #     ON t.name = s.name
        #     WHEN NOT MATCHED THEN
        #         INSERT (name) VALUES (s.name)
        #     """
        # amenities_stmt = ibm_db.prepare(conn, amenities_sql)
        #
        # list_amenities_sql = """
        #     MERGE INTO airbnb.list_amenities t
        #     USING (VALUES (?, ?)) s(listing_id, amenity_id)
        #     ON t.listing_id = s.listing_id AND t.amenity_id = s.amenity_id
        #     WHEN NOT MATCHED THEN
        #         INSERT (listing_id, amenity_id)
        #         VALUES (s.listing_id, s.amenity_id)
        #     """
        # list_amenities_stmt = ibm_db.prepare(conn, list_amenities_sql)


with open("airbnb document 1-6.json", encoding="utf-8") as f:
    data = json.load(f)
    transfer_json_data(data)
