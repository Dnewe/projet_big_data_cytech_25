
CREATE TABLE Dim_Borough (
     borough_id SERIAL PRIMARY KEY,
     borough_name VARCHAR(100)
);

CREATE TABLE Dim_Location (
      location_id INT PRIMARY KEY,
      zone_name VARCHAR(100),
      service_zone VARCHAR(50),

      borough_id INT REFERENCES Dim_Borough(borough_id)
);



CREATE TABLE Dim_Payment (
     payment_id INT PRIMARY KEY,
     payment_type VARCHAR(50)
);

CREATE TABLE Dim_Time (
  full_datetime TIMESTAMP PRIMARY KEY ,
  hour INT,
  day INT,
  month INT,
  year INT,
  quarter INT
);
CREATE TABLE Dim_RateCode (
                              RatecodeID INT PRIMARY KEY,
    description VARCHAR(100)
);


CREATE TABLE Vendor(
    vendorID INT PRIMARY KEY ,
    vendor_name VARCHAR(50)
);

CREATE TABLE Fact_Trips (
    id SERIAL PRIMARY KEY,

    passenger_count INT,
    trip_distance DOUBLE PRECISION,
    store_and_fwd_flag VARCHAR(1),

    fare_amount DOUBLE PRECISION,
    extra DOUBLE PRECISION,
    mta_tax DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    congestion_surcharge DOUBLE PRECISION,
    Airport_fee DOUBLE PRECISION,


    payment_type INT REFERENCES Dim_Payment(payment_id),
    PULocationID INT REFERENCES Dim_Location(location_id),
    DOLocationID INT REFERENCES Dim_Location(location_id),
    tpep_pickup_datetime TIMESTAMP REFERENCES Dim_Time(full_datetime),
    tpep_dropoff_datetime TIMESTAMP REFERENCES Dim_Time(full_datetime),
    RatecodeID INT REFERENCES Dim_RateCode(RatecodeID),
    VendorID INT REFERENCES Vendor(vendorID)
);


