---Cleaning and standardizing

---Create table and import dataset

CREATE TABLE  housing.nashville_housing (
	unique_id NUMERIC (50),
	parcel_id VARCHAR (50),
	land_use VARCHAR (50),
	property_address VARCHAR (50),
	sale_date DATE,
	sale_price VARCHAR(10),
	legal_reference VARCHAR(50),
	sold_as_vacant VARCHAR (5),
	owner_name VARCHAR(75),
	owner_address VARCHAR(50),
	acreage VARCHAR (10),
	tax_district VARCHAR(50),
	land_value NUMERIC(10),
	building_value NUMERIC(10),
	total_value NUMERIC(10),
	year_built NUMERIC(5),
	bedrooms NUMERIC(2),
	full_bath NUMERIC(2),
	half_bath NUMERIC(2)
);

SELECT * FROM housing.nashville_housing;

--Id if something went we can drop table and creat new one

DROP TABLE housing.nashville_housing;

COPY housing.nashville_housing 
FROM 'C:\Users\16469\Downloads\Nashville Housing Data for Data Cleaning.csv'
DELIMITER ','
CSV HEADER;

---Data CLeaning

--Populating property addresses based on parcelid
--If parcelid is the same we can assume that address the same aswell

SELECT *
FROM housing.nashville_housing
WHERE property_address IS NULL;

SELECT *
FROM housing.nashville_housing
ORDER BY parcel_id;

SELECT * 
FROM housing.nashville_housing n
JOIN housing.nashville_housing h
	ON n.parcel_id = h.parcel_id
	AND n.unique_id <> h.unique_id;

SELECT n.parcel_id, n.property_address, h.parcel_id, h.property_address
FROM housing.nashville_housing n
JOIN housing.nashville_housing h
	ON n.parcel_id = h.parcel_id
	AND n.unique_id <> h.unique_id
WHERE n.property_address IS NULL;

--Using COALESCE expression to fill up missing values

--Creating copy of the table 
CREATE TABLE housing.nashville_housing_2 AS
SELECT * FROM housing.nashville_housing;

SELECT * FROM housing.nashville_housing_2;

---Finding needed values to fill up NULL

SELECT n.parcel_id, n.property_address, h.parcel_id, h.property_address,
COALESCE(n.property_address, h.property_address)
FROM housing.nashville_housing_2 n
JOIN housing.nashville_housing_2 h
	ON n.parcel_id = h.parcel_id
	AND n.unique_id <> h.unique_id
WHERE n.property_address IS NULL;

--Updating Table

UPDATE housing.nashville_housing_2
SET property_address = COALESCE(housing.nashville_housing_2.property_address, h.property_address)
FROM housing.nashville_housing_2 h
WHERE housing.nashville_housing_2.property_address IS NULL
	AND housing.nashville_housing_2.parcel_id = h.parcel_id
	AND housing.nashville_housing_2.unique_id <> h.unique_id;

SELECT * FROM housing.nashville_housing_2;


---Breaking out property_address into separate columns - Address, City, State
--Using comma as delimiter

SELECT *
FROM housing.nashville_housing_2;

SELECT property_address,
SUBSTRING(property_address FROM 1 FOR POSITION(',' IN property_address)-1) AS property_street_address,
SUBSTRING(property_address FROM POSITION(',' IN property_address) + 1) AS property_city
FROM housing.nashville_housing_2;

--Creating and populationg new columns

ALTER TABLE housing.nashville_housing_2
ADD property_street_address VARCHAR (50);

UPDATE housing.nashville_housing_2
SET property_street_address = SUBSTRING(property_address FROM 1 FOR POSITION(',' IN property_address)-1);

ALTER TABLE housing.nashville_housing_2
ADD property_city_address VARCHAR (25);

UPDATE housing.nashville_housing_2
SET property_city_address = SUBSTRING(property_address FROM POSITION(',' IN property_address) + 1);

SELECT *
FROM housing.nashville_housing_2;


---Splitting the owner_address using SPLIT_PART

SELECT owner_address, SPLIT_PART(owner_address, ',', 1) AS owner_street_address,
SPLIT_PART(owner_address, ',', 2) AS owner_city_address,
SPLIT_PART(owner_address, ',', 3) AS owner_state_address
FROM housing.nashville_housing_2;

--Creating and populationg new columns

ALTER TABLE housing.nashville_housing_2
ADD owner_street_address VARCHAR (50);

UPDATE housing.nashville_housing_2
SET owner_street_address = SPLIT_PART(owner_address, ',', 1);

ALTER TABLE housing.nashville_housing_2
ADD owner_city_address VARCHAR (25);

UPDATE housing.nashville_housing_2
SET owner_city_address = SPLIT_PART(owner_address, ',', 2);

ALTER TABLE housing.nashville_housing_2
ADD owner_state_address VARCHAR (25);

UPDATE housing.nashville_housing_2
SET owner_state_address = SPLIT_PART(owner_address, ',', 3);

SELECT *
FROM housing.nashville_housing_2;


--- Changing 'Y' and 'N' to "Yes" and "No" in sold_as_vacant column

SELECT DISTINCT sold_as_vacant, COUNT (sold_as_vacant)
FROM housing.nashville_housing_2
GROUP BY sold_as_vacant
ORDER BY COUNT (sold_as_vacant);

SELECT sold_as_vacant,
CASE 
	WHEN sold_as_vacant = 'Y' THEN 'Yes'
	WHEN sold_as_vacant = 'N' THEN 'No'
	ELSE sold_as_vacant
END
FROM housing.nashville_housing_2;

UPDATE housing.nashville_housing_2
SET sold_as_vacant =
CASE 
	WHEN sold_as_vacant = 'Y' THEN 'Yes'
	WHEN sold_as_vacant = 'N' THEN 'No'
	ELSE sold_as_vacant
END;

---Removing Duplictes

--Creating copy of the table 
CREATE TABLE housing.nashville_housing_3 AS
SELECT * FROM housing.nashville_housing_2;

SELECT *
FROM housing.nashville_housing_3;

--Using ROW_NUMBER window function to identify duplicates

'''
--to check duplicated by unique_id will not work properly
WITH row_number_cte AS (SELECT *,
ROW_NUMBER() OVER(PARTITION BY unique_id) AS row_number
FROM housing.nashville_housing_3)

SELECT *
FROM row_number_cte
WHERE row_number > 1;'''


SELECT *,
	ROW_NUMBER() OVER (
	PARTITION BY parcel_id,
		property_address,
		sale_price,
		sale_date,
		legal_reference
	ORDER BY unique_id) as row_number
FROM housing.nashville_housing_3
ORDER BY parcel_id;

-- Creating CTE and diltering rows with row number more then 1

WITH row_number_cte AS (SELECT *,
	ROW_NUMBER() OVER (
	PARTITION BY parcel_id,
		property_address,
		sale_price,
		sale_date,
		legal_reference
	ORDER BY unique_id) as row_number
FROM housing.nashville_housing_3)

SELECT *
FROM row_number_cte
WHERE row_number > 1
ORDER BY property_address;

--Checkin how previous query is working

SELECT *
FROM housing.nashville_housing_3
WHERE parcel_id = '107 14 0 157.00';

--Deleting duplicates

WITH row_number_cte AS (
	SELECT unique_id
	FROM (
		SELECT unique_id,
		ROW_NUMBER() OVER (
		PARTITION BY parcel_id,
			property_address,
			sale_price,
			sale_date,
			legal_reference
		ORDER BY unique_id) as row_number
	FROM housing.nashville_housing_3) s
	WHERE row_number > 1
	)
DELETE FROM  housing.nashville_housing_clean AS (
	SELECT unique_id, paarcel_id, land_use,
	_3
WHERE unique_id IN (SELECT * FROM row_number_cte);

SELECT *
FROM housing.nashville_housing_3;

---Creatimg view while omitting unused columns
	
CREATE VIEW housing.nashville_housing_clean_view AS (
	SELECT unique_id, parcel_id, land_use,
	property_street_address, property_city_address,
	sale_date, sale_price, legal_reference, sold_as_vacant, 
	owner_name, owner_street_address, owner_city_address, owner_state_address,
	acreage, land_value, building_value, year_built, bedrooms,
	full_bath, half_bath
	FROM housing.nashville_housing_3);

SELECT * FROM housing.nashville_housing_clean_view;



