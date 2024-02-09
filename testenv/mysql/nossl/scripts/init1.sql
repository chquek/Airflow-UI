use sandy ;

-- CREATE TABLE
DROP TABLE IF EXISTS movies;
CREATE TABLE movies (
	    id integer PRIMARY KEY,
	    title VARCHAR(250) NOT NULL,
	    release_year SMALLINT,
	    genre char(30),
	    price decimal(4, 2)
);

-- LOAD DATAS
INSERT INTO movies(id, title, release_year, genre, price)
VALUES
    (1, 'The Shaw shank Redemption', 1994, 'HORROR', 15.99),
    (2, 'Ant Man', 2019, 'ADVENTURE', 15.00),
    (3, 'Fallen', 1996, 'HORROR', 23.99),
    (4, 'The barbershop', 2006, 'COMEDY', 6.50),
    (5, 'The last dance', 2021, 'SPORTS', 55.99),
    (6, 'Peter Pan', 2004, 'ADVENTURE', 15.99),
    (7, 'Fast & Furious 7', 2018, 'ACTION', 36.00),
    (8, 'Harry Potter', 2000, 'ACTION', 26.50),
    (9, 'Jungle book', 2004, 'ADVENTURE', 25.00);

-- CREATE TABLE
DROP TABLE IF EXISTS newemployee ;
CREATE TABLE newemployee  (
	empno 	char(40) , 
	firstname 	char(80), 
	lastname 	char(80), 
	hiredate 	date ,
	birthdate	date
);
