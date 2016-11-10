
# Databases Assignment 3

**Author:** Michael Seaman

**Due date:** 2016/11/10?


## The Rail Co. Database:
This application was created to help organize the constant purchases that the great
company Rail Co. TM gets at their many locations. The importing application imports
text from a csv file containing transaction history over the course of 2010 - 2016.
The transaction history contains info regarding what store, which item, and which
customer purchased what, but with tons of duplicate values. This application int-
elligently sifts through the lines, and inputs into a database what isn't already
there.

### Schema:

Customers ->  `railCoCustomers( customer_id INT UNSIGNED PRIMARY KEY NOT NULL AUTO_INCREMENT, first_name VARCHAR(25), last_name VARCHAR(25) )`

Customers' Phones -> `railCoCustomerPhones(phone_id INT UNSIGNED PRIMARY KEY NOT NULL AUTO_INCREMENT, customer_id INT UNSIGNED, phone_number VARCHAR(30), type VARCHAR(4), FOREIGN KEY (customer_id) REFERENCES railCoCustomers(customer_id))`

Stores -> `railCoStores(store_id INT UNSIGNED PRIMARY KEY NOT NULL AUTO_INCREMENT, street_address VARCHAR(50), zipcode INT UNSIGNED, city VARCHAR(20), state VARCHAR(2))`

Items -> `railCoItems(item_id INT UNSIGNED PRIMARY KEY NOT NULL, item_name VARCHAR(50), item_manf VARCHAR(50), price DECIMAL(8,2))`

Transactions -> `railCoTransactions(transaction_id INT UNSIGNED PRIMARY KEY NOT NULL AUTO_INCREMENT, item_id INT UNSIGNED, customer_id INT UNSIGNED, store_id INT UNSIGNED, timestamp DATETIME, FOREIGN KEY (item_id) REFERENCES railCoItems(item_id), FOREIGN KEY (customer_id) REFERENCES railCoCustomers(customer_id), FOREIGN KEY (store_id) REFERENCES railCoStores(store_id))`

Put simply - All Tables have PK's, but only the phone and transaction relation have FK
pointing to Customer and Customer+Item+Stores respectively.

---
## Usage


To create data:

```

python DataGeneration.py <#lines>

```
Data is automatically stored to 'salesData.csv'

To run the importing application:

```
export CLASSPATH=/PATH_TO_JDBC_DRIVER/mysql-connector-java-5.1.39:$CLASSPATH

javac *.java
java DataImportApplications

```


Using this java class is contingent on having the jdbc driver for interfacing with the
sql server. Before running the application, it is necessary to create a `CLASSPATH`
environment variable that stores the path to the driver. If not found, the application
will not start.



###Overview:
Now that you are familiar with designing a normalized database schema, your assignment
is to create a normalized database schema (at least 5 tables), a data generation tool and an
application to import the data.
Develop the following Applications/Database.
1. Create a normalized database schema. This database should consist of least 5 tables in
3NF.
2. Once you have completed your database schema, write a utility/tool that generates
data (not normalized) and exports it to a csv text file. This data that will be used to
populate the tables in your database. You may develop your application in any
language (I would recommend C#, python, Java to keep it consistent but not
required). Your data generation tool should take as a command line parameter the file
name to be created and the number tuples to be generated.
a. There are plenty of data generation libraries available, but a popular one is
Faker for php. This will require a learning curve if you don’t know php, but
there are others similar to Faker for python and Java. Having said that, it is up
to you how you will generate your data.
3. Last but not least, develop an application (Java) that imports your unstructured data
form your csv text file into your normalized database.

###Grading:
Your program will be evaluated for correctness and elegance. Make sure you code
includes methods and good naming conventions. Use all the good practice methods you
have learned throughout the semester.


## Honor Pledge

I pledge that all the work in this repository is my own!


Signed,

Michael Seaman
