# CS561-Big-Data-Management-Project1
CS561-Big-Data-Management-Project1
##Datasets:
####Customers (ID, Name, Age, CountryCode, Salary) 50,000 records
####Transactions (TransID, CustID, TransTotal, TransNumItems, TransDec) 5,000,000 records

##Query1
Write a job(s) that reports the customers whose CountryCode between 2 and 6 (inclusive).

##Query2
Write a job(s) that reports for every customer, the number of transactions that customer did and the total
sum of these transactions. The output file should have one line for each customer containing:
CustomerID, NumTransactions, TotalSum

Repeat Q2 twice, once with a map-reduce combiner and once without a combiner. In the submitted report,
compare the performance between the two cases and write down your conclusion.

##Query3
Write a job(s) that joins the Customers and Transactions datasets (based on the customer ID) and reports
for each customer the following info:
CustomerID, Name, Salary, NumOf Transactions, TotalSum, MinItems

Where NumOfTransactions is the total number of transactions done by the customer, TotalSum is the sum
of field “TransTotal” for that customer, and MinItems is the minimum number of items in transactions
done by the customer.

##Query4
Write a job(s) that reports for every country code, the number of customers having this code as well as
the min and max of TransTotal fields for the transactions done by those customers. The output file should
have one line for each country code containing:
CountryCode, NumberOfCustomers, MinTransTotal, MaxTransTotal

##Query5
Write a job(s) that reports the customer names who have the least number of transactions. Your output
should be the customer names, and the number of transactions.
