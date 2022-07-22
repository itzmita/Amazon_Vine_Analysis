# Amazon_Vine_Analysis
learning pySpark ETL
## Overview:
### Background
This project analyzes Amazon reviews written by members of the paid Amazon Vine program. The Amazon Vine program is a service that allows manufacturers and publishers to receive reviews for their products. Companies like SellBy pay a small fee to Amazon and provide products to Amazon Vine members, who are then required to publish a review.

### Resources:

• Data Source: [Amazon Review datasets](https://s3.amazonaws.com/amazon-reviews-pds/tsv/index.txt), [Mobile Electronics Review dataset](https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Mobile_Electronics_v1_00.tsv.gz)

• Software: Google Colab Notebook, PostgreSQL, pgAdmin 4, AWS RDS, AWS S3


We chose one of the datasets from the given list of 50 which contains reviews of a specific product. In this scenario, the url chosen was [Mobile Electronics Review dataset](https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Mobile_Electronics_v1_00.tsv.gz).  We used PySpark to perform the ETL process to extract the dataset, transform the data, connect to an AWS RDS instance, and load the transformed data into pgAdmin. Next, we used PySpark to determine if there is any bias toward favorable reviews from Vine members in your dataset. 


## Results:  
### Deliverable 1: Perform ETL on Amazon Product Reviews
• The dataset that was used for this deliverable is provided below

[Mobile Electronics Review dataset](https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Mobile_Electronics_v1_00.tsv.gz)

• The above dataset was pulled in a DataFrame and then transformed to create data for customers_table, products_table, review_id_table, and vine_table.
• The customers_table dataframe looks like this

![image](https://user-images.githubusercontent.com/3753839/180349860-3d4eb686-7a1e-4f7e-b370-a8544cc7990b.png)


• The products_table dataframe is as shown below

![image](https://user-images.githubusercontent.com/3753839/180349870-48516fa3-2a90-4a9a-8730-c9e100c2b4c2.png)


• The review_id table dataframe looks like this

![image](https://user-images.githubusercontent.com/3753839/180349894-fa8489f2-4172-41bd-bb5b-703570d6581b.png)


• The vine table dataframe is as shown below

![image](https://user-images.githubusercontent.com/3753839/180349915-e5e8ee17-4882-4d50-ba5f-03cc34c80537.png)


• Next we made a connected to the RDS database through pgAdmin and the above dataframes were loaded in respective tables.


### Deliverable 2: Determine Bias of Vine Reviews 
This deliverable was done using PySpark.

• The dataset that was analyzed was the same as in Deliverable1.
• We recreated the vine dataframe

![image](https://user-images.githubusercontent.com/3753839/180349941-9d062a79-e3f1-406f-b4f6-402531a477c8.png)


• Then the dataframe was filtered to show rows which has total_votes > 20

![image](https://user-images.githubusercontent.com/3753839/180349954-6df28ac2-1a76-43ce-901f-bace510607e3.png)


• Next from the above created dataframe we retrieved rows where the percentage of helpful_votes is equal to or greater than 50% 

![image](https://user-images.githubusercontent.com/3753839/180349968-562b5533-15f2-4311-99b1-7bbb6aff4230.png)


• Next we filtered rows that had a vine review

![image](https://user-images.githubusercontent.com/3753839/180349979-6bc4045e-51b4-41be-9185-5b148d01bc45.png)


• Next we showed rows that do not have a vine review

![image](https://user-images.githubusercontent.com/3753839/180349994-05c3d42f-34f6-4cc7-8e4e-ea6bf238087e.png)


• In this dataset we had 4 vine reviews as shown below

![image](https://user-images.githubusercontent.com/3753839/180350016-cfd65ad3-6090-4573-a09b-52910d203760.png)

And 1064 non-vine reviews

![image](https://user-images.githubusercontent.com/3753839/180350022-90d35ebf-a900-412d-ac74-d1cff236818c.png)


• This dataset had only 1 vine review and 527 non-vine reviews which are 5 star ratings.
![image](https://user-images.githubusercontent.com/3753839/180350056-3f781ca5-40e2-4ee4-8b23-c80c99942ba0.png)

![image](https://user-images.githubusercontent.com/3753839/180350077-ab58f227-fd82-4bd5-9ea1-887ea8e75718.png)


• 25% of vine reviews and approx 50% of non-vine reviews were 5 star ratings.
![image](https://user-images.githubusercontent.com/3753839/180350098-e052344d-41ce-4e95-84ec-393e963d2656.png)

![image](https://user-images.githubusercontent.com/3753839/180350104-05c8d7d4-0b84-4b10-9f3d-e38ab9f6c21d.png)



## Summary:
From this particular dataset I couldn’t find any bias in ratings. In fact the non-vine reviews are more positive here.
One additional criteria we can use is the verified_purchase column. From the 4 rows that we got vine reviews none of them were verified purchases.
We may check this column to see if we are looking at a random review or from someone who has purchased a product. 
