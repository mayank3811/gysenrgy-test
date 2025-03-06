# gysenrgy-test

This repo contains the spark codes used by Mayank Sharma to solve the GSynergy Data Engineer Interview Challenge.

the repo contains 1 er diagram which was used to create the relations between the dim and fact tables.

the quality check is the spark code used to solve the part where after creating the ER diagram, I had to perform some checks.

The load data contains spark code used to infer schema and write the tables to DWH i.e. bigquery in my case.

following the loading of data I had to create a table called mview_weekly_sales, which I directly created in bigquery as the data was small. the query I used is in the file mview_weekly_sales.

Bonus: I have created a logical transformation for incremental load. this can run in bigquery and create incrental load of data by checking the data that is already present and only insert the new data.
