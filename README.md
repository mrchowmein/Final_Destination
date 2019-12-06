# Final Destination Analytics

Link to the presentation: [Link](https://docs.google.com/presentation/d/1VzdHcnHTR7vvGU0Y7kmBNHEqORchQm-yhPloawFJiUo/edit?usp=sharing)

Tableau Demo: [Link](
https://public.tableau.com/views/FinalDestinationAug-Sept2018/Dashboard1?:embed=y&:display_count=yes&publish=yes&:origin=viz_share_link)


# Overview
Are you a city planner or a mobility operation? Ever wonder where people commute to from a part of a city? Ever wonder how they commuted?  Ever wonder what type of transportation mode they used? Wished all this information is in one place and can be viewed simultanously? Then Final Destination might be the pipeline you might be interested in. Final Destination is an alternative transportation analytics pipeline that aggregate historical trip data then shows you where to, when and how people commute in New York City from any spot within New York City.     

# How to Use 
To use, simply just pick a starting location (zip code) and the date and time information.  The Tableau map interface will show you all the destinations people from the starting location traveled to. 

The color of the destination indicate the number of travelers to that said location. Intersted in more analytics about the people going to that destination, roll your mouse over the destination and see the break down of how people traveled to that said location. Information currently supported is mode breakdown, subscriber ratio, credit card payment ratio, distance, time and whether a cab was hailed.

# How to Run
To run, use the run.sh script. You will need to pass a Postgres DB user name and pwd. This will run the Combined_Processing.scala script. This Spark program will create a table that joins all 3 dataset.  Running each of the other scala scripts for each dataset is not needed, unless one wants to create seperate tables in the Postgres DB.


# Implementation
![alt text](https://raw.githubusercontent.com/mrchowmein/Final_Destination/master/images/Screen%20Shot%202019-10-08%20at%202.02.27%20PM.png)

# Input Datasets
New York City Taxi and Limousine Green Taxi Data: ~9gb 

New York City Taxi and Limousine Yellow Taxi Data: ~230gb

New York City Motivate/Lyft CitiBike: ~18gb

