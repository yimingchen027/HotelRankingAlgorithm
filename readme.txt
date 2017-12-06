I split the hotel_califonia.tsv file into 2 json files. Each contain the hotel information for a city.

Then I import these 2 json files to MongoDB, each city's hotel information is stored in a collection.

Use command 

mongoimport.exe --db hotelRanking --collection Berkeley --type json --file berkeley.json
mongoimport.exe --db hotelRanking --collection SanFrancisco --type json --file sanfrancisco.json

to import all the data into MongoDB.

My program add a final_score field to each hotel document, the initial value of final_score is -1.

Then I compute the final_score according to my ranking function.

If a hotel haven't been reviewed by anyone, its final_score will be -1, then I'll not include this hotel in final rank.

There're 27 hotels in Berkeley, only 17 of them has been reviewed. So the rank of Berkeley only have 17 hotels.