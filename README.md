# 1. look in "ae" package ( "assessed exercise" )
# 2. use "WordCount_Main"
# 3. "Test" is for test ...obviously

My Assumption
1. Each Main_Link can be split from using “whitespace and tab” as delimiter only. Other symbol is counted as part of the article_title
2. for x=1 will calculate PageRank 1 iteration after initial score 1
3. If the text format is read from right to left, the output will be from right to left
4. Article dont have ">" at the first character. We use it to specify the main_link line
5. The article can have outlink to itself, creating self loop.
6. Output will be separated by tab



Below is my logic
VVVVVVVVVVVVVVVVVVVVVVV

We separate our work into 3 jobs.

Job1 
Mapper read each article from the input separated by "\n\n" then write only “Article_title” + “Revision_id”+ “Main_Links”
Reducer find the line with newest “Revision_id” then remove duplicate from “Main_Links” 
Score_of_Article is 1
Output is “Article_title ,    Score_of_Article ,   Main_Links”

Job2
This job will calculate PageRank and will do for each iteration 

Mapper read each line then produce 2 types of output
1. put each Main_Links as key (U) and the value is [ Article_title (V), Score_of _Article (PR(v)), number_of_article_from_v (L(v)) ]
2. send another line as Article_title + “  > “ + Main_Links   

Reducer calculate new PageRank score with the same key (U)
Use the Main_Links from line that have “ > “ symbol to append to the result 
Output is  “Article_title ,    Score_of_Article ,   Main_Links”
This output is the same as input, thus it can be used for next iteration

Job3
Cut off the Main_Link to produce only “Article_title , Score_of_Article”


I hope you can understand what I described above.

─────────▄▄───────────────────▄▄──
──────────▀█───────────────────▀█─
──────────▄█───────────────────▄█─
──█████████▀───────────█████████▀─
───▄██████▄─────────────▄██████▄──
─▄██▀────▀██▄─────────▄██▀────▀██▄
─██────────██─────────██────────██
─██───██───██─────────██───██───██
─██────────██─────────██────────██
──██▄────▄██───────────██▄────▄██─
───▀██████▀─────────────▀██████▀──
──────────────────────────────────
──────────────────────────────────
──────────────────────────────────
───────────█████████████──────────
──────────────────────────────────
──────────────────────────────────
