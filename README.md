Entity Resolution or How I Learned to work with Pandas 🐼

Problem:
   You are handed a huge data set, that contains company records imported from multiple systems, leading to duplicate entries with slight variations.
   To exemplify a bit, we might have something like: company_name: Adidas vs company_name: Adidas group 

The hypothesis: 
   You can group similar entities by looking at their similarities like: company name, website domain, phone number, etc. 
   If it looks like Adidas 👟, smells like Adidas 👟, it probably is  Adidas 👟.

Steps:
   Extracting the domain - either from the website url or from the domain url column
   Normalizing the company names - to increase the chances of finding similarities we had to strip the company names of some symbols and some other specifics like .inc, group, & co, etc.
   Compare and match - tried to match first by the domain, then used fuzzy matching for the company name, company category and short description
   
What worked well: 
   Domain extraction cleaned up a huge chunk of duplicates
   String normalization removed lots of edge cases
   Fuzzy logic filled in the blanck nicely. 

What can be improved:
   Using fuzzy logic for huge data sets seems slow and one way in which the performance could be improved is by working on small chunks of data. 
   To avoid false positives Adidas - Adibas  we should perhaps compare more fields, like company name with short description and long description, 
   company category with other columns related to the company's activities, etc/ 

Final Thoughts:
   Handleing large amounts of data is challenging. 
   Working on this project taught me a lot about cleaning and processing data and made me want to go deeper into the world of Pandas. 
