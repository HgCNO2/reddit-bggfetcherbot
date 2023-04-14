# reddit-bggfetcherbot
u/BGGFetcherBot is designed to reply to comments in r/boardgames if the comment includes game names in double brackets with links to the game on BoardGameGeek.com

## Changelog
### v0.3.0
Added error logging to an external file.

Added a datetime check to ensure the database being queried stays fresh every 7 days. 

### v0.2.0
Added pre-filtering before fuzzy matching to attempt to find verbatim matches earlier to ensure higher quality matches.

### v0.1.1
Correcting game names that might be in bold.

### v0.1.0
Initial commit of the reddit bot