# reddit-bggfetcherbot
u/BGGFetcherBot is designed to reply to comments in r/boardgames if the comment includes game names in double brackets with links to the game on BoardGameGeek.com

## Changelog
### v0.4.0
Stripped extra whitespace from game_name to attempt a better match (Issue #1)

Added regex to catch Fancy Pants Editor bracket escapes (Issue #2)

Added support for bold inside and outside brackets (Issue #3)

Adding game match and year to response (Issue #4)

Added escapes to all special regex characters to avoid catastrophic backtracking

### v0.3.1
Added r/BGGFetcherBot to subreddit listener for testing.

### v0.3.0
Added error logging to an external file.

Added a datetime check to ensure the database being queried stays fresh every 7 days. 

### v0.2.0
Added pre-filtering before fuzzy matching to attempt to find verbatim matches earlier to ensure higher quality matches.

### v0.1.1
Correcting game names that might be in bold.

### v0.1.0
Initial commit of the reddit bot