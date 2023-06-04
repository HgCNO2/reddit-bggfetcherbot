# reddit-bggfetcherbot
u/BGGFetcherBot is designed to reply to comments in r/boardgames and r/soloboardgaming if the comment includes game names in double brackets (or bold with a `!fetch` call in the comment body) with links to the game on BoardGameGeek.com.

## Changelog
### v2.0.0
Adding support for `!fetch` with bold game names instead of double brackets.

Adding support for calling the bot with a year range instead of just modifiers. (Issue #13)

Game query must be 200 characters or less.

Upgrading from TheFuzz to RapidFuzz for fuzzy matching.

Fuzzy matching both with `JaroWinkler` and `ratio` then going with the winner. (Issue #14)

Drop duplicate game IDs from the database. Apparently the boardgameexpansion links are all canonical to the boardgame equivalent.
### v1.0.1
Fixing call for game link using the index loc instead of the game name, to ensure the year and URL are specifically from the closest match. (Issue #14)
### v1.0.0
Bot now able to parse year and modifiers added to call (Issue #7):

	[[Everdell|2020]] will look for Everdell with release years of 2020.
	[[Everdell|2020+]] will look for Everdell with release years of 2020 or later.
	[[Everdell|2020-]] will look for Everdell with release years of 2020 or earlier.
Bot being added to r/soloboardgaming

Ordered data by game_year in descending order to improve exact matching to most recent publish date.
### v0.5.0
Unescaped the spaces in the query for possible matches (Issue #9)

Corrected replacement with new escape logic on line 64 (Issue #9)

Separated subreddits into separate variable to more easily read for future subreddit additions. 

Compile game finder regex on initial load before loop (Issue #8)

Created and implemented find_closest_match function for better repeatability and future work.
### v0.4.0
Stripped extra whitespace from game_name to attempt a better match (Issue #1)

Added regex to catch Fancy Pants Editor bracket escapes (Issue #2)

Added support for bold inside and outside brackets (Issue #3)

Adding game match and year to response (Issue #4)

Added escapes to all special regex characters to avoid catastrophic backtracking (Issue #5)
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