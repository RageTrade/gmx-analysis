# GMX Order Flow Analysis

In this repo, we analyze perpetuals trade data on GMX since inception in September 2021. 
We prove that the majority of volume is arbitrage and share the arb strategy details. The 
majority of code is in `gmx_analysis.ipynb`.


### Primary Challenge:

Because Arbiscan timestamps do not accurately reflect the actual transaction times (error is ±2 min),
detecting DEX-CEX arbitrage on Arbitrum is more difficult than on L1s. To determine which 
accounts are systematically arbitraging GMX's oracle price, we take a statistical approach and 
provide three methods for detecting arbitrage accounts. 

*Note: We define statistical significance as correctly classifying an arb bot with p < 0.05. 
Because we use three independent tests, each test has a significance threshold of p < ~0.017.*

### Method 1: Does the account have outsized expected arb profits?

Non-arbitrage account trade times do not relate to the difference between the GMX price 
and the Binance price. Thus, if an account sees outsized estimated arb profits based on their 
trade times and trade sizes, we can classify them as arbitrageurs. 

To quantify "outsized" profits on a per account basis, we define the null distribution as the 
`price_edge` (magnitude of the favorable price discrepancy) for trades that decrease position size
sampled on a per account basis.

This test works exceptionally well and we can see that the actual profits are much higher
than can be expected from a random sample of the null distribution. For 25 out of the top 100 
accounts by volume, we reject the null with p < 0.017.

![significance test](./graphs/test1_arb.png?raw=true "Test 1")

### Method 2: Oracle price differences between entering and exiting positions

Many bots run a strategy where they enter positions when the oracle price favors them and 
exit as soon as the price converges. Thus, on average, their `price_edge` is much higher 
when putting on positions than taking them off.

While not all arb bots found by Method 1 share this trait, any account that trades at a different
oracle price edge when it's entering vs exiting trades must be aware of the edge. We can see
stark difference for many found via the first method.

![test 2 arb](./graphs/test2_arb.png?raw=true "Test 2 Arb")

We classify 6 additional arbitrage accounts with this method.

![test 2 arb distributions](./graphs/decrease_median_distributions_arb.png?raw=true "Test 2 Distributions")

`0x3f6` and `0x914` look slightly borderline as they made so few trades, but we can still reject 
their null distributions with significance p < 0.017.

### Method 3: Trade Timing

Accounts that have a disproportionate number of trades at the exact same timestamps as accounts 
classified as arb via Method 1 and 2, must be using the same selection criteria for trade times.

To set up this test, we create a set of every single trade timestamp from previously identified
arb accounts. We consider these timestamps "tainted" by arb. If an account's trades share
a higher percentage of tainted timestamps than could be explained by randomness, the account
must be aware of the same trading signal (ie the oracle `price_edge`).

We define the timing null distribution as trade timestamps sampled from all accounts not
classified as arb via Method 1 or 2. We classify an additional 16 accounts as arbitrage using
this method.

### Results Overview

We define "Confirmed Arb Bots" as accounts that qualify as arbitrage under at least one of the
three statistical tests outlined above. Of the top 100 accounts by volume (collectively 
representing 82.8% of volume), we classify 48 accounts as arbitrage bots, making up 
about 68.5% of the volume from the top 100 accounts.

![top arb accounts](./graphs/top_accounts_volume.png?raw=true "Top Arb Accounts")

Looking at the Top 100 accounts with Log Volume scaling.

![top 100 accounts](./graphs/top_100_volume.png?raw=true "Top 100 Accounts")

The top accounts appear dominated by arbitrage. Note that while confirmed arbitrage bots had 
a median number of trades of 424, the "Retail" accounts only had a median of 45 trades. 
Thus, many non-arb accounts just don't have enough trade history to prove or disprove arbitrage.

![trade distribution](./graphs/volume_pie_chart.png?raw=true "Trade Distribution")

Since inception, arb bots have extracted an estimated $10M in arbitrage profits. They have,
however, also paid a fair amount of fees.

![arb fees](./graphs/fees_collected.png?raw=true "Arbitrage Fees")

Based on our classifications of arb and retail accounts, we checked whether GMX was getting 
exponential growth among retail traders. Looking at logarithmic weekly volumes since 
January 1st, there have been some steady increases.

![arb vs retail](./graphs/arb_vs_retail_volume.png?raw=true "Arbitrage vs Retail")

GMX offers the most liquid slippage-free perpetuals on Ethereum. Unlike some other perp
exchanges, we believe GMX has real retail users and the LP fees collected have been 
impressive. We hope this analysis helps you better understand the scope of arbitrage 
within slippage-free perpetual exchanges.