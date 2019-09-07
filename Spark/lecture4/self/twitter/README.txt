Please follow https://apps.twitter.com/app/new to create twitter credentials:
 * <comsumerKey>        - Twitter consumer key 
 * <consumerSecret>     - Twitter consumer secret
 * <accessToken>        - Twitter access token
 * <accessTokenSecret>  - Twitter access token secret

For help: https://www.slickremix.com/docs/how-to-get-api-keys-and-tokens-for-twitter/


How to run your code once assembled in JAR file

spark-submit --class <Main class name with qualified package> --packages org.apache.spark:spark-streaming-twitter_2.11:1.6.0 --master local[2] --deploy-mode client <Your jar name> <Consumer Key> <Consumer Secret> <Consumer Token> <Consumer Token Secret>

