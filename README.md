# IPAAN
Internet Performance Analytics for African Networks

This is the API server using Express.js as it developed over multiple iterations. It receives requests in JSON format and queries the PostgreSQL server running locally on my machine, then returns the response to the user.

Things that must still be developed include:
+ Developing Unit tests. Preferably not using Mocha as there was difficulty in getting Mocha's current format to integrate with the current library express was using on my server.
+ Using materialized views on the PostgreSQL database will likely require implementing a new querying method.
+ Testing the performance gain using the materialised views by Unit testing. (This is bulk of the research)
+ Pipelining data from the source on bigquery into the server through this server will require additional coding.
+ Develop new research questions to solve
