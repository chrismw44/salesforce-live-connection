# Salesforce live connection

This is a program that gathers and transforms data from the Salesforce graphql API for certain objects, every time there is a change data capture event from Salesforce on these objects.
Once the data is transformed in a pandas data frame, it is then published to a data source on Tableau Server to be accessed for building a Tableau dashboard. This way, the dashboard will always be showing data that is almost live.