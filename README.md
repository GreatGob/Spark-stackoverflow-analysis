## Analyze Stackoverflow User Behavior and Habits

### Source Data
- [Data Source](https://drive.google.com/drive/folders/1uq4TNKlSE-a_UuVSUSidartcUtRGmS30)

### Authentication Database
```
mongosh -u <username> -p <password> --authenticationDatabase <name>
```

### Import Data File to MongoDB
```
mongoimport --host <host> -u <username> -p <password> --authenticationDatabase <name> --type csv -d <database_name> -c <collection_name> --headerline --drop filename.csv
```

### Run Spark
```
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:<version> <filename>.py
```
