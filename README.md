## Analyze Stack Overflow user behavior and habits

- Source data:
    https://drive.google.com/drive/folders/1uq4TNKlSE-a_UuVSUSidartcUtRGmS30

- Authentication Database:
    mongosh -u admin -p admin --authenticationDatabase admin

- Import data file to MongoDB:
    mongoimport --host localhost:27017 -u admin -p admin --authenticationDatabase admin --type csv -d stack_overflow -c questions --headerline --drop Questions.csv
    mongoimport --host localhost:27017 -u admin -p admin --authenticationDatabase admin --type csv -d stack_overflow -c answers --headerline --drop Answers.csv

- Run Spark:
    spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.0 main.py

- Install venv:
    virtualenv stackoverflow_venv
    source stackoverflow_venv/bin/activate
    pip install -r requirements/local.txt
