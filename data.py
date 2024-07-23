from pyspark.sql import SparkSession

# запуск сессии Spark
spark = SparkSession.builder.getOrCreate();

# таблица с категориями
categories = spark.createDataFrame([
    {"id": 1, "category": "Fizzy"},
    {"id": 2, "category": "Dairy"},
    {"id": 3, "category": "Drinking"},
    {"id": 4, "category": "Meat"}
])

# таблица с продуктами
products = spark.createDataFrame([
    {"id": 1, "product": "Milk"},
    {"id": 2, "product": "Soda"},
    {"id": 3, "product": "Bread"}
    
])

# таблица-адаптер для связи категория-продукт
adapter = spark.createDataFrame([
    {"id_cat": 1, "id_prod": 2},
    {"id_cat": 2, "id_prod": 1},
    {"id_cat": 3, "id_prod": 1},
    {"id_cat": 3, "id_prod": 2}
   
])