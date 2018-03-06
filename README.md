# TP1 – Spark


## Chargement du fichier csv
```
scala> df = spark.read.format("csv").option("header", "true").load("/tmp/data_brut/train.csv")

```

## Vérification du typage implicite
```
scala> :type df
org.apache.spark.sql.DataFrame

```

## Affichage du schéma du DataFrame

```

scala> df
res2: org.apache.spark.sql.DataFrame = [PassengerId: string, Survived: string ... 10 more fields]

```

## Affichage de données (Action)

```
scala> df.show()

+-----------+--------+------+--------------------+------+----+-----+-----+----------------+-------+-----+--------+
|PassengerId|Survived|Pclass|                Name|   Sex| Age|SibSp|Parch|          Ticket|   Fare|Cabin|Embarked|
+-----------+--------+------+--------------------+------+----+-----+-----+----------------+-------+-----+--------+
|          1|       0|     3|Braund, Mr. Owen ...|  male|  22|    1|    0|       A/5 21171|   7.25| null|       S|
|          2|       1|     1|Cumings, Mrs. Joh...|female|  38|    1|    0|        PC 17599|71.2833|  C85|       C|
|          3|       1|     3|Heikkinen, Miss. ...|female|  26|    0|    0|STON/O2. 3101282|  7.925| null|       S|
|          4|       1|     1|Futrelle, Mrs. Ja...|female|  35|    1|    0|          113803|   53.1| C123|       S|
|          5|       0|     3|Allen, Mr. Willia...|  male|  35|    0|    0|          373450|   8.05| null|       S|
|          6|       0|     3|    Moran, Mr. James|  male|null|    0|    0|          330877| 8.4583| null|       Q|
|          7|       0|     1|McCarthy, Mr. Tim...|  male|  54|    0|    0|           17463|51.8625|  E46|       S|
|          8|       0|     3|Palsson, Master. ...|  male|   2|    3|    1|          349909| 21.075| null|       S|
|          9|       1|     3|Johnson, Mrs. Osc...|female|  27|    0|    2|          347742|11.1333| null|       S|
|         10|       1|     2|Nasser, Mrs. Nich...|female|  14|    1|    0|          237736|30.0708| null|       C|
|         11|       1|     3|Sandstrom, Miss. ...|female|   4|    1|    1|         PP 9549|   16.7|   G6|       S|
|         12|       1|     1|Bonnell, Miss. El...|female|  58|    0|    0|          113783|  26.55| C103|       S|
|         13|       0|     3|Saundercock, Mr. ...|  male|  20|    0|    0|       A/5. 2151|   8.05| null|       S|
|         14|       0|     3|Andersson, Mr. An...|  male|  39|    1|    5|          347082| 31.275| null|       S|
|         15|       0|     3|Vestrom, Miss. Hu...|female|  14|    0|    0|          350406| 7.8542| null|       S|
|         16|       1|     2|Hewlett, Mrs. (Ma...|female|  55|    0|    0|          248706|     16| null|       S|
|         17|       0|     3|Rice, Master. Eugene|  male|   2|    4|    1|          382652| 29.125| null|       Q|
|         18|       1|     2|Williams, Mr. Cha...|  male|null|    0|    0|          244373|     13| null|       S|
|         19|       0|     3|Vander Planke, Mr...|female|  31|    1|    0|          345763|     18| null|       S|
|         20|       1|     3|Masselmani, Mrs. ...|female|null|    0|    0|            2649|  7.225| null|       C|
+-----------+--------+------+--------------------+------+----+-----+-----+----------------+-------+-----+--------+
only showing top 20 rows

```

## Les requêtes
* Total de voyageurs par sex

```
scala> df.select($"passengerid", $"sex").groupBy("sex").count().show()
+------+-----+
|   sex|count|
+------+-----+
|female|  314|
|  male|  577|
+------+-----+

```

* Les survivants par sex
```
scala> df.createOrReplaceTempView("survived")

scala> val sqlDF = spark.sql("SELECT sex, COUNT(passengerid) as Survived_Number FROM survived WHERE survived = 1 GROUP BY sex")
sqlDF: org.apache.spark.sql.DataFrame = [sex: string, Survived_Number: bigint]

scala> sqlDF.show()
+------+---------------+
|   sex|Survived_Number|
+------+---------------+
|female|            233|
|  male|            109|
+------+---------------+
```

* Les morts par classe
```
scala>  df.filter($"survived" === 0).select($"passengerid", $"pclass", $"survived")
.groupBy($"pclass").agg(count($"passengerid").alias("Nombre_morts")).show
+------+------------+
|pclass|Nombre_morts|
+------+------------+
|     3|         372|
|     1|          80|
|     2|          97|
+------+------------+
```

* Les morts dont l'age est supérieur à 50

```
scala> df.filter($"age" > 50).agg(count($"passengerid").alias("Nombre_morts_sup_50")).show
+-------------------+
|Nombre_morts_sup_50|
+-------------------+
|                 64|
+-------------------+
```
* Les IDs des passagers survivants par jointure

```
scala> val query = spark.sql("SELECT train.passengerid  FROM train, train2  WHERE train.passengerid = train2.passengerid AND train2.survived = 1")
query: org.apache.spark.sql.DataFrame = [passengerid: string]

scala> query.show
+-----------+
|passengerid|
+-----------+
|          2|
|          3|
|          4|
|          9|
|         10|
|         11|
|         12|
|         16|
|         18|
|         20|
|         22|
|         23|
|         24|
|         26|
|         29|
|         32|
|         33|
|         37|
|         40|
|         44|
+-----------+
only showing top 20 rows
```
