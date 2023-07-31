from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, to_date, lit, when
from pyspark.sql import functions as f
'''Create  the session'''
spark = SparkSession.builder.appName("Dataframe Reporting").getOrCreate()
datasets_path = './data/'

df_full = spark.read.csv(datasets_path + 'output_csv_full.csv', header=True, inferSchema=True)
df_country = spark.read.csv(datasets_path + 'country_classification.csv', header=True, inferSchema=True)
df_good =  spark.read.csv(datasets_path + 'goods_classification.csv', header=True, inferSchema=True)
# Question 1: Convertir le format de la date '202206' vers '01/06/2022'
def question_1(df):
    rdf = df.withColumn("date", to_date(concat_ws("/", lit(
        "01"), col("time_ref").substr(5, 2), col(
        "time_ref").substr(1, 4)), "d/M/yyyy"))

    return rdf

# Question 2: Extraire l'année
def question_2(df):
    rdf = df.withColumn("year", f.year(df["date"]))
    return rdf

# Question 3: Ajouter le nom du pays
def question_3(df):
    rdf = df.join(df_country, on="country_code", how="left")
    return rdf

# Question 4: Ajouter une colonne is_goods (1 si Goods, 0 sinon)
def question_4(df):
    rdf = df.withColumn("details_good", when(col("product_type") == "Goods", 1).otherwise(0))
    return rdf

from pyspark.sql.types import IntegerType

# Question 5: Ajouter une colonne is_services (1 si Services, 0 sinon)
def question_5(df):
    rdf = df.withColumn("details_service", when(col("product_type") == "Services", 1).otherwise(0))
    return rdf


from pyspark.sql.functions import sum as _sum
# Question 6 : Classer les pays par Services et Goods
def question_6(df):
    df_exporters = df.filter(col("account") == 'Exports')

    grouped_df = df_exporters.groupby('country_label') \
        .agg(_sum('details_good').alias('total_goods'),
             _sum('details_service').alias('total_service'))
    sorted_df = grouped_df.orderBy(col("total_goods").desc(), col("total_service").desc())
    return sorted_df

# Question 7 : Classer les pays Importeurs par Services et Goods
def question_7(df):
    df_exporters = df.filter(col("account") == 'Imports')

    grouped_df = df_exporters.groupby('country_label') \
        .agg(_sum('details_good').alias('total_goods'),
             _sum('details_service').alias('total_service'))
    sorted_df = grouped_df.orderBy(col("total_goods").desc(), col("total_service").desc())
    return sorted_df

# Question 8 : regroupement des pays par good
def question_8(df):

    grouped_df = df.groupby('country_label') \
        .agg(_sum('details_good').alias('total_goods'))
    sorted_df = grouped_df.orderBy(col("total_goods").desc())
    return sorted_df

# Question 9 : regroupement des pays  par service
def question_9(df):

    grouped_df = df.groupby('country_label') \
        .agg(_sum('details_service').alias('total_service'))
    sorted_df = grouped_df.orderBy(col("total_service").desc())
    return sorted_df

# Question 10 : la liste des services exporté de la france
def question_10(df):
    df_export_france_services = df.filter((col("account") == "Exports") & (col("product_type") == "Services") &
                                          (col("country_code") == "FR"))
    return df_export_france_services

# Question 11 : la liste des services importé de la france
def question_11(df):
    df_import_france_goods = df.filter((col("account") == "Imports") & (col("product_type") == "Goods") &
                                       (col("country_code") == "FR"))
    return df_import_france_goods

# Question 12 : classement des services les moins demandés
def question_12(df):
    df_services = df.filter(col("product_type") == "Services")
    grouped_df = df_services.groupby("country_label") \
        .agg(_sum("details_service").alias("total_service"))
    sorted_df = grouped_df.orderBy(col("total_service").asc())
    return sorted_df

# Question 13 : classement des goods les plus demandé
def question_13(df):
    df_goods = df.filter(col("product_type") == "Goods")
    grouped_df = df_goods.groupby("country_label") \
        .agg(_sum("details_good").alias("total_goods"))
    sorted_df = grouped_df.orderBy(col("total_goods").desc())
    return sorted_df

# Question 14 : Ajouter la colonne status_import_export (négative si import > export, sinon positive par pays)
def question_14(df):
    grouped_df = df.groupby("country_label").agg(_sum("details_good").alias("total_goods"), _sum("details_service").alias("total_service"))
    rdf = grouped_df.withColumn("status_import_export", when((col("total_goods") - col("total_service")) > 0, "positive").otherwise("negative"))
    return rdf

# Question 15 : Ajouter la column difference_import_export
def question_15(df):
    grouped_df = df.groupby("country_label").agg(_sum("details_good").alias("total_goods"), _sum("details_service").alias("total_service"))
    rdf = grouped_df.withColumn("difference_import_export", col("total_goods") - col("total_service"))
    return rdf

# Question 16 : Ajouter la column Somme_good
def question_16(df):
    grouped_df = df.groupby("country_label").agg(_sum("details_good").alias("Somme_good"))
    return grouped_df

# Question 17 : Ajouter la column Somme_service
def question_17(df):
    grouped_df = df.groupby("country_label").agg(_sum("details_service").alias("Somme_service"))
    return grouped_df


# Question 18 : Ajouter la colonne pourcentages_good (pourcentage de la colonne good par rapport à tous les goods d'un seul pays)
def question_18(df):
   rdf = df.withColumn("pourcentages_good", (col("difference_import_export") / col("Total_goods")) * 100)
   return rdf


# Question 19 : Ajouter la colonne pourcentages_service (pourcentage de la colonne service par rapport à tous les goods d'un seul pays)
def question_19(df):
   rdf = df.withColumn("pourcentages_service", (col("difference_import_export") / col("total_Service")) * 100)
   return rdf

# Question 20 : regrouper les goods selon leur type
def question_20 (df):

    rdf = df.groupby("NZHSC_Level_1_Code_HS2").agg(_sum("NZHSC_Level_1_Code_HS2").alias("count_type"))
    return rdf

 #Question 21 : Classement des pays exportateur de pétrole
def question_21 (df):
    df_exporters_petrole = df.filter(
        (col("account") == "Exports") & (col("product_type") == "Goods") & (col("HS2") == "27"))
    grouped_df = df_exporters_petrole.groupby("country_label") \
        .agg(_sum("details_good").alias("total_goods"))
    sorted_df = grouped_df.orderBy(col("total_goods").desc())
    return sorted_df


# Question 22 : Classement des pays importateurs de viandes

def question_22(df):
    df_importers_viandes = df.filter((col("account") == "Imports") & (col("product_type") == "Goods") & (col("HS2") == "02"))
    grouped_df = df_importers_viandes.groupby("country_label") \
        .agg(_sum("details_good").alias("total_goods"))
    sorted_df = grouped_df.orderBy(col("total_goods").desc())
    return sorted_df







# Question 25 : ajouter une column description :
def question_25(df):
    rdf = df.withColumn("description", concat_ws(" ", lit("Le pays"), col("country_label"),
                                                  lit("fait un"),
                                                  when(col("account") == "Exports", lit("EXPORT")).otherwise(lit("IMPORT")),
                                                  lit("sur"),
                                                  when(col("product_type") == "Goods", lit("Goods")).otherwise(lit("Services"))))
    return rdf


#############Test

r1 = question_1(df_full)
r1.show() # change it to an export

r2 = question_2(r1)
r2.show() # change it to an export

r3 = question_3(r2)
r3.show() # change it to an export

r4 = question_4(r3)
r4.show() # change it to an export

r5 = question_5(r4)
r5.show() # change it to an export

r6 = question_6(r5)
r6.show() # change it to an export

r8 = question_8(r5)
r8.show()

r9 = question_9(r5)
r9.show()

r10 = question_10(r5)
r10.show()

r11 = question_11(r5)
r11.show()

r12 = question_12(r5)
r12.show()

r13 = question_13(r5)
r13.show()


r14 = question_14(r5)
r14.show()

r15 = question_15(r5)
r15.show()

r16 = question_16(r5)
r16.show()

r17 = question_17(r5)
r17.show()

r18 = question_18(r15)
r18.show()

r19 = question_19(r15)
r19.show()

r20 = question_20(df_good)
r20.show()


r25 = question_25(r3)
r25.show()