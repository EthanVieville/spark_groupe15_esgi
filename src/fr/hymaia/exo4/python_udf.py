import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from src.fr.hymaia.exo2.spark_session_provider import SparkSessionProvider


def main():
    # Obtenir une session Spark via SparkSessionProvider
    spark = SparkSessionProvider.get_spark_session()

    # Lire le fichier CSV contenant les données de ventes
    df_sell = spark.read.option("header", True).csv("./src/resources/exo4/sell.csv")

    # Ajouter la colonne "category_name" à la DataFrame en utilisant une UDF
    df_major_clients = add_category_name(df_sell)

    # Définir le chemin de sortie pour les résultats
    output_path = "./src/fr/hymaia/exo4/output/python_udf_output"

    # Écrire le résultat dans un fichier Parquet, en mode overwrite
    df_major_clients.write.mode('overwrite').parquet(output_path)

    # Réinitialiser la session Spark
    SparkSessionProvider.reset_session()


def add_category_name(df_sell):

    # l'objectif
    def category_name(category):
        return "food" if int(category) < 6 else "furniture"

    # UDF conversion
    category_name_udf = f.udf(category_name, StringType())

    # apply
    return df_sell.withColumn("category_name", category_name_udf(f.col("category")))


# Point d'entrée du programme
if __name__ == "__main__":
    main()
