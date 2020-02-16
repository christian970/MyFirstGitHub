# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import argparse

def main():
    """
    Cette methode est le point d'entrée de mon job.
    Elle va essentiellement faire 3 choses:
        - recuperer les arguments passés via la ligne de commande
        - creer une session spark
        - lancer le traitement
    Le traitement ne doit pas se faire dans la main pour des soucis de testabilité.
    """
    parser = argparse.ArgumentParser(
        description='Discover driving sessions into log files.')
    parser.add_argument('-v', "--etablissements.csv", help='Videos input file', required=True)
    parser.add_argument('-c', "--full.csv.gz", help='Categories input file', required=True)
    parser.add_argument('-o', '--output', help='Output file', required=True)

    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    process(spark,args.etablissements.csv,args.full.csv.gz,args.output)
    
    def process(spark, etablissements.csv, full.csv.gz, output):
    """
    Contient les traitements qui permettent de lire,transformer et sauvegarder mon resultat.
    :param spark: la session spark
    :param videos_file:  le chemin du dataset des etablissements.csv
    :param categories_file: le chemin du dataset des full.csv.gz
    :param output: l'emplacement souhaité du resultat
    """
  
    # Opérations sur le jeu de données des établissements
    
    #chargement du jeu de données des établissements
    df_ET = spark.read.option('header','true').option('delimiter',';').option('inferSchema','true').csv('/home/ec2-user/Dataset/etablissements.csv')
    #visualisation du jeu de données des établissements
    df_ET.printSchema()
    df_ET.show()
    df_ET.count()
    #visualisation des colonnes pouvant avoir de l'intérêt pour l'étude du datascientist du jeu de données des établissements: Boite postale, Adresse, Lieu dit, commune
    df_ET.select('Boite postale').show()
    df_ET.select('Adresse').show()
    df_ET.select('Lieu dit').show()
    df_ET.select('commune').show()
    #Suppression des lignes à valeurs nulles dans les colonnes du jeu de données des établissements:  Adresse, car deux établissements ne peuvent avoir la même adresse
    df_ET=df_ET.select('Adresse').distinct().show()
    
    #chargement et visualisation du jeu de données foncières
    df_AP = spark.read.option('header','true').option('inferSchema','true').csv('/home/ec2-user/Dataset/full.csv.gz')
    df_AP.printSchema()
    df_AP.show()
    df_AP.count()
    # visualisation des colonnes pouvant aider à la jointure
    df_AP.select('code_voie').show()
    df_AP.select('adresse_nom_voie').show()
    df_AP.select('adresse_code_voie').show()
    df_AP.select('adresse_numero').show()

    # Jointure entre les deux tableaux en utilisant la colonne Commune du dataset établissements et la colonne nom_commune 
    df = df_ET.join(df_AP,df_ET.Commune == df_AP.nom_commune,'full')

    df.write.parquet(output)


if __name__ == '__main__':
    main()