import db
import machineLearning
from load import loadingData
from sparkSession import start_sparkSession
from extract import open_file_csv
from pyspark.sql.functions import avg, substring, col, \
    round, lag
from pyspark.sql import Window

from transform import joinDataframe, verifier_longueur_noms_colonnes, \
    execute_election_transformation_for_year

spark = start_sparkSession()

# Chargement des fichiers CSV
dataframe_chomage_age = open_file_csv(spark, 'data/indicateurs/indicateur_taux_chomage_age.csv')
dataframe_chomage_sexe = open_file_csv(spark, 'data/indicateurs/indicateur_taux_chomage_sexe.csv')
dataframe_flux_migrateur = open_file_csv(spark, 'data/indicateurs/indicateur_population_etrangere.csv')
dataframe_pouvoir_achat = open_file_csv(spark, "data/indicateurs/indicateur_pouvoir_achat.csv")
dataframe_smic_IPC = open_file_csv(spark, "data/indicateurs/indicateur_evoluation_smic_ipc.csv")
dataframe_preoccupation = open_file_csv(spark, "data/indicateurs/indicateur_preocuppation.csv")
dataframe_incivilite = open_file_csv(spark, "data/indicateurs/indicateur_nombre_incivilite.csv")
dataframe_insecu_quartier = open_file_csv(spark, "data/indicateurs/indicateur_insecurite.csv")


# Traitement des données pour le taux de chômage par tranche d'âge
dataframe_response_age = dataframe_chomage_age.groupBy('Annee').agg(avg('15-24 ans'), avg('25-49 ans'),
                                                                    avg('50 ans ou plus'), avg('Ensemble'))
# Arrondir les moyennes à 2 décimales
for col_name in ['avg(15-24 ans)', 'avg(25-49 ans)', 'avg(50 ans ou plus)', 'avg(Ensemble)']:
    dataframe_response_age = dataframe_response_age.withColumn(col_name, round(dataframe_response_age[col_name], 2))

# Affichage des données pour le taux de chômage par tranche d'âge
dataframe_response_age.show()

# Traitement des données pour le taux de chômage par sexe
dataframe_response_sexe = dataframe_chomage_sexe.groupBy('Annee').agg(avg('Femmes'), avg('Hommes'))
# Arrondir les moyennes à 2 décimales
for col_name in ['avg(Femmes)', 'avg(Hommes)']:
    dataframe_response_sexe = dataframe_response_sexe.withColumn(col_name, round(dataframe_response_sexe[col_name], 2))

# Affichage des données pour le taux de chômage par sexe
dataframe_response_sexe.show()

# Fusion des données sur l'année
merged_dataframe = dataframe_response_sexe.join(dataframe_response_age, on="Annee", how="inner")
# Affichage des données fusionnées
merged_dataframe.show()

# Tri des données fusionnées par année
selected_sorted_dataframe = merged_dataframe.orderBy("Annee", ascending=True)
selected_sorted_dataframe.show()

# Traitement des données sur le flux migratoire
dataframe_flux_migrateur.show()

# Traitement des données sur le pouvoir d'achat
dataframe_pouvoir_achat.printSchema()
dataframe_pouvoir_achat_date = dataframe_pouvoir_achat.orderBy("Annee", ascending=True)
dataframe_pouvoir_achat_date.show()

# Traitement des données sur l'évolution du SMIC et de l'IPC
dataframe_smic_IPC.printSchema()
dataframe_smic_IPC_date = dataframe_smic_IPC.orderBy("Annee", ascending=True)
dataframe_smic_IPC_date_avg = dataframe_smic_IPC_date.groupBy('Annee').agg(avg('Smic horaire brut'),
                                                                           avg('Prix (y compris tabac)1'))
# Arrondir les moyennes à 2 décimales
for col_name in ['Smic horaire brut', 'Prix (y compris tabac)1']:
    dataframe_smic_IPC_date_avg = dataframe_smic_IPC_date_avg.withColumn(f'avg({col_name})',
                                                                         round(dataframe_smic_IPC_date_avg[f'avg({col_name})'], 2))
# Calcul de la différence entre le SMIC et l'IPC
dataframe_smic_IPC_final = dataframe_smic_IPC_date_avg.withColumn("resultat_soustraction",
                                                                   dataframe_smic_IPC_date_avg['avg(Smic horaire brut)'] -
                                                                   dataframe_smic_IPC_date_avg['avg(Prix (y compris tabac)1)'])
# Calcul du pourcentage de changement du SMIC et de l'IPC par rapport à l'année précédente
windowSpec = Window.orderBy("Annee")
dataframe_smic_IPC_final = dataframe_smic_IPC_final.withColumn("smic_evolution", round(
    (col("avg(Smic horaire brut)") - lag("avg(Smic horaire brut)", 1).over(windowSpec)) /
    lag("avg(Smic horaire brut)", 1).over(windowSpec) * 100, 2))
dataframe_smic_IPC_final = dataframe_smic_IPC_final.withColumn("prix_conso_evolution", round(
    (col("avg(Prix (y compris tabac)1)") - lag("avg(Prix (y compris tabac)1)", 1).over(windowSpec)) /
    lag("avg(Prix (y compris tabac)1)", 1).over(windowSpec) * 100, 2))

# Calcul de la moyenne globale du pourcentage de changement du SMIC et de l'IPC
average_smic_df = dataframe_smic_IPC_final.select(round(avg("smic_evolution"), 2).alias("moyenne_globale_smic"))
average_prix_conso_df = dataframe_smic_IPC_final.select(
    round(avg("prix_conso_evolution"), 2).alias("moyenne_globale_prix_conso"))

# Affichage des résultats
dataframe_smic_IPC_final.show()
average_smic_df.show()
average_prix_conso_df.show()

# Traitement des données sur les préoccupations
dataframe_preoccupation.printSchema()
dataframe_preoccupation.show()

# Traitement des données sur les tableaux 4001
# Extraction de l'année à partir de la colonne "Annee"
dataframe_incivilite = dataframe_incivilite.withColumn("Annee", substring(col("Annee"), 2, 4))

# Conversion de la colonne "Annee" en entier
dataframe_incivilite = dataframe_incivilite.withColumn("Annee", dataframe_incivilite["Annee"].cast("int"))

# Groupe des données par année et somme des valeurs de toutes les colonnes
df_cumul = dataframe_incivilite.groupBy("Annee").sum()

# Renommer les colonnes de somme en fonction des besoins
for col_name in dataframe_incivilite.columns:
    if col_name != "Annee":
        df_cumul = df_cumul.withColumnRenamed(f"sum({col_name})", f"{col_name}")

# Ajout de la nouvelle ligne à un nouveau DataFrame
df_cumul = verifier_longueur_noms_colonnes(df_cumul)
dataframe_4001_2 = df_cumul.select(df_cumul.columns[:5])

# Affichage du DataFrame résultant
dataframe_4001_2.show()

dataframe_insecu_quartier.printSchema()
dataframe_insecu_quartier.show()
# -------------------------------------------------------------------------

loadingData(selected_sorted_dataframe, 'taux_chomage')
loadingData(dataframe_pouvoir_achat, 'pouvoir_achat')
loadingData(dataframe_smic_IPC_final, 'evolution_smic_inflation')
loadingData(dataframe_4001_2, 'infractions_criminalite')
loadingData(dataframe_flux_migrateur, 'population_etrangere')
loadingData(dataframe_insecu_quartier, 'insecurite_france')

# -------------------------------------------------------------------------------------------------
merged_df = joinDataframe(dataframe_smic_IPC_date_avg, dataframe_pouvoir_achat_date)
merged_df = joinDataframe(merged_df, selected_sorted_dataframe)
df_insecu_ensemble = dataframe_insecu_quartier.filter(dataframe_insecu_quartier['Tranche age'] == 'Ensemble')
merged_df = joinDataframe(merged_df, df_insecu_ensemble)
merged_df = joinDataframe(merged_df, dataframe_flux_migrateur)
merged_df = joinDataframe(merged_df, dataframe_preoccupation)

loadingData(merged_df, 'tablefait')

connexion = db.connection("localhost","root","zDH-.42l3lSRIT1p","datawarehouse")
curseur = connexion.cursor()

# Création de la table de faits
db.create_fact_table(curseur)
for year in range(1990, 2024):
    execute_election_transformation_for_year(merged_df, df_cumul, year,spark)

# Apprentissage avec repartition 20% test et 80% entrainement + seed mis a 42
default_seed = 42
default_test_size = 0.2
machineLearning.start_machine_learning(default_seed,default_test_size)

# Trouver la meilleure seed
best_seed = machineLearning.find_best_seed()

# Stocker uniquement les résultats de la meilleure seed
best_scores = machineLearning.start_machine_learning_training_seed(best_seed)

# Afficher les résultats de la meilleure seed
print("Meilleure seed:", best_seed)
print("Résultats de la meilleure seed:", best_scores)

# Second apprentissage effectué avec repartition 40% test et 60% entrainement + utilisation d'une meilleur seed
new_test_size = 0.4
machineLearning.start_machine_learning(best_seed,new_test_size)
curseur.close()
