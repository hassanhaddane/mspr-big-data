import os

from pyspark.sql import functions as F

import db
from extract import open_file_csv


def transformationResultatPresidentielle(merged_df, df_indicateur, election_resultat_csv, candidat_csv, annee,
                                         curseur, spark):
    # Filtrage des données par année
    df = merged_df.filter(merged_df['Annee'] == annee)

    # Initialisation des variables
    list_indicateurs = []
    list_index_banni = [0, 13, 9, 8, 7, 6, 5]
    total_row = 0
    list_header_banni = ['Annee', 'sum(Annee)']

    # Calcul du total des indicateurs
    for row in df_indicateur.filter(df_indicateur['Annee'] == annee).collect():
        for col in df_indicateur.columns:
            if col not in list_header_banni:
                total_row += row[col]

    # Traitement des données de chaque ligne
    for row in df.collect():
        for index, item in enumerate(row):
            if index not in list_index_banni:
                if item is not None:
                    if isinstance(item, float):
                        list_indicateurs.append(item)
                    elif isinstance(item, str):
                        if ' ' in item:
                            list_indicateurs.append(float(item.replace(' ', '')))
                        elif ',' in item:
                            list_indicateurs.append(float(item.replace(',', '.')))
                        else:
                            list_indicateurs.append(float(item))
                else:
                    list_indicateurs.append(None)

    # Chargement des données électorales depuis un fichier CSV
    Election_DF = open_file_csv(spark, election_resultat_csv)
    Candidats_DF = open_file_csv(spark, candidat_csv)
    nombre_total_voix_exprimees = Election_DF.select(F.sum("Exprimes")).collect()[0][0]

    # Initialisation des variables pour stocker les informations des candidats
    infos_candidats = []
    nom_prec = None
    prenom_prec = None
    voix_totale = 0

    # Itération sur les colonnes des candidats pour récupérer les informations sur les partis politiques
    for i in range(16, len(Election_DF.columns), 6):
        nom_colonne = f"Nom{i}"
        prenom_colonne = f"Prenom{i + 1}"
        voix_colonne = f"Voix{i + 2}"

        # Récupération des données des colonnes
        noms = Election_DF.select(nom_colonne).collect()
        prenoms = Election_DF.select(prenom_colonne).collect()
        voix = Election_DF.select(voix_colonne).collect()

        for nom, prenom, voix in zip(noms, prenoms, voix):
            nom_prec = nom[0]
            prenom_prec = prenom[0]
            voix_totale = voix[0]

            if len(infos_candidats) > 0:
                candidat_existant = next((candidat for candidat in infos_candidats if
                                          candidat["name"] == nom_prec and candidat["firstname"] == prenom_prec), None)
                if candidat_existant:
                    candidat_existant["voix_totale"] += voix_totale
                else:
                    infos_candidats.append({"name": nom_prec, "firstname": prenom_prec, "voix_totale": voix_totale})
            else:
                infos_candidats.append({"name": nom_prec, "firstname": prenom_prec, "voix_totale": voix_totale})

    # Traitement des informations sur les candidats
    tableau_result = []
    parti_DF = open_file_csv(spark, 'data/parti-politique/partis.csv')
    for row in infos_candidats:
        inter = Candidats_DF.filter(Candidats_DF['Nom'] == row['name'].lower()).first()
        result = parti_DF.filter(parti_DF['Nom du parti'] == inter[2]).first()
        tableau_result.append({'Orientation_politique': result[1], 'total_vote': row["voix_totale"]})

    # Consolidation des données pour chaque orientation politique
    tableau_result_unique = []
    for item in tableau_result:
        deja_presente = False
        for unique_item in tableau_result_unique:
            if item['Orientation_politique'] == unique_item['Orientation_politique']:
                unique_item['total_vote'] += item['total_vote']
                deja_presente = True
                break
        if not deja_presente:
            tableau_result_unique.append(item)

    # Tri des orientations politiques dans un ordre spécifique
    tableau_ordre_orientation = ["Extreme gauche", "Gauche radicale", "Gauche", "Ecologie", "Centre", "Droite",
                                 "Extreme Droite", "Divers"]
    tableau_final = []
    for orientation in tableau_ordre_orientation:
        for row in tableau_result_unique:
            if row['Orientation_politique'] == orientation:
                tableau_final.append(row)
                break

    # Ajout des orientations politiques manquantes avec 0 votes
    orientation_existante = {row['Orientation_politique']: row for row in tableau_final}
    for orientation in tableau_ordre_orientation:
        if orientation not in orientation_existante:
            tableau_final.append({'Orientation_politique': orientation, 'total_vote': 0})

    # Récupération de l'orientation politique gagnante
    orientation_gagnante = max(tableau_final, key=lambda x: x['total_vote'])

    # Récupérer l'orientation politique de l'objet gagnant
    orientation_gagnante_nom = orientation_gagnante['Orientation_politique']

    # Calcul des pourcentages de votes pour chaque orientation politique
    list_total_vote = []
    list_total_vote.append(annee)
    list_total_vote.append(orientation_gagnante_nom)
    for row in tableau_final:
        pourcentage_voix = (row['total_vote'] / nombre_total_voix_exprimees) * 100
        list_total_vote.append(pourcentage_voix)

    # Construction de la liste des valeurs à insérer dans la base de données
    list_total_row = list_total_vote + list_indicateurs
    list_total_row.append(total_row)

    # Insertion des données dans la base de données
    curseur.execute('INSERT INTO table_fait VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
                    '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', list_total_row)


def transformationResultatLegislatives(merged_df, df_indicateurs, election_resultat_csv, annee, curseur, spark):
    # Filtrage des données par année
    df = merged_df.filter(merged_df['Annee'] == annee)

    # Initialisation des variables
    list_indicateurs = []
    list_index_banni = [0, 13, 9, 8, 7, 6, 5]
    total_row = 0
    list_header_banni = ['Annee', 'sum(Annee)']

    # Calcul du total des indicateurs
    for row in df_indicateurs.filter(df_indicateurs['Annee'] == annee).collect():
        for col in df_indicateurs.columns:
            if col not in list_header_banni:
                total_row += row[col]

    # Traitement des données de chaque ligne
    for row in df.collect():
        for index, item in enumerate(row):
            if index not in list_index_banni:
                if item is not None:
                    if isinstance(item, float):
                        list_indicateurs.append(item)
                    elif isinstance(item, str):
                        if ' ' in item:
                            list_indicateurs.append(float(item.replace(' ', '')))
                        elif ',' in item:
                            list_indicateurs.append(float(item.replace(',', '.')))
                        else:
                            list_indicateurs.append(float(item))
                else:
                    list_indicateurs.append(None)

    # Chargement des données électorales depuis un fichier CSV
    Election_DF = open_file_csv(spark, election_resultat_csv)
    nombre_total_voix_exprimees = Election_DF.select(F.sum("Exprimes")).collect()[0][0]

    # Initialisation des variables pour stocker les informations des partis politiques
    infos_parties = []
    parti_precedent = None
    voix_totale = 0

    # Itération sur les colonnes des candidats pour récupérer les informations sur les partis politiques
    for i in range(15, len(Election_DF.columns), 4):
        parti_colonne = f"Code Nuance{i}"
        voix_colonne = f"Voix{i + 1}"

        # Récupération des données des colonnes
        parti = Election_DF.select(parti_colonne).collect()
        voix = Election_DF.select(voix_colonne).collect()

        for parti, voix in zip(parti, voix):
            parti_precedent = parti[0]
            if parti_precedent is not None:
                voix_totale = voix[0]

                if len(infos_parties) > 0:
                    parti_existant = next((parti for parti in infos_parties if
                                           parti["Nom du parti"] == parti_precedent), None)
                    if parti_existant:
                        parti_existant["total_vote"] += voix_totale
                    else:
                        infos_parties.append({"Nom du parti": parti_precedent, "total_vote": voix_totale})
                else:
                    infos_parties.append({"Nom du parti": parti_precedent, "total_vote": voix_totale})

    # Traitement des informations sur les partis politiques
    tableau_result = []
    parti_DF = open_file_csv(spark, 'data/parti-politique/partis.csv')
    for row in infos_parties:
        if row['Nom du parti'] is not None:
            result = parti_DF.filter(parti_DF['Nom du parti'] == row["Nom du parti"]).first()
            tableau_result.append({'Orientation_politique': result[1], 'total_vote': row["total_vote"]})

    # Consolidation des données pour chaque orientation politique
    tableau_result_unique = []
    for item in tableau_result:
        deja_presente = False
        for unique_item in tableau_result_unique:
            if item['Orientation_politique'] == unique_item['Orientation_politique']:
                unique_item['total_vote'] += item['total_vote']
                deja_presente = True
                break
        if not deja_presente:
            tableau_result_unique.append(item)

    # Tri des orientations politiques dans un ordre spécifique
    tableau_ordre_orientation = ["Extreme gauche", "Gauche radicale", "Gauche", "Ecologie", "Centre", "Droite",
                                 "Extreme Droite", "Divers"]
    tableau_final = []
    for orientation in tableau_ordre_orientation:
        for row in tableau_result_unique:
            if row['Orientation_politique'] == orientation:
                tableau_final.append(row)
                break

    # Ajout des orientations politiques manquantes avec 0 votes
    orientation_existante = {row['Orientation_politique']: row for row in tableau_final}
    for orientation in tableau_ordre_orientation:
        if orientation not in orientation_existante:
            tableau_final.append({'Orientation_politique': orientation, 'total_vote': 0})

    # Récupération de l'orientation politique gagnante
    orientation_gagnante = max(tableau_final, key=lambda x: x['total_vote'])

    # Récupérer l'orientation politique de l'objet gagnant
    orientation_gagnante_nom = orientation_gagnante['Orientation_politique']

    # Calcul des pourcentages de votes pour chaque orientation politique
    list_total_vote = []
    list_total_vote.append(annee)
    list_total_vote.append(orientation_gagnante_nom)
    for row in tableau_final:
        pourcentage_voix = (row['total_vote'] / nombre_total_voix_exprimees) * 100
        list_total_vote.append(pourcentage_voix)

    # Construction de la liste des valeurs à insérer dans la base de données
    list_total_row = list_total_vote + list_indicateurs
    list_total_row.append(total_row)

    # Insertion des données dans la base de données
    curseur.execute('INSERT INTO table_fait VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
                    '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', list_total_row)


def transformationResultatEuropeens(merged_df, df_indicateurs, election_resultat_csv, annee, curseur, spark):
    # Filtrage des données par année
    df = merged_df.filter(merged_df['Annee'] == annee)

    # Initialisation des variables
    list_indicateurs = []
    list_index_banni = [0, 13, 9, 8, 7, 6, 5]
    total_row = 0
    list_header_banni = ['Annee', 'sum(Annee)']

    # Calcul du total des indicateurs
    for row in df_indicateurs.filter(df_indicateurs['Annee'] == annee).collect():
        for col in df_indicateurs.columns:
            if col not in list_header_banni:
                total_row += row[col]

    # Traitement des données de chaque ligne
    for row in df.collect():
        for index, item in enumerate(row):
            if index not in list_index_banni:
                if item is not None:
                    if isinstance(item, float):
                        list_indicateurs.append(item)
                    elif isinstance(item, str):
                        if ' ' in item:
                            list_indicateurs.append(float(item.replace(' ', '')))
                        elif ',' in item:
                            list_indicateurs.append(float(item.replace(',', '.')))
                        else:
                            list_indicateurs.append(float(item))
                else:
                    list_indicateurs.append(None)

    # Chargement des données électorales depuis un fichier CSV
    Election_DF = open_file_csv(spark, election_resultat_csv)
    nombre_total_voix_exprimees = Election_DF.select(F.sum("Exprimes")).collect()[0][0]

    # Initialisation des variables pour stocker les informations des partis politiques
    infos_parti = []
    libelle_DF = open_file_csv(spark, 'data/parti-politique/europeens/partis-euro.csv')
    parti_precedent = None
    voix_totale = 0

    # Détermination des indices de début et de séparateur en fonction de l'année
    if annee == 2004:
        index_start = 9
        index_separateur = 4
    elif annee == 1999 or annee == 1994:
        index_start = 14
        index_separateur = 5
    else:
        index_start = 16
        index_separateur = 7

    # Itération sur les colonnes des candidats pour récupérer les informations sur les partis politiques
    for i in range(index_start, len(Election_DF.columns), index_separateur):
        if annee == 2004 and annee == 2009:
            libelle_colonne = f"Libelle Abrege Liste{i + 1}"
            voix_colonne = f"Voix{i + 3}"
        elif annee == 2019:
            libelle_colonne = f"Libelle Abrege Liste{i}"
            voix_colonne = f"Voix{i + 3}"
        else:
            libelle_colonne = f"Libelle Abrege Liste{i + 1}"
            voix_colonne = f"Voix{i + 3}"

        # Récupération des données des colonnes
        if annee != 2019:
            try:
                libelle = Election_DF.select(libelle_colonne).collect()
                voix = Election_DF.select(voix_colonne).collect()
            except:
                break
        else:
            libelle = Election_DF.select(libelle_colonne).collect()
            voix = Election_DF.select(voix_colonne).collect()
        for libelle, voix in zip(libelle, voix):
            libelle_precedent = libelle[0]
            if libelle_precedent is not None:
                voix_totale = voix[0]

                if len(infos_parti) > 0:
                    orientation_existant = next((orientation for orientation in infos_parti if
                                                 orientation["Libelle parti"] == libelle_precedent), None)
                    if orientation_existant:
                        orientation_existant["total_vote"] += voix_totale
                    else:
                        infos_parti.append({"Libelle parti": libelle_precedent, "total_vote": voix_totale})
                else:
                    infos_parti.append({"Libelle parti": libelle_precedent, "total_vote": voix_totale})

    # Traitement des informations sur les partis politiques
    tableau_result = []
    orientation_gagnante = max(infos_parti, key=lambda x: x['total_vote'])
    result_orientation_politique_gagnante = libelle_DF.filter(
        libelle_DF['Libelle parti'] == orientation_gagnante["Libelle parti"]).first()
    for row in infos_parti:
        if row['Libelle parti'] is not None:
            result = libelle_DF.filter(libelle_DF['Libelle parti'] == row["Libelle parti"]).first()
            tableau_result.append({'Orientation_politique': result[1], 'total_vote': row["total_vote"]})

    # Consolidation des données pour chaque orientation politique
    tableau_result_unique = []
    for item in tableau_result:
        deja_presente = False
        for unique_item in tableau_result_unique:
            if item['Orientation_politique'] == unique_item['Orientation_politique']:
                unique_item['total_vote'] += item['total_vote']
                deja_presente = True
                break
        if not deja_presente:
            tableau_result_unique.append(item)

    # Tri des orientations politiques dans un ordre spécifique
    tableau_ordre_orientation = ["Extreme gauche", "Gauche radicale", "Gauche", "Ecologie", "Centre", "Droite",
                                 "Extreme Droite", "Divers"]
    tableau_final = []
    for orientation in tableau_ordre_orientation:
        for row in tableau_result_unique:
            if row['Orientation_politique'] == orientation:
                tableau_final.append(row)
                break

    # Ajout des orientations politiques manquantes avec 0 votes
    orientation_existante = {row['Orientation_politique']: row for row in tableau_final}
    for orientation in tableau_ordre_orientation:
        if orientation not in orientation_existante:
            tableau_final.append({'Orientation_politique': orientation, 'total_vote': 0})

    # Récupération de l'orientation politique gagnante
    orientation_gagnante_nom = result_orientation_politique_gagnante[1]

    # Calcul des pourcentages de votes pour chaque orientation politique
    list_total_vote = []
    list_total_vote.append(annee)
    list_total_vote.append(orientation_gagnante_nom)
    for row in tableau_final:
        pourcentage_voix = (row['total_vote'] / nombre_total_voix_exprimees) * 100
        list_total_vote.append(pourcentage_voix)

    # Construction de la liste des valeurs à insérer dans la base de données
    list_total_row = list_total_vote + list_indicateurs
    list_total_row.append(total_row)

    # Insertion des données dans la base de données
    curseur.execute('INSERT INTO table_fait VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
                    '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', list_total_row)


def transformationResultatRegional(merged_df, df_indicateurs, election_resultat_csv, annee, curseur, spark):
    # Filtrage des données par année
    df = merged_df.filter(merged_df['Annee'] == annee)

    # Initialisation des variables
    list_indicateurs = []
    list_index_banni = [0, 13, 9, 8, 7, 6, 5]
    total_row = 0
    list_header_banni = ['Annee', 'sum(Annee)']

    # Calcul du total des indicateurs
    for row in df_indicateurs.filter(df_indicateurs['Annee'] == annee).collect():
        for col in df_indicateurs.columns:
            if col not in list_header_banni:
                total_row += row[col]

    # Traitement des données de chaque ligne
    for row in df.collect():
        for index, item in enumerate(row):
            if index not in list_index_banni:
                if item is not None:
                    if isinstance(item, float):
                        list_indicateurs.append(item)
                    elif isinstance(item, str):
                        if ' ' in item:
                            list_indicateurs.append(float(item.replace(' ', '')))
                        elif ',' in item:
                            list_indicateurs.append(float(item.replace(',', '.')))
                        else:
                            list_indicateurs.append(float(item))
                else:
                    list_indicateurs.append(None)

    # Chargement des données électorales depuis un fichier CSV
    Election_DF = open_file_csv(spark, election_resultat_csv)
    nombre_total_voix_exprimees = Election_DF.select(F.sum("Exprimes")).collect()[0][0]

    # Initialisation des variables pour stocker les informations des partis politiques
    infos_parti = []
    parti_DF = open_file_csv(spark, 'data/parti-politique/partis.csv')
    parti_precedent = None
    voix_totale = 0

    # Détermination des indices de début et de séparateur en fonction de l'année
    if annee == 2004:
        index_start = 15
        index_separateur = 5
    elif annee == 2010:
        index_start = 15
        index_separateur = 6
    elif annee == 1998:
        index_start = 13
        index_separateur = 5
    else:
        index_start = 16
        index_separateur = 8

    # Itération sur les colonnes des candidats pour récupérer les informations sur les partis politiques
    for i in range(index_start, len(Election_DF.columns), index_separateur):
        if annee == 2010:
            libelle_colonne = f"Nuance Liste{i}"
            voix_colonne = f"Voix{i + 3}"
        elif annee == 2004 or annee == 1998:
            libelle_colonne = f"Nuance Liste{i}"
            voix_colonne = f"Voix{i + 2}"
        else:
            libelle_colonne = f"Nuance Liste{i}"
            voix_colonne = f"Voix{i + 4}"

        libelle = Election_DF.select(libelle_colonne).collect()
        voix = Election_DF.select(voix_colonne).collect()

        for libelle, voix in zip(libelle, voix):
            libelle_precedent = libelle[0]
            if libelle_precedent is not None:
                libelle_precedent = libelle[0][1:]
                voix_totale = voix[0]

                if len(infos_parti) > 0:
                    orientation_existant = next((orientation for orientation in infos_parti if
                                                 orientation["Nom du parti"] == libelle_precedent), None)
                    if orientation_existant:
                        orientation_existant["total_vote"] += voix_totale
                    else:
                        infos_parti.append({"Nom du parti": libelle_precedent, "total_vote": voix_totale})
                else:
                    infos_parti.append({"Nom du parti": libelle_precedent, "total_vote": voix_totale})

    # Traitement des informations sur les partis politiques
    tableau_result = []
    for row in infos_parti:
        if row['Nom du parti'] is not None:
            result = parti_DF.filter(parti_DF['Nom du parti'] == row["Nom du parti"]).first()
            tableau_result.append({'Orientation_politique': result[1], 'total_vote': row["total_vote"]})

    # Consolidation des données pour chaque orientation politique
    tableau_result_unique = []
    for item in tableau_result:
        deja_presente = False
        for unique_item in tableau_result_unique:
            if item['Orientation_politique'] == unique_item['Orientation_politique']:
                unique_item['total_vote'] += item['total_vote']
                deja_presente = True
                break
        if not deja_presente:
            tableau_result_unique.append(item)

    # Tri des orientations politiques dans un ordre spécifique
    tableau_ordre_orientation = ["Extreme gauche", "Gauche radicale", "Gauche", "Ecologie", "Centre", "Droite",
                                 "Extreme Droite", "Divers"]
    tableau_final = []
    for orientation in tableau_ordre_orientation:
        for row in tableau_result_unique:
            if row['Orientation_politique'] == orientation:
                tableau_final.append(row)
                break

    # Ajout des orientations politiques manquantes avec 0 votes
    orientation_existante = {row['Orientation_politique']: row for row in tableau_final}
    for orientation in tableau_ordre_orientation:
        if orientation not in orientation_existante:
            tableau_final.append({'Orientation_politique': orientation, 'total_vote': 0})

    # Sélection de l'orientation politique gagnante
    orientation_gagnante = max(tableau_final, key=lambda x: x['total_vote'])
    orientation_gagnante_nom = orientation_gagnante["Orientation_politique"]

    # Calcul des pourcentages de votes pour chaque orientation politique
    list_total_vote = []
    list_total_vote.append(annee)
    list_total_vote.append(orientation_gagnante_nom)
    for row in tableau_final:
        pourcentage_voix = (row['total_vote'] / nombre_total_voix_exprimees) * 100
        list_total_vote.append(pourcentage_voix)

    # Construction de la liste des valeurs à insérer dans la base de données
    list_total_row = list_total_vote + list_indicateurs
    list_total_row.append(total_row)

    # Insertion des données dans la base de données
    curseur.execute('INSERT INTO table_fait VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
                    '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', list_total_row)
    print('INSERT')


def execute_election_transformation_for_year(merged_df, df_cumul, year, spark):
    connexion = db.connection("localhost", "root", "zDH-.42l3lSRIT1p", "datawarehouse")
    print(year)
    # Transformation pour l'élection présidentielle
    presidentielle_file_path = f'data/elections/presidentielles/election_{year}.csv'
    if os.path.exists(presidentielle_file_path):
        curseur = connexion.cursor()
        transformationResultatPresidentielle(merged_df, df_cumul, presidentielle_file_path,
                                             f'data/candidats/candidats_{year}.csv',
                                             year, curseur, spark)
        curseur.close()

    # Transformation pour les élections législatives
    legislatives_file_path = f'data/elections/legislatives/election_legislative_{year}.csv'
    if os.path.exists(legislatives_file_path):
        curseur = connexion.cursor()
        transformationResultatLegislatives(merged_df, df_cumul, legislatives_file_path, year,
                                           curseur, spark)
        curseur.close()

    # Transformation pour les élections européennes
    europeens_file_path = f'data/elections/europeens/election_europeenne_{year}.csv'
    if os.path.exists(europeens_file_path):
        curseur = connexion.cursor()
        transformationResultatEuropeens(merged_df, df_cumul, europeens_file_path, year,
                                        curseur, spark)
        curseur.close()

    # Transformation pour les élections régionales
    regionales_file_path = f'data/elections/regionales/election_regionale_{year}.csv'
    if os.path.exists(regionales_file_path):
        curseur = connexion.cursor()
        transformationResultatRegional(merged_df, df_cumul, regionales_file_path, year, curseur, spark)
        curseur.close()


def verifier_longueur_noms_colonnes(df):
    from pyspark.sql.functions import col

    # Récupérer la liste des noms de colonnes du DataFrame
    noms_colonnes = df.columns

    # Définir une fonction pour raccourcir les noms de colonnes
    def raccourcir_nom_colonne(nom_colonne, index):
        max_longueur = 64
        if len(nom_colonne) > max_longueur:
            # Raccourcir le nom de colonne
            nom_raccourci = nom_colonne[:max_longueur - 10]
            return nom_raccourci
        else:
            return nom_colonne

    # Vérifier et raccourcir les noms de colonnes si nécessaire
    nouveaux_noms_colonnes = []
    for i, nom_colonne in enumerate(noms_colonnes):
        nouveau_nom = raccourcir_nom_colonne(nom_colonne, i)
        nouveaux_noms_colonnes.append(nouveau_nom)

    # Créer un dictionnaire de correspondance entre les anciens et les nouveaux noms de colonnes
    correspondance_noms_colonnes = dict(zip(noms_colonnes, nouveaux_noms_colonnes))

    # Renommer les colonnes dans le DataFrame
    for ancien_nom, nouveau_nom in correspondance_noms_colonnes.items():
        df = df.withColumnRenamed(ancien_nom, nouveau_nom)

    return df


def joinDataframe(dataframe_base, dataframe_join):
    return dataframe_base.join(dataframe_join, on="Annee", how="left")
