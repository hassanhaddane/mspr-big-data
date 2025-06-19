import mysql.connector


def connection(host, user, password, dbname):
    return mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=dbname
    )


def create_fact_table(cursor):
    cursor.execute('''DROP TABLE IF EXISTS table_fait;CREATE TABLE table_fait (
                            Annee INT,
                            Orientation_politique_gagnante VARCHAR(255),
                            total_vote_extreme_gauche DECIMAL(10,2),
                            total_vote_gauche_radicale DECIMAL(10,2),
                            total_vote_gauche DECIMAL(10,2),
                            total_vote_ecologie DECIMAL(10,2),
                            total_vote_centre DECIMAL(10,2),
                            total_vote_droite DECIMAL(10,2),
                            total_vote_extreme_droite DECIMAL(10,2),
                            total_vote_divers DECIMAL(10,2),
                            moyenne_smic_horaire_brut DECIMAL(10,2),
                            moyenne_ipc DECIMAL(10,2),
                            moyenne_pouvoir_achat_unite_conso DECIMAL(10,2),
                            moyenne_pouvoir_achat_revenu_brute DECIMAL(10,2),
                            moyenne_ensemble_chomage DECIMAL(10,2),
                            taux_insecurite_femmes DECIMAL(10,2),
                            taux_insecurite_hommes DECIMAL(10,2),
                            population_etrangere DECIMAL(10,2),
                            population_immigres DECIMAL(10,2),
                            population_totale_etrangere DECIMAL(10,2),
                            preoccupation_terrorisme DECIMAL(10,2),
                            preoccupation_chomage DECIMAL(10,2),
                            preoccupation_pauvrete DECIMAL(10,2),
                            preoccupation_sante DECIMAL(10,2),
                            preoccupation_deliquance DECIMAL(10,2),
                            preoccupation_racisme DECIMAL(10,2),
                            preoccupation_ecologie DECIMAL(10,2),
                            preoccupation_securite_routiere DECIMAL(10,2),
                            nombre_incivilite DECIMAL(10,2))''')
