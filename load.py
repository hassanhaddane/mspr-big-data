from sqlalchemy import create_engine


def loadingData(dataframe,nom_table):
    df_pandas = dataframe.toPandas()

    # Connexion à la base de données
    engine = create_engine('mysql://root:zDH-.42l3lSRIT1p@localhost/datawarehouse')

    # Création de la table SQL
    df_pandas.to_sql(name=nom_table, con=engine, if_exists='replace', index=False)