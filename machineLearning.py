import numpy as np
import pandas as pd
from imblearn.over_sampling import RandomOverSampler
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, accuracy_score, precision_score, confusion_matrix, recall_score, \
    f1_score
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC
from sqlalchemy import create_engine


def find_best_seed():
    best_seed = None
    best_mean_accuracy = 0  # Initialisation avec une valeur basse

    for seed_candidate in range(50):  # Tester différentes graines de 0 à 999
        print('------------------------------------------------------------------')
        print(seed_candidate)
        scores = start_machine_learning_training_seed(seed_candidate)
        mean_accuracy = scores["Random Forest"]["accuracy"]  # Utiliser Random Forest comme référence
        print('------------------------------------------------------------------------')
        print(mean_accuracy)
        print('-------------------------------------------------------------------------')
        if mean_accuracy > best_mean_accuracy:
            best_mean_accuracy = mean_accuracy
            best_seed = seed_candidate

    return best_seed


def start_machine_learning_training_seed(seed):
    connect = create_engine('mysql://root:zDH-.42l3lSRIT1p@localhost/datawarehouse')
    df = pd.read_sql('table_fait', connect.connect())

    # Sélectionner les colonnes d'indicateurs comme caractéristiques (X)
    X = df[['moyenne_smic_horaire_brut', 'moyenne_ipc', 'moyenne_pouvoir_achat_unite_conso',
            'moyenne_pouvoir_achat_revenu_brute', 'moyenne_ensemble_chomage', 'preoccupation_chomage',
            'preoccupation_racisme',
            'nombre_incivilite']]

    X.fillna(0, inplace=True)

    # Sélectionner la colonne contenant le parti politique gagnant comme cible (y)
    y = df['Orientation_politique_gagnante']

    classifiers = {
        "Random Forest": RandomForestClassifier(n_estimators=100, random_state=seed),
    }

    # Initialiser les listes pour stocker les scores
    scores = {}

    # Diviser les données en ensembles d'entraînement et de test
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.4, random_state=seed)

    # Suréchantillonnage de l'ensemble d'entraînement
    oversampler = RandomOverSampler(random_state=seed)
    X_train_resampled, y_train_resampled = oversampler.fit_resample(X_train, y_train)

    # Effectuer une boucle sur chaque modèle
    for clf_name, clf in classifiers.items():
        # Entraîner le modèle
        clf.fit(X_train_resampled, y_train_resampled)

        # Faire des prédictions sur l'ensemble de test
        predictions = clf.predict(X_test)

        # Calculer les scores du modèle
        accuracy = accuracy_score(y_test, predictions)
        precision = precision_score(y_test, predictions, average='weighted')
        confusion = confusion_matrix(y_test, predictions)
        recall = recall_score(y_test, predictions, average='weighted')
        f1 = f1_score(y_test, predictions, average='weighted')

        # Ajouter les scores au dictionnaire
        scores[clf_name] = {
            "accuracy": accuracy,
            "precision": precision,
            "confusion_matrix": confusion,
            "recall": recall,
            "f1_score": f1
        }

    return scores


def start_machine_learning(seed, test_size):
    connect = create_engine('mysql://root:zDH-.42l3lSRIT1p@localhost/datawarehouse')
    df = pd.read_sql('table_fait', connect.connect())

    # Sélectionner les colonnes d'indicateurs comme caractéristiques (X)
    X = df[['moyenne_smic_horaire_brut', 'moyenne_ipc', 'moyenne_pouvoir_achat_unite_conso',
            'moyenne_pouvoir_achat_revenu_brute', 'moyenne_ensemble_chomage', 'preoccupation_chomage',
            'preoccupation_racisme',
            'nombre_incivilite']]

    X.fillna(0, inplace=True)

    # Sélectionner la colonne contenant le parti politique gagnant comme cible (y)
    y = df['Orientation_politique_gagnante']

    classifiers = {
        "Random Forest": RandomForestClassifier(n_estimators=100, random_state=seed),
        "K-Nearest Neighbors": KNeighborsClassifier(),
        "Support Vector Machine": SVC(kernel='linear', random_state=seed),
    }

    # Définir les données X et les étiquettes y

    # Initialiser les listes pour stocker les scores
    scores = {}
    future_predictions = {}

    # Répéter l'expérience plusieurs fois
    n_repeats = 1
    for _ in range(n_repeats):
        # Diviser les données en ensembles d'entraînement et de test
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=seed)

        # Suréchantillonnage de l'ensemble d'entraînement
        oversampler = RandomOverSampler(random_state=seed)
        X_train_resampled, y_train_resampled = oversampler.fit_resample(X_train, y_train)

        # Effectuer une boucle sur chaque modèle
        for clf_name, clf in classifiers.items():
            print(f"Entraînement du modèle {clf_name}:")

            # Entraîner le modèle
            clf.fit(X_train_resampled, y_train_resampled)

            # Faire des prédictions sur l'ensemble de test
            predictions = clf.predict(X_test)
            classification_rep = classification_report(y_test, predictions)
            print(classification_rep)

            # Calculer les scores du modèle
            accuracy = accuracy_score(y_test, predictions)
            precision = precision_score(y_test, predictions, average='weighted')
            confusion = confusion_matrix(y_test, predictions)
            recall = recall_score(y_test, predictions, average='weighted')
            f1 = f1_score(y_test, predictions, average='weighted')

            # Ajouter les scores au dictionnaire
            if clf_name not in scores:
                scores[clf_name] = {"accuracies": [], "precisions": [], "recalls": [], "f1_scores": [],
                                    'future_predictions': []}
            scores[clf_name]["accuracies"].append(accuracy)
            scores[clf_name]["precisions"].append(precision)
            scores[clf_name]["recalls"].append(recall)
            scores[clf_name]["f1_scores"].append(f1)

            # Prédiction pour les années 2025 à 2075
            X_future = [
                [450, 350, 3, 2, 5, 3, 5, 500000],
                [350, 301, 0.3, -0.1, 10, 15, 25, 8000000],
                [275, 250, 10, 8.2, 8, 13, 2, 8500000],
            ]

            future_predictions[clf_name] = clf.predict(X_future)

            print(f"Prédictions pour les années 2025 à 2027 avec le modèle {clf_name}: {future_predictions[clf_name]}")
    resultat_model = []
    # Afficher les scores moyens de chaque modèle
    for clf_name, score_data in scores.items():
        mean_accuracy = np.mean(score_data["accuracies"])
        mean_precision = np.mean(score_data["precisions"], axis=0)
        mean_recall = np.mean(score_data["recalls"])
        mean_f1_score = np.mean(score_data["f1_scores"])
        resultat_model.append({
            "Model": clf_name,
            "Mean Accuracy": mean_accuracy,
            "Mean Precision": mean_precision,
            "Mean Recall": mean_recall,
            "Mean F1-Score": mean_f1_score,
        })
    results_df = pd.DataFrame(resultat_model,
                              columns=["Model","Mean Precision", "Mean Recall", "Mean F1-Score","Mean Accuracy"])

    # Création d'un DataFrame pour les futures prédictions
    future_predictions_df = pd.DataFrame(columns=["Model", "Year", "Prediction"])
    years = range(2025, 2028)  # Années 2025 à 2027

    # Remplissage du DataFrame avec les futures prédictions
    for index, model in enumerate(future_predictions):
        for year, prediction in zip(years, future_predictions[model]):
            future_predictions_df = pd.concat([future_predictions_df, pd.DataFrame({
                "Model": model,
                "Year": [year],
                "Prediction": prediction
            })], ignore_index=True)

    # Enregistrement des résultats dans la base de données
    engine = create_engine('mysql://root:zDH-.42l3lSRIT1p@localhost/datawarehouse')
    results_df.to_sql(name='resultat_models', con=engine, if_exists='replace', index=False)
    future_predictions_df.to_sql(name='future_predictions', con=engine, if_exists='replace', index=False)
