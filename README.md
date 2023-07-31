ZOHAYR Abdelmeniym
# HadoopProject

Description du Projet
Ce projet vise à réaliser des opérations de traitement et d'analyse de données en utilisant PySpark et Python.
Les données sont extraites à partir de fichiers CSV et sont manipulées pour répondre à différentes questions sur les exportations et importations de différents pays.



Structure du Projet
Le projet est organisé de la manière suivante :

datasets: Ce répertoire contient les fichiers CSV contenant les données nécessaires pour le projet, tels que output_csv_full.csv, country_classification.csv, etc.

src: Ce répertoire contient le code source du projet.

src/utils: Ce sous-répertoire contient les utilitaires et fonctions réutilisables pour le projet, tels que Utils.py et UserDefinedFunction.py.

src/common: Ce sous-répertoire contient les fichiers contenant les constantes et paramètres d'application, tels que ApplicationProperties.py et Constants.py.

src/main: Ce sous-répertoire contient le code principal du projet, y compris les fonctions pour répondre aux différentes questions.

scripts: Ce répertoire contient les scripts bash pour effectuer différentes opérations sur les données, tels que verify_input.sh, copie_data_input.sh, etc.

workflow: Ce répertoire contient le fichier workflow.xml qui décrit le flux de travail du projet pour lancer les opérations sur les données en utilisant Apache Spark.

output: Ce répertoire contiendra les fichiers de sortie générés par le projet.

Liste des Fonctions pour Chaque Question
question_1(df) : Convertir le format de la date '202206' en '01/06/2022'.
question_2(df) : Extraire l'année à partir de la colonne de date.
question_3(df) : Ajouter le nom du pays en fusionnant avec le fichier de classification des pays.
question_4(df) : Ajouter une colonne details_good (1 si Goods, 0 sinon) en fonction de la colonne product_type.
question_5(df) : Ajouter une colonne details_service (1 si Services, 0 sinon) en fonction de la colonne product_type.
question_6(df) : Classer les pays exportateurs par Services et Goods.
question_7(df) : Classer les pays importateurs par Services et Goods.
question_8(df) : Regroupement par Good.
question_9(df) : Regroupement par Service.
question_10(df) : Liste des services exportés de la France.
Note : Les autres questions et fonctionnalités seront également ajoutées au fur et à mesure du développement.

Auteurs
Ce projet a été réalisé par Zohayr Abdelmeniym.

Remarque
Ce projet est destiné à des fins d'apprentissage et de démonstration et peut être soumis à des modifications et des mises à jour.
