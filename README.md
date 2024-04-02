# Analyse de Sentiment de Commentaires de Films

Ce projet utilise diverses technologies pour récupérer des commentaires et des informations sur plusieurs films, les analyser à l'aide d'un modèle d'analyse de sentiment, et afficher les résultats via un dashboard sur Kibana.
Technologies utilisées

- Pyspark: Pour le traitement des données en parallèle et la manipulation de gros volumes de données.
- Elasticsearch & Kibana: Pour stocker et visualiser les données récupérées.
- Kafka: Pour la mise en file d'attente des données provenant de différentes sources.
- TensorFlow: Pour entraîner le modèle d'analyse de sentiment.
- Twitter API: Pour récupérer les commentaires sur les films à partir de Twitter.
- Beautiful Soup: Pour le scraping des données à partir de différentes sources web.
- Pandas: Pour la manipulation et l'analyse des données.

## Objectifs du Projet

- Récupération des données: Collecter les commentaires et les informations sur les films à partir de différentes sources telles que Twitter et des sites web de critiques de films.

- Stockage des données: Stocker les données récupérées dans Elasticsearch pour une gestion facile et une analyse ultérieure.

- Traitement des données en temps réel: Utiliser Kafka pour traiter les données en temps réel et les rendre disponibles pour l'analyse.

- Entraînement du modèle d'analyse de sentiment: Utiliser TensorFlow pour entraîner un modèle d'analyse de sentiment capable de classifier les commentaires des films en positifs, négatifs ou neutres.

- Création du dashboard sur Kibana: Utiliser Kibana pour visualiser les résultats de l'analyse de sentiment sous forme de graphiques interactifs et de tableaux de bord.
