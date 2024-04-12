# README - Travaux d'extraction et de transformation de données

| Name   | Date |
|:-------|:---------------|
|ONSI CHHIMA       | September, 2023|

-----

### Resources

- Notebook d'extraction: Extraction_Process_FlightRadar24_API.ipynb
- Notebook de transformation: Transformation_Process_FlightRadar24_API.ipynb
- Notebook de chargement : Load_Process_FlightRadar24_API.ipynb
- Récupération des données brutes: [FlightRadar24](https://www.flightradar24.com)
- Librairie Python pour l'API FlightRadar24: [FlightRadarAPI](https://github.com/JeanExtreme002/FlightRadarAPI)

- Plateforme utlisée: [Databricks](https://databricks.com/)

- un fichier Makefile est fournis pour ficiliter le lancement de toutes les camandes dans les prochains travaux sur le pipeline **CICD**

-----

Ce README explore la raison pour laquelle nous avons choisi Databricks comme plateforme de **lakehouse**, ainsi que notre approche en utilisant le modèle **Bronze-Silver-Gold**. De plus, nous présenterons un modèle MVP expliquant le flux de données entre la couche Bronze de la classe "Extractor" vers les couches Silver et Gold de la classe "Transformer". Ensuite, nous aborderons la mise en place de l'automatisation du job en utilisant **DBX** de Databricks. À la fin, nous fournirons des informations sur la configuration de DBX pour exécuter cette tâche. Nous discuterons également des perspectives futures et des améliorations possibles, telles que le déploiement via **Docker** et l'intégration d'**Airflow** pour une automatisation encore plus poussée.

-----
### Pourquoi Databricks ?
Le choix de Databricks s'est imposé en raison de ses fonctionnalités avancées qui simplifient considérablement le processus de manipulation de données à grande échelle. Cette plateforme offre un environnement Spark convivial, favorisant ainsi l'approfondissement de la compréhension de Spark. De plus, Databricks étant une plateforme cloud, toute la complexité liée à la configuration de l'infrastructure est éliminée. Enfin, il est à noter que Databricks jouit d'une grande popularité, ce qui se traduit par un accès facile à de nombreuses ressources susceptibles de venir en aide en cas de difficultés rencontrées.

-----

###Architecture en Médaillon
L'architecture en médaillon est une approche de conception des données visant à organiser de manière logique les données au sein d'un lakehouse, en les améliorant progressivement à mesure qu'elles traversent différentes couches : **Bronze, Silver et Gold**.
L'application de cette approche en médaillon m'a personnellement permis de développer une meilleure compréhension des données. Au fil du processus, j'ai souvent dû revenir en arrière pour résoudre des problèmes liés à l'extraction de données brutes, ce qui m'a poussé à affiner mes compétences dans ce domaine.
 De plus, j'ai saisi l'importance des transformations légères appliquées lors du passage des données de la couche Bronze à la couche Silver. Cela a renforcé ma capacité à gérer et à améliorer la qualité des données tout au long du flux de travail, tout en maintenant une approche incrémentielle et flexible.

-----
### Modèle MVP

Le modèle MVP est une représentation simplifiée du flux de données entre les couches Bronze, Silver et Gold, qui comprend également l'intégration de **Docker** et le stockage de fichiers CSV/JSON dans **MongoDB**. Il s'agit d'une version simplifiée du flux de données réel, qui est plus complexe et qui comprend plusieurs étapes supplémentaires. Le modèle MVP est présenté ci-dessous :

- **Bronze** : Extraction des données brutes à partir de FlightRadar24 et stockage dans un dossier de stockage sur le DBFS de Databricks. Cette étape est orchestrée à l'aide de Docker pour garantir un environnement isolé et cohérent.

- **Silver** : Transformation des données brutes en un format tabulaire et stockage dans un dossier de stockage sur le DBFS de Databricks. Docker est également utilisé ici pour assurer la reproductibilité des transformations.

- **Gold** : Transformation des données tabulaires en un format CSV. Les fichiers CSV résultants sont stockés dans un dossier de stockage sur le DBFS de Databricks. De plus, une liaison avec MongoDB est établie pour stocker les données sous forme de documents JSON. Cette intégration avec MongoDB permet de conserver une copie des données dans une base de données **NoSQL**, offrant une flexibilité supplémentaire pour les requêtes et l'analyse.

Ainsi, chaque étape du pipeline ETL MVP, de l'extraction à la transformation en passant par le chargement, est associée à Docker pour garantir la cohérence et à MongoDB pour stocker les données dans un format JSON pour une utilisation future.

-----
![Schéma du modèle MVP](https://i.ibb.co/6ZfFzsg/Flight-Radar-API.png)

-----
### Automatisation du job

L'automatisation du job est réalisée à l'aide de **DBX**, une fonctionnalité de Databricks qui permet d'exécuter des notebooks à l'aide d'un script Python. Le script Python est exécuté à l'aide d'un **cron job** qui est configuré pour s'exécuter à une heure précise. 

La configuration du fichier JSON pour assurer l'automatisation du job est présentée ci-dessous :
```{
   "default":{
      "jobs":[
         {
            "name":"airlines_pipeline",
            "schedule":{
               "quartz_cron_expression":"0 0 7 * * ?",
               "timezone_id":"Europe/Brussels",
               "pause_status":"UNPAUSED"
            },
            "email_notifications":{
               "on_start":[
                  "onsi.chhima@gmail.com"
               ],
               "on_success":[
                  "onsi.chhima@gmail.com"
               ],
               "on_failure":[
                  "onsi.chhima@gmail.com"
               ]
            },
            "job_clusters":[
               {
                  "job_cluster_key":"shared-cluster",
                  "new_cluster":{
                     "spark_version":"10.4.x-scala2.12",
                     "azure_attributes":{
                        "availability":"SPOT_WITH_FALLBACK_AZURE",
                        "first_on_demand":1,
                        "spot_bid_max_price":-1
                     },
                     "node_type_id":"Standard_DS4_v2",
                     "driver_node_type_id":"Standard_DS5_v2",
                     "enable_elastic_disk":true,
                     "autoscale":{
                        "min_workers":2,
                        "max_workers":10
                     }
                  }
               }
            ],
            "max_concurrent_runs":1,
            "tasks":[
               {
                  "task_key":"extract",
                  "description":"Extract different table",
                  "job_cluster_key":"shared-cluster",
                  "libraries":[
                     
                  ],
                  "max_retries":0,
                  "spark_python_task":{
                     "python_file":"extract.py",
                     "parameters":[
                        
                     ]
                  }
               },
               {
                  "task_key":"transform",
                  "description":"Tansform all data for all questions",
                  "job_cluster_key":"shared-cluster",
                  "libraries":[
                     
                  ],
                  "email_notifications":{
                     "on_start":[
                        
                     ],
                     "on_success":[
                        
                     ],
                     "on_failure":[
                        
                     ]
                  },
                  "max_retries":0,
                  "spark_python_task":{
                     "python_file":"sefa/anomaly_detector/jobs/autoloader/transform.py",
                     "parameters":[
                        
                     ]
                  },
                  "depends_on":[
                     {
                        "task_key":"extract"
                     }
                  ]
               }
            ]
         }
      ]
   }
}
```
Afin de deployer ce Job , on peut utiliser cette commande :
```
dbx deploy --jobs JOB_NAME  --deployment-file conf/job_deployment.json -e target_environement
```

Pour lancer le job, on peut utiliser cette commande :
```
dbx launch --job JOB_NAME  --deployment-file conf/job_deployment.json -e target_environement
```
-----
### Les améliorations possibles
Bien que le pipeline ETL actuel ait été conçu pour être efficace et fiable, il existe toujours des opportunités pour l'améliorer et le rendre encore plus robuste. Voici quelques axes d'amélioration à considérer :

- **Contrôle des Logs avec Apache Airflow** : L'utilisation d'Apache Airflow pourrait permettre la mise en place d'un système de journalisation avancé qui capture et stocke les journaux de manière centralisée. Cela faciliterait la surveillance, le débogage et la gestion des erreurs.

- **Documentation Détaillée** : La création d'une documentation détaillée qui reflète les choix de conception et les étapes du pipeline serait essentielle. Cela aiderait les futurs utilisateurs et collaborateurs à comprendre le fonctionnement du pipeline, à diagnostiquer les problèmes et à apporter des améliorations.

- **Utilisation de Docker pour le Déploiement** : L'intégration de Docker dans le processus pourrait être envisagée pour créer des conteneurs isolés pour les notebooks, facilitant ainsi le déploiement sur différentes plates-formes et assurant la cohérence de l'environnement d'exécution.

- **Utilisation d'une Base de Données** : Au lieu de stocker des fichiers CSV sur le DBFS de Databricks, il pourrait être envisagé de les stocker dans une base de données telle que MongoDB. Cela permettrait une gestion plus efficace des données, une évolutivité accrue et une meilleure intégration avec d'autres applications.

- **Modélisation de Données Évoluée** : L'exploration de modèles de données plus avancés, tels que les schémas en étoile ou les data marts, pour la couche Gold, pourrait être envisagée afin d'optimiser les performances des requêtes analytiques.

En mettant en œuvre ces améliorations potentielles, il serait possible de renforcer la fiabilité, l'efficacité et la maintenabilité du pipeline ETL, tout en ouvrant la voie à une utilisation plus avancée des technologies telles qu'Apache Airflow et Docker. Cela permettrait de travailler de manière plus productive et d'obtenir des informations plus précieuses à partir des données, tout en considérant Apache Airflow comme une alternative à DBX.

-----
### Remarque sur la Non-Réponse

Il est important de noter que la question **5** pourra rester sans réponse en raison du manque d'informations ou de données manquantes dans l'extraction de l'API. En particulier, les informations concernant le modèle et le constructeur d'un avion peuvent être absentes dans les données brutes fournies par l'API.

Supposons qu'on a réussi à extraire la donnée Model , le début de la requêtre peut être comme le suivant : 

```
dataframe_model = flights_df.select("id", "origin_airport_iata", "model")\
                                 .na.drop(subset=['model'])\
                                 .filter(F.col('model') != 'NaN')
    airports_origin = airports_df.select("iata", "continent")\
                                 .toDF(*["origin_airport_iata", "origin_continent"])\
                                 .filter(F.col('continent') != 'NaN')    
    model_by_continent = flights_model_df.join(airports_origin, "origin_airport_iata", how='inner')
    model_by_continent = model_by_continent.groupby(["origin_continent", "model"]).agg(F.count("id").alias('active_flights'))    
```