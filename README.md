# ETL com Python

Um projeto de ETL que faz o EXTRACT de arquivos compartilhado em um diretório do Google Drive, faz a TRANSFORM dos arquivos, utlizando Duckdb, em seguida faz o LOAD para um banco de dados PosgreSQL na Cloud.

## Ferramentas:

- Python
- Duckdb
- Gdown
- Sqlalchemy
- Pandas

## Duckdb

Dando uma atenção especial para Duckdb, ele é uma base de dados do tipo OLAP (Online Analytical Processing) que são otimizadas para consultas complexas e análises de grandes volumes de dados, sendo projetadas para fornecer uma plataforma eficaz de análise e tomada de decisões, permitindo que os usuários explorem dados multidimensionais de maneira flexível e rápida.

E uma das coisa mais interessante, é extremamente rápido e permite que você leia e grave dados armazenados em arquivos CSV, JSON e Parquet diretamente, sem precisar carregá-los primeiro no banco de dados.
