/*


On va devoir rassembler les infos concernant les clients dans une seule table. 

*/
DROP TABLE achats

CREATE TABLE achats (
    achat_date_heure DATETIME,
    FK_product_id INT,
    FK_categorie_id BIGINT,
    categorie_code NVARCHAR(MAX),
    marque NVARCHAR(MAX),
    prix MONEY,
    FK_session_id NVARCHAR(MAX),
    FK_client_id INT,
    client_prenom NVARCHAR(MAX),
    client_nom NVARCHAR(MAX),
    client_cp NVARCHAR(MAX)
)


BULK INSERT achats
FROM '/var/opt/mssql/data/purchases.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
)

SELECT *
FROM dbo.achats
-- lol ça marche 

SELECT DISTINCT FK_client_id
FROM dbo.achats


-- création de la deuxième table
CREATE TABLE clients (
    client_id INT,
    client_prenom NVARCHAR(MAX),
    client_nom NVARCHAR(MAX),
    client_cp NVARCHAR(MAX),
    client_age INT
)

BULK INSERT clients
FROM '/var/opt/mssql/data/customer_database.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
)
-- création de la 3ème table
CREATE TABLE export_crm (
    client_id INT,
    client_prenom NVARCHAR(MAX),
    client_nom NVARCHAR(MAX),
    client_cp NVARCHAR(MAX),
    client_age INT
)

BULK INSERT export_crm
FROM '/var/opt/mssql/data/crm_export.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
)


-- dans la table achats, on a des événements achat. Les attributs qui concernent 
-- l'identité des acheteurs sont les derniers. Soit ils sont enregistrés dans la plateforme
-- et on a un id (FK_client_id). Soit ils ne sont pas enregistrés et alors ils ont rempli 
-- les champs prénom, nom et cp. 
SELECT *
FROM dbo.achats
WHERE FK_client_id LIKE 2505

SELECT *
FROM dbo.clients

SELECT *
FROM dbo.export_crm
-- mes tables raw sont prêtes

-- on jette un coup d'oeil sur les dates. On a un mois de données (oct 2022). 
-- c'est relativement stable sur le mois, pas d'explosion
SELECT DAY(achat_date_heure), COUNT(*)
FROM dbo.achats
GROUP BY DAY(achat_date_heure)
ORDER BY DAY(achat_date_heure) ASC
-- on jette un oeil sur les marques. 
-- Samsung, Apple, Xiammoi, Huawei... téléphonie avant tout ?
SELECT marque, COUNT(*)
FROM dbo.achats
GROUP BY marque
ORDER BY COUNT(marque) DESC
-- est-ce qu'il y a des clients compulsifs ? 
SELECT FK_client_id, COUNT(*)
FROM dbo.achats
GROUP BY FK_client_id
ORDER BY COUNT(FK_client_id) DESC
-- et ils dépensent beaucoup ?
SELECT FK_client_id, SUM(prix)
FROM dbo.achats
GROUP BY FK_client_id
ORDER BY SUM(prix) DESC
-- j'ai quand même un client à +80k ! 
-- j'ai aussi +5Mi en guest ! sur 21Mi d'achats...
-- belle startup ^^
SELECT SUM(prix)
FROM dbo.achats


-- bon, je vais faire une table... avec un focus sur les clients et qui gardera 
-- toute mon info raw. Y compris l'origine de la table.
-- Est-ce qu'on garderait une colonne dans laquelle j'aurais le nombre d'achats ?
-- Oui !
-- et les dépenses... aussi !
SELECT FK_client_id, client_prenom, client_nom, client_cp, NULL, COUNT(achat_date_heure), SUM(prix), 0, 0, 1
FROM dbo.achats
GROUP BY FK_client_id, client_prenom, client_nom, client_cp
ORDER BY COUNT(achat_date_heure) DESC
-- cette query est nickel pour peupler ma table de travail

DROP TABLE table_de_travail
-- créons la table sur laquelle on pourra partir à la chasse aux doublons 
CREATE TABLE table_de_travail (
    client_id INT,
    client_prenom NVARCHAR(MAX),
    client_nom NVARCHAR(MAX),
    client_cp NVARCHAR(MAX),
    client_age INT,
    client_nbre_achats INT,
    client_depense MONEY,
    client_est_dans_crm_export INT,
    client_est_dans_clients INT,
    client_est_dans_achats INT
)


-- On va peupler cette table avec la table achats
INSERT INTO table_de_travail(
        client_id,
        client_prenom,
        client_nom,
        client_cp,
        client_age,
        client_nbre_achats,
        client_depense,
        client_est_dans_crm_export,
        client_est_dans_clients,
        client_est_dans_achats
        ) 
    SELECT 
        FK_client_id, 
        client_prenom, 
        client_nom, 
        client_cp, 
        NULL, 
        COUNT(achat_date_heure), 
        SUM(prix), 
        0, 
        0, 
        COUNT(achat_date_heure)
    FROM dbo.achats
    GROUP BY FK_client_id, client_prenom, client_nom, client_cp


SELECT *
FROM table_de_travail
WHERE client_id LIKE 2505
--ORDER BY client_depense DESC
-- ok

-- maintenant avec la table clients et la table export_crm
-- on vérifie que les entéres sont uniques dans les tables
SELECT COUNT(*)
FROM dbo.clients
GROUP BY client_id
HAVING COUNT(client_id) > 1

SELECT COUNT(*)
FROM dbo.export_crm
GROUP BY client_id
HAVING COUNT(client_id) > 1

SELECT *
FROM dbo.export_crm
ORDER BY client_id ASC
-- ok

INSERT INTO table_de_travail (
        client_id,
        client_prenom,
        client_nom,
        client_cp,
        client_age,
        client_nbre_achats,
        client_depense,
        client_est_dans_crm_export,
        client_est_dans_clients,
        client_est_dans_achats
        ) 
    SELECT 
        client_id,
        client_prenom,
        client_nom,
        client_cp,
        client_age,
        NULL,
        NULL,
        0,
        1,
        0
    FROM dbo.clients

INSERT INTO table_de_travail (
        client_id,
        client_prenom,
        client_nom,
        client_cp,
        client_age,
        client_nbre_achats,
        client_depense,
        client_est_dans_crm_export,
        client_est_dans_clients,
        client_est_dans_achats
        ) 
    SELECT 
        client_id,
        client_prenom,
        client_nom,
        client_cp,
        client_age,
        NULL,
        NULL,
        1,
        0,
        0
    FROM dbo.export_crm

-- simple. 23k puis 7k ajoutées. 

SELECT COUNT(*)
FROM dbo.table_de_travail
WHERE client_est_dans_clients LIKE 1

SELECT COUNT(*)
FROM dbo.table_de_travail
WHERE client_est_dans_crm_export LIKE 1

SELECT COUNT(*)
FROM dbo.table_de_travail
-- 64k entrées... avec... des doublons !!!
-- la chasse est ouverte !

SELECT *
FROM dbo.table_de_travail
WHERE client_prenom IS NOT NULL
ORDER BY client_nom, client_prenom, client_id

/* Listons les hypothèses de doublon :
1. même id, même prénom, même nom, même cp, même âge / tables origines différentes (ex: Stecca Accen)
2. même prénom, même nom, même cp, même âge... même table ! / id différent ! (ex: Adam Adams)
3. même prénom, même nom, même cp... 1 des deux ages IS NULL ET 1 des deux id IS NULL 
    (ex: ALicia Adams de NW347EE, on dirait qu'elle a acheté une fois en guest)
... et d'autres?.... on verra.
*/


-- faisons une sauvegarde de ma table de travail. Au cas où

CREATE TABLE table_de_travail_sauvegarde (
    client_id INT,
    client_prenom NVARCHAR(MAX),
    client_nom NVARCHAR(MAX),
    client_cp NVARCHAR(MAX),
    client_age INT,
    client_nbre_achats INT,
    client_depense MONEY,
    client_est_dans_crm_export INT,
    client_est_dans_clients INT,
    client_est_dans_achats INT
)

INSERT INTO table_de_travail_sauvegarde 
    SELECT *
FROM dbo.table_de_travail




-- à force de regarder le dataset, je propose de considérer que si les client_prenom, nom, cp et age 
-- de 2 entrées sont les mêmes, c'est un doublon. Même si l'id est différent. La probabilité 
-- qu'il s'agisse de personnes différentes avec ces 4 entrées identiques est très faible. 
-- on pourrait approfondir cette hypothèse avec des données population. Mais on laisse ça pour plus tard.
-- donc, hypothèse 1 et 2, on va les attaquer de front. 

SELECT client_prenom, client_nom, client_cp, client_age, COUNT(*)
FROM dbo.table_de_travail
GROUP BY client_prenom, client_nom, client_cp, client_age
HAVING COUNT(*) = 2
ORDER BY COUNT(*) DESC
-- et donc si on change le HAVING COUNT(*) par 2, puis 3, puis 4, puis 5
-- on va pouvoir mesurer le nbre de doublon sur une identité prénom, nom, cp et age. 
-- 5 => 0 pas de quintuplé !
-- 4 => 181 quadruplés ! 
-- 3 => 1.420 triplés 
-- 2 => 6.493 jumeaux
-- on va ajouter 2 colonnes pour travailler : une avec le nbre de doublon (1, 2, 3 ou 4) et une avec le rang 
-- d'un doublon (si on a des triplés, on aura 1 entrée de rang 1, une entrée de rang 2 et une entrée de rang 3)
-- on gardera nos entrées de rang 1 et on supprimera les entrées de rang > 1, mais on veut garder les 
-- infos de leurs jumeaux/triplés.... soit les tables d'origine... mais aussi un id différent le cas échéant. 
-- c'est parti !


DROP TABLE dbo.table_de_travail_temp

GO

CREATE TABLE table_de_travail_temp (
    client_id INT,
    bis_client_id INT,
    client_prenom NVARCHAR(MAX),
    client_nom NVARCHAR(MAX),
    client_cp NVARCHAR(MAX),
    client_age INT,
    client_nbre_achats INT,
    client_depense MONEY,
    client_est_dans_crm_export INT,
    client_est_dans_clients INT,
    client_est_dans_achats INT,
    rang_client_id INT,
    nbre_doublon INT
)
GO
INSERT INTO dbo.table_de_travail_temp
    SELECT 
        client_id,
        NULL,
        client_prenom,
        client_nom,
        client_cp,
        client_age,
        client_nbre_achats,
        client_depense,
        client_est_dans_crm_export,
        client_est_dans_clients,
        client_est_dans_achats,
        IIF((client_id IS NOT NULL AND client_prenom IS NOT NULL), ROW_NUMBER() OVER (PARTITION BY client_prenom, client_nom, client_cp, client_age ORDER BY client_est_dans_clients), NULL),
        IIF((client_id IS NOT NULL AND client_prenom IS NOT NULL), COUNT(*) OVER (PARTITION BY client_prenom, client_nom, client_cp, client_age), NULL)
    FROM dbo.table_de_travail

GO
-- on a nos 2 colonnes qui vont nous permettre de travailler. Et on a CONVERT car on va additionner comme ça
-- on garde toute l'info d'origine des doublons. 
-- faisons quelques vérifications
-- SELECT COUNT(*)
-- FROM dbo.table_de_travail_temp
--WHERE rang_client_id = 2


-- on va rentrer dans un boucle qui va examiner du rang 2 au rang 4 et rappatrier les infos par une 
-- self-join de ma table avec elle-même filtrée sur les doublons. 
-- on garde toute la puissance de sql avec cette approche ! 

DECLARE @compte_rang INT = 2

WHILE @compte_rang <= 4
BEGIN

    WITH table_doublons AS (
        SELECT * 
        FROM dbo.table_de_travail_temp
        WHERE client_prenom IS NOT NULL AND nbre_doublon > 1 AND rang_client_id = @compte_rang
        -- ORDER BY client_prenom, client_nom, client_cp, client_age
    )

    UPDATE ttt
    SET ttt.bis_client_id = IIF(ttt.client_id NOT LIKE td.client_id, td.client_id, NULL),
        ttt.client_nbre_achats = ttt.client_nbre_achats + td.client_nbre_achats,
        ttt.client_depense = ttt.client_depense + td.client_depense,
        ttt.client_est_dans_achats = ttt.client_est_dans_achats + td.client_est_dans_achats,
        ttt.client_est_dans_clients = ttt.client_est_dans_clients + td.client_est_dans_clients,
        ttt.client_est_dans_crm_export = ttt.client_est_dans_crm_export + td.client_est_dans_crm_export
    FROM dbo.table_de_travail_temp ttt 
    JOIN table_doublons td
        ON ttt.client_prenom = td.client_prenom
        AND ttt.client_nom = td.client_nom
        AND ttt.client_cp = td.client_cp
        AND ttt.client_age = td.client_age
    WHERE ttt.rang_client_id = 1 

    SET @compte_rang = @compte_rang + 1
END 

-- on voit que chaque opération a affecté 
-- 2 => 8094 lignes 
-- 3 => 1601 lignes 
-- 4 => 181 lignes
-- ce qui nous rend heureux ! car ça correspond exactement aux nombres de jumeaux 
-- triplés et quadruplés qu'on avait identifié (il faut ajouter les quadruplés aux triplés
-- pour réconcilier les résultats).
-- maintenant qu'on est heureux, on va pouvoir DELETE. 
-- en fait on va créer une table tempbis

SELECT * FROM dbo.table_de_travail_temp
WHERE rang_client_id IS NULL
ORDER BY client_prenom, client_nom, client_cp, client_age

-- faisons quelques vérifications avant de delete mes doublons. 
SELECT COUNT(*) FROM dbo.table_de_travail_temp
WHERE rang_client_id = 1 OR client_prenom IS NULL

SELECT COUNT(*) FROM dbo.table_de_travail_temp

DROP TABLE table_de_travail_temp_bis
GO
CREATE TABLE table_de_travail_temp_bis (
    client_id INT,
    bis_client_id INT,
    client_prenom NVARCHAR(MAX),
    client_nom NVARCHAR(MAX),
    client_cp NVARCHAR(MAX),
    client_age INT,
    client_nbre_achats INT,
    client_depense MONEY,
    client_est_dans_crm_export INT,
    client_est_dans_clients INT,
    client_est_dans_achats INT,
    rang_client_id INT,
    nbre_doublon INT
)
GO
INSERT INTO table_de_travail_temp_bis 
    SELECT * FROM dbo.table_de_travail_temp
    WHERE rang_client_id = 1 OR client_prenom IS NULL OR client_id IS NULL

SELECT COUNT(*)
FROM dbo.table_de_travail_temp_bis
-- il nous reste 54.686 entrées ! C'est ce qu'on attendait !
-- voyons tous les NULL maintenant....

SELECT *
FROM dbo.table_de_travail_temp_bis
--WHERE nbre_doublon < 5
ORDER BY client_prenom, client_nom, client_cp, client_age
-- j'ai 24.961 lignes avec des id sans prenom, nom, etc... de la table achat. 

UPDATE dbo.table_de_travail_temp_bis
SET nbre_doublon = NULL
WHERE rang_client_id >=5

SELECT *
FROM dbo.table_de_travail
WHERE client_id LIKE 2505
