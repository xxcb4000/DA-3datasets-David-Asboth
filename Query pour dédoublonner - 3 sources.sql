/*

Pour rappel, les infos proviennent de 3 fichiers plats csv. Nous travaillons en t-sql. 
La première étape va être de bulk insert les fichiers sur mon serveur. 
On le fait un peu à la hache avec des NVARCHAR(max), on peut bien entendu changer les types a posteriori pour optimiser éventuellement. 
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
-- on vérifie que les entrées sont uniques dans les tables
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
-- on pourrait approfondir cette hypothèse avec des données population/stat de nom et prénom. Mais on laisse ça pour plus tard.
-- donc, hypothèse 1 et 2, on va les attaquer de front. 

SELECT client_prenom, client_nom, client_cp, client_age, COUNT(*)
FROM dbo.table_de_travail
GROUP BY client_prenom, client_nom, client_cp, client_age
HAVING COUNT(*) = 2
ORDER BY COUNT(*) DESC
-- et donc si on change le HAVING COUNT(*) par 2, puis 3, puis 4, puis 5
-- on va pouvoir mesurer le nbre de doublons sur une identité prénom, nom, cp et age. 
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
-- on a nos 2 colonnes qui vont nous permettre de travailler. 
-- on garde toute l'info d'origine des doublons. 
-- faisons quelques vérifications
-- SELECT COUNT(*)
-- FROM dbo.table_de_travail_temp
--WHERE rang_client_id = 2


-- on va rentrer dans un boucle qui va examiner du rang 2 au rang 4 et rappatrier les infos sur l'entrée de rang 1 par une 
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
-- triplés et quadruplés qu'on avait identifié (il faut ajouter les quadruplés aux triplés, etc.
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
WHERE nbre_doublon IS NULL
ORDER BY client_prenom, client_nom, client_cp, client_age
-- j'ai 24.961 lignes avec des id sans prenom, nom, etc... de la table achat. 

--
/*
PAUSE... ici j'ai encore un peu chipoté.
Pourquoi? Parce que j'ai fait un update qui me semblait renir la route sur les doublons de 
ma dbo.achats qui n'avaient pas de client_id mais avaient un prenom, nom et cp qu'on
retrouvait dans la partie de ma table déjà triée. 
MAIS !!!! 
MAIS.... j'ai essayé de vérifier que mon update affectait le même nombre de row 
que une query avec un intersect / except... 
Mon update affectait 2.192 lignes.... mais mon intersect en comptait 2.174....
et un except pour sortir les doublons des 8.300 sans id => 6.126... + 2.174 = 8.300 c'était ok!
Donc il y avait un souci avec mon update...
MAIS !!!
Mais si je faisais un except dans l'autre sens (il n'est pas commutatif)... j'arrivais à 19.094.
+ 2.174 != 21.425 !! donc... j'étais dans la panade. 
Puis... je me suis rappelé qu'un intersect ne prenait pas en compte les doublons....
Etait-il posisble que... certaines entrées sur la base du prenom, nom et cp soient présentes plus
que 2 fois..... et bien oui !!! 21 d'entre elles sont identiques sur prenom, nom et cp mais diffèrent
par l'âge ! je ne les ai donc pas considérées comme doublon dans le tri précécent !
Mais... à laquelle des 2 entrées de ma table précédente vais-je affecter les info de l'entrée que j'ai
dans ma table achat...? je n'ai pas l'age... du coup... j'ai décidé de sortir les 21 entrées de la table
achats car je ne pouvais les affecter à aucune des 2 entrées sauf de façon arbitraire. 
*/

/*
UPDATE ttb1
SET ttb1.client_nbre_achats = ttb2.client_nbre_achats,
    ttb1.client_depense = ttb2.client_depense,
    ttb1.client_est_dans_achats = ttb2.client_est_dans_achats,
    ttb1.nbre_doublon = ttb1.nbre_doublon + ttb2.client_est_dans_achats
FROM dbo.table_de_travail_temp_bis ttb1 
JOIN dbo.table_de_travail_temp_bis ttb2
    ON ttb1.client_prenom = ttb2.client_prenom 
    AND ttb1.client_nom = ttb2.client_nom 
    AND ttb1.client_cp = ttb2.client_cp
WHERE ttb1.client_id IS NOT NULL AND ttb1.client_prenom IS NOT NULL AND ttb2.client_id IS NULL

SELECT COUNT(*)
FROM (
SELECT client_prenom, client_nom, client_cp
FROM dbo.table_de_travail_temp_bis
WHERE client_id IS NULL
INTERSECT
SELECT client_prenom, client_nom, client_cp
FROM dbo.table_de_travail_temp_bis
WHERE client_id IS NOT NULL AND client_prenom IS NOT NULL
) AS table_intersect

SELECT COUNT(*)
FROM (
SELECT client_prenom, client_nom, client_cp
FROM dbo.table_de_travail_temp_bis
WHERE client_id IS NOT NULL AND client_prenom IS NOT NULL
EXCEPT
SELECT client_prenom, client_nom, client_cp
FROM dbo.table_de_travail_temp_bis
WHERE client_id IS NULL
) tout_sauf_intersec

SELECT COUNT(*)
FROM (
SELECT client_prenom, client_nom, client_cp
FROM dbo.table_de_travail_temp_bis
WHERE client_id IS NULL
EXCEPT
SELECT client_prenom, client_nom, client_cp
FROM dbo.table_de_travail_temp_bis
WHERE client_id IS NOT NULL AND client_prenom IS NOT NULL
) tout_sauf_intersec

SELECT COUNT(*) 
FROM dbo.table_de_travail_temp_bis
WHERE client_id IS NULL

SELECT *
FROM dbo.table_de_travail_temp_bis
WHERE client_prenom LIKE 'SOPHIE' AND client_nom LIKE 'WILKINSON' AND client_cp LIKE 'DE13HN'

*/
