

DROP VIEW IF EXISTS bordeaux.timeserie_norm;
CREATE VIEW bordeaux.timeserie_norm AS
  SELECT * FROM (
    SELECT DISTINCT ident AS id
      ,name
      ,available_stand
      ,available_bike
      ,ts
    FROM bordeaux.timeserie
    ORDER BY ident,ts
      ) AS A;


DROP VIEW IF EXISTS lyon.timeserie_norm;
CREATE VIEW lyon.timeserie_norm AS
  SELECT * FROM (
    SELECT DISTINCT number AS id
      ,S.nom AS name
      ,available_bike_stands AS available_stand
      ,available_bikes AS available_bike
      ,last_update AS ts
    FROM lyon.timeserie
    LEFT JOIN lyon.pvostationvelov AS S ON (S.idstation::int = number) ) AS A;
