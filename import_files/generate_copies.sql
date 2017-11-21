BEGIN;
-- First, we build shelving location
INSERT INTO asset.copy_location (name, owning_lib)
        SELECT  DISTINCT l.location, ou.id
          FROM  staging_items l JOIN actor.org_unit ou
                ON (l.owning_lib = ou.shortname)
;

-- Create circ modifiers for in-db circulation
-- This is very, very crude but satisfies the FK constraints
INSERT INTO config.circ_modifier (code, name, description, sip2_media_type, magnetic_media)
        SELECT  DISTINCT item_type as code,
          item_type AS name,
          LOWER(item_type) AS description,
          '001' AS sip2_media_type,
          FALSE AS magnetic_media
          FROM  staging_items
          WHERE item_type NOT IN (SELECT code FROM config.circ_modifier);

-- Import call numbers for bibrecord->library mappings
INSERT INTO asset.call_number (creator,editor,record,label,owning_lib)
        SELECT  DISTINCT 1, 1, l.bibkey , l.callnum, ou.id
          FROM  staging_items l
                JOIN biblio.record_entry b ON (l.bibkey = b.id)
                JOIN actor.org_unit ou ON (l.owning_lib = ou.shortname);

-- Import base copy data
INSERT INTO asset.copy (
        circ_lib, creator, editor, create_date, barcode,
        status, location, loan_duration,
        fine_level, circ_modifier, deposit, ref, call_number)
        SELECT  DISTINCT  ou.id AS circ_lib,
                1 AS creator,
                1 AS editor,
                l.createdate AS create_date,
                l.barcode AS barcode,
                0 AS status,
                cl.id AS location,
                2 AS loan_duration,
                2 AS fine_level,
                CASE
                        WHEN l.item_type IN ('REFERENCE', 'DEPOSIT_BK') THEN 'BOOK'
                        ELSE l.item_type
                END AS circ_modifier,
                CASE
                        WHEN l.item_type = 'DEPOSIT_BK' THEN TRUE
                        ELSE FALSE
                END AS deposit,
                CASE
                        WHEN l.item_type = 'REFERENCE' THEN TRUE
                        ELSE FALSE
                END AS ref,
                cn.id AS call_number
          FROM  staging_items l
                JOIN actor.org_unit ou
                        ON (l.owning_lib = ou.shortname)
                JOIN asset.copy_location cl
                        ON (ou.id = cl.owning_lib AND l.location = cl.name)
                JOIN asset.call_number cn
                        ON (ou.id = cn.owning_lib AND l.bibkey = cn.record AND l.callnum = cn.label)
;
COMMIT;
