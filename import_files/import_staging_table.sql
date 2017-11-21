CREATE TABLE staging_items (
        callnum text, -- call number label
        bibkey  int,  -- biblio.record_entry_id
        createdate      date,
        location        text,
        barcode         text,
        item_type       text,
        owning_lib      text  -- actor.org_unit.shortname
);
