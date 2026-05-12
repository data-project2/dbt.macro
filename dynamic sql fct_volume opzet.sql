DECLARE
    v_columns ARRAY;
    v_tables ARRAY;
    v_sql STRING;
    v_select STRING;
    v_col STRING;
    v_table STRING;
    v_exists NUMBER;
    v_schema STRING;
    v_table_name STRING;
BEGIN

    -- Gewenste output kolommen
    v_columns := ARRAY_CONSTRUCT(
        'METRIC_SOURCE',
        'VOLUME',
        'VOLUME_STARTDATE',
        'VOLUME_ENDDATE',
        'PRICING_RATE',
        'ENERGY_DIRECTION',
        'UNIT_OF_MEASUREMENT',
        'COMMODITY',
        'CUSTOMER_NUMBER',
        'COMMERCIALCONTRACT_NUMBER',
        'DELIVERYPOINT_NUMBER',
        'DELIVERYAGREEMENT_BK',
        'METERREADING_BK'
    );

    -- Tabellen
    v_tables := ARRAY_CONSTRUCT(
        'silver.LDM_METERREADING_VOLUMES',
        'SILVER.LDM_CONTRACTED_VOLUMES'
    );

    v_sql := '';

    FOR t IN 0 TO ARRAY_SIZE(v_tables)-1 DO
        
        v_table := v_tables[t];

        -- Schema + tabel splitsen indien nodig
        IF (POSITION('.' IN v_table) > 0) THEN
            v_schema := SPLIT_PART(v_table,'.',1);
            v_table_name := SPLIT_PART(v_table,'.',2);
        ELSE
            v_schema := CURRENT_SCHEMA();
            v_table_name := v_table;
        END IF;

        v_select := 'SELECT ';

        FOR c IN 0 TO ARRAY_SIZE(v_columns)-1 DO
            
            v_col := v_columns[c];

            -- Controle of kolom bestaat
            SELECT COUNT(*)
            INTO :v_exists
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = UPPER(:v_schema)
              AND TABLE_NAME   = UPPER(:v_table_name)
              AND COLUMN_NAME  = UPPER(:v_col);

            IF (v_exists > 0) THEN
                v_select := v_select || v_col;
            ELSE
                v_select := v_select || 'NULL AS ' || v_col;
            END IF;

            IF (c < ARRAY_SIZE(v_columns)-1) THEN
                v_select := v_select || ', ';
            END IF;

        END FOR;

        v_select := v_select || ' FROM ' || v_table;

        IF (t > 0) THEN
            v_sql := v_sql || ' UNION ALL ';
        END IF;

        v_sql := v_sql || v_select;

    END FOR;

    -- Voor debugging kun je dit tijdelijk gebruiken:
     RETURN v_sql;

    --EXECUTE IMMEDIATE v_sql;

END;


--v_sql:
SELECT NULL AS METRIC_SOURCE, NULL AS VOLUME, NULL AS VOLUME_STARTDATE, NULL AS VOLUME_ENDDATE, PRICING_RATE, ENERGY_DIRECTION, NULL AS UNIT_OF_MEASUREMENT, COMMODITY, NULL AS CUSTOMER_NUMBER, NULL AS COMMERCIALCONTRACT_NUMBER, NULL AS DELIVERYPOINT_NUMBER, DELIVERYAGREEMENT_BK, METERREADING_BK FROM silver.LDM_METERREADING_VOLUMES UNION ALL SELECT METRIC_SOURCE, VOLUME, VOLUME_STARTDATE, VOLUME_ENDDATE, PRICING_RATE, ENERGY_DIRECTION, UNIT_OF_MEASUREMENT, COMMODITY, CUSTOMER_NUMBER, COMMERCIALCONTRACT_NUMBER, DELIVERYPOINT_NUMBER, NULL AS DELIVERYAGREEMENT_BK, NULL AS METERREADING_BK FROM SILVER.LDM_CONTRACTED_VOLUMES