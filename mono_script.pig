-- REGISTER /home/dawid/pig/lib/piggybank.jar;
-- DEFINE OVER org.apache.pig.piggybank.evaluation.Over('lagged_MotorPowSupCurrent_1:float');
-- DEFINE STITCH org.apache.pig.piggybank.evaluation.Stitch;
REGISTER 'fuzzy_udfs.py' using streaming_python as fuzzy_functions;
-- REGISTER 'udaf_mono.py' using streaming_python as udaf_mono;


raw_data = LOAD 'generated_data_2.csv' USING PigStorage(',') AS (
    Index:int,
    MACHINE_ID:int,
    MotorPowSupCurrent:float,
    Bearing1Temp:float,
    LubrOilPressure:int,
    ENBLOCK_ID:int,
    Timestamp:float
);


trimmed_raw_data = FILTER raw_data BY Index > 0;
ranked_data = RANK trimmed_raw_data;
grouped_data = GROUP ranked_data BY (int)($1/5);


trend_data = FOREACH grouped_data {
    processed_data = fuzzy_functions.mono(ranked_data.MotorPowSupCurrent);
    GENERATE ranked_data.Index, processed_data AS Trend;
};

flattened_data = FOREACH trend_data GENERATE FLATTEN($0) AS Index;


ILLUSTRATE trend_data;

-- Wszystkie proby zamieszczone ponizej dotycza wywolania funkcji UDAF

-- %DECLARE window_size 3

-- selected_columns = FOREACH raw_data GENERATE Index, MotorPowSupCurrent;
-- filtered_data = FILTER selected_columns BY (Index is not null) AND (MotorPowSupCurrent is not null);

-- -- Group and apply the UDAF to check the last 5 records
-- result = FOREACH (GROUP raw_data ALL) GENERATE udaf_mono.Last5Records(raw_data.MotorPowSupCurrent);

-- -- Output the result
-- DUMP result;

-- %DECLARE ling_params_inc "safe;0;0;420;440/alarm;410;430;800;800"
-- %DECLARE ling_params_dec "safe;0;0;400;420/alarm;390;410;800;800"

-- flattened_data = FOREACH trend_data GENERATE Index, Trend;
-- DESCRIBE flattened_data;

-- filtered_data_alarm = FOREACH raw_data GENERATE Index, MotorPowSupCurrent, trend_data, fuzzy_functions.assign_ling('safe;0;0;400;420/alarm;390;410;800;800', MotorPowSupCurrent);

-- DUMP filtered_data_alarm;



-- ranked_data = RANK raw_data;
-- grouped_data = GROUP ranked_data BY (ranked_data % 5);

-- -- For each group, collect the values
-- result_data = FOREACH grouped_data {
--     values = ranked_data.your_field;
--     GENERATE FLATTEN(values) AS your_field_group;
-- }
-- ILLUSTRATE result_data;

-- trend_data = FOREACH grouped_data {
--     processed_data = fuzzy_functions.mono(ranked_data);
--     GENERATE processed_data AS Trend;
-- }

-- alarm_data = FOREACH trend_data GENERATE fuzzy_functions.assign_ling('safe;0;0;420;440/alarm;410;430;800;800', MotorPowSupCurrent);

-- DUMP alarm_data;


-- inc_data = FILTER B BY fuzzy_functions.assign_ling('$ling_params_inc', ranked_data.MotorPowSupCurrent) == 'inc';
-- dec_data = FILTER B BY fuzzy_functions.assign_ling('$ling_params_dec', ranked_data.MotorPowSupCurrent) == 'dec';

-- ILLUSTRATE inc_data;

-- C = FOREACH B GENERATE
--     fuzzy_functions.assign_ling('$ling_params_dec', ranked_data.MotorPowSupCurrent);
-- DUMP C;


-- REGISTER 'fuzzy_udfs.py' using streaming_python as fuzzy_functions;

-- filtered_data = FILTER raw_data BY fuzzy_functions.assign_ling('low;0;0;80;100/normal;80;110;320;360/high;320;360;400;400', MotorPowSupCurrent) == 'high';

-- result = FOREACH filtered_data GENERATE MotorPowSupCurrent, fuzzy_functions.assign_ling('low;0;0;80;100/normal;80;110;320;360/high;320;360;400;400', MotorPowSupCurrent) AS LING_VAL;

-- DUMP result;

-- selected_columns = FOREACH raw_data GENERATE Index, MotorPowSupCurrent;
-- filtered_data = FILTER selected_columns BY (Index is not null) AND (MotorPowSupCurrent is not null);


-- B = FOREACH (GROUP filtered_data BY Index) {
--     filtered_data_order = ORDER filtered_data BY Index;
--     GENERATE FLATTEN(STITCH(filtered_data_order, OVER(filtered_data, 'lag', 0, 3, 3, 0)));
-- };

-- B_ORDER = ORDER B BY stitched::Index; 

-- C = FOREACH B_ORDER GENERATE MotorPowSupCurrent, lagged_MotorPowSupCurrent_1;

-- DUMP C;

-- ranked_data = RANK raw_data;

-- grouped_data = GROUP ranked_data BY (int)($1/5);

-- B = FOREACH grouped_data {
--     processed_data = fuzzy_functions.mono(ranked_data);
--     GENERATE processed_data;
-- }

-- C = FOREACH (GROUP B BY result) {
--     COUNT_occurrences = COUNT(B);
--     GENERATE group AS result, COUNT_occurrences;
-- }

-- D = FILTER C BY result == 'dec' OR result == 'inc' OR result == 'con';

-- DUMP D;

-- selected_columns = FOREACH raw_data GENERATE Index, MotorPowSupCurrent;
-- filtered_data = FILTER selected_columns BY (Index is not null) AND (MotorPowSupCurrent is not null);


-- B = FOREACH (GROUP filtered_data BY MotorPowSupCurrent) {
--     filtered_data_order = ORDER filtered_data BY Index;
--     GENERATE FLATTEN(STITCH(filtered_data_order, OVER(filtered_data.MotorPowSupCurrent, 'lead', 0, 3, 3, 0)));
-- };

-- B_ORDER = ORDER B BY stitched::Index; 

-- DUMP B_ORDER;