raw_data = LOAD 'generated_data_1.csv' USING PigStorage(',') AS (
    Index:int,
    MACHINE_ID:int,
    MotorPowSupCurrent:float,
    Bearing1Temp:float,
    LubrOilPressure:int,
    ENBLOCK_ID:int,
    Timestamp:float
);

REGISTER 'fuzzy_udfs.py' using streaming_python as fuzzy_functions;

assigned_ling = FOREACH raw_data GENERATE fuzzy_functions.assign_ling('low;0;0;80;100/normal;80;110;320;360/high;320;360;400;400', MotorPowSupCurrent) AS CURR_LEVEL, Bearing1Temp;

grouped_data = GROUP assigned_ling BY CURR_LEVEL;

result = FOREACH grouped_data GENERATE group AS CURR_LEVEL, AVG(assigned_ling.Bearing1Temp) AS AVG_TEMP;

DUMP result;