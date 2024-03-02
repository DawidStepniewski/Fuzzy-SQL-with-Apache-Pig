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

filtered_data = FILTER raw_data BY fuzzy_functions.assign_ling('low;0;0;80;100/normal;80;110;320;360/high;320;360;400;400', MotorPowSupCurrent) == 'high';

result = FOREACH filtered_data GENERATE MotorPowSupCurrent, 
fuzzy_functions.assign_ling('low;0;0;80;100/normal;80;110;320;360/high;320;360;400;400', MotorPowSupCurrent) AS LING_VAL;

DUMP result;