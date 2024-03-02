raw_data_1 = LOAD 'generated_data_1.csv' USING PigStorage(',') AS (
    Index:int,
    MACHINE_ID:int,
    MotorPowSupCurrent:float,
    Bearing1Temp:float,
    LubrOilPressure:int,
    ENBLOCK_ID:int,
    Timestamp:float
);

raw_data_2 = LOAD 'generated_data_2.csv' USING PigStorage(',') AS (
    Index:int,
    MACHINE_ID:int,
    MotorPowSupCurrent:float,
    Bearing1Temp:float,
    LubrOilPressure:int,
    ENBLOCK_ID:int,
    Timestamp:float
);

REGISTER 'fuzzy_udfs.py' using streaming_python as fuzzy_functions;

data_assigned_1 = FOREACH raw_data_1 GENERATE 
    Index, 
    MACHINE_ID, 
    MotorPowSupCurrent, 
    fuzzy_functions.assign_ling('low;0;0;5;10/normal;8;12;18;22/high;18;20;24;28', MotorPowSupCurrent) AS assign_ling_value, 
    Bearing1Temp, 
    LubrOilPressure, 
    ENBLOCK_ID, 
    Timestamp;

data_assigned_2 = FOREACH raw_data_2 GENERATE 
    Index, 
    MACHINE_ID, 
    MotorPowSupCurrent, 
    fuzzy_functions.assign_ling('low;0;0;5;10/normal;8;12;18;22/high;18;20;24;28', MotorPowSupCurrent) AS assign_ling_value, 
    Bearing1Temp, 
    LubrOilPressure, 
    ENBLOCK_ID, 
    Timestamp;

data_filtered_1 = FILTER data_assigned_1 BY assign_ling_value != 'Not classified';
data_filtered_2 = FILTER data_assigned_2 BY assign_ling_value != 'Not classified';

joined_data = JOIN data_filtered_1 BY assign_ling_value, data_filtered_2 BY assign_ling_value;

result = FOREACH joined_data GENERATE 
    data_filtered_1::MotorPowSupCurrent,   
    data_filtered_2::MotorPowSupCurrent AS MotorPowSupCurrent2; 

DUMP result;