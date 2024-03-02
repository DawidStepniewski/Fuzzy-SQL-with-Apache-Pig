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

filtered_data = FILTER raw_data BY fuzzy_functions.around_trapez(MotorPowSupCurrent, 190, 200, 200, 210) >= 0.5;

result = FOREACH filtered_data GENERATE 
          MotorPowSupCurrent AS motor_pow_sup_current, 
          fuzzy_functions.around_trapez(MotorPowSupCurrent, 190, 200, 200, 210) AS memb_degree;

DUMP result;
