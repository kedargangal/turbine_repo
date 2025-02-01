from sqlalchemy import Table, Column, Integer, Float, String,  MetaData

raw_data = Table(
    'raw_turbines', MetaData(),
    Column('timestamp', String),
    Column('turbine_id', Integer),
    Column('wind_speed', Float),
    Column('wind_direction', Integer),
    Column('power_output', Float),

)

cleansed_data = Table(
    'cleansed_turbines', MetaData(),
    Column('timestamp', String),
    Column('turbine_id', Integer),
    Column('wind_speed', Float),
    Column('wind_direction', Integer),
    Column('power_output', Float),
    Column('date', String)
)

summary_statistics_data = Table(
    'summary_statistics_turbines', MetaData(),
    Column('turbine_id', Integer),
    Column('date', String),
    Column('avg_power_output', Float),
    Column('min_power_output', Float),
    Column('max_power_output', Float)
)

anomaly_data = Table(
    'anomaly_turbines', MetaData(),
    Column('turbine_id', Integer),
    Column('date', String),
    Column('power_output', Float),
    Column('avg_power_output', Float),
    Column('std_deviation_power_output', Float),
    Column('anomaly', Float)
)

