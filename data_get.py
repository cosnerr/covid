
import helper as h
import pandas as pd

# Edit the following 3 variables before kicking off the script.
start = "2020-03-06"
end = "2020-07-25"
state_abb = "IN"

date_list = pd.date_range(start=start, end=end)

my_db_conn = h.db_conn_get()
cursor = my_db_conn.cursor()
cursor.execute(f'CREATE SCHEMA IF NOT EXISTS {h.schema};')
my_db_conn.commit()

my_engine = h.engine_get()

date_list_ext = [str(f'https://covidtracking.com/api/v1/states/{state_abb.lower()}/{x.strftime(format="%Y%m%d")}.json')
                 for x in date_list]

final_result_dict = h.call_api_append_results(date_list_ext)

df = pd.DataFrame(final_result_dict).transpose()
df.columns = map(str.lower, df.columns)  # Prevents column quotes by SqlAlchemy during table creation.


# For simplicity, I used Pandas Dataframe which uses SqlAlchemy engine.
try:
    h.log_me(f'Add rows to {h.schema}.{h.table}')

    df.to_sql(
        name=h.table,
        con=my_engine,
        schema=h.schema,
        if_exists='append')

except Exception as e:
    h.log_me(e)
finally:
    my_db_conn.close()



