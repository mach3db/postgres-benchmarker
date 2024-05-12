import psycopg
from psycopg.rows import dict_row
import datetime
import csv
import pandas

########################################################################################################################
# database connection parameters

dbname = ''
user = ''
host = 'mach3db.com'
password = ''
connection_string = f"dbname={dbname} user={user} host={host} password={password} sslmode=require"

########################################################################################################################
# table and query

table = ''

query = f"""
            
            SELECT * FROM {table} 
            
"""
########################################################################################################################


def get_column_names():
    column_query = f"""
        SELECT *
        FROM {table}
        LIMIT 0
    """

    with psycopg.connect(connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute(column_query)
            return [desc[0] for desc in cur.description]


def time_to_fetch_and_output_csv(fetchall=False):
    base_dir = ''

    date_string = datetime.datetime.now().strftime('%c').replace(':', '-')
    filename_string = f'mach3db-benchmarket-output-{date_string}.csv'

    output_filename = f'{base_dir}{filename_string}'

    with psycopg.connect(connection_string) as conn:
        with conn.cursor() as cur:
            with open(output_filename, 'w', newline='') as f:
                writer = csv.writer(f)

                writer.writerow(
                    get_column_names()
                )

                if fetchall:
                    start_time = datetime.datetime.now()

                    cur.execute(query)

                    writer.writerows(
                        cur.fetchall()
                    )

                    end_time = datetime.datetime.now()

                    time_taken = (end_time - start_time).total_seconds()

                    return time_taken
                else:
                    start_time = datetime.datetime.now()

                    cur.execute(query)

                    row = cur.fetchone()

                    while row:
                        writer.writerow(row)
                        row = cur.fetchone()

                    end_time = datetime.datetime.now()

                    time_taken = (end_time - start_time).total_seconds()

                    return time_taken


def time_to_fetch_and_return(as_dict=False, as_df=False):
    if as_dict:
        row_factory = dict_row
    else:
        row_factory = None

    with psycopg.connect(connection_string, row_factory=row_factory) as conn:
        with conn.cursor() as cur:
            start_time = datetime.datetime.now()

            cur.execute(query)

            results = cur.fetchall()

            end_time = datetime.datetime.now()

            time_taken = (end_time - start_time).total_seconds()

            if as_df:
                if as_dict:
                    results = pandas.DataFrame(results)
                else:
                    results = pandas.DataFrame(results, columns=get_column_names())

            return results, time_taken


def time_to_fetch():
    with psycopg.connect(connection_string) as conn:
        with conn.cursor() as cur:
            start_time = datetime.datetime.now()

            cur.execute(query)

            end_time = datetime.datetime.now()

            time_taken = (end_time - start_time).total_seconds()

            return time_taken


if __name__ == '__main__':
    ####################################################################################################################

    # test parameters

    MODE_CHOICES = 'query_only', 'return_results', 'output_csv'

    SELECTED_MODE = 'query_only'

    # return_results sub-parameters
    as_dict = False
    as_df = True
    rows_to_print = 5
    set_trace = True

    # output_csv sub-parameters
    fetch_all = False

    ####################################################################################################################

    if SELECTED_MODE == 'query_only':
        print(
            f'Query time taken in seconds: {time_to_fetch()}'
        )
    elif SELECTED_MODE == 'output_csv':
        print(
            f'Query time taken in seconds: {time_to_fetch_and_output_csv(fetchall=fetch_all)}'
        )
    elif SELECTED_MODE == 'return_results':
        results, time_taken_in_seconds = time_to_fetch_and_return(
            as_dict=as_dict,
            as_df=as_df
        )

        if as_df:
            print(
                results.head(
                    rows_to_print
                )
            )
        else:
            if not as_dict:
                print(
                    get_column_names()
                )

            for row_to_print in results[:rows_to_print]:
                print(
                    row_to_print
                )

        print(
            f'Query time taken in seconds: {time_taken_in_seconds}'
        )

        if set_trace:
            import pdb; pdb.set_trace()
    else:
        print(f'INVALID MODE SELECTED! CHOICES ARE: {MODE_CHOICES}')
