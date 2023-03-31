import pandas as pd
import psycopg2
from datetime import datetime
from lifetimes import BetaGeoFitter
from lifetimes.utils import summary_data_from_transaction_data
from lifetimes.utils import calculate_alive_path
import json
import boto3
from botocore.exceptions import ClientError
from io import StringIO # python3; python2: BytesIO 

def get_secret():

    secret_name = "qa/ca/alivescore"
    region_name = "us-west-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']
    secret_data = json.loads(secret)

    return secret_data


def connect():
    # Connect to database
    # Connection keyvalues are placeholders
    secrets = get_secret()
    try:
        conn = psycopg2.connect(
            host=secrets["HOST"],
            database=secrets["DB"],
            user=secrets["USER"],
            password=secrets["PASS"],
            connect_timeout=1000)
        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        return None


def process_activity_data():
    # creating a SQL query to pass into the function
    query = """
            SELECT public.orders.id, public.orders.company_id, 
                    public.orders.created as order_created, 
                    to_timestamp(public.companies.created_at) as company_created 
            FROM public.orders join public.companies on public.orders.company_id = public.companies.id
            WHERE public.orders.status in ('Ready','Fixing','Unpaid')
            AND public.orders.status != 'Draft' AND public.orders.status!='Pending' 
            ORDER BY order_created DESC
            """

    # creating a list with columns names to pass into the function
    column_names = ['id', 'company_id', 'order_created', 'company_created']
    # opening the connection
    conn = connect()
    # loading our dataframe
    sql_query = pd.read_sql_query(query, conn)

    df = pd.DataFrame(sql_query, columns=column_names)
    # closing the connection
    conn.close()
    # Let’s see if we loaded the df successfully
    df['order_created'] = pd.to_datetime(df.order_created).date
    df['company_created'] = pd.to_datetime(df.company_created).date
    df_rfmt = summary_data_from_transaction_data(df,
                                                 'company_id',
                                                 'order_created',
                                                 observation_period_end='2024-01-01')
    bgf = BetaGeoFitter(penalizer_coef=0)
    bgf.fit(df_rfmt['frequency'], df_rfmt['recency'], df_rfmt['T'])
    df_companies = df[['company_id', 'company_created']]
    df_p_alive_histogram = pd.DataFrame()
    customers_list = list(df['company_id'].unique())
    df_companies = df_companies.drop_duplicates()

    for customer in customers_list:
        example_customer_orders = df.loc[df['company_id'] == customer]
        # company_created = df_companies.loc[df_companies['company_id']== customer, 'company_created'].item()
        #datetime_object = datetime.strptime(company_created, '%Y-%m-%d %H:%M:%S')
        datetime_object = min(
            df.loc[df['company_id'] == customer]['order_created'])
        # print(datetime_object)
        diff = datetime.now().date() - datetime_object
        days_since_birth = diff.days
        company_id = df_companies.loc[df_companies['company_id']
                                      == customer, 'company_id'].item()
        path = calculate_alive_path(
            bgf, example_customer_orders, 'order_created', days_since_birth, freq='D')
        path_dates = pd.date_range(start=min(
            example_customer_orders['order_created']), periods=len(path), freq='D')
        orders_range = path_dates.get_loc(
            max(example_customer_orders['order_created']).strftime("%Y-%m-%d"))

        if max(example_customer_orders['order_created']) != None and orders_range > 0:
            current_alive_p = path.iloc[-1][0],
            current_alive_p = current_alive_p[0]
            average_alive_p = path.iloc[:orders_range].mean()[0],
            average_alive_p = average_alive_p[0]
            median_alive_p = path.iloc[:orders_range].median()
            df_p_alive_histogram = df_p_alive_histogram.append(
                {
                    'company_id':  company_id,
                    'current_alive_p': current_alive_p,
                    'average_alive_p': average_alive_p,
                    'median_alive_p': median_alive_p}, ignore_index=True
            )
        else:
            df_p_alive_histogram = df_p_alive_histogram.append(
                {
                    'company_id':  company_id,
                    'current_alive_p': None,
                    'average_alive_p': None,
                    'median_alive_p': None}, ignore_index=True
            )

    # df_p_alive_histogram.to_csv('df_p_alive_histogram.csv')
    return df_p_alive_histogram


def lambda_handler(event, context):
    
    results = process_activity_data()
    bucket = 'my_bucket_name' # already created on S3
    csv_buffer = StringIO()
    results.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, 'test_data.csv').put(Body=csv_buffer.getvalue())

    # Filesaving open returning head
    return results.head(5)


if __name__ == "__main__":
    lambda_handler("", "")