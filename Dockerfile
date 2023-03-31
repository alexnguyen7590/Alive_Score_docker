FROM public.ecr.aws/lambda/python:3.9

COPY Alive_score_lambda_function.py Alive_score_lambda_function.py 

RUN pip3 install future pandas numpy pytz psycopg2 datetime lifetimes

CMD ["Alive_score_lambda_function.lambda_handler"]