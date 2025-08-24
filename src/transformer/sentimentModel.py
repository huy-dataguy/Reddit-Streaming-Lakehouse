from gradio_client import Client
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def predict_sentiment(text):
    try:
        client = Client("dataguychill/sentiment")
        result = client.predict(text, api_name="/predict")
        return result
    except Exception as e:
        return f"Error: {str(e)}"

sentimentUdf = udf(predict_sentiment, StringType())