from datetime import datetime
import json
import boto3
import joblib

model = joblib.load('model/model.pkl')

with open('model/model_metadata.json', 'r', encoding="utf-8") as f:
    model_info = json.load(f)

cloudwatch = boto3.client('cloudwatch')

def write_real_data(data, prediction):
    now = datetime.now()
    now_formatted = now.strftime("%d-%m-%Y %H:%M")
    file_name = f"{now.strftime('%Y-%m-%d')}_quantum_finance_prediction_data.csv"

    data["price"] = prediction
    data["timestamp"] = now_formatted
    data["model_version"] = model_info["version"]

    s3 = boto3.client('s3')
    bucket_name = 'w-fiap-ds-mlops'
    s3_path = 'quantum-finance-real-data-10dtsr'

    try:
        existing_object = s3.get_object(Bucket=bucket_name, Key=f"{s3_path}/{file_name}")
        existing_data = existing_object['Body'].read().decode('utf-8').strip().split('\n')
        existing_data.append(','.join(map(str, data.values())))
        update_content = '\n'.join(existing_data)
    except s3.exceptions.NoSuchKey:
        update_content = ','.join(data.keys()) + '\n' + ','.join(map(str, data.values()))

    s3.put_object(Body=update_content, Bucket=bucket_name, Key=f"{s3_path}/{file_name}")

def input_metrics(data, prediction):
    cloudwatch.put_metric_data(
        MetricData=[
            {
                'MetricName': 'Score Prediction',
                'Value': prediction,
                'Dimensions': [{'Name': "Currency", 'Value': "BRL"}]
            },
        ],
        Namespace='Qunatum Finance Model'
    )

    for key, value in data.items():
        cloudwatch.put_metric_data(
            MetricData=[
                {
                    'MetricName': 'Laptop Feature',
                    'Value': 1,
                    'Unit': 'Count',
                    'Dimensions': [{'Name': key, 'Value': str(value)}]
                },
            ],
            Namespace='Qunatum Finance Features'
        )

def prepare_payload(data):
    """
    Prepara os dados para o modelo com validação de categorias codificadas.
    """

    # Validação de valores categóricos
    conditions = {
        "Occupation": set(range(1, 14 + 1)),  # 1 a 14
        "Credit_Mix": {1, 2, 3},
        "Payment_of_Min_Amount": {0, 1},
        "Payment_Behaviour": {1, 2, 3, 4, 5}
    }

    for key, valid_values in conditions.items():
        if int(data[key]) not in valid_values:
            raise ValueError(f"Valor inválido para '{key}': {data[key]}. Esperado: {valid_values}")

    # Construção do vetor de entrada
    data_processed = [
        float(data["Age"]),
        int(data["Occupation"]),
        float(data["Annual_Income"]),
        int(data["Num_Bank_Accounts"]),
        int(data["Num_Credit_Card"]),
        int(data["Interest_Rate"]),
        int(data["Num_of_Loan"]),
        float(data["Delay_from_due_date"]),
        float(data["Num_of_Delayed_Payment"]),
        float(data["Num_Credit_Inquiries"]),
        int(data["Credit_Mix"]),
        float(data["Outstanding_Debt"]),
        float(data["Credit_Utilization_Ratio"]),
        float(data["Credit_History_Age"]),
        int(data["Payment_of_Min_Amount"]),
        float(data["Total_EMI_per_month"]),
        float(data["Amount_invested_monthly"]),
        int(data["Payment_Behaviour"]),
        float(data["Monthly_Balance"])
    ]

    return data_processed

def handler(event, context=False):
    print(event)
    print(context)

    if "body" in event:
        print("Body found in event, invoke by API Gateway")
        body = json.loads(event.get("body", "{}"))
    else:
        print("Body not found in event, invoke by Lambda")
        body = event

    data = body.get("data", {})
    print("Payload:", data)

    try:
        data_processed = prepare_payload(data)
        prediction = model.predict([data_processed])
        prediction = int(prediction[0])
    except Exception as e:
        print(f"Erro ao processar: {e}")
        return {
            "statusCode": 400,
            "body": json.dumps({"error": str(e)})
        }

    print(f"Prediction: {prediction}")

    input_metrics(data, prediction)
    write_real_data(data, prediction)

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps({
            "prediction": prediction,
            "version": model_info["version"]
        })
    }
