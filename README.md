## Pub/Sub -> Dataflow -> BigQuery

Pocs for analyzing a system using the gcp stack for managing audit logs;


<br>

Most of the codes were developed quickly, just for study and testing purposes;

python main.py --streaming --input_subscription projects/[PROJECT_ID]/subscriptions/test-sub --output_table [PROJECT_ID]:test.example --output_schema "timestamp:TIMESTAMP,attr1:FLOAT,msg:STRING"


python main.py --streaming --input_subscription projects/[PROJECT_ID]/subscriptions/test-sub --output_table [PROJECT_ID]:test.example --output_schema "timestamp:TIMESTAMP,attr1:FLOAT,msg:STRING" --runner DataflowRunner --project [PROJECT_ID] --region us-central1 --temp_location gs://[STORAGE]/temp --job_name dataflow-custom-pipeline-v1 --max_num_workers 2
