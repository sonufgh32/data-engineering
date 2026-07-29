import base64
import csv
import io
import json


COLUMNS = ["device_id", "temperature", "humidity", "pressure", "timestamp"]


def _get_encoded_record(record: dict) -> tuple[str, str]:
    payload_field = "kafkaRecordValue" if "kafkaRecordValue" in record else "data"
    encoded_record = record.get(payload_field)
    if not encoded_record:
        raise ValueError("Firehose record has no payload")
    return payload_field, encoded_record


def _to_csv_row(encoded_record: str) -> str:
    payload = json.loads(base64.b64decode(encoded_record).decode("utf-8"))
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=COLUMNS, extrasaction="ignore")
    writer.writerow({column: payload.get(column, "") for column in COLUMNS})
    return output.getvalue()


def lambda_handler(event, context):
    transformed_records = []

    for record in event["records"]:
        encoded_record = None
        payload_field = "kafkaRecordValue" if "kafkaRecordValue" in record else "data"
        try:
            payload_field, encoded_record = _get_encoded_record(record)
            csv_row = _to_csv_row(encoded_record)
            transformed_records.append(
                {
                    "recordId": record["recordId"],
                    "result": "Ok",
                    payload_field: base64.b64encode(csv_row.encode("utf-8")).decode("utf-8"),
                }
            )
        except (
            KeyError,
            TypeError,
            UnicodeDecodeError,
            ValueError,
            json.JSONDecodeError,
            csv.Error,
        ):
            transformed_records.append(
                {
                    "recordId": record["recordId"],
                    "result": "ProcessingFailed",
                    payload_field: encoded_record or "",
                }
            )

    return {"records": transformed_records}