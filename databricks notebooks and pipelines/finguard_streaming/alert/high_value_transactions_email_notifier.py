from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib


def send_email(to_email, subject, body, from_email, app_password):
    """Sends an email using Gmail SMTP server."""
    msg = MIMEMultipart()
    msg["From"] = from_email
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html"))

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(from_email, app_password)
        server.send_message(msg)


def create_alert_email_body(alert_data):
    """Creates an HTML body for a high-value transaction alert."""
    return f"""
<html>
<head></head>
<body>
    <p>High-value transaction detected!</p>
    <p>Alert details:</p>
    <ul>
        <li>Transaction ID: {alert_data["transaction_id"]}</li>
        <li>Amount: {alert_data["amount"]}</li>
        <li>Timestamp: {alert_data["timestamp"]}</li>
    </ul>
</body>
</html>
    """


EMAIL_FROM = "kkarwa04@gmail.com"
try:
    APP_PASSWORD = dbutils.secrets.get("finguard-scope","gmail_api_key")
except Exception as e:
    print(f"Failed to retrieve Gmail API key from secrets: {e}")
    APP_PASSWORD = None


@dp.foreach_batch_sink(name="email_notifier_sink")
def send_high_value_alert_emails(df, batch_id):
    """ForEachBatch sink that sends email alerts for high-value transactions."""

    if APP_PASSWORD is None:
        print(
            f"Batch {batch_id}: Gmail API key is not available, skipping email notifications"
        )
        return

    rows = df.collect()
    print(f"Batch {batch_id}: Processing {len(rows)} alert(s)...")

    success_count = 0
    failure_count = 0

    for row in rows:
        try:
            alert_data = {
                "alert_id": row.alert_id,
                "customer_name": row.customer_name,
                "transaction_id": row.transaction_id,
                "transaction_amount": float(row.transaction_amount),
                "currency": row.currency,
                "transaction_limit": float(row.transaction_limit),
                "merchant_name": row.merchant_name,
                "merchant_category": row.merchant_category,
                "transaction_timestamp": str(row.transaction_timestamp),
                "city": row.city,
                "country": row.country,
            }

            subject = f"High Value Transaction Alert - {alert_data['alert_id']}"
            body = create_alert_email_body(alert_data)

            send_email(row.customer_email, subject, body, EMAIL_FROM, APP_PASSWORD)

            success_count += 1
            print(
                f"Email sent to {row.customer_email} for transaction {row.transaction_id}"
            )

        except Exception as e:
            failure_count += 1
            print(f"Error processing alert {row.alert_id}: {e}")

    print(
        f"Batch {batch_id} complete: {success_count} succeeded, {failure_count} failed"
    )


@dp.append_flow(target="email_notifier_sink")
def high_value_alert_stream():
    """Streaming flow that reads high-value transaction alerts."""
    return spark.readStream.table("finguard.gold.high_value_transactions_alert")
