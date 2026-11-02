from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

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

def create_fraud_alert_email_body(alert_data):
    """Creates an HTML email body for a fraud alert."""
    html = f"""
    <html>
    <head></head>
    <body>
        <h1>Fraud Alert</h1>
        <p>The following suspicious activity has been detected:</p>
        <table>
            <tr>
                <th>Transaction ID</th>
                <th>Amount</th>
                <th>Timestamp</th>
            </tr>
    """
    for transaction_id, amount, timestamp in alert_data:
        html += f"""
            <tr>
                <td>{transaction_id}</td>
                <td>{amount}</td>
                <td>{timestamp}</td>
            </tr>
        """
    html += """
        </table>
    </body>
    </html>
    """
    return html

EMAIL_FROM = "kkarwa04@gmail.com"
EMAIL_TO = "kkarwa04@gmail.com"
try:
    APP_PASSWORD = dbutils.secrets.get("finguard-scope", "gmail_api_key")
except Exception as e:
    print(f"Failed to retrieve Gmail API key: {e}")
    APP_PASSWORD = None

@dp.foreach_batch_sink(name="fraud_email_notifier_sink")
def send_fraud_alert_emails(df, batch_id):
    """ForEachBatch sink that sends emails alerts for fraud card transactions."""

    if APP_PASSWORD is None:
        print(f"Batch {batch_id}: Gmail API key not available, skipping email notifications")
        return
    
    rows = df.collect()
    print(f"Batch {batch_id}: Processing {len(rows)} fraud alert(s)...")

    success_count = 0
    failure_count = 0

    for row in rows:
        try:
            card_number_masked = str(row.card_number)[-4:] if row.card_number else "****"
            
            alert_data = {
                'alert_id': row.alert_id,
                'alert_type': row.alert_type,
                'alert_timestamp': str(row.transaction_timestamp),
                'customer_name': row.customer_name,
                'transaction_id': row.transaction_id,
                'card_number_masked': card_number_masked,
                'amount': float(row.amount),
                'currency': row.currency,
                'merchant_name': row.merchant_name,
                'merchant_category': row.merchant_category,
                'transaction_type': row.transaction_type,
                'payment_channel': row.payment_channel,
                'transaction_city': row.transaction_city,
                'transaction_country': row.transaction_country,
                'transaction_timestamp': str(row.transaction_timestamp),
                'is_international': row.is_international,
                'transaction_status': row.transaction_status,
                'watchlist_id': row.watchlist_id,
                'watch_type': row.watch_type,
                'risk_level': row.risk_level,
                'action': row.action,
                'reason_description': row.reason_description,
                'reported_by': row.reported_by,
                'reported_source': row.reported_source,
                'watchlist_city': row.watchlist_city if row.watchlist_city else 'N/A',
                'watchlist_country': row.watchlist_country if row.watchlist_country else 'N/A',
                'watchlist_effective_from': str(row.watchlist_effective_from)
            }
            
            subject = f"FRAUD ALERT - {alert_data['risk_level']} Risk - {alert_data['alert_id']}"
            body = create_fraud_alert_email_body(
                [(alert_data['transaction_id'], alert_data['amount'], alert_data['transaction_timestamp'])]
            )
            send_email(row.customer_email, subject, body, EMAIL_FROM, APP_PASSWORD)
            success_count += 1
            print(f"Fraud alert email sent to {row.customer_email} for transaction {row.transaction_id}")

        except Exception as e:
            failure_count += 1
            print(f"Failed to send fraud alert email for transaction {row.transaction_id}: {e}")

    print(f"Batch {batch_id} complete: {success_count} succeeded, {failure_count} failed")

@dp.append_flow(target="fraud_email_notifier_sink")
def fraud_card_alert_stream():
    """Streaming flow that reads fraud card alerts."""
    return spark.readStream.table("finguard.gold.fraud_card_alert")