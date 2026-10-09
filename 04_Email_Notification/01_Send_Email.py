# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Create the widget for run_id
dbutils.widgets.text("run_id", "123456")
job_run_id = dbutils.widgets.get("run_id")

if not job_run_id:
    raise ValueError("job_run_id is required")

print("Job Run ID:", job_run_id)

# COMMAND ----------

# DBTITLE 1,Extract pipeline run audit details (fixed alias syntax)
audit_df = spark.sql(f"""
SELECT
    p.table_id AS Table_ID,
    t.table_name AS Table_Name,
    t.source_system as Source_System,
    p.layer AS Layer,
    p.start_time AS Start_Time,
    p.end_time AS End_Time,
    p.status AS Status,
    p.number_of_records AS Number_Of_Records,
    p.error_message AS Error_Message
FROM banking.metadata.pipeline_runs p
LEFT JOIN banking.metadata.tables t
    ON p.table_id = t.table_id
WHERE p.run_id = {job_run_id}
ORDER BY p.table_id
""")

display(audit_df)

# COMMAND ----------

from pyspark.sql.functions import col

total = audit_df.count()
failed = audit_df.filter(col("status") != "SUCCESS").count()

if failed > 0:
    overall_status = "FAILED"
else:
    overall_status = "SUCCESS"

print("Total Tables:", total)
print("Failed Tables:", failed)
print("Overall Status:", overall_status)

# COMMAND ----------

gmail_smpt_key = dbutils.secrets.get(
            scope="banking-scope",
            key="smtp_password_for_email"
        )
gmail_account_sender = dbutils.secrets.get(
    scope="banking-scope",
    key="gmail_account_sender"
)
gmail_account_receiver = dbutils.secrets.get(
    scope="banking-scope",
    key="gmail_account_receiver"
)


# COMMAND ----------

pdf = audit_df.toPandas()

html_table = pdf.to_html(
    index=False,
    border=1,
    justify="center",
    classes="table table-striped"
)

print(html_table)

# COMMAND ----------

body = f"""
<html>
<body>

<p><b>This is an auto-generated email. Please do not reply.</b></p>

<p>
End-to-end run for <b>Source → Bronze → Silver → Gold</b> layers has completed.
Below is the execution summary:
</p>

<br>

{html_table}

<br>

<p>
For any queries please reach out to: {gmail_account_sender}
<b>/b>
</p>

<br>

<p>
Thanks<br>
<b>NeoBank Support Team</b>
</p>

</body>
</html>
"""

print(body)

# COMMAND ----------

subject = f"NeoBank | {overall_status} | End to End Run"

print(subject)

# COMMAND ----------

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Gmail account
EMAIL_SENDER = gmail_account_sender
APP_PASSWORD = gmail_smpt_key
EMAIL_RECEIVER = gmail_account_receiver

# Create message
msg = MIMEMultipart()
msg["From"] = EMAIL_SENDER
msg["To"] = EMAIL_RECEIVER
msg["Subject"] = subject

msg.attach(MIMEText(body, "html"))

try:
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(EMAIL_SENDER, APP_PASSWORD)
        server.send_message(msg)

    print("✅ Email sent successfully!")

except Exception as e:
    print("❌ Error sending email:", e)