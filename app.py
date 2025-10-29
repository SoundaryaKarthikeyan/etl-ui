from flask import Flask, render_template, request, jsonify, session, redirect, url_for
import boto3
import pandas as pd
from pycognito import Cognito
import logging

app = Flask(__name__)
app.secret_key = "supersecretkey123456"  # Replace with a strong secret in production

# --- AWS Config ---
REGION = "eu-north-1"
USER_POOL_ID = "eu-north-1_vHEU8wBW9"
CLIENT_ID = "53jjmqe9kppcrbfadh75pd7092"
CLIENT_SECRET = "tpthiui7bi1ng8pjj4hl6kq0od5bc4mcbk6p590bci7u2f1phhc"
S3_BUCKET = "etl-project-data-bucket1"
TRANSACTIONS_KEY = "processed/transactions.csv"

# AWS client
s3 = boto3.client("s3", region_name=REGION)

response = s3.list_objects_v2(Bucket="etl-project-data-bucket1", Prefix="processed/transactions.csv")
print(response)
# Setup logging
logging.basicConfig(level=logging.INFO)

# --- Fetch transactions from CSV ---
def get_transactions(account_id):
    logging.info(f"Fetching transactions for account_id: {account_id}")

    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=TRANSACTIONS_KEY)
        df = pd.read_csv(obj["Body"])

        # Normalize column names
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

        logging.info(f"CSV Columns: {df.columns.tolist()}")
        logging.info(f"Sample data:\n{df.head(3)}")

        # Filter rows
        filtered = df[df["customer_id"].astype(str) == str(account_id)]
        logging.info(f"Records found: {len(filtered)}")

        return filtered.to_dict(orient="records")
    except Exception as e:
        logging.error(f"Error fetching transactions: {e}")
        return []

# --- Routes ---
@app.route("/")
def home():
    if "username" not in session:
        return redirect(url_for("login_page"))
    return render_template("index.html")

@app.route("/login", methods=["GET", "POST"])
def login_page():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]

        try:
            user = Cognito(
                USER_POOL_ID,
                CLIENT_ID,
                client_secret=CLIENT_SECRET,
                username=username
            )

            # Authenticate
            user.authenticate(password=password)

            # Save session
            session["username"] = username
            session["token"] = user.access_token

            return redirect(url_for("home"))
        except Exception as e:
            logging.error(f"Login failed: {e}")
            return render_template("index.html", error=f"Login failed: {e}")

    return render_template("index.html")

@app.route("/api/transactions", methods=["POST"])
def api_transactions():
    if "username" not in session:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json()
    account_id = data.get("account_id")

    if not account_id:
        return jsonify({"error": "Missing account_id"}), 400

    tx = get_transactions(account_id)

    if not tx:
        return jsonify({"message": f"No transactions found for account {account_id}"}), 404

    return jsonify({"transactions": tx})

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login_page"))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)

