from flask import Flask, render_template, request, jsonify, session, redirect, url_for
import boto3
import pandas as pd
from pycognito import Cognito

app = Flask(__name__)
app.secret_key = "supersecretkey123456"  # Replace with a strong secret in production

# --- AWS Config ---
REGION = "eu-north-1"
USER_POOL_ID = "eu-north-1_vHEU8wBW9"  # Your user pool
CLIENT_ID = "53jjmqe9kppcrbfadh75pd7092"  # App client WITHOUT secret
S3_BUCKET = "etl-project-data-bucket1"
TRANSACTIONS_KEY = "processed/transactions.csv"

s3 = boto3.client("s3", region_name=REGION)

# --- Fetch transactions from CSV ---
def get_transactions(account_id):
    obj = s3.get_object(Bucket=S3_BUCKET, Key=TRANSACTIONS_KEY)
    df = pd.read_csv(obj["Body"])
    df = df[df["customer_id"].astype(str) == str(account_id)]
    return df.to_dict(orient="records")

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
            user = Cognito(USER_POOL_ID, CLIENT_ID, username=username)
            user.authenticate(password=password)  # No secret_hash needed
            session["username"] = username
            session["token"] = user.access_token
            return redirect(url_for("home"))
        except Exception as e:
            return render_template("index.html", error=str(e))
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
    return jsonify({"transactions": tx})

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login_page"))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)

