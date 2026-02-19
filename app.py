"""
Flask app: credit union branch scraper UI.
"""
import os
from flask import Flask, render_template, request, jsonify

from branch_scraper import scrape_branches

app = Flask(__name__)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/scrape", methods=["POST"])
def scrape():
    data = request.get_json() or {}
    url = (data.get("url") or "").strip()
    branch_input = (data.get("branch_names") or "").strip()
    if not url:
        return jsonify({"error": "Please enter a website URL."}), 400
    if not branch_input:
        return jsonify({"error": "Please enter at least one branch name (comma-separated)."}), 400
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    branch_names = [n.strip() for n in branch_input.split(",") if n.strip()]
    try:
        results = scrape_branches(url, branch_names)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    if results and results[0].get("error"):
        return jsonify({"error": results[0]["error"]}), 400
    return jsonify({"branches": results})


if __name__ == "__main__":
    app.run(debug=True, port=5000)
