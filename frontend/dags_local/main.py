#!/usr/bin/env python3
"""
main.py - Flask app para upload, listagem e exclusão de arquivos .py
na pasta /root/dags de um droplet DigitalOcean, com autenticação via sessão.
"""

import os
from pathlib import Path
from functools import wraps
from flask import (
    Flask, request, redirect, url_for,
    send_from_directory, abort, session, render_template
)
from werkzeug.utils import secure_filename
from datetime import datetime

# --- Configurações ---
UPLOAD_FOLDER = os.environ.get("UPLOAD_FOLDER", "/root/dags")
MAX_CONTENT_LENGTH = 50 * 1024 * 1024  # 50 MB
ALLOWED_EXTENSIONS = {"py"}

# Credenciais (podem ser sobrescritas por variáveis de ambiente)
APP_USERNAME = os.environ.get("APP_USERNAME", "admin")
APP_PASSWORD = os.environ.get("APP_PASSWORD", "admin")

Path(UPLOAD_FOLDER).mkdir(parents=True, exist_ok=True)

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret")


# --- Helpers ---

def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def format_size(size_bytes):
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    else:
        return f"{size_bytes / (1024 * 1024):.1f} MB"


def login_required(f):
    """Decorator que redireciona para /login se o usuário não estiver autenticado."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated


# --- Rotas ---

@app.route("/login", methods=["GET", "POST"])
def login():
    """Página de login."""
    if session.get("logged_in"):
        return redirect(url_for("index"))

    error = None
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        if username == APP_USERNAME and password == APP_PASSWORD:
            session["logged_in"] = True
            session.permanent = False
            return redirect(url_for("index"))
        error = "Usuário ou senha incorretos."

    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    """Encerra a sessão e redireciona para o login."""
    session.clear()
    return redirect(url_for("login"))


@app.route("/", methods=["GET", "POST"])
@login_required
def index():
    """Página principal: upload e listagem de DAGs."""
    message = ""
    if request.method == "POST":
        if "file" not in request.files:
            message = "Nenhum arquivo no formulário."
        else:
            f = request.files["file"]
            if f.filename == "":
                message = "Nenhum arquivo selecionado."
            elif not allowed_file(f.filename):
                message = "Extensão não permitida. Somente arquivos .py são aceitos."
            else:
                filename = secure_filename(f.filename)
                dest = os.path.join(app.config["UPLOAD_FOLDER"], filename)
                f.save(dest)
                return redirect(url_for("index"))

    files = []
    try:
        for name in sorted(os.listdir(app.config["UPLOAD_FOLDER"]), reverse=True):
            full = os.path.join(app.config["UPLOAD_FOLDER"], name)
            if os.path.isfile(full) and name.endswith(".py"):
                stat = os.stat(full)
                files.append({
                    "name": name,
                    "size": stat.st_size,
                    "size_fmt": format_size(stat.st_size),
                    "mtime": datetime.fromtimestamp(stat.st_mtime).strftime("%d/%m/%Y %H:%M")
                })
    except FileNotFoundError:
        pass

    return render_template("index.html", files=files, message=message)


@app.route("/uploads/<path:name>")
@login_required
def download_file(name):
    """Faz download de um arquivo da pasta de DAGs."""
    try:
        return send_from_directory(app.config["UPLOAD_FOLDER"], name, as_attachment=True)
    except FileNotFoundError:
        abort(404)


@app.route("/delete/<path:name>", methods=["GET", "POST"])
@login_required
def delete_file(name):
    """Exclui um arquivo da pasta de DAGs com proteção contra path traversal."""
    full = os.path.join(app.config["UPLOAD_FOLDER"], name)
    full_real = os.path.realpath(full)
    base_real = os.path.realpath(app.config["UPLOAD_FOLDER"])
    if not full_real.startswith(base_real + os.sep) and full_real != base_real:
        abort(403)
    if os.path.exists(full_real) and os.path.isfile(full_real):
        os.remove(full_real)
        return redirect(url_for("index"))
    abort(404)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
