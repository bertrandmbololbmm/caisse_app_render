import os, io, zipfile, ssl, smtplib, datetime as dt, uuid
from decimal import Decimal
from email.message import EmailMessage
from dotenv import load_dotenv
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, send_file, make_response
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, login_user, login_required, current_user, logout_user, UserMixin
from werkzeug.security import generate_password_hash, check_password_hash

try:
    import pdfkit
    PDF_AVAILABLE = True
except Exception:
    PDF_AVAILABLE = False

load_dotenv()
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
app = Flask(__name__, template_folder="templates", static_folder="static")
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY","dev-secret")
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///" + os.path.join(BASE_DIR, "caisse.db")
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)
login_manager = LoginManager(app)
login_manager.login_view = "login"

class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(120), unique=True, nullable=False)
    name = db.Column(db.String(80), nullable=False)
    password_hash = db.Column(db.String(200), nullable=False)
    role = db.Column(db.String(20), default="user")
    ops = db.relationship("Operation", backref="user", lazy=True)

    def set_password(self, pw): self.password_hash = generate_password_hash(pw)
    def check_password(self, pw): return check_password_hash(self.password_hash, pw)
    def can_edit_any(self): return self.role=="admin"
    def can_edit_own(self): return self.role in {"admin","editor"}
    def read_only(self): return self.role=="viewer"

class InviteToken(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    token = db.Column(db.String(64), unique=True, nullable=False, index=True)
    role = db.Column(db.String(20), nullable=False, default="viewer")
    expires_at = db.Column(db.DateTime, nullable=False)
    used = db.Column(db.Boolean, default=False)

class Category(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(80), unique=True, nullable=False)
    ops = db.relationship("Operation", backref="category", lazy=True)

class Operation(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.Date, nullable=False, default=dt.date.today)
    type = db.Column(db.String(20), nullable=False)
    label = db.Column(db.String(200), nullable=False)
    amount = db.Column(db.Numeric(12,2), nullable=False, default=0)
    note = db.Column(db.Text)
    designation = db.Column(db.String(120))
    quantity = db.Column(db.Integer)
    unit_price = db.Column(db.Numeric(12,2))
    category_id = db.Column(db.Integer, db.ForeignKey('category.id'))
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    created_at = db.Column(db.DateTime, default=dt.datetime.utcnow)

@login_manager.user_loader
def load_user(user_id): return User.query.get(int(user_id))

def parse_date(s, default=None):
    if not s: return default
    return dt.datetime.strptime(s, "%Y-%m-%d").date()

def totals(query):
    rows = query.all()
    entree = sum([float(o.amount) for o in rows if o.type=="entree"])
    depense = sum([float(o.amount) for o in rows if o.type=="depense"])
    vente = sum([float(o.amount) for o in rows if o.type=="vente"])
    solde = entree + vente - depense
    return {"entree": entree, "depense": depense, "vente": vente, "solde": solde}, rows

def require_roles(*roles):
    def wrapper(fn):
        from functools import wraps
        @wraps(fn)
        def inner(*args, **kwargs):
            if not current_user.is_authenticated: return login_manager.unauthorized()
            if current_user.role not in roles:
                flash("Accès refusé.", "danger")
                return redirect(url_for("journal"))
            return fn(*args, **kwargs)
        return inner
    return wrapper

@app.route("/login", methods=["GET","POST"])
def login():
    if request.method == "POST":
        email = request.form["email"].strip().lower()
        pw = request.form["password"]
        user = User.query.filter_by(email=email).first()
        if user and user.check_password(pw):
            login_user(user)
            return redirect(url_for("journal"))
        flash("Identifiants invalides.", "danger")
    return render_template("login.html")

@app.route("/register", methods=["GET", "POST"])
def register():
    invite_token = request.args.get("invite")
    role = "user"
    if invite_token:
        it = InviteToken.query.filter_by(token=invite_token, used=False).first()
        if not it or it.expires_at < dt.datetime.utcnow():
            flash("Lien d'invitation invalide ou expiré.", "danger")
            return redirect(url_for("login"))
        role = it.role
    if request.method == "POST":
        name = request.form["name"]
        email = request.form["email"].strip().lower()
        password = request.form["password"]
        if User.query.filter_by(email=email).first():
            flash("Email déjà utilisé.", "warning")
            return redirect(request.url)
        user = User(name=name, email=email, role=("admin" if User.query.count()==0 else role))
        user.set_password(password)
        db.session.add(user)
        if invite_token:
            it.used = True
        db.session.commit()
        flash("Compte créé. Connectez-vous.", "success")
        return redirect(url_for("login"))
    return render_template("register.html", invite=invite_token, role=role)

@app.route("/logout")
@login_required
def logout():
    logout_user(); return redirect(url_for("login"))

@app.route("/")
@login_required
def index(): return redirect(url_for("journal"))

@app.route("/journal")
@login_required
def journal():
    q = Operation.query.order_by(Operation.date.asc(), Operation.id.asc())
    type_filter = request.args.get("type")
    start = parse_date(request.args.get("start"))
    end = parse_date(request.args.get("end"))
    if type_filter in {"entree","depense","vente"}: q = q.filter_by(type=type_filter)
    if start: q = q.filter(Operation.date >= start)
    if end: q = q.filter(Operation.date <= end)
    if request.args.get("category"): q = q.filter(Operation.category_id == int(request.args["category"]))
    summary, rows = totals(q)
    balance=[]; running=0.0
    for o in rows:
        delta = float(o.amount) if o.type in ("entree","vente") else -float(o.amount)
        running += delta; balance.append(running)
    cats = Category.query.order_by(Category.name.asc()).all()
    return render_template("journal.html", rows=rows, balance=balance, summary=summary,
                           start=start, end=end, type_filter=type_filter, cats=cats)

@app.route("/operation/new", methods=["GET","POST"])
@login_required
@require_roles("admin","editor")
def op_new():
    cats = Category.query.order_by(Category.name.asc()).all()
    if request.method == "POST":
        type_ = request.form["type"]
        date = parse_date(request.form["date"], dt.date.today())
        label = request.form["label"]
        note = request.form.get("note")
        category_id = request.form.get("category_id") or None
        if category_id: category_id = int(category_id)
        designation = request.form.get("designation") or None
        quantity = request.form.get("quantity")
        unit_price = request.form.get("unit_price")
        amount = request.form.get("amount")
        if type_=="vente":
            qv = int(quantity) if quantity else None
            up = Decimal(unit_price) if unit_price else None
            if (not amount) and qv is not None and up is not None:
                amount = qv * up
        amount = Decimal(str(amount)) if amount else Decimal("0")
        op = Operation(date=date, type=type_, label=label, note=note,
                       category_id=category_id, designation=designation,
                       quantity=int(quantity) if quantity else None,
                       unit_price=Decimal(unit_price) if unit_price else None,
                       amount=amount, user_id=current_user.id)
        db.session.add(op); db.session.commit()
        flash("Opération enregistrée.", "success")
        return redirect(url_for("journal"))
    return render_template("form.html", cats=cats)

@app.route("/operation/<int:op_id>/edit", methods=["GET","POST"])
@login_required
def op_edit(op_id):
    op = Operation.query.get_or_404(op_id)
    if not (current_user.can_edit_any() or (current_user.can_edit_own() and op.user_id==current_user.id)):
        flash("Modification non autorisée.", "danger"); return redirect(url_for("journal"))
    cats = Category.query.order_by(Category.name.asc()).all()
    if request.method == "POST":
        op.type = request.form["type"]
        op.date = parse_date(request.form["date"], op.date)
        op.label = request.form["label"]
        op.note = request.form.get("note")
        category_id = request.form.get("category_id") or None
        op.category_id = int(category_id) if category_id else None
        op.designation = request.form.get("designation") or None
        op.quantity = int(request.form["quantity"]) if request.form.get("quantity") else None
        op.unit_price = Decimal(request.form["unit_price"]) if request.form.get("unit_price") else None
        amount = request.form.get("amount")
        if op.type=="vente" and (not amount) and op.quantity is not None and op.unit_price is not None:
            amount = op.quantity * op.unit_price
        op.amount = Decimal(str(amount)) if amount else Decimal("0")
        db.session.commit()
        flash("Opération mise à jour.", "success")
        return redirect(url_for("journal"))
    return render_template("form.html", op=op, cats=cats)

@app.route("/operation/<int:op_id>/delete", methods=["POST"])
@login_required
def op_delete(op_id):
    op = Operation.query.get_or_404(op_id)
    if not (current_user.can_edit_any() or (current_user.can_edit_own() and op.user_id==current_user.id)):
        flash("Suppression non autorisée.", "danger"); return redirect(url_for("journal"))
    db.session.delete(op); db.session.commit()
    flash("Opération supprimée.", "info"); return redirect(url_for("journal"))

@app.route("/categories", methods=["GET","POST"])
@login_required
@require_roles("admin")
def categories():
    if request.method == "POST":
        name = request.form["name"].strip()
        if name and not Category.query.filter_by(name=name).first():
            db.session.add(Category(name=name)); db.session.commit()
            flash("Catégorie ajoutée.", "success")
        else: flash("Nom vide ou déjà existant.", "warning")
        return redirect(url_for("categories"))
    cats = Category.query.order_by(Category.name.asc()).all()
    return render_template("categories.html", cats=cats)

@app.route("/categories/<int:cat_id>/delete", methods=["POST"])
@login_required
@require_roles("admin")
def category_delete(cat_id):
    cat = Category.query.get_or_404(cat_id)
    db.session.delete(cat); db.session.commit()
    flash("Catégorie supprimée.", "info")
    return redirect(url_for("categories"))

@app.route("/admin/users")
@login_required
@require_roles("admin")
def admin_users():
    users = User.query.order_by(User.id.asc()).all()
    invites = InviteToken.query.order_by(InviteToken.id.desc()).all()
    return render_template("users.html", users=users, invites=invites)

@app.route("/admin/invite/create")
@login_required
@require_roles("admin")
def admin_invite_create():
    role = request.args.get("role","viewer")
    if role not in {"viewer","editor"}: role="viewer"
    token = uuid.uuid4().hex
    it = InviteToken(token=token, role=role, expires_at=dt.datetime.utcnow()+dt.timedelta(days=7))
    db.session.add(it); db.session.commit()
    link = url_for("register", invite=token, _external=True)
    flash(f"Lien d'invitation {role} créé : {link}", "success")
    return redirect(url_for("admin_users"))

import pandas as pd
@app.route("/rapports")
@login_required
def rapports():
    q = Operation.query
    summary, rows = totals(q)
    return render_template("rapports.html", summary=summary)

@app.route("/export/csv")
@login_required
def export_csv():
    q = Operation.query.order_by(Operation.date.asc(), Operation.id.asc())
    type_filter = request.args.get("type")
    start = parse_date(request.args.get("start"))
    end = parse_date(request.args.get("end"))
    if type_filter in {"entree","depense","vente"}: q = q.filter_by(type=type_filter)
    if start: q = q.filter(Operation.date >= start)
    if end: q = q.filter(Operation.date <= end)
    if request.args.get("category"): q = q.filter(Operation.category_id == int(request.args["category"]))
    rows = q.all()
    data = [{
        "date": o.date.isoformat(),
        "type": o.type, "label": o.label, "amount": float(o.amount),
        "category": o.category.name if o.category else "",
        "designation": o.designation or "", "quantity": o.quantity or "",
        "unit_price": float(o.unit_price) if o.unit_price else "", "note": o.note or "",
    } for o in rows]
    df = pd.DataFrame(data)
    csv_path = os.path.join(BASE_DIR, "export.csv")
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    return send_file(csv_path, as_attachment=True, download_name="journal.csv")

@app.route("/export/pdf")
@login_required
def export_pdf():
    if not PDF_AVAILABLE:
        flash("Export PDF indisponible : installez wkhtmltopdf.", "warning")
        return redirect(url_for("journal"))
    rendered = render_template("rapports.html", summary={"entree":0,"vente":0,"depense":0,"solde":0})
    pdf = pdfkit.from_string(rendered, False)
    response = make_response(pdf)
    response.headers['Content-Type'] = 'application/pdf'
    response.headers['Content-Disposition'] = 'attachment; filename=journal.pdf'
    return response

@app.route("/dashboard")
@login_required
def dashboard(): return render_template("dashboard.html")

@app.route("/api/monthly")
@login_required
def api_monthly():
    rows = Operation.query.all()
    by_month = {}
    for o in rows:
        key = o.date.strftime("%Y-%m")
        if key not in by_month: by_month[key] = {"entree":0.0, "depense":0.0, "vente":0.0}
        by_month[key][o.type] += float(o.amount)
    months = sorted(by_month.keys())
    net = [by_month[m]["entree"] + by_month[m]["vente"] - by_month[m]["depense"] for m in months]
    entree = [by_month[m]["entree"] for m in months]
    depense = [by_month[m]["depense"] for m in months]
    vente = [by_month[m]["vente"] for m in months]
    return jsonify({"labels": months, "net": net, "entree": entree, "depense": depense, "vente": vente})

@app.route("/backup")
@login_required
def backup():
    user = os.getenv("GMAIL_USER")
    pwd  = os.getenv("GMAIL_APP_PASSWORD")
    to   = os.getenv("GMAIL_TO", user or "")
    if not (user and pwd and to):
        flash("Configurez GMAIL_USER / GMAIL_APP_PASSWORD / GMAIL_TO dans .env (ou variables Render).", "warning")
        return redirect(url_for("journal"))
    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, "w", zipfile.ZIP_DEFLATED) as z:
        z.write(os.path.join(BASE_DIR, "caisse.db"), "caisse.db")
    zip_bytes.seek(0)
    msg = EmailMessage()
    msg["Subject"] = f"Sauvegarde Caisse - {dt.date.today().isoformat()}"
    msg["From"] = user; msg["To"] = to
    msg.set_content("Sauvegarde automatique de la base Caisse.")
    msg.add_attachment(zip_bytes.read(), maintype="application", subtype="zip",
                       filename=f"caisse_backup_{dt.date.today().isoformat()}.zip")
    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx) as smtp:
        smtp.login(user, pwd); smtp.send_message(msg)
    flash("Sauvegarde envoyée sur Gmail ✅", "success")
    return redirect(url_for("journal"))

@app.cli.command("init-db")
def init_db():
    db.create_all()
    for n in ["loyer","internet","eau","CIE","matériel","transport","nourriture","facebook","divers","vente"]:
        if not Category.query.filter_by(name=n).first():
            db.session.add(Category(name=n))
    db.session.commit()
    print("Base initialisée.")

if __name__ == "__main__":
    with app.app_context(): db.create_all()
    app.run(debug=True)
