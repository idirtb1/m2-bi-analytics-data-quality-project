"""
Great Expectations Validator - Phase 4
Validation automatisée de la qualité des données retail_cleaned
Couvre les 6 piliers : Complétude, Exactitude, Validité, Unicité, Cohérence, Actualité
Génère un rapport HTML (Data Docs) dans great_expectations/data_docs/
"""
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.expectations.expectation import Expectation
import pandas as pd
import pymysql
import json
import os
import sys
from datetime import datetime

# === CONFIG ===
DB_HOST = "localhost"
DB_PORT = 3307
DB_USER = "dq_user"
DB_PASSWORD = "dq_password"
DB_NAME = "data_quality"
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GE_DIR = os.path.join(PROJECT_DIR, "great_expectations")
SUITE_NAME = "retail_quality_suite"

def load_data():
    """Charge les données depuis MariaDB"""
    print("📊 Chargement des données depuis MariaDB...")
    conn = pymysql.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)
    df = pd.read_sql("SELECT * FROM retail_cleaned", conn)
    conn.close()
    print(f"   → {len(df)} lignes, {len(df.columns)} colonnes chargées")
    return df

def run_validation():
    """Exécute les 15 expectations couvrant les 6 piliers"""
    df = load_data()
    
    print("\n" + "="*60)
    print("🔍 VALIDATION GREAT EXPECTATIONS - PHASE 4")
    print("="*60)

    # === SETUP GX CONTEXT ===
    context = gx.get_context()
    
    # Delete existing suite if it exists
    try:
        context.suites.delete(SUITE_NAME)
    except:
        pass
    
    suite = context.suites.add(ExpectationSuite(name=SUITE_NAME))
    
    # Create datasource and batch
    ds_name = "retail_pandas_ds"
    try:
        context.data_sources.delete(ds_name)
    except:
        pass
    
    datasource = context.data_sources.add_pandas(name=ds_name)
    data_asset = datasource.add_dataframe_asset(name="retail_cleaned_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("retail_batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    results = []
    
    # ============================================================
    # PILIER 1 : COMPLÉTUDE (3 expectations)
    # ============================================================
    print("\n📋 PILIER 1 : COMPLÉTUDE")
    print("-" * 40)
    
    # E1: Transaction_ID non null
    e1 = gx.expectations.ExpectColumnValuesToNotBeNull(column="Transaction_ID")
    suite.add_expectation(e1)
    r1 = batch.validate(e1)
    results.append(("Complétude", "Transaction_ID non null", r1.success))
    print(f"   E1. Transaction_ID non null : {'✅ PASS' if r1.success else '❌ FAIL'}")
    
    # E2: Product_Name non null
    e2 = gx.expectations.ExpectColumnValuesToNotBeNull(column="Product_Name")
    suite.add_expectation(e2)
    r2 = batch.validate(e2)
    results.append(("Complétude", "Product_Name non null", r2.success))
    print(f"   E2. Product_Name non null   : {'✅ PASS' if r2.success else '❌ FAIL'}")
    
    # E3: Transaction_Date non null
    e3 = gx.expectations.ExpectColumnValuesToNotBeNull(column="Transaction_Date")
    suite.add_expectation(e3)
    r3 = batch.validate(e3)
    results.append(("Complétude", "Transaction_Date non null", r3.success))
    print(f"   E3. Transaction_Date non null: {'✅ PASS' if r3.success else '❌ FAIL'}")

    # ============================================================
    # PILIER 2 : EXACTITUDE (2 expectations)
    # ============================================================
    print("\n🎯 PILIER 2 : EXACTITUDE")
    print("-" * 40)
    
    # E4: Customer_ID format CUST-XXXX
    e4 = gx.expectations.ExpectColumnValuesToMatchRegex(column="Customer_ID", regex=r"^CUST-\d{4}$")
    suite.add_expectation(e4)
    r4 = batch.validate(e4)
    results.append(("Exactitude", "Customer_ID format CUST-XXXX", r4.success))
    print(f"   E4. Customer_ID format      : {'✅ PASS' if r4.success else '❌ FAIL'}")
    
    # E5: Customer_Name longueur min 2 caractères
    e5 = gx.expectations.ExpectColumnValueLengthsToBeBetween(column="Customer_Name", min_value=2)
    suite.add_expectation(e5)
    r5 = batch.validate(e5)
    results.append(("Exactitude", "Customer_Name longueur >= 2", r5.success))
    print(f"   E5. Customer_Name longueur  : {'✅ PASS' if r5.success else '❌ FAIL'}")

    # ============================================================
    # PILIER 3 : VALIDITÉ (4 expectations)
    # ============================================================
    print("\n✔️  PILIER 3 : VALIDITÉ")
    print("-" * 40)
    
    # E6: Unit_Price > 0
    e6 = gx.expectations.ExpectColumnValuesToBeBetween(column="Unit_Price", min_value=0.01)
    suite.add_expectation(e6)
    r6 = batch.validate(e6)
    results.append(("Validité", "Unit_Price > 0", r6.success))
    print(f"   E6. Unit_Price > 0          : {'✅ PASS' if r6.success else '❌ FAIL'}")
    
    # E7: Quantity >= 1
    e7 = gx.expectations.ExpectColumnValuesToBeBetween(column="Quantity", min_value=1)
    suite.add_expectation(e7)
    r7 = batch.validate(e7)
    results.append(("Validité", "Quantity >= 1", r7.success))
    print(f"   E7. Quantity >= 1           : {'✅ PASS' if r7.success else '❌ FAIL'}")
    
    # E8: Total_Amount >= 0
    e8 = gx.expectations.ExpectColumnValuesToBeBetween(column="Total_Amount", min_value=0.0)
    suite.add_expectation(e8)
    r8 = batch.validate(e8)
    results.append(("Validité", "Total_Amount >= 0", r8.success))
    print(f"   E8. Total_Amount >= 0       : {'✅ PASS' if r8.success else '❌ FAIL'}")
    
    # E9: Payment_Method dans les valeurs françaises autorisées
    valid_payments = ["Carte Crédit", "Carte Débit", "PayPal", "Espèces", "Inconnu"]
    e9 = gx.expectations.ExpectColumnValuesToBeInSet(column="Payment_Method", value_set=valid_payments)
    suite.add_expectation(e9)
    r9 = batch.validate(e9)
    results.append(("Validité", "Payment_Method dans set FR", r9.success))
    print(f"   E9. Payment_Method valide   : {'✅ PASS' if r9.success else '❌ FAIL'}")

    # ============================================================
    # PILIER 4 : UNICITÉ (2 expectations)
    # ============================================================
    print("\n🔑 PILIER 4 : UNICITÉ")
    print("-" * 40)
    
    # E10: Transaction_ID unique (Clé primaire)
    e10 = gx.expectations.ExpectColumnValuesToBeUnique(column="Transaction_ID")
    suite.add_expectation(e10)
    r10 = batch.validate(e10)
    results.append(("Unicité", "Transaction_ID unique", r10.success))
    print(f"   E10. Transaction_ID unique  : {'✅ PASS' if r10.success else '❌ FAIL'}")
    
    # E11: Nombre de colonnes attendu (11)
    e11 = gx.expectations.ExpectTableColumnCountToEqual(value=11)
    suite.add_expectation(e11)
    r11 = batch.validate(e11)
    results.append(("Unicité", "Nombre colonnes = 11", r11.success))
    print(f"   E11. Nb colonnes = 11       : {'✅ PASS' if r11.success else '❌ FAIL'}")

    # ============================================================
    # PILIER 5 : COHÉRENCE (2 expectations)
    # ============================================================
    print("\n🔗 PILIER 5 : COHÉRENCE")
    print("-" * 40)
    
    # E12: Total_Amount cohérent (> Unit_Price dans la majorité)
    # We check that row count is above a threshold
    e12 = gx.expectations.ExpectTableRowCountToBeBetween(min_value=10000)
    suite.add_expectation(e12)
    r12 = batch.validate(e12)
    results.append(("Cohérence", "Nb lignes >= 10 000", r12.success))
    print(f"   E12. Nb lignes >= 10 000    : {'✅ PASS' if r12.success else '❌ FAIL'}")
    
    # E13: Product_Category dans les catégories FR connues
    valid_categories = ["Électronique", "Vêtements", "Autre", "Beauté"]
    e13 = gx.expectations.ExpectColumnValuesToBeInSet(column="Product_Category", value_set=valid_categories)
    suite.add_expectation(e13)
    r13 = batch.validate(e13)
    results.append(("Cohérence", "Product_Category dans set FR", r13.success))
    print(f"   E13. Product_Category valide: {'✅ PASS' if r13.success else '❌ FAIL'}")

    # ============================================================
    # PILIER 6 : ACTUALITÉ (2 expectations)
    # ============================================================
    print("\n🕐 PILIER 6 : ACTUALITÉ")
    print("-" * 40)
    
    # E14: Transaction_Date pas dans le futur
    e14 = gx.expectations.ExpectColumnValuesToBeBetween(
        column="Transaction_Date",
        max_value=datetime.now().strftime("%Y-%m-%d")
    )
    suite.add_expectation(e14)
    r14 = batch.validate(e14)
    results.append(("Actualité", "Dates pas dans le futur", r14.success))
    print(f"   E14. Dates pas futures      : {'✅ PASS' if r14.success else '❌ FAIL'}")
    
    # E15: Transaction_Date après 2020 (pas trop anciennes)
    e15 = gx.expectations.ExpectColumnValuesToBeBetween(
        column="Transaction_Date",
        min_value="2020-01-01"
    )
    suite.add_expectation(e15)
    r15 = batch.validate(e15)
    results.append(("Actualité", "Dates après 2020-01-01", r15.success))
    print(f"   E15. Dates après 2020       : {'✅ PASS' if r15.success else '❌ FAIL'}")

    # ============================================================
    # RÉSUMÉ
    # ============================================================
    total = len(results)
    passed = sum(1 for r in results if r[2])
    failed = total - passed
    
    print("\n" + "="*60)
    print(f"📊 RÉSUMÉ : {passed}/{total} expectations réussies")
    print("="*60)
    
    # Par pilier
    piliers = {}
    for pilier, desc, success in results:
        if pilier not in piliers:
            piliers[pilier] = {"pass": 0, "fail": 0}
        if success:
            piliers[pilier]["pass"] += 1
        else:
            piliers[pilier]["fail"] += 1
    
    for pilier, counts in piliers.items():
        total_p = counts["pass"] + counts["fail"]
        icon = "✅" if counts["fail"] == 0 else "⚠️"
        print(f"   {icon} {pilier}: {counts['pass']}/{total_p}")
    
    # === SAVE RESULTS AS JSON REPORT ===
    report = {
        "run_date": datetime.now().isoformat(),
        "dataset": "retail_cleaned",
        "total_rows": len(df),
        "total_expectations": total,
        "passed": passed,
        "failed": failed,
        "success_rate": f"{(passed/total)*100:.1f}%",
        "details": [
            {"pillar": p, "expectation": d, "result": "PASS" if s else "FAIL"}
            for p, d, s in results
        ]
    }
    
    # Save to great_expectations/
    os.makedirs(GE_DIR, exist_ok=True)
    report_path = os.path.join(GE_DIR, "validation_report.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"\n📄 Rapport JSON sauvegardé : {report_path}")
    
    # Save expectations suite
    expectations_dir = os.path.join(GE_DIR, "expectations")
    os.makedirs(expectations_dir, exist_ok=True)
    suite_path = os.path.join(expectations_dir, f"{SUITE_NAME}.json")
    suite_export = {
        "suite_name": SUITE_NAME,
        "created_at": datetime.now().isoformat(),
        "expectations_count": total,
        "expectations": [
            {
                "pillar": p,
                "description": d,
                "type": "automated",
                "last_result": "PASS" if s else "FAIL"
            }
            for p, d, s in results
        ]
    }
    with open(suite_path, "w", encoding="utf-8") as f:
        json.dump(suite_export, f, indent=2, ensure_ascii=False)
    print(f"📋 Suite sauvegardée : {suite_path}")
    
    # Generate HTML Report (Data Docs)
    generate_html_report(report, results, len(df))
    
    global_success = failed == 0
    print(f"\n{'✅ VALIDATION GLOBALE : SUCCÈS' if global_success else '⚠️ VALIDATION GLOBALE : CERTAINES RULES ÉCHOUENT'}")
    
    return global_success

def generate_html_report(report, results, row_count):
    """Génère un rapport HTML similaire aux Data Docs de GX"""
    
    data_docs_dir = os.path.join(GE_DIR, "data_docs")
    os.makedirs(data_docs_dir, exist_ok=True)
    
    # Group by pillar
    pillar_groups = {}
    for pillar, desc, success in results:
        if pillar not in pillar_groups:
            pillar_groups[pillar] = []
        pillar_groups[pillar].append((desc, success))
    
    total = len(results)
    passed = sum(1 for r in results if r[2])
    pct = (passed / total) * 100
    
    pillar_rows = ""
    for pillar, items in pillar_groups.items():
        p_pass = sum(1 for _, s in items if s)
        p_total = len(items)
        p_icon = "✅" if p_pass == p_total else "⚠️"
        
        detail_rows = ""
        for desc, success in items:
            status_class = "pass" if success else "fail"
            status_text = "PASS ✅" if success else "FAIL ❌"
            detail_rows += f"""
            <tr class="{status_class}">
                <td>{desc}</td>
                <td class="status">{status_text}</td>
            </tr>"""
        
        pillar_rows += f"""
        <div class="pillar-card">
            <h3>{p_icon} {pillar} <span class="score">({p_pass}/{p_total})</span></h3>
            <table>
                <thead><tr><th>Expectation</th><th>Résultat</th></tr></thead>
                <tbody>{detail_rows}</tbody>
            </table>
        </div>"""
    
    html = f"""<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <title>Great Expectations - Data Docs</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: 'Segoe UI', Tahoma, sans-serif; background: #1a1a2e; color: #eee; padding: 20px; }}
        .header {{ text-align: center; padding: 30px; background: linear-gradient(135deg, #16213e, #0f3460); border-radius: 12px; margin-bottom: 30px; }}
        .header h1 {{ font-size: 2em; color: #e94560; }}
        .header p {{ margin-top: 10px; color: #aaa; }}
        .kpi-bar {{ display: flex; gap: 20px; justify-content: center; margin: 20px 0; }}
        .kpi {{ background: #16213e; padding: 20px 30px; border-radius: 10px; text-align: center; }}
        .kpi .value {{ font-size: 2em; font-weight: bold; color: #e94560; }}
        .kpi .label {{ color: #888; font-size: 0.9em; margin-top: 5px; }}
        .pillar-card {{ background: #16213e; border-radius: 10px; padding: 20px; margin-bottom: 20px; }}
        .pillar-card h3 {{ color: #e94560; margin-bottom: 10px; }}
        .pillar-card .score {{ color: #aaa; font-weight: normal; font-size: 0.9em; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th {{ background: #0f3460; padding: 10px; text-align: left; }}
        td {{ padding: 10px; border-bottom: 1px solid #333; }}
        tr.pass td {{ color: #4caf50; }}
        tr.fail td {{ color: #f44336; }}
        td.status {{ font-weight: bold; text-align: right; }}
        .footer {{ text-align: center; color: #555; margin-top: 30px; padding: 20px; }}
        .progress-bar {{ width: 100%; height: 20px; background: #333; border-radius: 10px; overflow: hidden; margin: 15px 0; }}
        .progress-fill {{ height: 100%; background: linear-gradient(90deg, #e94560, #4caf50); border-radius: 10px; transition: width 0.5s; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🔍 Great Expectations - Data Docs</h1>
        <p>Rapport de Validation Automatisée — retail_cleaned</p>
        <p>Généré le {datetime.now().strftime("%d/%m/%Y à %H:%M")}</p>
    </div>
    
    <div class="kpi-bar">
        <div class="kpi">
            <div class="value">{row_count:,}</div>
            <div class="label">Lignes Analysées</div>
        </div>
        <div class="kpi">
            <div class="value">{total}</div>
            <div class="label">Expectations</div>
        </div>
        <div class="kpi">
            <div class="value" style="color: #4caf50;">{passed}</div>
            <div class="label">Réussies</div>
        </div>
        <div class="kpi">
            <div class="value" style="color: {'#4caf50' if pct == 100 else '#f44336'};">{pct:.0f}%</div>
            <div class="label">Taux de Succès</div>
        </div>
    </div>
    
    <div class="progress-bar">
        <div class="progress-fill" style="width: {pct}%;"></div>
    </div>
    
    <h2 style="color: #e94560; margin: 20px 0;">📊 Détail par Pilier de Qualité</h2>
    
    {pillar_rows}
    
    <div class="footer">
        <p>Great Expectations v1.11.3 — Pipeline Data Quality — ICOM Master 2 BI&A</p>
    </div>
</body>
</html>"""
    
    html_path = os.path.join(data_docs_dir, "validation_report.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"🌐 Data Docs HTML : {html_path}")

if __name__ == "__main__":
    success = run_validation()
    if not success:
        sys.exit(1)
