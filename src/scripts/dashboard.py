import streamlit as st
import pandas as pd
from trino.dbapi import connect
import plotly.express as px
from datetime import datetime, timedelta
import requests

st.set_page_config(
    page_title="🏔️ CDC Iceberg Lakehouse Dashboard",
    page_icon="🏔️",
    layout="wide"
)

# Style CSS personnalisé
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #00ffd0;
        font-weight: 700;
        margin-bottom: 0.5rem;
        text-align: center;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #a0aec0;
        text-align: center;
        margin-bottom: 2rem;
    }
    div[data-testid="stMetric"] {
        background-color: #1a202c !important;
        border: 1px solid #2d3748 !important;
        border-radius: 10px !important;
        padding: 15px !important;
    }
    div[data-testid="stMetric"] * {
        color: #ffffff !important;
    }
</style>
""", unsafe_allow_html=True)

st.markdown('<div class="main-header">🏔️ Dashboard CDC Iceberg Lakehouse</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Visualisation Temps Réel PostgreSQL ➔ Apache Iceberg (via Kafka, Debezium, Spark & Nessie)</div>', unsafe_allow_html=True)

# Connexion Trino
def get_trino_conn():
    return connect(
        host="localhost",
        port=8081,
        user="admin",
        catalog="iceberg",
        schema="iceberg_warehouse"
    )

# Exécution de requêtes SQL
def run_query(query):
    try:
        conn = get_trino_conn()
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()
        conn.close()
        return pd.DataFrame(rows, columns=columns)
    except Exception as e:
        err_msg = str(e)
        if "Schema" in err_msg or "Table" in err_msg or "does not exist" in err_msg:
            return "MISSING_TABLES"
        st.sidebar.error(f"Erreur de connexion à Trino : {e}")
        return pd.DataFrame()

# Bouton de rafraîchissement manuel dans la barre latérale
st.sidebar.title("Actions")
if st.sidebar.button("🔄 Rafraîchir les données", use_container_width=True):
    st.rerun()

# Récupération des statistiques globales
df_cust_count = run_query("SELECT COUNT(*) as count FROM iceberg.iceberg_warehouse.customers")

if isinstance(df_cust_count, str) and df_cust_count == "MISSING_TABLES":
    st.warning("⚠️ **Les tables Apache Iceberg ne sont pas encore créées dans le catalogue Trino.**\n\n"
               "Assurez-vous d'avoir :\n"
               "1. Démarré le connecteur Debezium (`register-postgres-connector.sh`)\n"
               "2. Lancé le job Spark Streaming (`submit_job.sh`) qui initialise le schéma et les tables dans le Data Lakehouse.")
    
    st.info("💡 Les conteneurs Docker démarrent-ils correctement ? Vérifiez le statut du connecteur ci-dessous.")
    
    # Affichage du statut du connecteur quand même
    st.subheader("🔌 Statut du Connecteur Debezium PostgreSQL (Kafka Connect)")
    try:
        resp = requests.get("http://localhost:8083/connectors/postgres-cdc-connector/status", timeout=2)
        if resp.status_code == 200:
            st.json(resp.json())
        else:
            st.error("Le connecteur 'postgres-cdc-connector' n'a pas été trouvé. Veuillez l'enregistrer.")
    except Exception as e:
        st.error(f"Impossible de se connecter à Kafka Connect (localhost:8083) : {e}")
        
    st.stop()

# Si les tables existent, charger le reste
df_ord_count = run_query("SELECT COUNT(*) as count FROM iceberg.iceberg_warehouse.orders")
df_inv_count = run_query("SELECT COUNT(*) as count FROM iceberg.iceberg_warehouse.inventory")

cust_count = df_cust_count['count'].iloc[0] if not df_cust_count.empty else 0
ord_count = df_ord_count['count'].iloc[0] if not df_ord_count.empty else 0
inv_count = df_inv_count['count'].iloc[0] if not df_inv_count.empty else 0

# Affichage des KPIs
col1, col2, col3 = st.columns(3)
with col1:
    st.metric(label="👥 Clients Répliqués (Iceberg)", value=cust_count)
with col2:
    st.metric(label="📦 Commandes Répliquées (Iceberg)", value=ord_count)
with col3:
    st.metric(label="🏬 Articles en Stock", value=inv_count)

st.markdown("---")

tab1, tab2, tab3 = st.tabs(["📊 Données en Temps Réel", "⏱️ Voyage dans le Temps (Time Travel)", "🔌 Statut de la Pipeline"])

with tab1:
    st.subheader("🛒 Dernières Commandes Répliquées")
    df_orders = run_query("SELECT order_id, customer_id, total_amount, status, payment_method, _ts FROM iceberg.iceberg_warehouse.orders ORDER BY order_id DESC LIMIT 10")
    if not df_orders.empty:
        if '_ts' in df_orders.columns:
            df_orders['temps_replication'] = pd.to_datetime(df_orders['_ts'] / 1000, unit='s', errors='coerce')
        st.dataframe(df_orders, use_container_width=True)
    else:
        st.info("Aucune commande trouvée.")

    st.subheader("👥 Derniers Clients Répliqués")
    df_cust = run_query("SELECT customer_id, name, email, phone, loyalty_tier FROM iceberg.iceberg_warehouse.customers ORDER BY customer_id DESC LIMIT 10")
    if not df_cust.empty:
        st.dataframe(df_cust, use_container_width=True)
    else:
        st.info("Aucun client trouvé.")

    st.subheader("📈 Répartition des Commandes par Statut")
    df_status = run_query("SELECT status, COUNT(*) as total FROM iceberg.iceberg_warehouse.orders GROUP BY status")
    if not df_status.empty:
        fig = px.pie(df_status, values='total', names='status', title='Répartition des Commandes par Statut',
                     color_discrete_sequence=px.colors.qualitative.Pastel)
        st.plotly_chart(fig, use_container_width=True)
        
    st.subheader("📊 Clients par Catégorie de Fidélité")
    df_tier = run_query("SELECT loyalty_tier, COUNT(*) as total FROM iceberg.iceberg_warehouse.customers GROUP BY loyalty_tier")
    if not df_tier.empty:
        fig2 = px.bar(df_tier, x='loyalty_tier', y='total', title='Clients par Catégorie de Fidélité',
                      labels={'loyalty_tier': 'Catégorie', 'total': 'Nombre de Clients'},
                      color='loyalty_tier', color_discrete_sequence=px.colors.qualitative.Safe)
        st.plotly_chart(fig2, use_container_width=True)

    st.subheader("🏬 Niveaux de Stock (Inventaire)")
    df_inv = run_query("SELECT sku, product_name, category, stock_quantity, unit_price FROM iceberg.iceberg_warehouse.inventory ORDER BY stock_quantity ASC")
    if not df_inv.empty:
        fig_inv = px.bar(df_inv, x='product_name', y='stock_quantity', color='category', 
                         title="Quantités en Stock par Article",
                         labels={'stock_quantity': 'Stock Restant', 'product_name': 'Produit'},
                         color_discrete_sequence=px.colors.qualitative.Bold)
        st.plotly_chart(fig_inv, use_container_width=True)

with tab2:
    st.subheader("⏱️ Exploration Temporelle (Time Travel)")
    st.write("Le format **Apache Iceberg** enregistre l'historique de toutes les transactions. Vous pouvez requêter la base de données telle qu'elle était dans le passé !")
    
    st.markdown("### Étape 1 : Choisir le décalage temporel")
    minutes_ago = st.slider("Remonter dans le temps de (en minutes) :", min_value=1, max_value=30, value=2, step=1)
    
    target_time = datetime.now() - timedelta(minutes=minutes_ago)
    target_time_str = target_time.strftime('%Y-%m-%d %H:%M:%S')
    
    st.info(f"Visualisation de l'état des données tel qu'il était le : **{target_time_str}** (il y a {minutes_ago} minute(s))")
    
    table_to_audit = st.selectbox("Sélectionner la table à auditer :", ["customers", "orders", "inventory"])
    
    query_tt = f"SELECT * FROM iceberg.iceberg_warehouse.{table_to_audit} FOR TIMESTAMP AS OF TIMESTAMP '{target_time_str}'"
    
    st.markdown("### Étape 2 : Requête SQL exécutée")
    st.code(query_tt, language='sql')
    
    st.markdown("### Étape 3 : Données historiques chargées")
    df_tt = run_query(query_tt)
    if isinstance(df_tt, pd.DataFrame):
        if not df_tt.empty:
            st.dataframe(df_tt, use_container_width=True)
            st.success(f"✓ {len(df_tt)} lignes récupérées avec succès de la version historique.")
        else:
            st.warning("Aucune donnée disponible pour cette période ou la table était vide à cet instant précis.")
    else:
        st.error("Une erreur s'est produite lors de l'exécution de la requête de Time Travel.")

with tab3:
    st.subheader("🔌 Kafka Connect - Connecteur Debezium")
    try:
        resp = requests.get("http://localhost:8083/connectors/postgres-cdc-connector/status", timeout=2)
        if resp.status_code == 200:
            st.success("✓ Kafka Connect est actif et le connecteur est enregistré.")
            st.json(resp.json())
        else:
            st.warning("⚠️ Kafka Connect est actif, mais le connecteur n'est pas encore enregistré.")
    except Exception as e:
        st.error(f"❌ Impossible de joindre Kafka Connect à l'adresse http://localhost:8083. Erreur : {e}")

    st.subheader("🐋 Conteneurs Docker Actifs")
    st.info("Vous pouvez vérifier les logs des conteneurs via votre terminal avec : `docker compose logs -f`")
