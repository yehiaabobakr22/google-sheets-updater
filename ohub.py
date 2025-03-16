import streamlit as st
import gspread
import pandas as pd
from sqlalchemy import create_engine
import json
import requests


# Function to fetch JSON file from Google Drive
def get_service_account_json():
    url = "https://drive.google.com/uc?export=download&id=1f4JXoVoU2JOu8Jsea2mZSTu_kqpAjFQy"
    response = requests.get(url)
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        st.error("Failed to load credentials.")
        return None


# Streamlit UI
st.title("Google Sheets Auto-Updater")
st.write("Click the button below to update Google Sheets with fresh data.")

# Load credentials
creds = get_service_account_json()
if creds:
    gc = gspread.service_account_from_dict(creds)
    sh = gc.open_by_key('1RE07O8LrGfY0QaV23kTqJbrMrZiqH3XoYSK3jsdBbs0')

    # Database Connections
    db_configs = {
        "arqam": "postgresql://arqam_analytics:drp528esvc@arqam-db.statsbomb.com:5432/arqam_dev",
        "ops": "postgresql://analysis_team:ZvVU9ajncL@ops-management-db.statsbomb.com:5432/ops_management",
        "ops2": "postgresql://analysis_team:ZvVU9ajncL@ops-management-db.statsbomb.com:5432/ops_management",
        "matchstatus": "postgresql://matchstatus_ro:98aaFHA7sgS66fd@primary-db-prod.cluster-cpnvwmhjbrie.eu-west-2.rds.amazonaws.com:5432/matchstatus",
    }

    # Queries
    queries = {
        "Source": (db_configs["arqam"], """SELECT m.id match_id, c."name" competition, cast(m.match_date as varchar), m.kick_off_time, 
                    m.match_day, m.match_name, s."name" season, c2."name" country, m.match_status, m.home_team_id, 
                    m.away_team_id, t."name" home_team, t2."name" away_team, m.competition_season_id, c.id Comp_id 
                    FROM matches m 
                    JOIN competition_season cs ON cs.id = m.competition_season_id
                    JOIN season s ON s.id = cs.season_id
                    JOIN competitions c ON c.id = cs.competition_id
                    JOIN countries c2 ON c2.id = c.country_id
                    JOIN teams t ON m.home_team_id = t.id
                    JOIN teams t2 ON m.away_team_id = t2.id
                    WHERE m.match_date BETWEEN '2023-01-01' AND NOW() + INTERVAL '30' day 
                    AND m.match_name NOT LIKE '%%review%%'
                    ORDER BY m.match_date, m.kick_off_time"""),

        "Matches Squads": (db_configs["ops"], """SELECT m.id, s."name" squad, cast(ss.date as varchar), s2.shift_type
                        FROM matches m
                        LEFT JOIN squad_shift_matches ssm ON ssm.match_id = m.id
                        LEFT JOIN squads_shifts ss ON ss.id = ssm.squad_shift_id
                        LEFT JOIN squads s ON s.id = ss.squad_id
                        LEFT JOIN shifts s2 ON s2.id = ss.shift_id"""),

        "ps1": (db_configs["ops2"], """SELECT coalesce(coalesce(t.id,cs.id),0) id, pc."type", pc.value priority, p.sla_offset hours
                        FROM priorities p
                        LEFT JOIN priority_categories pc ON pc.id = p.priority_category_id 
                        LEFT JOIN teams t ON t.priority_id = p.id
                        LEFT JOIN competition_seasons cs ON cs.priority_id = p.id"""),

        "ct": (db_configs["matchstatus"], """SELECT arqam_id, cast(completion_time as varchar), cast(first_import as varchar), cast(last_import as varchar) 
                        FROM (WITH query AS (
                        SELECT arqam_id, sbd_id, iq_id, cast(matches."date" as varchar) match_date, kick_off_time, type, "dateTime"
                        FROM matches,
                        jsonb_to_recordset(matches.history) AS x("type" text, "dateTime" timestamp, message json)
                        WHERE arqam_id NOT NULL
                        ORDER BY "dateTime"
                        ) 
                        SELECT arqam_id,
                        MIN (CASE WHEN type = 'COLLECTION_COMPLETE' THEN date_trunc('second',"dateTime")+ interval '2 hour' END) AS completion_time,
                        MIN(CASE WHEN type = 'IQ_IMPORT_SUCCESS' THEN date_trunc('second',"dateTime")+ interval '2 hour' ELSE NULL END) AS first_import,
                        MAX(CASE WHEN type = 'IQ_IMPORT_SUCCESS' THEN date_trunc('second',"dateTime")+ interval '2 hour' ELSE NULL END) AS last_import
                        FROM query GROUP BY arqam_id) AS t1
                        WHERE t1.completion_time >= '2024-01-01'""")
    }

    # Run Update on Button Click
    if st.button("Update Sheets"):
        for sheet_name, (db_url, query) in queries.items():
            try:
                engine = create_engine(db_url)
                df = pd.read_sql(query, engine)
                sheet = sh.worksheet(sheet_name)
                sheet.batch_clear(["A:Z"])
                sheet.update([df.columns.values.tolist()] + df.values.tolist())
                st.success(f"{sheet_name} updated successfully!")
            except Exception as e:
                st.error(f"Error updating {sheet_name}: {str(e)}")
else:
    st.error("Could not authenticate. Check credentials.")
