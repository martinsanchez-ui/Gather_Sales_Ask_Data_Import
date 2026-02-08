####################################################################################################
#	Author: Chris O'Brien
#	Created On: Sat Nov 06 2021
#	File: gather_sales_data_import_test.py
#	Description: Sweeps the BI email for returned files and updates the data changes in
#                bi_warehouse_prd.gather_sales_data_f.  Creates relationship for office_manager if
#                the contact exists but is not in the relationships table.
#   Note: BI Automation User ID: 2769
####################################################################################################
import datetime
import os
import re
import shutil
import sys
import time
import warnings
from operator import itemgetter
import argparse

import MySQLdb as mysql
import pandas as pd
import requests
import unidecode
from dotenv import load_dotenv
from pgeocode import GeoDistance
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from exchangelib import Account, Configuration, FileAttachment, Identity, IMPERSONATION, OAuth2Credentials
from exchangelib.version import Version, EXCHANGE_O365

from etl_utilities.RMLogger import Logger
from etl_utilities.utils import load_secrets, send_teams_message


load_dotenv()

DB_LEVEL = "prod"
ENV_FILE_PATH = os.path.abspath('/datafiles/app_credentials.env')
SCRIPT_PATH = sys.path[0] + os.sep
DOWNLOAD_STAGING_FOLDER = os.path.join(SCRIPT_PATH, "files", "downloads")
LOG_LOCATION = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gsd_askdata_logs", "log.log")
SEND_TEAMS_MESSAGE_SUMMARY = "Gather_Sales_Data_Ask_Data_Import"
SEND_TEAMS_MESSAGE_ACTIVITY_TITLE = "Data Not Updated"
EMAIL_LOOKBACK_DAYS = int(os.getenv("GSD_EMAIL_LOOKBACK_DAYS", "180"))
GSD_DRY_RUN = os.getenv("GSD_DRY_RUN", "0") == "1"
GSD_AUDIT_RUN = os.getenv("GSD_AUDIT_RUN", "0") == "1"

SECRETS = load_secrets(ENV_FILE_PATH, values_required=["dbmaster_hostname", "dbmaster_username", "dbmaster_password", "dbmaster_database", "bi_hostname", "bi_username", 
                                                       "bi_password", "bi_database", "client_id", "client_secret", "tenant_id",  "primary_smtp_address", "outlook_server", 
                                                       "set_office_status_url", "set_office_status_api_key" ])

log = Logger(log_file_location=LOG_LOCATION, log_file_backup_count=50, logging_level="DEBUG")

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
pd.options.display.width = 0
warnings.filterwarnings("ignore", category=mysql.Warning)
warnings.filterwarnings("ignore", category=UnicodeWarning)

log.info("Beginning processing...")
start_ts = time.time()

log.info("Opening database connection...")
if DB_LEVEL == "prod":
    db = mysql.connect(SECRETS.get("dbmaster_hostname"), SECRETS.get("dbmaster_username"), SECRETS.get("dbmaster_password"), SECRETS.get("dbmaster_database"), autocommit=True)
    bi = mysql.connect(SECRETS.get("bi_hostname"), SECRETS.get("bi_username"), SECRETS.get("bi_password"), SECRETS.get("bi_database"), autocommit=True)
    log.info("Database connection open.")
else:
    raise ValueError("Unknown db_level value!")


def is_auth_failure(error):
    error_text = str(error).lower()
    auth_markers = ["aadsts", "refresh_token", "invalid_client", "invalid_grant", "oauth"]
    return any(marker in error_text for marker in auth_markers)


def ensure_folder_exists(folder_path, folder_label):
    log.info("Checking for {fl} folder...".format(fl=folder_label))
    if not os.path.exists(folder_path):
        log.info("{fl} folder does not exist.  Creating...".format(fl=folder_label))
        try:
            os.makedirs(folder_path, exist_ok=True)
        except OSError as e:
            log.error("Failed to create {fl} folder {tf}: {e}".format(fl=folder_label, tf=folder_path, e=e))
            raise
        log.info("{fl} folder: {tf} created.".format(fl=folder_label, tf=folder_path))
    else:
        log.info("{fl} folder already exists.".format(fl=folder_label))


def resolve_file_path_with_fallback(local_path, staging_folder):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    if os.path.exists(local_path):
        return local_path

    base = os.path.basename(local_path)
    fallback_folders = [
        staging_folder,
        os.path.join(SCRIPT_PATH, "files"),
        os.path.join(SCRIPT_PATH, "backup"),
        os.path.join(SCRIPT_PATH, "error"),
        os.path.join(SCRIPT_PATH, "dryrun_backup"),
    ]

    for folder in fallback_folders:
        candidate_path = os.path.join(folder, base)
        if os.path.exists(candidate_path):
            return candidate_path

    return None


def update_companies_table(office_id, employee_count=0, phone_number=""):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    log.info("Updating company info in xrms.companies for company_id: {oid}".format(oid=office_id))
    update_list = []

    if employee_count > 0:
        update_list.append("employees={emp_cnt}".format(emp_cnt=employee_count))

    if phone_number != "":
        update_list.append("phone='{phone}'".format(phone=phone_number))

    if len(update_list) > 0:
        update_sql = """UPDATE xrms.companies SET """
        part = ", ".join(update_list)
        update_sql = update_sql + "{part} WHERE company_id={oid};".format(part=part, oid=office_id)
        update_cur = db.cursor()

        try:
            update_cur.execute(update_sql.format(emp_cnt=employee_count, oid=office_id))
            db.commit()
            log.info("Company table update successful.")
        except mysql.Error as e:
            log.error("Update failed with MySQL error: {e}".format(e=e))
            last_sql = getattr(update_cur, "_last_executed", None)
            if last_sql:
                log.error("Attempted query: {q}".format(q=last_sql))

        update_cur.close()
    else:
        log.error("Error in update list length!")


def lookup_activity_id(workflow_id):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    log.info("Getting activity_id for workflow ID: {wid}".format(wid=workflow_id))

    lookup_activity_id_sql = """
    SELECT
	    a.activity_id
    FROM
	    xrms.activities a 
    WHERE
	    1=1
	    AND a.on_what_table = 'cases' 
	    AND on_what_id = {workflow_id} 
	    AND activity_record_status = 'a'
    ORDER BY
        a.activity_id DESC
    LIMIT
        1
    ;
    """

    lookup_activity_id_cur = db.cursor()
    lookup_activity_id_cur.execute(lookup_activity_id_sql.format(workflow_id=workflow_id))
    activity_id = lookup_activity_id_cur.fetchone()
    lookup_activity_id_cur.close()

    if activity_id is not None:
        log.info("Activity ID found: {at}".format(at=activity_id[0]))
        return activity_id[0]
    else:
        log.error("No activity ID could be found!")
        return 0

def update_notes_table(workflow_id, note_text):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    activity_id = lookup_activity_id(workflow_id)
    
    notes_insert_sql = """INSERT INTO xrms.notes (note_description, on_what_table, on_what_id, entered_at, entered_by, dismissed_at, dismissed_by, note_record_status, parent_note_id, category_id, deleted_by, activity_id, case_id, opportunity_id, system_note, flags, lifespan_start, lifespan_end)
    VALUES(%s, 'cases', %s, current_timestamp(), 2769, '0000-00-00 00:00:00', 0, 'a', 0, 5, 0, %s, 0, 0, 0, 0, '0000-00-00', '0000-00-00');"""
    
    notes_insert_params = (note_text, workflow_id, activity_id)
    notes_insert_cur = db.cursor()

    if activity_id > 0:
        try:
            notes_insert_cur.execute(notes_insert_sql, notes_insert_params)
            db.commit()
            # Get inserted ID
            note_insert_id = notes_insert_cur.lastrowid
            log.info("Notes insert successful.  Inserted ID: {nc}".format(nc=note_insert_id))
        except mysql.Error as e:
            log.error("Insert failed with MySQL error: {e}".format(e=e))
            last_sql = getattr(notes_insert_cur, "_last_executed", None)
            if last_sql:
                log.error("Attempted query: {q}".format(q=last_sql))
    else:
        log.error("Could not add note to case {cid}, activity ID not found!".format(cid=workflow_id))

    notes_insert_cur.close()


def update_gsd_table(status_name, office_id):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    update_sql = """UPDATE bi_warehouse_prd.gather_sales_data_f SET recieved_dtt=current_timestamp(), status_name='{status_name}' WHERE office_id={office_id};"""
    update_cur = bi.cursor()

    try:
        update_cur.execute(update_sql.format(status_name=status_name, office_id=office_id))
        bi.commit()
        log.info("GSD table update successful for office ID: {oid}".format(oid=int(office_id)))
    except mysql.Error as e:
        log.error("Update failed with MySQL error: {e}".format(e=e))

    update_cur.close()


def parse_reference_date_from_filename(filename):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    basename = os.path.basename(filename)
    match = re.search(r"gather_sales_data_(\d{8})", basename, re.IGNORECASE)
    if match:
        try:
            return datetime.datetime.strptime(match.group(1), "%Y%m%d")
        except ValueError:
            return None
    return None


def is_attachment_already_processed(df_or_office_ids, reference_dt, stats=None):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    if reference_dt is None:
        return False

    if hasattr(df_or_office_ids, "columns") and "office_id" in df_or_office_ids.columns:
        office_ids = df_or_office_ids["office_id"].tolist()
    else:
        office_ids = df_or_office_ids

    office_ids = [int(office_id) for office_id in office_ids if pd.notna(office_id)]
    office_ids = sorted(set(office_ids))

    if not office_ids:
        return False

    if reference_dt.tzinfo is not None:
        reference_dt = reference_dt.replace(tzinfo=None)

    office_id_list = ",".join(str(office_id) for office_id in office_ids)
    lookup_sql = """
        SELECT office_id, recieved_dtt
        FROM bi_warehouse_prd.gather_sales_data_f
        WHERE office_id IN ({office_id_list});
    """

    lookup_cur = bi.cursor()
    lookup_cur.execute(lookup_sql.format(office_id_list=office_id_list))
    rows = lookup_cur.fetchall()
    lookup_cur.close()

    recieved_by_office = {row[0]: row[1] for row in rows}

    total_count = len(office_ids)
    processed_count = 0

    for office_id in office_ids:
        recieved_dtt = recieved_by_office.get(office_id)
        if recieved_dtt is not None and recieved_dtt >= reference_dt:
            processed_count += 1

    pending_count = total_count - processed_count

    if stats is not None:
        stats["total"] = total_count
        stats["processed"] = processed_count
        stats["pending"] = pending_count

    return pending_count == 0


def name_check(row):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    if row["First Name"] != "" and row["Last Name"] != "":
        return str(row["First Name"]).strip() + " " + str(row["Last Name"]).strip()
    else:
        return "Unknown"


def get_record(case_id):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    sql = """
     SELECT 
     gsd.case_id, 
     gsd.office_id, 
     REPLACE(TRIM(gsd.line_1), '#', '') AS line_1,
     REPLACE(CONVERT(TRIM(gsd.primary_network) USING ASCII),'&','and') AS primary_network, 
     gsd.employee_count, 
     gsd.office_phone, 
     gsd.manager_id, 
     CONVERT(TRIM(gsd.manager_name) USING ASCII) AS manager_name, 
     CONVERT(TRIM(gsd.meetings) USING ASCII) AS meetings
     FROM 
       bi_warehouse_prd.gather_sales_data_f gsd 
    WHERE 
       gsd.case_id = {case_id};"""

    log.info("Getting record for Case ID: {case_id}".format(case_id=case_id))
    get_cur = bi.cursor(mysql.cursors.DictCursor)
    get_cur.execute(sql.format(case_id=case_id))
    rows = get_cur.fetchall()
    log.info(rows)
    recs_returned = len(rows)
    log.info("Records found: {rr}".format(rr=recs_returned))

    if recs_returned == 1:
        return rows[0]
    if recs_returned == 2:
        return rows[0]
    else:
        err = "Multiple records found for case_id {case_id}!".format(case_id=case_id)
        log.error(err)
        raise RuntimeError(err)


def get_city_from_zip(row):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    if row["City"] == "":

        zsql = """
    SELECT
        zd.city_name
    FROM
        bi_warehouse_prd.zip_code_d zd
    WHERE
        1=1
        AND zd.preferred = 'P'
        AND zd.zip_code = '{zc}';"""

        zcur = bi.cursor(mysql.cursors.DictCursor)
        zcur.execute(zsql.format(zc=row["Postal Code"]))
        result = zcur.fetchone()
        zcur.close()

        if result is not None:
            return result["city_name"]
        else:
            return "Unknown"
    else:
        return row["City"]


def get_short_state(state_name):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    if state_name.lower() == "kansas":  ## Seems to be some bug that only affects SQL queries for Kansas???  It's in the mysql module.
        return 'KS'
    else:
        ssql = """SELECT sd.state_code FROM bi_warehouse_prd.state_d sd WHERE sd.state_name = '{state_name}';""".format(
            state_name=state_name.strip())

        scur = bi.cursor(mysql.cursors.DictCursor)
        scur.execute(ssql)
        result = scur.fetchone()
        scur.close()

        if result["state_code"] is not None:
            return result["state_code"]
        else:
            err = "No state code found when looking up short state abbreviation!"
            log.error(err)
            raise RuntimeError(err)


def get_state_from_zip(row):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    zsql = """
    SELECT
	    sd.state_name,
        sd.state_code
    FROM
	    bi_warehouse_prd.zip_code_d zd
	    JOIN bi_warehouse_prd.state_d sd ON (zd.state_code = sd.state_code)
    WHERE
	    1=1
	    AND zd.preferred = 'P'
	    AND zd.zip_code = '{zc}'
    ;
    """

    zcur = bi.cursor(mysql.cursors.DictCursor)
    zcur.execute(zsql.format(zc=row["Postal Code"]))

    result = zcur.fetchone()
    zcur.close()

    if result is not None:
        return (result["state_name"], result["state_code"])
    else:
        res = get_record(row["Case ID"])
        ls = res["state"]
        ss = get_short_state(ls)
        return (ls, ss)


def remove_middle_name_and_suffix(full_name):
    # Split the full name into parts
    parts = full_name.split()

    # Check if there are more than two parts (first name, middle name, last name)
    if len(parts) > 2:
        # Remove middle name (single uppercase letter with or without a period) and suffix (mixed case Roman numerals)
        cleaned_full_name = re.sub(r'\s+[A-Z]\.?\s*', ' ', full_name)
        cleaned_full_name = re.sub(r'\s+[IVXLCDMivxlcdm]+\s*$', '', cleaned_full_name)
        return cleaned_full_name.strip()

    # If there are only two parts, return the original full name
    return full_name


def split_name(contact_name):
    parts = contact_name.strip().split()
    # Condense the first name if it consists of two single characters (initials)
    if len(parts) >= 2 and all(len(part) == 1 for part in parts[:-1]):
        first_name = ''.join(parts[:-1])
    else:
        first_name = parts[0]
    # Adjust the first name to only use the first two letters for matching
    first_name = first_name[:2]
    last_name = parts[-1]
    return first_name, last_name

def lookup_contact(contact_name):
    log.debug(f"Entering {sys._getframe().f_code.co_name}()")

    first_name, last_name = split_name(contact_name)

    sql = """
             SELECT 
                cd.contact_id,   
                od.office_id, 
                TRIM(cd.full_name) as full_name,   
                CAST(od.zip_code AS CHAR) as zip_code,
                REPLACE(TRIM(od.primary_network), '®', '') as primary_network,
                od.state
            FROM   
                bi_warehouse_prd.contact_d cd
            JOIN 
                bi_warehouse_prd.office_d od ON(cd.office_id=od.office_id)
            WHERE 
            1=1 
            AND cd.contact_record_status = %s
            -- Explicit utf8mb4 conversion avoids collation mismatch with LIKE.
            AND CONVERT(cd.first_name USING utf8mb4) COLLATE utf8mb4_general_ci
                LIKE CONVERT(%s USING utf8mb4) COLLATE utf8mb4_general_ci
            AND CONVERT(cd.last_name USING utf8mb4) COLLATE utf8mb4_general_ci
                = CONVERT(%s USING utf8mb4) COLLATE utf8mb4_general_ci;
          """

    params = ("Active", f"{first_name}%", f"{last_name}")

    # Assuming database connection details are defined elsewhere
    db = mysql.connect(
        SECRETS.get("bi_hostname"),
        SECRETS.get("bi_username"),
        SECRETS.get("bi_password"),
        SECRETS.get("bi_database"),
        charset="utf8mb4",
        use_unicode=True
    )
        
    try:
        # Create a new DataFrame from the SQL query using read_sql
        manager_df = pd.read_sql(sql, db, params=params)
    finally:
        db.close()

    manager_dict = manager_df.to_dict(orient="records")
    log.info(manager_dict)

    return manager_dict


def lookup_nickname(contact_name, office_id):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    lookup_sql = """SELECT
            cd.nickname
        FROM
            bi_warehouse_prd.contact_d cd
        WHERE
            CONVERT(cd.full_name USING utf8mb4) COLLATE utf8mb4_general_ci = %s
            AND cd.office_id = %s
            AND cd.contact_record_status = "Active";"""

    lookup_params = (contact_name, office_id)
    lookup_cur = bi.cursor()

    nickname = None  # prevenir errores si el SELECT falla

    try:
        lookup_cur.execute(lookup_sql, lookup_params)
        nickname = lookup_cur.fetchone()
    except mysql.Error as e:
        log.error("Lookup nickname failed with MySQL error: {e}".format(e=e))
        # Si la versión del driver soporta _last_executed:
        last_sql = getattr(lookup_cur, "_last_executed", None)
        if last_sql:
            log.error("Attempted query: {q}".format(q=last_sql))
    finally:
        lookup_cur.close()

    if nickname is not None:
        return nickname[0]
    else:
        return 0


def lookup_lastname(contact_name, office_id):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    lookup_sql = """SELECT
            cd.last_name
        FROM
            bi_warehouse_prd.contact_d cd
        WHERE
            CONVERT(cd.full_name USING utf8mb4) COLLATE utf8mb4_general_ci = %s
            AND cd.office_id = %s
            AND cd.contact_record_status = "Active";"""

    lookup_params = (contact_name, office_id)
    lookup_cur = bi.cursor()

    last_name = None  # prevenir errores si el SELECT falla

    try:
        lookup_cur.execute(lookup_sql, lookup_params)
        last_name = lookup_cur.fetchone()
    except mysql.Error as e:
        log.error("Lookup lastname failed with MySQL error: {e}".format(e=e))
        last_sql = getattr(lookup_cur, "_last_executed", None)
        if last_sql:
            log.error("Attempted query: {q}".format(q=last_sql))
    finally:
        lookup_cur.close()

    if last_name is not None:
        return last_name[0]
    else:
        return 0


def check_for_existing_relationship(contact_id, office_id):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    check_sql = """
           SELECT
               r.relationship_id
           FROM
               xrms.relationships r
           WHERE
               r.from_id = {contact_id}
               AND r.to_id = {office_id}
               AND r.type_id = 19
               AND r.ended_on = 0;"""

    check_cur = db.cursor()
    check_cur.execute(check_sql.format(contact_id=contact_id, office_id=office_id))
    relationship_id = check_cur.fetchone()
    check_cur.close()

    if not relationship_id == None:
        return relationship_id[0]
    else:
        return 0


def get_radius(zipcode_a, zipcode_b):
    dist = GeoDistance('us')
    cal_distance = dist.query_postal_code(zipcode_a, zipcode_b)
    radius = (cal_distance, 'mi')
    radius = radius[0]
    return radius

def lookup_contact_name_with_contact_id(contact_id):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    sql = f"""
                SELECT 
                    cd.contact_id as 'Contact ID',   
                    cd.full_name as 'Full Name'
                FROM   
                    bi_warehouse_prd.contact_d cd
                JOIN 
                    bi_warehouse_prd.office_d od ON(cd.office_id=od.office_id)WHERE 1=1 AND cd.contact_id = "{contact_id}"
                AND 
                    cd.contact_record_status = "Active";"""

    db = mysql.connect(SECRETS.get("bi_hostname"), SECRETS.get("bi_username"), SECRETS.get("bi_password"), SECRETS.get("bi_database"))
    db1 = mysql.connect(SECRETS.get("bi_hostname"), SECRETS.get("bi_username"), SECRETS.get("bi_password"), SECRETS.get("bi_database"), charset='utf8')
    
    try:
        manager_df = pd.read_sql(sql, db)
    except:
        manager_df = pd.read_sql(sql, db1)

    db.close()

    return manager_df

def relationship_with_office_same_person_different_id(office_id, manager_name):
    log.debug("Entering relationship_with_office_same_person_different_id()")
    log.info(f"Searching for relationship for office ID {office_id}...")

    contact_id_sql = f"""
    SELECT 
        r.to_id
    FROM 
        xrms.relationships r
    WHERE 
        r.ended_on = 0 
        AND r.type_id = 5 
        AND r.from_id = {office_id}"""

    select_cur = db.cursor()

    try:
        log.info("Checking relationship...")
        select_cur.execute(contact_id_sql)
        # Fetch the relationship IDs from the result set
        results = select_cur.fetchall()
        if results:
            # Assuming you want the first result's first element if multiple; adjust as needed
            contact_id = results[0][0]
            manager_df = lookup_contact_name_with_contact_id(contact_id)
            # Check if any row in 'Full Name' column matches 'manager_name'
            if manager_df['Full Name'].eq(manager_name).any():
                log.info("Relationship found - same name, different contact ID. Not creating contact...")
                return "True"
            else:
                log.info("No relation found - names are different. Creating contact...")
                return "False"
        else:
            log.info('No relationship set. Creating contact...')
            return "False"

    except Exception as e:
        log.error(e)

def suggested_relationship_with_office_same_person_different_id(office_id, manager_name):
    log.debug("Entering suggested_relationship_with_office_same_person_different_id()")
    log.info(f"Searching for relationship for office ID {office_id}...")

    contact_id_sql = f"""
    SELECT 
        r.to_id
    FROM 
        xrms.relationships r
    WHERE 
        r.ended_on = 0 
        AND r.type_id = 94
        AND r.from_id = {office_id}"""

    select_cur = db.cursor()

    try:
        log.info("Checking relationship...")
        select_cur.execute(contact_id_sql)
        # Fetch the relationship IDs from the result set
        results = select_cur.fetchall()
        if results:
            # Assuming you want the first result's first element if multiple; adjust as needed
            contact_id = results[0][0]
            manager_df = lookup_contact_name_with_contact_id(contact_id)
            # Check if any row in 'Full Name' column matches 'manager_name'
            if manager_df['Full Name'].eq(manager_name).any():
                log.info("Relationship found - same name, different contact ID. Not creating contact...")
                return "True"
            else:
                log.info("No relation found - names are different. Creating contact...")
                return "False"
        else:
            log.info('No relationship set. Creating contact...')
            return "False"

    except Exception as e:
        log.error(e)


def relationship_with_office_different_manager(office_id):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    log.info("Searching for relationships for office ID {oid}...".format(oid=office_id))

    rel_sql = f"""
    SELECT 
        r.relationship_id
    FROM 
        xrms.relationships r
    WHERE 
        r.ended_on = 0 AND r.type_id = 5 AND r.from_id = {office_id}

    UNION

    SELECT 
        r.relationship_id
    FROM 
        xrms.relationships r
    WHERE 
        r.ended_on = 0 AND r.type_id = 19 AND r.to_id = {office_id};""".format(office_id=office_id)

    update_cur = db.cursor()

    try:
        log.info("Checking relationship...")
        update_cur.execute(rel_sql)
        # Fetch the relationship IDs from the result set
        relationship_ids = [row[0] for row in update_cur.fetchall()]
        # Update the relationships with the specified IDs
        if relationship_ids:
            string = "Relationship found - putting new name in suggested decision maker box"
            log.info(string)
            result = "True"
            return result
        else:
            string = "No relationships found for office ID {oid}.".format(oid=office_id)
            log.info(string)
            result = "False"
            return result
    except mysql.Error as e:
        string = "Select failed with MySQL error: {e}".format(e=e)
        log.error(string)
        db.rollback()
        return string

def contactinfo_insert(contact_id, contact_info, ctype):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    cci_insert_sql = """INSERT INTO xrms.contacts_contactinfo(userid, contacttype, contactinfo, display_version, primaryemail, secondaryemail, contactinfo_opt_in_id, 
    primarycontact, custlabel, lastupdate, row_status, import_id, discriminator, marketo_primary_contact) 
    VALUES (%s, %s, %s, '', %s, 'no', -1, %s, %s, current_timestamp(), 'a', 0, %s, 0);"""

    if ctype.lower() == "c":
        contacttype = "Phone"
        primaryemail = "no"
        primarycontact = "yes"
        custlabel = "Phone"
        discriminator = "Phone"
    else:
        log.critical("Invalid contact info type!")
        raise RuntimeError("Invalid contact info type!")

    cci_insert_params = (contact_id, contacttype, contact_info, primaryemail, primarycontact, custlabel, discriminator)

    cci_insert_cur = db.cursor()

    try:
        cci_insert_cur.execute(cci_insert_sql, cci_insert_params)
        db.commit()
        # Get inserted ID
        cci_insert_id = cci_insert_cur.lastrowid
        log.info("Insert query: {isq}".format(isq=cci_insert_sql % cci_insert_params))
        log.info("Insert successful.  Inserted ID: {nc}".format(nc=cci_insert_id))
    except mysql.Error as e:
        log.error("Insert failed with MySQL error: {e}".format(e=e))

    cci_insert_cur.close()

def load_contacts_crm_api(df):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    def _bool_to_str(value):
        return str(value) if isinstance(value, bool) else value

    insert_sql = """INSERT INTO xrms.contacts(company_id, last_name, first_names, gender, title, email, cell_phone, facebook, twitter, linkedin, entered_at, entered_by, direct_phone, source_photo_url) VALUES(%s, %s, %s, '', '', '', '', '', '', '', current_timestamp(), 3510, %s, '');"""

    for idx, row in df.iterrows():
        log.info("Processing insert record: {iu}".format(iu=idx + 1))
        last_name = unidecode.unidecode(_bool_to_str(row["last_name"]).encode("utf-8").decode("ascii", "ignore").strip().title()) if \
        row["last_name"] != 0 else ""
        first_name = unidecode.unidecode(_bool_to_str(row["first_name"]).encode("utf-8").decode("ascii", "ignore").strip().title()) if \
        row["first_name"] != 0 else ""

        direct_phone = str(row["office_phone"]) if row["office_phone"] != "0" else ""
        company_id = row["office_id"]

        insert_params = (company_id, last_name, first_name, direct_phone)

        log.info("Checking for existing contact based on contact info")
        log.info("No existing contact found or existing check is turned off. Inserting...")
        log.info("insert_params: {ip}".format(ip=insert_params))
        log.info("Insert query: {isq}".format(isq=insert_sql % insert_params))

        insert_cur = db.cursor()
        try:
            insert_cur.execute(insert_sql, insert_params)
            db.commit()
            # Get inserted contact ID
            contact_id = insert_cur.lastrowid
            log.info("Insert successful.  New contact ID: {nc}".format(nc=contact_id))
            log.info("Inserting contact info for new contact...")

            if direct_phone != "":
                log.info("Inserting direct phone...")
                contactinfo_insert(contact_id, direct_phone, ctype="c")
                log.info("Done.")

        except mysql.Error as e:
            log.error("Insert failed with MySQL error: {e}".format(e=e))

        insert_cur.close()

    return contact_id

def suggested_decision_box(contact_id, office_id):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    log.info(
        "Placing contact in suggested decision maker for contact ID {cid} to office ID {oid}...".format(cid=contact_id,
                                                                                                        oid=office_id))

    rel_sql = """INSERT IGNORE INTO xrms.relationships (from_id, to_id, type_id, is_primary) VALUES(%s, %s, %s, 1);"""

    params1 = (contact_id, office_id, 93)
    params2 = (office_id, contact_id, 94)

    rel_cur = db.cursor()

    try:
        log.info("Creating relationship 1...")
        rel_cur.execute(rel_sql, params1)
        db.commit()
        # Get inserted ID
        r1_insert_id = rel_cur.lastrowid
        log.info("Relationship 1 insert successful.  Inserted ID: {nc}".format(nc=r1_insert_id))
    except mysql.Error as e:
        log.error("Insert failed with MySQL error: {e}".format(e=e))

    try:
        log.info("Creating relationship 2...")
        rel_cur.execute(rel_sql, params2)
        db.commit()
        # Get inserted ID
        r2_insert_id = rel_cur.lastrowid
        log.info("Relationship 2 insert successful.  Inserted ID: {nc}".format(nc=r2_insert_id))
    except mysql.Error as e:
        log.error("Insert failed with MySQL error: {e}".format(e=e))

    rel_cur.close()

    return r2_insert_id


def create_relationships(contact_id, office_id):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    log.info("Creating relationships for contact ID {cid} to office ID {oid}...".format(cid=contact_id, oid=office_id))

    rel_sql = """INSERT IGNORE INTO xrms.relationships (from_id, to_id, type_id, is_primary) VALUES(%s, %s, %s, 1);"""

    params1 = (contact_id, office_id, 19)
    params2 = (office_id, contact_id, 5)

    rel_cur = db.cursor()

    try:
        log.info("Creating relationship 1...")
        rel_cur.execute(rel_sql, params1)
        db.commit()
        # Get inserted ID
        r1_insert_id = rel_cur.lastrowid
        log.info("Relationship 1 insert successful.  Inserted ID: {nc}".format(nc=r1_insert_id))
    except mysql.Error as e:
        log.error("Insert failed with MySQL error: {e}".format(e=e))

    try:
        log.info("Creating relationship 2...")
        rel_cur.execute(rel_sql, params2)
        db.commit()
        # Get inserted ID
        r2_insert_id = rel_cur.lastrowid
        log.info("Relationship 2 insert successful.  Inserted ID: {nc}".format(nc=r2_insert_id))
    except mysql.Error as e:
        log.error("Insert failed with MySQL error: {e}".format(e=e))

    rel_cur.close()

    return r2_insert_id


def update_status(workflow_id, status_id):
    try:
        log.debug("Entering {}()".format(sys._getframe().f_code.co_name))
        log.info("Updating status of workflow_id ({wid}) to {sid}...".format(wid=workflow_id, sid=status_id))

        api_url = SECRETS.get("set_office_status_url")
        api_key = SECRETS.get("set_office_status_api_key")

        params = {
            "api_key": api_key,
            "workflows": [{
                "workflow_id": workflow_id,
                "workflow_type": "case",
                "status_id": status_id
            }
            ]
        }

        r = requests.post(api_url, json=params, verify=False)
        log.info(r)
    except Exception as e:
        log.error(e)
        send_teams_message(summary=SEND_TEAMS_MESSAGE_SUMMARY, activityTitle=f"Couldn't change status for workflow ID: {workflow_id}",
                           activitySubtitle=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), text=e)


def load_excel_file(filename):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    try:
        excel_df = pd.read_excel(filename, sheet_name="Data Updated", dtype={"PhoneNumber": str, "Postal Code": str})
        excel_df.columns = excel_df.columns.str.strip()
        excel_df = excel_df.rename(columns={"PhoneNumber": "Phone Number"})

        excel_df["Phone Number"] = excel_df["Phone Number"].fillna("Unknown")
        excel_df["Address"] = excel_df["Address"].str.strip().fillna("")
        excel_df["Address"] = excel_df["Address"].replace('&amp;', '&', regex=True)
        excel_df["Postal Code"] = excel_df["Postal Code"].fillna(0)
        excel_df["City"] = excel_df["City"].str.strip().fillna("")
        excel_df["City"] = excel_df.apply(get_city_from_zip, axis=1)
        excel_df["City"] = excel_df["City"].str.title()
        excel_df["State"] = excel_df["State"].str.title()
        excel_df["Primary Network"] = excel_df["Primary Network"].str.strip().fillna("Unknown").replace('&amp;', '&', regex=True)
        excel_df["Employee Number"] = excel_df["Employee Number"].fillna(0).astype("int")
        excel_df["First Name"] = excel_df["First Name"].fillna("")
        excel_df["Last Name"] = excel_df["Last Name"].fillna("")
        excel_df["Manager Name"] = excel_df.apply(name_check, axis=1)
        excel_df["Manager Name"] = excel_df["Manager Name"].str.title()
        excel_df["Manager ID"] = excel_df["Manager ID"].fillna(0).astype("int")
        excel_df["Meeting Schedule"] = excel_df["Meeting Schedule"].fillna("Unknown").str.strip()
        excel_df["Meeting Schedule"] = excel_df["Meeting Schedule"].str.title().replace('&amp;', '&', regex=True)

        return excel_df

    except Exception as e:
        error = "Error in load_excel_file: {}".format(e)
        log.error(error)
        send_teams_message(summary=SEND_TEAMS_MESSAGE_SUMMARY, activityTitle=SEND_TEAMS_MESSAGE_ACTIVITY_TITLE,
                           activitySubtitle=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), text=error)

def load_other_statuses(filename):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))
    return pd.read_excel(filename, sheet_name="Other Statuses", engine='openpyxl')

def sort_key(manager):
    return manager['radius']

def compare_and_update(df_dict):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))
    ## Compare DB record to Dataframe record and update changes
    db_dict = get_record(df_dict["case_id"])
    db_dict['manager_name'] = db_dict['manager_name'].lower()
    df_dict['manager_name'] = remove_middle_name_and_suffix(df_dict['manager_name'])
    df_dict['manager_name'] = df_dict['manager_name'].lower()
    df_dict['meetings'] = df_dict['meetings'].lower()
    db_dict['meetings'] = db_dict['meetings'].lower()

    d1_keys = set(df_dict.keys())
    d2_keys = set(db_dict.keys())
    intersect_keys = d1_keys.intersection(d2_keys)
    modified = {}
    for i in intersect_keys:
        if db_dict[i] != df_dict[i]: modified.update({i: (db_dict[i], df_dict[i])})

    log.info("DB Dict: {db}".format(db=db_dict))  # coming from SQL
    log.info("DF Dict: {df}".format(df=df_dict))  # coming from file

    log.info("DF dict is {df}".format(df=df_dict['office_id']))
    log.info("DB dict is {db}".format(db=db_dict['office_id']))

    log.info("-" * 50)
    log.info("Office ID: {oid} | Case ID: {cid}".format(oid=df_dict["office_id"], cid=df_dict["case_id"]))

    if len(modified) > 0:
        comp_update_list = []
        updated_addr = False
        for field in modified:
            log.info("Difference found: {field} {mf}".format(field=field, mf=modified[field]))
            if field in ["employee_count", "office_phone"]:
                if field == "employee_count":
                    comp_update_list.append("employee_count")
                else:
                    comp_update_list.append("office_phone")
                if len(comp_update_list) > 0:
                    if len(comp_update_list) == 2:
                        update_companies_table(office_id=df_dict["office_id"], employee_count=df_dict["employee_count"],phone_number=df_dict["office_phone"])
                    elif len(comp_update_list) == 1:
                        if comp_update_list[0] == "employee_count":
                            update_companies_table(office_id=df_dict["office_id"], employee_count=df_dict["employee_count"],phone_number=df_dict["office_phone"])
                        else:
                            update_companies_table(office_id=df_dict["office_id"], phone_number=df_dict["office_phone"])
                    else:
                        log.error("Incorrect number of fields during company update call!")
                else:
                    continue

            elif (field in ["line_1"]):
                ## Do note update with old and new addresses
                if not updated_addr:
                    log.info("Address change found:")
                    log.info("Old address: {line_1}".format(line_1=db_dict["line_1"]))
                    log.info("New address: {line_1}".format(line_1=df_dict["line_1"]))
                    log.info(df_dict['case_id'])
                    note_msg = "Address change found during Gather Sales Data processing:<br><br>Old address: {oline_1}<br>New address: {nline_1}".format(oline_1=db_dict["line_1"], nline_1=df_dict["line_1"])
                    log.info(note_msg)
                    log.info(updated_addr)
                    update_notes_table(df_dict["case_id"], note_msg.format(oline_1=db_dict["line_1"], nline_1=df_dict["line_1"]))
                    updated_addr = True

            elif field == "primary_network":
                continue

            elif field == "manager_name":
                df_dict["manager_name"] = df_dict["manager_name"].title()
                log.info("Looking up contact by name: {cc}".format(cc=df_dict["manager_name"]))
                nickname = lookup_nickname(df_dict["manager_name"], df_dict["office_id"])
                lastname_query = lookup_lastname(df_dict["manager_name"], df_dict["office_id"])
                firstname = db_dict["manager_name"].split()[0]
                try:
                    lastname = db_dict["manager_name"].split()[1]
                except:
                    lastname = 'unknown'
                manager_dict = lookup_contact(df_dict["manager_name"])
                count = sum([1 for d in manager_dict if 'contact_id' in d])  # counts how many 'contact_id" in list
                log.info(count)
                if count == 0:
                    log.info('Checking relationship first with only office id to see if manager is DM under different contact_id')
                    result = relationship_with_office_same_person_different_id(df_dict["office_id"], df_dict["manager_name"])
                    if result == "True":
                        log.info('Contact is already set as DM putting results to 271.')
                    else:
                        result = suggested_relationship_with_office_same_person_different_id
                        if result == "True":
                            log.info('Contact is already set as Suggested DM putting results to 271.')
                        else:
                            df = pd.DataFrame([df_dict])
                            new_contact_id = load_contacts_crm_api(df)
                            if new_contact_id > 0:
                                log.info("Created new contact, checking for existing relationship with only office id with different manager...")
                                result = relationship_with_office_different_manager(df_dict["office_id"])
                                if result == "True":
                                    log.info("Manager relationship exists, put new manager in suggested decision maker box")
                                    suggested_decision_box(new_contact_id, office_id=df_dict["office_id"])
                                elif result == "False":
                                    log.info("No relationship found - Creating relationships....")
                                    create_relationships(new_contact_id, office_id=df_dict["office_id"])
                elif count == 1:
                    radius = get_radius(manager_dict[0]["zip_code"], df_dict["zip_code"])
                    log.info(f'Radius: {radius}')
                    log.info(manager_dict)
                    log.info(df_dict)
                    if (radius==0.0) or (0.1 <= radius <= 60.0 and df_dict["manager_name"] == manager_dict[0]["full_name"]) or (df_dict["state"] == manager_dict[0]["state"] and radius <= 60 and nickname == firstname and lastname_query == lastname) or (df_dict["state"] == manager_dict[0]["state"] and radius <= 60 and df_dict["manager_name"] ==manager_dict[0]["full_name"]):
                        contact_id = list(map(itemgetter('contact_id'), manager_dict))
                        contact_id = [int(x) for x in contact_id]
                        log.info(contact_id)
                        if any(y > 0 for y in contact_id):
                            contact_id = str(contact_id)[1:-1]
                            ## Contact ID found, check for existing relationship
                            log.info("Contact ID found!  CID: {cid}".format(cid=contact_id))
                            log.info("Checking for existing relationships...")
                            rel = check_for_existing_relationship(contact_id, df_dict["office_id"])
                            if rel > 0:
                                ## Existing relationship found
                                log.warning("*** Relationship found! ***")
                                log.info("Existing relationship (ID: {rel}) found.  Skipping relationship creation.".format(rel=rel))
                            else:
                                log.info('Relationship not found. Checking relationship first with only office id to see if manager is DM under different contact_id')
                                result = relationship_with_office_same_person_different_id(df_dict["office_id"],df_dict["manager_name"])
                                if result == "True":
                                    log.info('Contact is already set as DM putting results to 271.')
                                else:
                                    result = suggested_relationship_with_office_same_person_different_id
                                    if result == "True":
                                        log.info('Contact is already set as Suggested DM putting results to 271.')
                                    else:
                                        log.info("No relationship found, checking for existing relationship with only office id with different manager...")
                                        result = relationship_with_office_different_manager(df_dict["office_id"])
                                        if result == "True":
                                            log.info("Manager relationship exists, put new manager in suggested decision maker box")
                                            suggested_decision_box(contact_id=contact_id, office_id=df_dict["office_id"])
                                        elif result == "False":
                                            log.info("No relationship found - Creating relationships....")
                                            create_relationships(contact_id=contact_id, office_id=df_dict["office_id"])
                        else:
                                log.info('Checking relationship first with only office id to see if manager is DM under different contact_id')
                                result = relationship_with_office_same_person_different_id(df_dict["office_id"],df_dict["manager_name"])
                                if result == "True":
                                    log.info('Contact is already set as DM putting results to 271.')
                                else:
                                    result = suggested_relationship_with_office_same_person_different_id
                                    if result == "True":
                                        log.info('Contact is already set as Suggested DM putting results to 271.')
                                    else:
                                        df = pd.DataFrame([df_dict])
                                        new_contact_id = load_contacts_crm_api(df)
                                        if new_contact_id > 0:
                                            log.info("Created new contact, checking for existing relationship with only office id with different manager...")
                                            result = relationship_with_office_different_manager(df_dict["office_id"])
                                            if result == "True":
                                                log.info("Manager relationship exists, put new manager in suggested decision maker box")
                                                suggested_decision_box(new_contact_id, office_id=df_dict["office_id"])
                                            elif result == "False":
                                                log.info("No relationship found - Creating relationships....")
                                                create_relationships(new_contact_id, office_id=df_dict["office_id"])
                    else:
                        log.info('Checking relationship first  with only office id to see if manager is DM under different contact_id')
                        result = relationship_with_office_same_person_different_id(df_dict["office_id"],df_dict["manager_name"])
                        if result == "True":
                            log.info('Contact is already set as DM putting results to 271.')        
                        else:
                            result = suggested_relationship_with_office_same_person_different_id
                            if result == "True":
                                log.info('Contact is already set as Suggested DM putting results to 271.')
                            else:
                                df = pd.DataFrame([df_dict])
                                new_contact_id = load_contacts_crm_api(df)
                                if new_contact_id > 0:
                                    log.info("Created new contact, checking for existing relationship with only office id with different manager...")
                                    result = relationship_with_office_different_manager(df_dict["office_id"])
                                    if result == "True":
                                        log.info("Manager relationship exists, put new manager in suggested decision maker box")
                                        suggested_decision_box(new_contact_id, office_id=df_dict["office_id"])
                                    elif result == "False":
                                        log.info("No relationship found - Creating relationships....")
                                        create_relationships(new_contact_id, office_id=df_dict["office_id"])
                else:
                    manager_list = []
                    for manager in manager_dict:
                        radius = get_radius(manager["zip_code"], df_dict["zip_code"])
                        manager['radius'] = radius
                        count = 0
                        log.info(manager)
                        if 0 < radius <= 60:
                            count = count + 1
                            if manager["state"] == df_dict["state"]:
                                log.info("State name in DB: {state}. State name in file: {state_file}".format(state=manager["state"], state_file=df_dict["state"]))
                                count = count + 1
                                manager["count"] = count
                                if manager["primary_network"] == df_dict["primary_network"]:
                                    count = count + 2
                                    log.info("Primary Network name in DB: {prm_ntw}. Primary Network name in file: {prm_ntw_file}".format(prm_ntw=manager["primary_network"],prm_ntw_file=df_dict["primary_network"]))
                                    log.info("Count is: {ct}".format(ct=count))
                                    manager["count"] = count
                                    manager_list.append(manager)
                                else:
                                        count = count + 0
                                        log.info("Primary Network not found during Gather Sales Data processing for Manager")
                                        pass
                            else:
                                    count = count + 0
                                    log.info("State not equal: {state}. State name in file: {state_file}".format(
                                        state=manager["state"], state_file=df_dict["state"]))

                        elif radius == -1:
                            log.info('Radius was not found, checking primary_network and state')
                            if manager["primary_network"] == df_dict["primary_network"]:
                                count = count + 2
                                log.info(
                                "Primary Network name in DB: {prm_ntw}. Primary Network name in file: {prm_ntw_file}".format(
                                    prm_ntw=manager["primary_network"], prm_ntw_file=df_dict["primary_network"]))
                                log.info("Count is: {ct}".format(ct=count))
                                if manager["state"] == df_dict["state"] or radius == 0.0:
                                    log.info("State name in DB: {state}. State name in file: {state_file}".format(
                                    state=manager["state"], state_file=df_dict["state"]))
                                    count = count + 1
                                    manager["count"] = count
                                    manager_list.append(manager)
                                else:
                                    count = count + 0
                                    log.info("State not equal: {state}. State name in file: {state_file}".format(
                                        state=manager["state"], state_file=df_dict["state"]))
                                    pass
                        elif radius == 0.0:
                            log.info('Radius Matched fully')
                            count = count + 3
                            manager["count"] = count
                            manager_list.append(manager)
                        else:
                            log.info('Nothing Matched')
                    try:
                        threshold = 60
                        log.info(manager_list)
                        if (manager_list[0]['count'] >= 3 and abs(manager_list[0]["radius"] - 0) <= threshold and manager_list[0]["contact_id"]):
                            sorted(manager_list, key=lambda manager: manager['radius'], reverse=False)
                            log.info("Score for row: {score}".format(score=manager_list[0]['count']))
                            log.info("Radius for the office is: {rd}".format(rd=manager_list[0]["radius"]))
                            contact_id = manager_list[0]["contact_id"]
                            log.info("Contact ID found!  CID: {cid}".format(cid=contact_id))
                            log.info("Checking for existing relationships...")
                            rel = check_for_existing_relationship(contact_id, df_dict["office_id"])
                            if rel > 0:
                                ## Existing relationship found
                                log.warning("*** Relationship found! ***")
                                log.info("Existing relationship (ID: {rel}) found.  Skipping relationship creation.".format(rel=rel))
                            else:
                                log.info('Checking relationship first with only office id to see if manager is DM under different contact_id')
                                result = relationship_with_office_same_person_different_id(df_dict["office_id"],df_dict["manager_name"])
                                if result == "True":
                                    log.info('Contact is already set as DM putting results to 271.')
                                else:
                                    result = suggested_relationship_with_office_same_person_different_id
                                    if result == "True":
                                        log.info('Contact is already set as Suggested DM putting results to 271.')
                                    else:
                                        log.info("No relationship found, checking for existing relationship with only office id with different manager...")
                                        result = relationship_with_office_different_manager(df_dict["office_id"])
                                        if result == "True":
                                            log.info("Manager relationship exists, put new manager in suggested decision maker box")
                                            suggested_decision_box(contact_id=contact_id, office_id=df_dict["office_id"])
                                        elif result == "False":
                                            log.info("No relationship found - Creating relationships....")
                                            create_relationships(contact_id=contact_id, office_id=df_dict["office_id"])
                        else:
                            log.info('Checking relationship first with only office id to see if manager is DM under different contact_id')
                            result = relationship_with_office_same_person_different_id(df_dict["office_id"],df_dict["manager_name"])
                            if result == "True":
                                log.info('Contact is already set as DM putting results to 271.')
                            else:
                                result = suggested_relationship_with_office_same_person_different_id
                                if result == "True":
                                    log.info('Contact is already set as Suggested DM putting results to 271.')
                                else:
                                    df = pd.DataFrame([df_dict])
                                    new_contact_id = load_contacts_crm_api(df)
                                    if new_contact_id > 0:
                                        log.info("Created new contact, checking for existing relationship with only office id with different manager...")
                                        result = relationship_with_office_different_manager(df_dict["office_id"])
                                        if result == "True":
                                            log.info("Manager relationship exists, put new manager in suggested decision maker box")
                                            suggested_decision_box(new_contact_id, office_id=df_dict["office_id"])
                                        elif result == "False":
                                            log.info("No relationship found - Creating relationships....")
                                            create_relationships(new_contact_id, office_id=df_dict["office_id"])
                    except IndexError:
                        log.info('Checking relationship first with only office id to see if manager is DM under different contact_id')
                        result = relationship_with_office_same_person_different_id(df_dict["office_id"],df_dict["manager_name"])
                        if result == "True":
                            log.info('Contact is already set as DM putting results to 271.')
                        else:
                            result = suggested_relationship_with_office_same_person_different_id
                            if result == "True":
                                log.info('Contact is already set as Suggested DM putting results to 271.')
                            else:
                                df = pd.DataFrame([df_dict])
                                new_contact_id = load_contacts_crm_api(df)
                                if new_contact_id > 0:
                                    log.info("Created new contact, checking for existing relationship with only office id with different manager...")
                                    result = relationship_with_office_different_manager(df_dict["office_id"])
                                    if result == "True":
                                        log.info("Manager relationship exists, put new manager in suggested decision maker box")
                                        suggested_decision_box(new_contact_id, office_id=df_dict["office_id"])
                                    elif result == "False":
                                        log.info("No relationship found - Creating relationships....")
                                        create_relationships(new_contact_id, office_id=df_dict["office_id"])
                    except Exception as e:
                        log.error(e)
                        send_teams_message(summary=SEND_TEAMS_MESSAGE_SUMMARY, activityTitle=SEND_TEAMS_MESSAGE_ACTIVITY_TITLE,
                                           activitySubtitle=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), text=e)
            else:
                continue

        if updated_addr == True:
            log.info("Address has been updated - moving to status: 1912")
            update_status(workflow_id=df_dict["case_id"], status_id=1912)
        else:
            update_status(workflow_id=df_dict["case_id"], status_id=271)
            log.info('Moved to status: 271')
    else:
        update_status(workflow_id=df_dict["case_id"], status_id=271)
        log.info('No changes found! Status changed to 271')

def get_cleaned_data_from_email(dry_run=False, audit_only=False):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    backup_folder = SCRIPT_PATH + "backup"
    error_folder = SCRIPT_PATH + "error"
    dryrun_backup_folder = SCRIPT_PATH + "dryrun_backup"
    staging_folder = DOWNLOAD_STAGING_FOLDER
    downloaded_files = []

    ensure_folder_exists(backup_folder, "backup")
    ensure_folder_exists(error_folder, "error")
    ensure_folder_exists(staging_folder, "download staging")
    if dry_run or audit_only:
        ensure_folder_exists(dryrun_backup_folder, "dryrun_backup")

    try:
        credentials = OAuth2Credentials(client_id=SECRETS.get("client_id"), client_secret=SECRETS.get("client_secret"), tenant_id=SECRETS.get("tenant_id"),
                                        identity=Identity(primary_smtp_address=SECRETS.get("primary_smtp_address")))
        config = Configuration(credentials=credentials, server=SECRETS.get("outlook_server"), version=Version(build=EXCHANGE_O365))
        account = Account(primary_smtp_address=SECRETS.get("primary_smtp_address"), config=config, autodiscover=False, access_type=IMPERSONATION)
    except Exception as e:
        log.error("AUTH_FAILURE: {err}".format(err=e))
        send_teams_message(summary=SEND_TEAMS_MESSAGE_SUMMARY, activityTitle="AUTH_FAILURE",
                           activitySubtitle=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), text=str(e))
        raise

    log.info("Phase A: getting inbox items matching filter for download only...")
    lookback_start = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=EMAIL_LOOKBACK_DAYS)
    log.info("Filtering emails received after {ls} (lookback days: {ld})".format(ls=lookback_start, ld=EMAIL_LOOKBACK_DAYS))
    try:
        items = account.inbox.filter(subject="ReminderMedia: Gather Sales Data - AskData",
                                     datetime_received__gte=lookback_start).order_by("datetime_received")
        items_count = items.count()
        log.info("Items found: {ic}".format(ic=items_count))
        if items_count > 0:
            emails_with_excel = 0
            excel_attachments = 0
            attachment_index = 0
            for item in items:
                received_ts = str(item.datetime_received)[:-6]
                log.info("Message received timestamp: {rt} | subject: {subject}".format(rt=received_ts, subject=item.subject))
                item_has_excel = False
                for attachment in item.attachments:
                    if isinstance(attachment, FileAttachment):
                        attachment_size = getattr(attachment, "size", None)
                        log.info(
                            "Attachment found: {an} (size={asize}) - Checking file attachment...".format(
                                an=attachment.name,
                                asize=attachment_size if attachment_size is not None else "unknown",
                            )
                        )
                        att_file_ext = os.path.splitext(attachment.name)[1].lstrip(".").lower()
                        if att_file_ext in ("xls", "xlsx"):
                            attachment_index += 1
                            item_has_excel = True
                            safe_original_name = os.path.basename(attachment.name)
                            received_prefix = item.datetime_received.strftime("%Y%m%dT%H%M%S")
                            local_filename = "{prefix}_{idx}_{name}".format(
                                prefix=received_prefix,
                                idx=str(attachment_index).zfill(3),
                                name=safe_original_name,
                            )
                            local_path = os.path.join(staging_folder, local_filename)
                            time.sleep(5)  ## Adding a 5 second delay for the file to finish being saved to see if this helps with file not found errors
                            with open(local_path, 'wb') as f:
                                f.write(attachment.content)
                            if not os.path.exists(local_path):
                                error = "Phase A download verification failed (missing file) for {fn} at {lp}".format(
                                    fn=attachment.name,
                                    lp=local_path,
                                )
                                log.error(error)
                                send_teams_message(summary=SEND_TEAMS_MESSAGE_SUMMARY, activityTitle=SEND_TEAMS_MESSAGE_ACTIVITY_TITLE,
                                                   activitySubtitle=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), text=error)
                                continue

                            file_size = os.path.getsize(local_path)
                            if file_size <= 0:
                                error = "Phase A download verification failed (0-byte file) for {fn} at {lp}".format(
                                    fn=attachment.name,
                                    lp=local_path,
                                )
                                log.error(error)
                                send_teams_message(summary=SEND_TEAMS_MESSAGE_SUMMARY, activityTitle=SEND_TEAMS_MESSAGE_ACTIVITY_TITLE,
                                                   activitySubtitle=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), text=error)
                                continue

                            log.info('Saved attachment to {lp} (size={sz} bytes)'.format(lp=local_path, sz=file_size))
                            reference_dt = parse_reference_date_from_filename(attachment.name) or item.datetime_received
                            downloaded_files.append((local_path, reference_dt, attachment.name, item.datetime_received))
                            excel_attachments += 1
                        else:
                            log.info("Skipping non-Excel attachment: {an}".format(an=attachment.name))
                if item_has_excel:
                    emails_with_excel += 1
            log.info(
                "Downloaded {count} excel attachment(s) from {emails} email(s).".format(
                    count=excel_attachments,
                    emails=emails_with_excel,
                )
            )
        else:
            log.info("No emails found to process.")
    except Exception as e:
        if is_auth_failure(e):
            log.error("AUTH_FAILURE: {err}".format(err=e))
            send_teams_message(summary=SEND_TEAMS_MESSAGE_SUMMARY, activityTitle="AUTH_FAILURE",
                               activitySubtitle=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), text=str(e))
        raise

    if not downloaded_files:
        log.info("Phase B: no downloaded files to process.")
        return

    for local_path, reference_dt, original_filename, email_received_dt in downloaded_files:
        resolved_path = local_path
        if not os.path.exists(local_path):
            log.error("Phase B could not find expected staging file at {lp}; attempting fallback lookup.".format(lp=local_path))
            fallback_path = resolve_file_path_with_fallback(local_path, staging_folder)
            if fallback_path is not None:
                resolved_path = fallback_path
                log.info("Phase B resolved file path via fallback: {rp}".format(rp=resolved_path))
            else:
                msg = "Phase B could not locate file {fn}. Expected path {lp}; file not found in staging/files/backup/error/dryrun_backup.".format(
                    fn=original_filename,
                    lp=local_path,
                )
                log.error(msg)
                send_teams_message(summary=SEND_TEAMS_MESSAGE_SUMMARY, activityTitle=SEND_TEAMS_MESSAGE_ACTIVITY_TITLE,
                                   activitySubtitle=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), text=msg)
                continue

        log.info(
            "Phase B processing file: {sn} reference_dt={rd} resolved_path={rp}".format(
                sn=original_filename,
                rd=reference_dt,
                rp=resolved_path,
            )
        )
        try:
            get_cleaned_data_from_file(
                resolved_path,
                reference_dt=reference_dt,
                dry_run=dry_run,
                audit_only=audit_only,
                dryrun_backup_folder=dryrun_backup_folder,
            )
        except Exception as e:
            log.error(
                "Phase B failed for file {fn} at {lp}: {err}".format(
                    fn=original_filename,
                    lp=local_path,
                    err=e,
                )
            )
            send_teams_message(
                summary=SEND_TEAMS_MESSAGE_SUMMARY,
                activityTitle=SEND_TEAMS_MESSAGE_ACTIVITY_TITLE,
                activitySubtitle=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                text="Phase B failed for file {fn} at {lp}: {err}".format(
                    fn=original_filename,
                    lp=local_path,
                    err=e,
                ),
            )


def get_cleaned_data_from_file(filename, reference_dt=None, dry_run=False, audit_only=False, dryrun_backup_folder=None):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    backup_folder = SCRIPT_PATH + "backup"
    error_folder = SCRIPT_PATH + "error"
    if dryrun_backup_folder is None:
        dryrun_backup_folder = SCRIPT_PATH + "dryrun_backup"

    ensure_folder_exists(backup_folder, "backup")
    ensure_folder_exists(error_folder, "error")
    if dry_run or audit_only:
        ensure_folder_exists(dryrun_backup_folder, "dryrun_backup")

    source_path = filename
    base = os.path.basename(source_path)
    error_path = os.path.join(error_folder, base)
    backup_path = os.path.join(backup_folder, base)
    log.info("Filename passed in: {filename}".format(filename=filename))
    log.info("Source path resolved: {sp}".format(sp=source_path))
    log.info("Backup path resolved: {bp}".format(bp=backup_path))
    log.info("Error path resolved: {ep}".format(ep=error_path))
    log.info("Checking file....")
    if os.path.exists(source_path):
        log.info("File found, proceeding...")
        att_file_name, att_file_ext = os.path.splitext(base)
        att_file_ext = att_file_ext.lstrip(".").lower()
        if att_file_ext == "xls" or att_file_ext == "xlsx":
            log.info("Processing file: {sn}".format(sn=source_path))
            log.info("Getting returned data...")
            returned_data_df = load_excel_file(source_path)
            returned_data_df_len = len(returned_data_df)
            other_statuses_df = load_other_statuses(source_path)
            other_statuses_df.columns = other_statuses_df.columns.str.strip()
            other_statuses_df_len = len(other_statuses_df)
            log.info("Main data loaded.  Records found: {rf}".format(rf=returned_data_df_len))
            log.info("Other status data loaded.  Records found: {rf}".format(rf=other_statuses_df_len))
            log.info("Checking column names...")
            returned_data_df.columns = map(str.lower, returned_data_df.columns)
            returned_data_df.columns = returned_data_df.columns.str.replace(" ", "_")
            column_list = ["call_date", "phone_number", "first_name", "last_name", "address", "city", "state",
                           "postal_code", "status_name", "recording_link", "primary_network", "employee_number",
                           "meeting_schedule", "manager_id", "office_id", "case_id", "manager_name"]
            for column_name in column_list:
                col_found = False
                if column_name in returned_data_df.columns:
                    col_found = True

                log.info("Looking for column \"{cn}\" in dataframe... {cf}".format(cn=column_name,
                                                                                   cf="Found." if col_found else "Not found!"))
                if not col_found:
                    log.critical("Column \"{cn}\" was not found in the dataframe.  Exiting.".format(cn=column_name))
                    ## Kick out the file
                    log.critical("Kicking the file to the error directory...")
                    log.critical("Moving returned file {sn} to error folder...".format(sn=filename))
                    log.critical("Rename source: {sp}".format(sp=source_path))
                    log.critical("Rename destination: {dp}".format(dp=error_path))
                    if dry_run or audit_only:
                        log.critical("Dry run/audit mode: moving file to dryrun backup folder instead of error.")
                        dryrun_error_path = os.path.join(dryrun_backup_folder, base)
                        log.critical("Rename source: {sp}".format(sp=source_path))
                        log.critical("Rename destination: {dp}".format(dp=dryrun_error_path))
                        os.rename(source_path, dryrun_error_path)
                        log.critical("Report file moved to dryrun backup folder.")
                    else:
                        os.rename(source_path, error_path)
                        log.critical("Report file moved to error folder.")
                    log.critical("Sending teams message...")
                    send_teams_message(summary=SEND_TEAMS_MESSAGE_SUMMARY, activityTitle=SEND_TEAMS_MESSAGE_ACTIVITY_TITLE,
                                       activitySubtitle=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
                                       text="A file kicked out.  Returned dataframe has a missing column, please verify the data.")
                    
                    log.critical("Alert message sent.")
                    raise IOError("Returned dataframe has a missing column, please verify the data!")

            if reference_dt is None:
                reference_dt = parse_reference_date_from_filename(filename)
            if reference_dt is None:
                reference_dt = datetime.datetime.now()

            reference_stats = {}
            already_processed = is_attachment_already_processed(returned_data_df, reference_dt, stats=reference_stats)
            if already_processed:
                log.info(
                    "{mode} SKIP already processed based on recieved_dtt: {fn} reference_dt={rd} "
                    "(office_ids total={tot}, processed={proc}, pending={pend})".format(
                        mode="AUDIT" if audit_only else "DRY RUN" if dry_run else "PROCESS",
                        fn=filename,
                        rd=reference_dt,
                        tot=reference_stats.get("total", 0),
                        proc=reference_stats.get("processed", 0),
                        pend=reference_stats.get("pending", 0),
                    )
                )
                if os.path.exists(source_path):
                    if dry_run or audit_only:
                        log.info("Dry run/audit mode: moving file to dryrun backup folder.")
                        dryrun_path = os.path.join(dryrun_backup_folder, base)
                        log.info("Rename source: {sp}".format(sp=source_path))
                        log.info("Rename destination: {dp}".format(dp=dryrun_path))
                        os.rename(source_path, dryrun_path)
                        log.info("Report file moved to dryrun backup folder.")
                    else:
                        log.info("File found, moving to backup.")
                        log.info("Rename source: {sp}".format(sp=source_path))
                        log.info("Rename destination: {dp}".format(dp=backup_path))
                        os.rename(source_path, backup_path)
                        log.info("Report file moved to backup folder.")
                return

            log.info(
                "{mode} attachment: {fn} reference_dt={rd} "
                "(office_ids total={tot}, processed={proc}, pending={pend})".format(
                    mode="AUDIT" if audit_only else "DRY RUN" if dry_run else "PROCESS",
                    fn=filename,
                    rd=reference_dt,
                    tot=reference_stats.get("total", 0),
                    proc=reference_stats.get("processed", 0),
                    pend=reference_stats.get("pending", 0),
                )
            )

            if dry_run or audit_only:
                if os.path.exists(source_path):
                    log.info("Dry run/audit mode: moving file to dryrun backup folder.")
                    dryrun_path = os.path.join(dryrun_backup_folder, base)
                    log.info("Rename source: {sp}".format(sp=source_path))
                    log.info("Rename destination: {dp}".format(dp=dryrun_path))
                    os.rename(source_path, dryrun_path)
                    log.info("Report file moved to dryrun backup folder.")
                return

            returned_data_df = returned_data_df.rename(
                {"phone_number": "office_phone", "address": "line_1", "postal_code": "zip_code",
                 "employee_number": "employee_count", "meeting_schedule": "meetings"}, axis=1)
            returned_data_dict = returned_data_df.to_dict(orient="records")
            for rec in returned_data_dict:
                rec['meetings'] = rec['meetings'].replace('&Amp', '&')
                rec['meetings'] = rec['meetings'].replace('&amp', '&')
                rec['meetings'] = rec['meetings'].replace('amp', ' & ')
                rec['primary_network'] = rec['primary_network'].replace('&Amp', '&')
                rec['primary_network'] = rec['primary_network'].replace(' amp ', ' & ')
                rec['primary_network'] = rec['primary_network'].replace('&amp', '&')
                rec['primary_network'] = rec['primary_network'].replace('&', 'and')
                rec['line_1'] = rec['line_1'].replace('&amp', '&')
                rec['line_1'] = rec['line_1'].replace('#', '')
                compare_and_update(rec)
                update_gsd_table(status_name=rec["status_name"], office_id=rec["office_id"])

            if other_statuses_df_len > 0:
                log.info("Updating other statuses:")
                log.info("-" * 50)
                other_statuses_df.columns = map(str.lower, other_statuses_df.columns)
                other_statuses_df.columns = other_statuses_df.columns.str.replace(" ", "_")

                other_statuses_dict = other_statuses_df.to_dict(orient="records")

                for other_rec in other_statuses_dict:
                    case_id = other_rec['case_id']
                    update_status(workflow_id=case_id, status_id=1694)
                    update_gsd_table(status_name=other_rec["status_name"], office_id=other_rec["office_id"])

            log.info("Moving returned file {sn} to backup folder...".format(sn=filename))
            if os.path.exists(source_path):
                log.info("File found, moving.")
                log.info("Rename source: {sp}".format(sp=source_path))
                log.info("Rename destination: {dp}".format(dp=backup_path))
                os.rename(source_path, backup_path)
                log.info("Report file moved to backup folder.")
            else:
                log.error("File not found during backup.  Path: {pa}".format(pa=source_path))
    else:
        err = "File not found!"
        log.info(err)
        raise IOError(err)


def main():
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))
    parser = argparse.ArgumentParser(description="Gather Sales Data AskData Import")
    parser.add_argument("--audit", action="store_true", help="Run audit-only pass with no updates.")
    parser.add_argument("--dry-run", action="store_true", help="Run dry-run pass with no updates.")
    args, _unknown = parser.parse_known_args()
    dry_run = GSD_DRY_RUN or args.dry_run
    audit_only = GSD_AUDIT_RUN or args.audit
    try:
        log.info("Run mode: dry_run={dr} audit_only={au}".format(dr=dry_run, au=audit_only))
        if audit_only and dry_run:
            log.info("Both dry_run and audit_only enabled; audit_only will take precedence.")
            dry_run = False
        get_cleaned_data_from_email(dry_run=dry_run, audit_only=audit_only)
        ## For manual processing: 1) Comment out the line above and uncomment the line below and change the filename.  Don't forget to reverse these directions when finished!
        ## get_cleaned_data_from_file(filename="Set of Data 2000 April 4 to 8 2022.xlsx")
    except Exception as e:
        log.critical("Critical error has occurred, sounding alarm!  Error info: {e}".format(e=e))
        send_teams_message(summary=SEND_TEAMS_MESSAGE_SUMMARY, activityTitle=SEND_TEAMS_MESSAGE_ACTIVITY_TITLE,
                           activitySubtitle=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), text=e)
    finally:
        processed_file_folder = "files"
        processed_files_path = os.path.join(SCRIPT_PATH, processed_file_folder)
        ensure_folder_exists(processed_files_path, "files")
        staging_folder = DOWNLOAD_STAGING_FOLDER
        for file_name in os.listdir(SCRIPT_PATH):
            processed_file_path = os.path.join(SCRIPT_PATH, file_name)
            if not os.path.isfile(processed_file_path):
                continue

            if not (file_name.endswith(".xlsx") or file_name.endswith(".xls")):
                continue

            if processed_file_path.startswith(staging_folder + os.sep) or processed_file_path.startswith(processed_files_path + os.sep):
                log.info("Skipping cleanup sweep for managed folder file: {fp}".format(fp=processed_file_path))
                continue

            new_file_path = os.path.join(SCRIPT_PATH, processed_file_folder, file_name)
            if os.path.isfile(new_file_path):
                os.remove(new_file_path)
            log.info("Cleanup sweep moving file from {sp} to {dp}".format(sp=processed_file_path, dp=new_file_path))
            shutil.move(processed_file_path, new_file_path)
        log.info("Closing database connection...")
        bi.close()
        db.close()
        log.info("Database connection closed.")
        end_ts = time.time()
        final_time = end_ts - start_ts
        log.info("Processing finished.  Time: {mt} minutes ({et} seconds)".format(mt=round(final_time / 60, 3),
                                                                                  et=round(final_time, 3)))
        log.info("-" * 120)


if __name__ == '__main__':
    main()
