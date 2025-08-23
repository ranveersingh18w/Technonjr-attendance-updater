import os
import re
import logging
import time
import random
from playwright_extra.sync_api import sync_playwright_extra, TimeoutError as PlaywrightTimeoutError
from supabase import create_client, Client
import pandas as pd

# --- Logger Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- CONFIGURATION ---
ATTENDANCE_URL = "http://103.159.68.60:3535/attendance"
HEADLESS_MODE = True 

# --- Supabase Credentials (from GitHub Secrets) ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
logging.info("--- Configuration ---")
logging.info(f"URL: {ATTENDANCE_URL}")
logging.info(f"Headless Mode: {HEADLESS_MODE}")
logging.info(f"Supabase URL Loaded: {'Yes' if SUPABASE_URL else 'No'}")
logging.info(f"Supabase Key Loaded: {'Yes' if SUPABASE_KEY else 'No'}\n")


# --- Helper Functions ---
def sanitize_table_name(name):
    """Sanitizes a string to be a valid table name."""
    name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    name = re.sub(r'__+', '_', name)
    return name.strip('_').lower()

def sanitize_column_name(name):
    """Sanitizes a column name by replacing invalid characters."""
    return name.replace('/', '_')

# --- Supabase Interaction ---
def recreate_table_for_upload(supabase: Client, table_name: str, df: pd.DataFrame):
    """Drops and recreates a Supabase table with a new schema based on the DataFrame."""
    logging.info(f"    -> Recreating table '{table_name}'...")
    
    drop_sql = f'DROP TABLE IF EXISTS public."{table_name}";'
    supabase.rpc('execute_sql', {'sql': drop_sql}).execute()
    logging.info(f"    -> Table '{table_name}' dropped.")
    time.sleep(2)

    columns_definitions = []
    for col_name in df.columns:
        if col_name == "Roll_No":
            columns_definitions.append(f'"{col_name}" TEXT PRIMARY KEY')
        else:
            columns_definitions.append(f'"{col_name}" TEXT')
            
    create_sql = f'CREATE TABLE public."{table_name}" ({", ".join(columns_definitions)});'
    
    try:
        supabase.rpc('execute_sql', {'sql': create_sql}).execute()
        logging.info(f"    -> Table '{table_name}' created.")
        
        enable_rls_sql = f'ALTER TABLE public."{table_name}" ENABLE ROW LEVEL SECURITY;'
        supabase.rpc('execute_sql', {'sql': enable_rls_sql}).execute()

        create_policy_sql = f"""
        DROP POLICY IF EXISTS "Allow public access" ON public."{table_name}";
        CREATE POLICY "Allow public access" ON public."{table_name}"
        FOR ALL USING (true) WITH CHECK (true);
        """
        supabase.rpc('execute_sql', {'sql': create_policy_sql}).execute()
        logging.info(f"    -> RLS and policy applied to '{table_name}'.")
        time.sleep(5)
    except Exception as e:
        logging.error(f"    -> ❌ FAILED to create table or apply policy. Error: {e}")
        raise

def upload_to_supabase(supabase: Client, subject_name: str, student_records: list):
    """Processes scraped data and uploads it to a Supabase table."""
    if not student_records:
        logging.warning(f"No records found for '{subject_name}', skipping upload.")
        return

    table_name = sanitize_table_name(subject_name)
    logging.info(f"\n======= UPLOADING TO SUPABASE TABLE: {table_name} =======")

    long_format_data = []
    for record in student_records:
        for date, status in record['attendance_data'].items():
            long_format_data.append({
                'Roll_No': record['roll_no'], 'Name': record['student_name'],
                'Section': record.get('section', 'Unknown'), 'Date': date, 'Status': status
            })
    
    if not long_format_data:
        logging.warning(f"    -> No attendance dates were processed for '{subject_name}'.")
        return

    df_long = pd.DataFrame(long_format_data)
    df_pivot = df_long.pivot_table(
        index=['Roll_No', 'Name', 'Section'], columns='Date', values='Status', aggfunc='first'
    )
    
    date_cols = df_pivot.columns.tolist()
    sorted_date_cols = sorted(date_cols, key=lambda d: pd.to_datetime(d, format='%d/%m/%Y'))
    df_pivot = df_pivot[sorted_date_cols]
    df_pivot.columns = [sanitize_column_name(col) for col in df_pivot.columns]
    df_final = df_pivot.reset_index()
    logging.info(f"    -> Processed data into a table with {df_final.shape[0]} rows and {df_final.shape[1]} columns.")

    recreate_table_for_upload(supabase, table_name, df_final)
    records_to_upload = df_final.where(pd.notna(df_final), None).to_dict(orient='records')

    logging.info(f"    -> Attempting to insert {len(records_to_upload)} records into '{table_name}'...")
    try:
        supabase.table(table_name).insert(records_to_upload).execute()
        logging.info(f"    -> ✅ Successfully inserted data for '{subject_name}'.")
    except Exception as e:
        logging.error(f"    -> ❌ FAILED to insert data for '{subject_name}'. Error: {e}")

# --- Scraping Logic ---
def get_data_for_course(page):
    """Scrapes all attendance pages for a selected course by navigating backwards."""
    all_student_records = []
    logging.info("      -> Starting to scrape data for the selected course.")

    # --- Navigate to the last page ---
    logging.info("      -> Trying to find the 'Next' button to go to the last page.")
    page_count = 1
    while True:
        try:
            # Wait for the button to be available before trying to interact
            page.wait_for_selector('button:has-text("Next")', state="attached", timeout=10000)
            next_button = page.get_by_role("button", name="Next")
            if not next_button.is_enabled():
                logging.info(f"      -> 'Next' button is disabled. Reached the last page (Page {page_count}).")
                break
            next_button.click()
            page.wait_for_load_state('networkidle', timeout=30000)
            page_count += 1
        except Exception:
            logging.info("      -> 'Next' button not found or timed out. Assuming this is the last page.")
            break

    # --- Scrape backwards from the last page ---
    page_num = 0
    while True:
        page_num += 1
        logging.info(f"      -> Scraping page set {page_num} (from end to start)...")
        try:
            page.wait_for_selector("table > tbody > tr:first-child", timeout=20000)
        except PlaywrightTimeoutError:
            logging.warning("      -> WARNING - Timed out waiting for table content. The page might be empty.")
            pass

        page_data = page.evaluate("""() => {
            const records = [];
            const headerCells = Array.from(document.querySelectorAll('thead th'));
            const dateHeaderMap = {};
            headerCells.forEach((th, index) => {
                const headerText = th.innerText.trim();
                if (/^\\d{2}\\/\\d{2}\\/\\d{4}$/.test(headerText)) { dateHeaderMap[headerText] = index; }
            });
            const studentRows = document.querySelectorAll('tbody tr');
            studentRows.forEach(row => {
                const cells = row.querySelectorAll('td');
                if (cells.length < 2) return;
                const record = {
                    'roll_no': cells[0].innerText.trim(),
                    'student_name': cells[1].innerText.trim(),
                    'attendance_data': {}
                };
                for (const [date, columnIndex] of Object.entries(dateHeaderMap)) {
                    const cell = cells[columnIndex];
                    if (cell) {
                        let status = 'Unknown';
                        if (cell.querySelector('svg.lucide-check')) status = 'P';
                        else if (cell.querySelector('svg.lucide-x')) status = 'A';
                        else if (cell.innerText.trim() === 'NA') status = 'NA';
                        record.attendance_data[date] = status;
                    }
                }
                records.push(record);
            });
            return records;
        }""")
        
        all_student_records.extend(page_data)

        try:
            page.wait_for_selector('button:has-text("Previous")', state="attached", timeout=10000)
            prev_button = page.get_by_role("button", name="Previous")
            if not prev_button.is_enabled():
                logging.info("      -> 'Previous' button is disabled. Reached the first page.")
                break
            prev_button.click()
            page.wait_for_load_state('networkidle', timeout=30000)
        except Exception:
            logging.info("      -> 'Previous' button not found or timed out. Stopping scrape for this course.")
            break
            
    logging.info(f"      -> Finished scraping for this course. Total records found: {len(all_student_records)}")
    return all_student_records

def run_scraper():
    """Main function to orchestrate the browser automation and scraping process."""
    all_subjects_data = {}
    # Use playwright_extra to launch a stealth browser
    with sync_playwright_extra() as p:
        logging.info(">>> Launching stealth browser...")
        browser = p.chromium.launch(headless=HEADLESS_MODE)
        page = browser.new_page()
        page.set_default_timeout(90000) 
        
        try:
            logging.info(f">>> Navigating to {ATTENDANCE_URL}")
            page.goto(ATTENDANCE_URL, wait_until="domcontentloaded", timeout=120000)
            
            # Wait for the page to settle after initial load
            logging.info(">>> Page loaded. Waiting for dynamic content...")
            time.sleep(random.uniform(5, 8))

            # --- Apply Filters ---
            logging.info(">>> Applying filters...")
            page.locator('label:has-text("Select Department") + button').click()
            time.sleep(random.uniform(1, 2.5))
            page.get_by_role("option", name="Computer Science and Engineering", exact=True).click()
            time.sleep(random.uniform(2, 3.5))

            page.locator('label:has-text("Select Batch") + button').click()
            time.sleep(random.uniform(1, 2.5))
            page.get_by_role("option", name="2024-2028", exact=True).click()
            time.sleep(random.uniform(2, 3.5))

            page.locator('label:has-text("Select Semester") + button').click()
            time.sleep(random.uniform(1, 2.5))
            page.get_by_role("option", name="Semester 3", exact=True).click()
            
            section_dropdown_selector = 'label:has-text("Select Section") + button'
            page.wait_for_selector(section_dropdown_selector, state="visible", timeout=30000)
            logging.info(">>> Filters applied. Ready to loop through sections.\n")

            # --- Loop Through Sections, Types, and Courses ---
            sections = ["Section Section A", "Section Section B", "Section Section C"]
            for section_name in sections:
                logging.info(f"======= PROCESSING SECTION: {section_name.replace('Section Section ', '')} =======")
                page.locator(section_dropdown_selector).click()
                time.sleep(random.uniform(1, 2.5))
                page.get_by_role("option", name=section_name, exact=True).click()
                page.wait_for_load_state('networkidle')
                time.sleep(random.uniform(2, 4))

                for attendance_type in ["RTU Classes", "Labs"]:
                    logging.info(f"  --- Processing Type: {attendance_type} ---")
                    page.locator('label:has-text("Select Attendance Type") + button').click()
                    time.sleep(random.uniform(1, 2.5))
                    page.get_by_role("option", name=attendance_type, exact=True).click()
                    page.wait_for_load_state('networkidle')
                    time.sleep(random.uniform(2, 4))

                    course_dropdown = page.locator('label:has-text("Select Course") + button')
                    course_dropdown.click()
                    listbox_locator = 'div[role="listbox"]'
                    page.wait_for_selector(listbox_locator, state="visible", timeout=15000)
                    course_list_locators = page.locator(f'{listbox_locator} [role="option"]:not(:has-text("Overall Attendance"))').all()
                    course_list_names = [item.inner_text() for item in course_list_locators]
                    page.keyboard.press("Escape")
                    
                    for course_name_with_code in course_list_names:
                        subject_name = course_name_with_code.split(' (')[0].strip()
                        logging.info(f"    -> Scraping Course: {course_name_with_code}")
                        course_dropdown.click()
                        time.sleep(random.uniform(1, 2))
                        page.get_by_role("option", name=course_name_with_code, exact=True).click()
                        
                        course_data = get_data_for_course(page)
                        
                        clean_section_name = section_name.replace('Section Section', 'Section')
                        for record in course_data:
                            record['section'] = clean_section_name
                            
                        if subject_name not in all_subjects_data:
                            all_subjects_data[subject_name] = []
                        all_subjects_data[subject_name].extend(course_data)

        except Exception as e:
            logging.error(f"\n>>> ❌ AN ERROR OCCURRED: {e}")
            logging.info(">>> Taking a screenshot of the page: scraper_error.png")
            page.screenshot(path="scraper_error.png")
        finally:
            logging.info("\n>>> Closing browser.")
            browser.close()
    return all_subjects_data

if __name__ == "__main__":
    if not SUPABASE_URL or not SUPABASE_KEY:
        logging.error("❌ CRITICAL: Supabase credentials are not set as environment variables.")
    else:
        try:
            supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
            logging.info("--- Starting Scraper ---")
            start_time = time.time()
            all_data = run_scraper()
            if all_data:
                logging.info("\n--- Scraper finished. Starting Supabase upload process. ---")
                for subject, records in all_data.items():
                    upload_to_supabase(supabase, subject, records)
            else:
                logging.warning("\n--- WARNING: No data was scraped. Nothing to upload. ---")
            end_time = time.time()
            logging.info(f"\n--- ✅ Process complete! Total time: {end_time - start_time:.2f}s ---")
        except Exception as e:
            logging.error(f"❌ A critical error occurred in the main process: {e}")
