import os
import re
import logging
import time
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
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
    logging.info(f"    -> Recreating table '{table_name}' for a fresh upload...")
    
    # 1. Drop the old table if it exists
    drop_sql = f'DROP TABLE IF EXISTS public."{table_name}";'
    supabase.rpc('execute_sql', {'sql': drop_sql}).execute()
    logging.info(f"    -> Table '{table_name}' dropped successfully.")
    time.sleep(2) # Give Supabase a moment to process the drop

    # 2. Build the CREATE TABLE SQL statement from the DataFrame columns
    columns_definitions = []
    for col_name in df.columns:
        if col_name == "Roll_No":
            columns_definitions.append(f'"{col_name}" TEXT PRIMARY KEY')
        else:
            columns_definitions.append(f'"{col_name}" TEXT')
            
    create_sql = f'CREATE TABLE public."{table_name}" ({", ".join(columns_definitions)});'
    
    try:
        # Execute the create table statement
        supabase.rpc('execute_sql', {'sql': create_sql}).execute()
        logging.info(f"    -> Table '{table_name}' created with fresh schema.")
        
        # 3. Enable Row Level Security (RLS) on the new table
        logging.info(f"    -> Enabling RLS on '{table_name}'...")
        enable_rls_sql = f'ALTER TABLE public."{table_name}" ENABLE ROW LEVEL SECURITY;'
        supabase.rpc('execute_sql', {'sql': enable_rls_sql}).execute()

        # 4. Create a policy to allow public read access
        logging.info(f"    -> Creating 'Allow public access' policy on '{table_name}'...")
        create_policy_sql = f"""
        DROP POLICY IF EXISTS "Allow public access" ON public."{table_name}";
        CREATE POLICY "Allow public access" ON public."{table_name}"
        FOR ALL USING (true) WITH CHECK (true);
        """
        supabase.rpc('execute_sql', {'sql': create_policy_sql}).execute()
        logging.info("    -> RLS and policy applied successfully.")

        # Wait for the schema cache to refresh before uploading data
        logging.info("    -> Waiting 5 seconds for schema cache to refresh...")
        time.sleep(5)
    except Exception as e:
        logging.error(f"    -> ❌ FAILED to create table or apply policy. Error: {e}")
        raise

def upload_to_supabase(supabase: Client, subject_name: str, student_records: list):
    """Processes scraped data and uploads it to a Supabase table."""
    if not student_records:
        logging.warning(f"No records for '{subject_name}', skipping.")
        return

    table_name = sanitize_table_name(subject_name)
    logging.info(f"\n======= UPLOADING TO SUPABASE TABLE: {table_name} =======")

    # Convert the list of records into a long-format DataFrame
    long_format_data = []
    for record in student_records:
        for date, status in record['attendance_data'].items():
            long_format_data.append({
                'Roll_No': record['roll_no'], 'Name': record['student_name'],
                'Section': record.get('section', 'Unknown'), 'Date': date, 'Status': status
            })
    
    if not long_format_data:
        logging.warning(f"    -> No attendance dates found for '{subject_name}'.")
        return

    # Pivot the DataFrame to get dates as columns and sort them
    df_long = pd.DataFrame(long_format_data)
    df_pivot = df_long.pivot_table(
        index=['Roll_No', 'Name', 'Section'], columns='Date', values='Status', aggfunc='first'
    )

    date_cols = df_pivot.columns.tolist()
    sorted_date_cols = sorted(date_cols, key=lambda d: pd.to_datetime(d, format='%d/%m/%Y'))
    df_pivot = df_pivot[sorted_date_cols]
    df_pivot.columns = [sanitize_column_name(col) for col in df_pivot.columns]
    df_final = df_pivot.reset_index()

    # Recreate the table in Supabase and upload the data
    recreate_table_for_upload(supabase, table_name, df_final)
    records_to_upload = df_final.where(pd.notna(df_final), None).to_dict(orient='records')

    logging.info(f"    -> Inserting {len(records_to_upload)} records into '{table_name}'...")
    try:
        supabase.table(table_name).insert(records_to_upload).execute()
        logging.info(f"    -> ✅ Successfully saved data for '{subject_name}'.")
    except Exception as e:
        logging.error(f"    -> ❌ FAILED to save data for '{subject_name}'. Error: {e}")

# --- Scraping Logic ---
def get_data_for_course(page):
    """Scrapes all attendance pages for a selected course by navigating backwards."""
    all_student_records = []

    # --- NEW: Go to the last page first ---
    logging.info("      -> Navigating to the last page of records...")
    while True:
        try:
            next_button = page.get_by_role("button", name="Next")
            if not next_button.is_enabled():
                logging.info("      -> Reached the last page.")
                break
            next_button.click()
            page.wait_for_load_state('networkidle', timeout=30000)
        except Exception:
            logging.info("      -> 'Next' button not found or timed out. Assuming last page.")
            break

    # --- MODIFIED: Scrape backwards from the last page ---
    page_num = 0
    while True:
        page_num += 1
        logging.info(f"      -> Scraping backwards, page set {page_num}...")
        try:
            page.wait_for_selector("table > tbody > tr:first-child", timeout=20000)
        except PlaywrightTimeoutError:
            logging.warning("      -> Timed out waiting for table content on backward pass.")
            # Continue to see if we can still navigate back
            pass

        # Scrape data from the current page using JavaScript evaluation
        page_data = page.evaluate("""() => {
            const records = [];
            const headerCells = Array.from(document.querySelectorAll('thead th'));
            const dateHeaderMap = {};
            // Find all date columns in the header
            headerCells.forEach((th, index) => {
                const headerText = th.innerText.trim();
                if (/^\\d{2}\\/\\d{2}\\/\\d{4}$/.test(headerText)) {
                    dateHeaderMap[headerText] = index;
                }
            });
            const studentRows = document.querySelectorAll('tbody tr');
            studentRows.forEach(row => {
                const cells = row.querySelectorAll('td');
                if (cells.length < 2) return; // Skip malformed rows
                const record = {
                    'roll_no': cells[0].innerText.trim(),
                    'student_name': cells[1].innerText.trim(),
                    'attendance_data': {}
                };
                // Extract attendance status for each date
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

        # Find the "Previous" button and click it
        try:
            prev_button = page.get_by_role("button", name="Previous")
            if not prev_button.is_enabled():
                logging.info("      -> Reached the first page. Scraping complete for this course.")
                break
            
            prev_button.click()
            page.wait_for_load_state('networkidle', timeout=30000)
        except Exception:
            logging.info("      -> 'Previous' button not found or timed out. Stopping backward scrape.")
            break
            
    return all_student_records

def run_scraper():
    """Main function to orchestrate the browser automation and scraping process."""
    all_subjects_data = {}
    with sync_playwright() as p:
        logging.info(">>> Launching browser...")
        browser = p.chromium.launch(headless=HEADLESS_MODE)
        page = browser.new_page()
        page.set_default_timeout(60000) # Set a generous default timeout
        try:
            logging.info(f">>> Navigating to {ATTENDANCE_URL}")
            page.goto(ATTENDANCE_URL, wait_until="networkidle", timeout=90000)

            # --- Apply Filters ---
            logging.info(">>> Applying filters...")
            page.locator('label:has-text("Select Department") + button').click()
            page.get_by_role("option", name="Computer Science and Engineering", exact=True).click()
            page.locator('label:has-text("Select Batch") + button').click()
            page.get_by_role("option", name="2024-2028", exact=True).click()
            page.locator('label:has-text("Select Semester") + button').click()
            page.get_by_role("option", name="Semester 3", exact=True).click()
            
            section_dropdown_selector = 'label:has-text("Select Section") + button'
            page.wait_for_selector(section_dropdown_selector, state="visible", timeout=30000)

            # --- Loop Through Sections, Types, and Courses ---
            sections = ["Section Section A", "Section Section B", "Section Section C"]
            for section_name in sections:
                logging.info(f"\n======= PROCESSING SECTION: {section_name.replace('Section Section', 'Section')} =======")
                page.locator(section_dropdown_selector).click()
                page.get_by_role("option", name=section_name, exact=True).click()
                page.wait_for_load_state('networkidle')

                for attendance_type in ["RTU Classes", "Labs"]:
                    logging.info(f"\n  --- Processing Type: {attendance_type} ---")
                    page.locator('label:has-text("Select Attendance Type") + button').click()
                    page.get_by_role("option", name=attendance_type, exact=True).click()
                    page.wait_for_load_state('networkidle')

                    # Get the list of available courses
                    course_dropdown = page.locator('label:has-text("Select Course") + button')
                    course_dropdown.click()
                    listbox_locator = 'div[role="listbox"]'
                    page.wait_for_selector(listbox_locator, state="visible", timeout=15000)
                    course_list_locators = page.locator(f'{listbox_locator} [role="option"]:not(:has-text("Overall Attendance"))').all()
                    course_list_names = [item.inner_text() for item in course_list_locators]
                    page.keyboard.press("Escape") # Close the dropdown
                    
                    # Scrape each course
                    for course_name_with_code in course_list_names:
                        subject_name = course_name_with_code.split(' (')[0].strip()
                        logging.info(f"\n    -> Scraping Course: {course_name_with_code}")
                        course_dropdown.click()
                        page.get_by_role("option", name=course_name_with_code, exact=True).click()
                        
                        # Get data for the selected course
                        course_data = get_data_for_course(page)
                        
                        # Add section info to each record
                        clean_section_name = section_name.replace('Section Section', 'Section')
                        for record in course_data:
                            record['section'] = clean_section_name
                            
                        # Aggregate data from all sections
                        if subject_name not in all_subjects_data:
                            all_subjects_data[subject_name] = []
                        all_subjects_data[subject_name].extend(course_data)
        except Exception as e:
            logging.error(f"\n>>> AN ERROR OCCURRED: {e}")
            page.screenshot(path="scraper_error.png") # Save a screenshot on error
        finally:
            logging.info("\n>>> Closing browser.")
            browser.close()
    return all_subjects_data

if __name__ == "__main__":
    if not SUPABASE_URL or not SUPABASE_KEY:
        logging.error("Supabase credentials are not set. Please set SUPABASE_URL and SUPABASE_KEY environment variables.")
    else:
        try:
            supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
            logging.info("\n--- Starting Scraper and Supabase Upload Process ---")
            start_time = time.time()
            all_data = run_scraper()
            if all_data:
                for subject, records in all_data.items():
                    upload_to_supabase(supabase, subject, records)
            else:
                logging.warning("No data was scraped, skipping upload.")
            end_time = time.time()
            logging.info(f"\n--- Process complete! Total time: {end_time - start_time:.2f}s ---")
        except Exception as e:
            logging.error(f"A critical error occurred in the main process: {e}")

